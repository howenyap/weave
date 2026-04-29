mod fetcher;
mod frontier;
mod prioritizer;
mod traits;

use std::collections::HashSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::time::Duration;

use env_logger::Env;
use fetcher::{CrawlerIdentity, FetchFailure, FetchReport, HttpFetcher, HttpFetcherConfig};
use frontier::MercatorFrontier;
use log::{error, info, warn};
use prioritizer::ConstantPrioritizer;
use scraper::{Html, Selector};
use thiserror::Error;
use tokio::task::JoinSet;
use traits::{
    CrawlUrl, CrawlUrlError, FetchCompletion, FrontierLease, NextUrl, Priority, UrlFrontier,
};
use url::Url;

const MAX_FETCHES: usize = 1_000_000;
const DEFAULT_OUTPUT_DIR: &str = "output";
const LINKS_FILE_NAME: &str = "links.txt";
const PRIORITY_LEVELS: u8 = 4;
const BACK_QUEUE_COUNT: usize = 1_024;
const MAX_CONCURRENT_FETCHES: usize = 128;
const POLITENESS_MULTIPLIER: u32 = 10;
const CRAWLER_NAME: &str = "weave";
const CRAWLER_VERSION: &str = "0.1";
const ROBOTS_CACHE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(256).expect("robots cache capacity is non-zero");
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().filter_or("RUST_LOG", "info")).init();

    let mut args = env::args().skip(1);
    let seed_url = match args.next() {
        Some(seed_url) => seed_url,
        None => {
            eprintln!("usage: cargo run -- <seed_url> [output_dir]");
            std::process::exit(1);
        }
    };
    let output_dir = args
        .next()
        .unwrap_or_else(|| DEFAULT_OUTPUT_DIR.to_string());

    let seed = Url::parse(&seed_url)?;
    let fetcher = HttpFetcher::new(HttpFetcherConfig {
        identity: CrawlerIdentity::new(CRAWLER_NAME, CRAWLER_VERSION)?,
        robots_cache_capacity: ROBOTS_CACHE_CAPACITY,
        request_timeout: REQUEST_TIMEOUT,
    })?;
    let anchor_selector = Selector::parse("a")?;
    fs::create_dir_all(&output_dir)?;
    let links = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(format!("{output_dir}/{LINKS_FILE_NAME}"))?;
    let mut writer = BufWriter::new(links);
    let default_priority = Priority::new(1).expect("default priority is non-zero");
    let priority_levels = Priority::new(PRIORITY_LEVELS).expect("priority level count is non-zero");
    let prioritizer = ConstantPrioritizer::new(default_priority);
    let mut frontier = MercatorFrontier::new(
        prioritizer,
        priority_levels,
        BACK_QUEUE_COUNT,
        POLITENESS_MULTIPLIER,
    );
    let mut seen = HashSet::new();
    let mut successful_fetches = 0;
    let mut attempted_fetches = 0;
    let mut active_fetches = JoinSet::new();

    enqueue_url(&mut frontier, &mut seen, &mut writer, seed)?;

    loop {
        let mut wait_until = None;

        while successful_fetches + active_fetches.len() < MAX_FETCHES
            && active_fetches.len() < MAX_CONCURRENT_FETCHES
        {
            match frontier.next() {
                Some(NextUrl::Ready(lease)) => {
                    attempted_fetches += 1;
                    let current_url = lease.entry.url.clone();
                    let fetcher = fetcher.clone();

                    info!(
                        "fetching (successes {successful_fetches}/{MAX_FETCHES}, attempt {attempted_fetches}): {current_url}"
                    );
                    active_fetches.spawn(async move {
                        let report = fetcher.fetch(&current_url).await;

                        FetchOutcome { lease, report }
                    });
                }
                Some(NextUrl::WaitUntil(ready_at)) => {
                    wait_until = Some(ready_at);
                    break;
                }
                None => break,
            }
        }

        let Some(outcome) = (match (active_fetches.is_empty(), wait_until) {
            // no active fetches, can only wait for frontier cooldown
            (true, Some(ready_at)) => {
                sleep_until_std(ready_at).await;
                continue;
            }
            // no active fetches and empty frontier
            (true, None) => break,
            // has active fetches and frontier cooldown, wait for either one to complete
            (false, Some(ready_at)) => {
                tokio::select! {
                    result = active_fetches.join_next() => result.transpose()?,
                    _ = sleep_until_std(ready_at) => continue,
                }
            }
            // has active fetches but empty frontier, wait for active fetch to complete
            (false, None) => active_fetches.join_next().await.transpose()?,
        }) else {
            break;
        };

        let FetchOutcome { lease, report } = outcome;

        if report.requested_url != report.final_url {
            info!(
                "redirected: {} -> {}",
                report.requested_url, report.final_url
            );
        }

        let fetch_duration = report.duration;
        let duration_ms = fetch_duration.as_millis();
        let final_url = report.final_url.clone();
        match &report.result {
            Ok(document) => {
                info!(
                    "completed: requested={} final={} duration_ms={} result=ok status={} content_type={} bytes={}",
                    report.requested_url,
                    final_url,
                    duration_ms,
                    document.status,
                    document.content_type.as_deref().unwrap_or("-"),
                    document.body.len()
                );
            }
            Err(FetchFailure::ExcludedByRobots) => {
                info!(
                    "completed: requested={} final={} duration_ms={} result=excluded_by_robots",
                    report.requested_url, final_url, duration_ms
                );
            }
            Err(FetchFailure::Error(error)) => {
                info!(
                    "completed: requested={} final={} duration_ms={} result=error error={}",
                    report.requested_url, final_url, duration_ms, error
                );
            }
        }

        let document = match report.result {
            Ok(document) => document,
            Err(e) => {
                match e {
                    FetchFailure::ExcludedByRobots => {
                        warn!("excluded by robots.txt: {final_url}");
                    }
                    FetchFailure::Error(error) => {
                        warn!("failed to fetch {final_url}: {error}");
                    }
                }

                frontier.complete(FetchCompletion {
                    lease,
                    fetch_duration,
                });

                continue;
            }
        };

        if document.status.is_success() && is_html(&document.content_type) {
            let html = Html::parse_document(&document.body);

            for element in html.select(&anchor_selector) {
                let Some(href) = element.value().attr("href") else {
                    continue;
                };

                let Ok(next_url) = final_url.url().join(href) else {
                    continue;
                };

                if !next_url.has_host() {
                    continue;
                }

                if let Err(error) = enqueue_url(&mut frontier, &mut seen, &mut writer, next_url) {
                    error!("failed to write discovered URL: {error}");
                    continue;
                }
            }
        }

        frontier.complete(FetchCompletion {
            lease,
            fetch_duration,
        });

        successful_fetches += 1;

        if successful_fetches >= MAX_FETCHES {
            active_fetches.abort_all();
            break;
        }
    }

    info!("done after {successful_fetches} successful fetches and {attempted_fetches} attempts");
    Ok(())
}

struct FetchOutcome {
    lease: FrontierLease,
    report: FetchReport,
}

async fn sleep_until_std(deadline: std::time::Instant) {
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
}

fn is_html(content_type: &Option<String>) -> bool {
    content_type
        .as_deref()
        .map(|content_type| {
            content_type
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .eq_ignore_ascii_case("text/html")
        })
        .unwrap_or(false)
}

fn enqueue_url(
    frontier: &mut impl UrlFrontier,
    seen: &mut HashSet<String>,
    writer: &mut BufWriter<File>,
    url: Url,
) -> Result<bool, EnqueueUrlError> {
    let normalized = url.to_string();

    if seen.insert(normalized.clone()) {
        frontier.add(CrawlUrl::new(url)?);
        writeln!(writer, "{normalized}")?;
        writer.flush()?;
        return Ok(true);
    }

    Ok(false)
}

#[derive(Debug, Error)]
enum EnqueueUrlError {
    #[error(transparent)]
    CrawlUrl(#[from] CrawlUrlError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
