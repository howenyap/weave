mod fetcher;
mod frontier;
mod prioritizer;
mod traits;

use std::collections::HashSet;
use std::env;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::thread;
use std::time::{Duration, Instant};

use env_logger::Env;
use fetcher::{CrawlerIdentity, FetchFailure, HttpFetcher, HttpFetcherConfig};
use frontier::MercatorFrontier;
use log::{error, info, warn};
use prioritizer::ConstantPrioritizer;
use scraper::{Html, Selector};
use thiserror::Error;
use traits::{CrawlUrl, CrawlUrlError, FetchCompletion, NextUrl, Priority, UrlFrontier};
use url::Url;

const MAX_FETCHES: usize = 1000;
const DEFAULT_OUTPUT_PATH: &str = "output.txt";
const PRIORITY_LEVELS: u8 = 4;
const BACK_QUEUE_COUNT: usize = 8;
const POLITENESS_MULTIPLIER: u32 = 10;
const CRAWLER_NAME: &str = "weave";
const CRAWLER_VERSION: &str = "0.1";
const ROBOTS_CACHE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(256).expect("robots cache capacity is non-zero");
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(Env::default().filter_or("RUST_LOG", "info")).init();

    let mut args = env::args().skip(1);
    let seed_url = match args.next() {
        Some(seed_url) => seed_url,
        None => {
            eprintln!("usage: cargo run -- <seed_url> [output_file]");
            std::process::exit(1);
        }
    };
    let output_path = args
        .next()
        .unwrap_or_else(|| DEFAULT_OUTPUT_PATH.to_string());

    let seed = Url::parse(&seed_url)?;
    let mut fetcher = HttpFetcher::new(HttpFetcherConfig {
        identity: CrawlerIdentity::new(CRAWLER_NAME, CRAWLER_VERSION)?,
        robots_cache_capacity: ROBOTS_CACHE_CAPACITY,
        request_timeout: REQUEST_TIMEOUT,
    })?;
    let anchor_selector = Selector::parse("a")?;
    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_path)?;

    let mut writer = BufWriter::new(output);
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
    let mut fetches = 0;

    enqueue_url(&mut frontier, &mut seen, &mut writer, seed)?;

    while fetches < MAX_FETCHES {
        let lease = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            Some(NextUrl::WaitUntil(ready_at)) => {
                let now = Instant::now();

                if ready_at > now {
                    thread::sleep(ready_at - now);
                }

                continue;
            }
            None => break,
        };

        fetches += 1;
        let current_url = lease.entry.url.clone();
        info!("fetching ({fetches}/{MAX_FETCHES}): {current_url}");

        let report = fetcher.fetch(&current_url);

        if report.requested_url != report.final_url {
            info!(
                "redirected: {} -> {}",
                report.requested_url, report.final_url
            );
        }

        let document = match report.result {
            Ok(document) => document,
            Err(FetchFailure::ExcludedByRobots) => {
                warn!("excluded by robots.txt: {}", report.final_url);
                frontier.complete(FetchCompletion {
                    lease,
                    fetch_duration: report.duration,
                });
                continue;
            }
            Err(FetchFailure::Error(error)) => {
                warn!("failed to fetch {}: {error}", report.final_url);
                frontier.complete(FetchCompletion {
                    lease,
                    fetch_duration: report.duration,
                });
                continue;
            }
        };

        if !document.status.is_success() || !is_html(&document.content_type) {
            frontier.complete(FetchCompletion {
                lease,
                fetch_duration: report.duration,
            });
            continue;
        }

        let html = Html::parse_document(&document.body);

        for element in html.select(&anchor_selector) {
            let Some(href) = element.value().attr("href") else {
                continue;
            };

            let Ok(next_url) = report.final_url.url().join(href) else {
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

        frontier.complete(FetchCompletion {
            lease,
            fetch_duration: report.duration,
        });
    }

    info!("done after {fetches} fetches");
    Ok(())
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
    writer: &mut BufWriter<std::fs::File>,
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
