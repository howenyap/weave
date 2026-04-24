mod frontier;
mod prioritizer;
mod traits;

use std::collections::HashSet;
use std::env;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::thread;
use std::time::Instant;

use frontier::MercatorFrontier;
use prioritizer::ConstantPrioritizer;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use thiserror::Error;
use traits::{CrawlUrl, CrawlUrlError, FetchCompletion, NextUrl, Priority, UrlFrontier};
use url::Url;

const MAX_FETCHES: usize = 100;
const DEFAULT_OUTPUT_PATH: &str = "output.txt";
const PRIORITY_LEVELS: u8 = 4;
const BACK_QUEUE_COUNT: usize = 3;
const POLITENESS_MULTIPLIER: u32 = 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let client = Client::builder().build()?;
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
        let current_url = lease.entry.url.url().clone();
        println!("fetching ({fetches}/{MAX_FETCHES}): {current_url}");

        let fetch_started = Instant::now();
        let Ok(response) = client.get(current_url.clone()).send() else {
            eprintln!("failed to fetch {current_url}");
            frontier.complete(FetchCompletion {
                lease,
                fetch_duration: fetch_started.elapsed(),
            });
            continue;
        };

        let Ok(body) = response.text() else {
            eprintln!("failed to read response body for {current_url}");
            frontier.complete(FetchCompletion {
                lease,
                fetch_duration: fetch_started.elapsed(),
            });
            continue;
        };
        let fetch_duration = fetch_started.elapsed();

        let document = Html::parse_document(&body);

        for element in document.select(&anchor_selector) {
            let Some(href) = element.value().attr("href") else {
                continue;
            };

            let Ok(next_url) = current_url.join(href) else {
                continue;
            };

            if !next_url.has_host() {
                continue;
            }

            if let Err(error) = enqueue_url(&mut frontier, &mut seen, &mut writer, next_url) {
                eprintln!("failed to write discovered URL: {error}");
                continue;
            }
        }

        frontier.complete(FetchCompletion {
            lease,
            fetch_duration,
        });
    }

    println!("done after {fetches} fetches");
    Ok(())
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
