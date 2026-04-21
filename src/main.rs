use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

use reqwest::blocking::Client;
use scraper::{Html, Selector};
use url::Url;

const MAX_FETCHES: usize = 100;
const DEFAULT_OUTPUT_PATH: &str = "output.txt";

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
    let mut frontier = VecDeque::new();
    let mut seen = HashSet::new();
    let mut fetches = 0;

    enqueue_url(&mut frontier, &mut seen, &mut writer, seed)?;

    while fetches < MAX_FETCHES
        && let Some(current_url) = frontier.pop_front()
    {
        fetches += 1;
        println!("fetching ({fetches}/{MAX_FETCHES}): {current_url}");

        let Ok(response) = client.get(current_url.clone()).send() else {
            eprintln!("failed to fetch {current_url}");
            continue;
        };

        let Ok(body) = response.text() else {
            eprintln!("failed to read response body for {current_url}");
            continue;
        };

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
    }

    println!("done after {fetches} fetches");
    Ok(())
}

fn enqueue_url(
    frontier: &mut VecDeque<Url>,
    seen: &mut HashSet<String>,
    writer: &mut BufWriter<std::fs::File>,
    url: Url,
) -> std::io::Result<bool> {
    let normalized = url.to_string();

    if seen.insert(normalized.clone()) {
        frontier.push_back(url);
        writeln!(writer, "{normalized}")?;
        writer.flush()?;
        return Ok(true);
    }

    Ok(false)
}
