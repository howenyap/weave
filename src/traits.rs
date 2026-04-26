use std::fmt;
use std::num::NonZeroU8;
use std::time::{Duration, Instant};
use thiserror::Error;
use url::Url;

pub trait UrlFrontier {
    fn add(&mut self, url: CrawlUrl);
    fn next(&mut self) -> Option<NextUrl>;
    fn complete(&mut self, completion: FetchCompletion);
}

pub trait UrlPrioritizer {
    fn get_priority(&self, url: &CrawlUrl) -> Priority;
}

pub type Priority = NonZeroU8;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrawlUrl {
    url: Url,
    host: String,
}

impl CrawlUrl {
    pub fn new(url: Url) -> Result<Self, CrawlUrlError> {
        let Some(host) = url.host_str().map(str::to_owned) else {
            return Err(CrawlUrlError::MissingHost { url });
        };

        if !matches!(url.scheme(), "http" | "https") {
            return Err(CrawlUrlError::UnsupportedScheme {
                scheme: url.scheme().to_string(),
                url,
            });
        }

        Ok(Self { url, host })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Display for CrawlUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(formatter)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontierEntry {
    pub url: CrawlUrl,
    pub priority: Priority,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontierLease {
    pub queue_index: BackQueueIndex,
    pub entry: FrontierEntry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackQueueIndex(usize);

impl BackQueueIndex {
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    pub fn get(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct FetchCompletion {
    pub lease: FrontierLease,
    pub fetch_duration: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NextUrl {
    Ready(FrontierLease),
    WaitUntil(Instant),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CrawlUrlError {
    #[error("URL has no host: {url}")]
    MissingHost { url: Url },
    #[error("unsupported URL scheme {scheme}: {url}")]
    UnsupportedScheme { scheme: String, url: Url },
}
