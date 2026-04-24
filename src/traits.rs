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
        match url.host_str().map(str::to_owned) {
            Some(host) => Ok(Self { url, host }),
            None => return Err(CrawlUrlError::MissingHost { url }),
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn url(&self) -> &Url {
        &self.url
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
}
