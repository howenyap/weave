use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use log::warn;
use lru::LruCache;
use reqwest::header::{CONTENT_TYPE, LOCATION};
use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode, Url};
use robotstxt::DefaultMatcher;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::traits::{CrawlUrl, CrawlUrlError};

const MAX_REDIRECTS: usize = 10;

#[derive(Clone, Debug)]
pub struct HttpFetcher {
    client: Client,
    identity: CrawlerIdentity,
    robots_cache: Arc<Mutex<RobotsCache>>,
}

#[derive(Clone, Debug)]
pub struct CrawlerIdentity {
    name: String,
    version: String,
}

#[derive(Clone, Debug)]
pub struct HttpFetcherConfig {
    pub identity: CrawlerIdentity,
    pub robots_cache_capacity: NonZeroUsize,
    pub request_timeout: Duration,
}

#[derive(Debug)]
pub struct FetchReport {
    pub requested_url: CrawlUrl,
    pub final_url: CrawlUrl,
    pub duration: Duration,
    pub result: Result<FetchedDocument, FetchFailure>,
}

impl FetchReport {
    fn new(
        requested_url: CrawlUrl,
        final_url: CrawlUrl,
        started: Instant,
        result: Result<FetchedDocument, FetchFailure>,
    ) -> Self {
        Self {
            requested_url,
            final_url,
            duration: started.elapsed(),
            result,
        }
    }
}

#[derive(Debug)]
pub enum FetchFailure {
    ExcludedByRobots,
    Error(FetchError),
}

#[derive(Clone, Debug)]
pub struct FetchedDocument {
    pub status: StatusCode,
    pub content_type: Option<String>,
    pub body: String,
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("HTTP request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("failed to read response body: {0}")]
    Body(#[source] reqwest::Error),
    #[error("redirect response did not include a Location header")]
    RedirectWithoutLocation,
    #[error("redirect response included an invalid Location header: {0}")]
    InvalidRedirectHeader(#[source] reqwest::header::ToStrError),
    #[error("redirect location could not be resolved: {0}")]
    InvalidRedirectUrl(#[source] url::ParseError),
    #[error("redirect limit exceeded")]
    TooManyRedirects,
    #[error(transparent)]
    FinalUrl(#[from] CrawlUrlError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RobotsRules {
    AllowAll,
    DisallowAll,
    Rules(String),
}

#[derive(Debug)]
struct RobotsCache {
    entries: LruCache<OriginKey, RobotsRules>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OriginKey {
    scheme: String,
    host: String,
    port: Option<u16>,
}

impl HttpFetcher {
    pub fn new(config: HttpFetcherConfig) -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent(config.identity.http_user_agent())
            .timeout(config.request_timeout)
            .redirect(Policy::none())
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            client,
            identity: config.identity,
            robots_cache: Arc::new(Mutex::new(RobotsCache::new(config.robots_cache_capacity))),
        })
    }

    pub async fn fetch(&self, url: &CrawlUrl) -> FetchReport {
        let started = Instant::now();
        let requested_url = url.clone();
        let mut current_url = url.clone();
        let mut redirects = 0;

        loop {
            if !self.is_allowed_by_robots(&current_url).await {
                return FetchReport::new(
                    requested_url,
                    current_url,
                    started,
                    Err(FetchFailure::ExcludedByRobots),
                );
            }

            let response = match self.client.get(current_url.url().clone()).send().await {
                Ok(response) => response,
                Err(error) => {
                    return FetchReport::new(
                        requested_url,
                        current_url,
                        started,
                        Err(FetchFailure::Error(FetchError::Request(error))),
                    );
                }
            };

            if is_followable_redirect(response.status()) {
                if redirects >= MAX_REDIRECTS {
                    return FetchReport::new(
                        requested_url,
                        current_url,
                        started,
                        Err(FetchFailure::Error(FetchError::TooManyRedirects)),
                    );
                }

                let Some(location) = response.headers().get(LOCATION) else {
                    return FetchReport::new(
                        requested_url,
                        current_url,
                        started,
                        Err(FetchFailure::Error(FetchError::RedirectWithoutLocation)),
                    );
                };

                let location = match location.to_str() {
                    Ok(location) => location,
                    Err(error) => {
                        return FetchReport::new(
                            requested_url,
                            current_url,
                            started,
                            Err(FetchFailure::Error(FetchError::InvalidRedirectHeader(
                                error,
                            ))),
                        );
                    }
                };

                let next_url = match current_url.url().join(location) {
                    Ok(next_url) => next_url,
                    Err(error) => {
                        return FetchReport::new(
                            requested_url,
                            current_url,
                            started,
                            Err(FetchFailure::Error(FetchError::InvalidRedirectUrl(error))),
                        );
                    }
                };

                current_url = match CrawlUrl::new(next_url) {
                    Ok(next_url) => next_url,
                    Err(error) => {
                        return FetchReport::new(
                            requested_url,
                            current_url,
                            started,
                            Err(FetchFailure::Error(FetchError::FinalUrl(error))),
                        );
                    }
                };

                redirects += 1;

                continue;
            }

            let status = response.status();
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = match response.text().await {
                Ok(body) => body,
                Err(error) => {
                    return FetchReport::new(
                        requested_url,
                        current_url,
                        started,
                        Err(FetchFailure::Error(FetchError::Body(error))),
                    );
                }
            };

            return FetchReport::new(
                requested_url,
                current_url,
                started,
                Ok(FetchedDocument {
                    status,
                    content_type,
                    body,
                }),
            );
        }
    }

    async fn is_allowed_by_robots(&self, url: &CrawlUrl) -> bool {
        let origin = OriginKey::from_crawl_url(url);

        if let Some(rules) = self.robots_cache.lock().await.get(&origin) {
            return rules.allows(url.url(), self.identity.robots_user_agent());
        }

        let rules = self.fetch_robots_rules(url).await;
        let rules = self.robots_cache.lock().await.insert(origin, rules);

        rules.allows(url.url(), self.identity.robots_user_agent())
    }

    async fn fetch_robots_rules(&self, url: &CrawlUrl) -> RobotsRules {
        let mut robots_url = robots_url(url.url());
        let mut redirects = 0;

        let response = loop {
            let response = match self.client.get(robots_url.clone()).send().await {
                Ok(response) => response,
                Err(error) => {
                    warn!(
                        "failed to fetch robots.txt for {} using {}: {error}; disallowing crawl",
                        url.host(),
                        self.identity.http_user_agent()
                    );
                    return RobotsRules::DisallowAll;
                }
            };

            if !response.status().is_redirection() {
                break response;
            }

            if !is_followable_redirect(response.status()) {
                warn!(
                    "robots.txt for {} returned non-followable redirect {}; disallowing crawl",
                    url.host(),
                    response.status()
                );
                return RobotsRules::DisallowAll;
            }

            if redirects >= MAX_REDIRECTS {
                warn!(
                    "robots.txt redirect limit exceeded for {}; disallowing crawl",
                    url.host()
                );
                return RobotsRules::DisallowAll;
            }

            let Some(location) = response.headers().get(LOCATION) else {
                warn!(
                    "robots.txt redirect for {} did not include Location; disallowing crawl",
                    url.host()
                );
                return RobotsRules::DisallowAll;
            };

            let location = match location.to_str() {
                Ok(location) => location,
                Err(error) => {
                    warn!(
                        "robots.txt redirect for {} included invalid Location: {error}; disallowing crawl",
                        url.host()
                    );
                    return RobotsRules::DisallowAll;
                }
            };

            robots_url = match robots_url.join(location) {
                Ok(next_url) => next_url,
                Err(error) => {
                    warn!(
                        "robots.txt redirect for {} could not be resolved: {error}; disallowing crawl",
                        url.host()
                    );
                    return RobotsRules::DisallowAll;
                }
            };

            redirects += 1;
        };

        if response.status().is_client_error() {
            return RobotsRules::AllowAll;
        }

        if response.status().is_server_error() {
            warn!(
                "robots.txt for {} returned {}; disallowing crawl",
                url.host(),
                response.status()
            );

            return RobotsRules::DisallowAll;
        }

        if !response.status().is_success() {
            warn!(
                "robots.txt for {} returned {}; allowing crawl",
                url.host(),
                response.status()
            );

            return RobotsRules::AllowAll;
        }

        let is_robots_txt_url = robots_url
            .path()
            .to_ascii_lowercase()
            .ends_with("robots.txt");
        if !is_robots_txt_url {
            warn!(
                "robots.txt for {} redirected to non-robots URL {}; disallowing crawl",
                url.host(),
                robots_url
            );

            return RobotsRules::DisallowAll;
        }

        match response.text().await {
            Ok(body) => RobotsRules::Rules(body),
            Err(error) => {
                warn!(
                    "failed to read robots.txt for {} using {}: {error}; disallowing crawl",
                    url.host(),
                    self.identity.http_user_agent()
                );
                RobotsRules::DisallowAll
            }
        }
    }
}

impl CrawlerIdentity {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> anyhow::Result<Self> {
        let name = name.into();
        let version = version.into();

        ensure!(!name.trim().is_empty(), "crawler name must not be empty");
        ensure!(
            !version.trim().is_empty(),
            "crawler version must not be empty"
        );

        Ok(Self { name, version })
    }

    pub fn http_user_agent(&self) -> String {
        format!("{}/{}", self.name, self.version)
    }

    pub fn robots_user_agent(&self) -> &str {
        &self.name
    }
}

impl Default for HttpFetcherConfig {
    fn default() -> Self {
        Self {
            identity: CrawlerIdentity::new("weave", "0.1").expect("default identity is valid"),
            robots_cache_capacity: NonZeroUsize::new(256).expect("default cache capacity is valid"),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl RobotsRules {
    fn allows(&self, url: &Url, user_agent: &str) -> bool {
        match self {
            Self::AllowAll => true,
            Self::DisallowAll => false,
            Self::Rules(body) => {
                let mut matcher = DefaultMatcher::default();
                matcher.one_agent_allowed_by_robots(body, user_agent, url.as_str())
            }
        }
    }
}

impl RobotsCache {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            entries: LruCache::new(capacity),
        }
    }

    fn get(&mut self, key: &OriginKey) -> Option<RobotsRules> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: OriginKey, rules: RobotsRules) -> RobotsRules {
        self.entries.get_or_insert(key, || rules).clone()
    }
}

impl OriginKey {
    fn from_crawl_url(url: &CrawlUrl) -> Self {
        Self {
            scheme: url.url().scheme().to_ascii_lowercase(),
            host: url.host().to_ascii_lowercase(),
            port: url.url().port(),
        }
    }
}

fn is_followable_redirect(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    )
}

fn robots_url(url: &Url) -> Url {
    let mut robots_url = url.clone();

    robots_url.set_path("/robots.txt");
    robots_url.set_query(None);
    robots_url.set_fragment(None);

    let _ = robots_url.set_username("");
    let _ = robots_url.set_password(None);
    robots_url
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn crawler_identity_formats_http_user_agent() {
        let identity = test_identity();

        assert_eq!(identity.http_user_agent(), "weave/0.1");
    }

    #[test]
    fn crawler_identity_uses_name_for_robots_user_agent() {
        let identity = test_identity();

        assert_eq!(identity.robots_user_agent(), "weave");
    }

    #[test]
    fn empty_crawler_name_fails_identity_construction() {
        let result = CrawlerIdentity::new(" ", "0.1");

        assert_eq!(
            result.unwrap_err().to_string(),
            "crawler name must not be empty"
        );
    }

    #[test]
    fn empty_crawler_version_fails_identity_construction() {
        let result = CrawlerIdentity::new("weave", " ");

        assert_eq!(
            result.unwrap_err().to_string(),
            "crawler version must not be empty"
        );
    }

    #[test]
    fn robots_url_uses_origin_root() {
        assert_eq!(
            robots_url(&url("https://example.com/a/b?x=1#frag")).as_str(),
            "https://example.com/robots.txt"
        );
    }

    #[test]
    fn robots_url_preserves_non_default_port() {
        assert_eq!(
            robots_url(&url("https://example.com:8443/a/b")).as_str(),
            "https://example.com:8443/robots.txt"
        );
    }

    #[test]
    fn robots_rules_block_disallowed_path() {
        let rules = RobotsRules::Rules("User-agent: *\nDisallow: /private\n".to_string());

        assert!(!rules.allows(&url("https://example.com/private/page"), "weave"));
        assert!(rules.allows(&url("https://example.com/public/page"), "weave"));
    }

    #[tokio::test]
    async fn cache_reuses_rules_for_same_host() {
        let server = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nAllow: /\n",
            ),
            response("/one", 200, "text/html", "<a href=\"/two\">two</a>"),
            response("/two", 200, "text/html", "done"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher.fetch(&crawl_url(&server.url("/one"))).await.result,
            Ok(_)
        ));
        assert!(matches!(
            fetcher.fetch(&crawl_url(&server.url("/two"))).await.result,
            Ok(_)
        ));

        assert_eq!(server.request_count("/robots.txt"), 1);
    }

    #[tokio::test]
    async fn cache_evicts_when_capacity_is_exceeded() {
        let first = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nAllow: /\n",
            ),
            response("/one", 200, "text/html", "one"),
            response("/two", 200, "text/html", "two"),
        ]);
        let second = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nAllow: /\n",
            ),
            response("/one", 200, "text/html", "one"),
        ]);
        let fetcher = test_fetcher(1);

        assert!(matches!(
            fetcher.fetch(&crawl_url(&first.url("/one"))).await.result,
            Ok(_)
        ));
        assert!(matches!(
            fetcher.fetch(&crawl_url(&second.url("/one"))).await.result,
            Ok(_)
        ));
        assert!(matches!(
            fetcher.fetch(&crawl_url(&first.url("/two"))).await.result,
            Ok(_)
        ));

        assert_eq!(first.request_count("/robots.txt"), 2);
        assert_eq!(second.request_count("/robots.txt"), 1);
    }

    #[tokio::test]
    async fn blocked_page_is_not_requested() {
        let server = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nDisallow: /blocked\n",
            ),
            response("/blocked", 200, "text/html", "blocked"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher
                .fetch(&crawl_url(&server.url("/blocked")))
                .await
                .result,
            Err(FetchFailure::ExcludedByRobots)
        ));

        assert_eq!(server.request_count("/blocked"), 0);
    }

    #[tokio::test]
    async fn not_found_robots_allows_page_fetch() {
        let server = TestServer::new(vec![
            response("/robots.txt", 404, "text/plain", "missing"),
            response("/page", 200, "text/html", "ok"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher.fetch(&crawl_url(&server.url("/page"))).await.result,
            Ok(_)
        ));
    }

    #[tokio::test]
    async fn robots_server_error_disallows_page_fetch() {
        let server = TestServer::new(vec![
            response("/robots.txt", 500, "text/plain", "bad"),
            response("/page", 200, "text/html", "ok"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher.fetch(&crawl_url(&server.url("/page"))).await.result,
            Err(FetchFailure::ExcludedByRobots)
        ));
        assert_eq!(server.request_count("/page"), 0);
    }

    #[tokio::test]
    async fn robots_network_error_disallows_page_fetch() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher
                .fetch(&crawl_url(&format!("http://{address}/page")))
                .await
                .result,
            Err(FetchFailure::ExcludedByRobots)
        ));
    }

    #[tokio::test]
    async fn robots_redirect_to_disallowing_rules_blocks_page_fetch() {
        let server = TestServer::new(vec![
            redirect_response("/robots.txt", "/redirected-robots.txt"),
            response(
                "/redirected-robots.txt",
                200,
                "text/plain",
                "User-agent: *\nDisallow: /blocked\n",
            ),
            response("/blocked", 200, "text/html", "blocked"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher
                .fetch(&crawl_url(&server.url("/blocked")))
                .await
                .result,
            Err(FetchFailure::ExcludedByRobots)
        ));
        assert_eq!(server.request_count("/robots.txt"), 1);
        assert_eq!(server.request_count("/redirected-robots.txt"), 1);
        assert_eq!(server.request_count("/blocked"), 0);
    }

    #[tokio::test]
    async fn robots_redirect_to_non_robots_url_disallows_page_fetch() {
        let server = TestServer::new(vec![
            redirect_response("/robots.txt", "/login"),
            response(
                "/login",
                200,
                "text/html",
                "<html><body>User-agent: *\nAllow: /</body></html>",
            ),
            response("/page", 200, "text/html", "ok"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher.fetch(&crawl_url(&server.url("/page"))).await.result,
            Err(FetchFailure::ExcludedByRobots)
        ));
        assert_eq!(server.request_count("/robots.txt"), 1);
        assert_eq!(server.request_count("/login"), 1);
        assert_eq!(server.request_count("/page"), 0);
    }

    #[tokio::test]
    async fn non_followable_robots_redirect_disallows_page_fetch() {
        let server = TestServer::new(vec![
            response("/robots.txt", 300, "text/plain", "multiple choices"),
            response("/page", 200, "text/html", "ok"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher.fetch(&crawl_url(&server.url("/page"))).await.result,
            Err(FetchFailure::ExcludedByRobots)
        ));
        assert_eq!(server.request_count("/page"), 0);
    }

    #[tokio::test]
    async fn redirect_to_disallowed_same_origin_path_is_not_requested() {
        let server = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nDisallow: /private\n",
            ),
            redirect_response("/start", "/private"),
            response("/private", 200, "text/html", "private"),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher
                .fetch(&crawl_url(&server.url("/start")))
                .await
                .result,
            Err(FetchFailure::ExcludedByRobots)
        ));
        assert_eq!(server.request_count("/start"), 1);
        assert_eq!(server.request_count("/private"), 0);
    }

    #[tokio::test]
    async fn redirect_to_disallowed_other_origin_is_not_requested() {
        let target = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nDisallow: /blocked\n",
            ),
            response("/blocked", 200, "text/html", "blocked"),
        ]);
        let source = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nAllow: /\n",
            ),
            redirect_response("/start", &target.url("/blocked")),
        ]);
        let fetcher = test_fetcher(8);

        assert!(matches!(
            fetcher
                .fetch(&crawl_url(&source.url("/start")))
                .await
                .result,
            Err(FetchFailure::ExcludedByRobots)
        ));
        assert_eq!(source.request_count("/start"), 1);
        assert_eq!(target.request_count("/robots.txt"), 1);
        assert_eq!(target.request_count("/blocked"), 0);
    }

    #[tokio::test]
    async fn allowed_redirect_sets_final_url() {
        let server = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nAllow: /\n",
            ),
            redirect_response("/start", "/final"),
            response("/final", 200, "text/html", "final"),
        ]);
        let fetcher = test_fetcher(8);

        let report = fetcher.fetch(&crawl_url(&server.url("/start"))).await;
        let Ok(_) = report.result else {
            panic!("expected fetched document");
        };

        assert_eq!(report.requested_url.url().as_str(), server.url("/start"));
        assert_eq!(report.final_url.url().as_str(), server.url("/final"));
    }

    #[tokio::test]
    async fn non_followable_page_3xx_without_location_is_fetched() {
        let server = TestServer::new(vec![
            response(
                "/robots.txt",
                200,
                "text/plain",
                "User-agent: *\nAllow: /\n",
            ),
            response("/multiple", 300, "text/html", "choose"),
        ]);
        let fetcher = test_fetcher(8);

        let report = fetcher.fetch(&crawl_url(&server.url("/multiple"))).await;
        let Ok(document) = report.result else {
            panic!("expected fetched document");
        };

        assert_eq!(document.status, StatusCode::MULTIPLE_CHOICES);
        assert_eq!(report.final_url.url().as_str(), server.url("/multiple"));
        assert_eq!(server.request_count("/multiple"), 1);
    }

    fn test_fetcher(robots_cache_capacity: usize) -> HttpFetcher {
        HttpFetcher::new(HttpFetcherConfig {
            identity: test_identity(),
            robots_cache_capacity: NonZeroUsize::new(robots_cache_capacity)
                .expect("test cache capacity is non-zero"),
            request_timeout: Duration::from_secs(5),
        })
        .unwrap()
    }

    fn test_identity() -> CrawlerIdentity {
        CrawlerIdentity::new("weave", "0.1").expect("test identity is valid")
    }

    fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    fn crawl_url(raw: &str) -> CrawlUrl {
        CrawlUrl::new(url(raw)).unwrap()
    }

    fn response(path: &str, status: u16, content_type: &str, body: &str) -> TestResponse {
        TestResponse {
            path: path.to_string(),
            status,
            content_type: content_type.to_string(),
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn redirect_response(path: &str, location: &str) -> TestResponse {
        TestResponse {
            path: path.to_string(),
            status: 302,
            content_type: "text/plain".to_string(),
            headers: vec![("Location".to_string(), location.to_string())],
            body: String::new(),
        }
    }

    #[derive(Clone, Debug)]
    struct TestResponse {
        path: String,
        status: u16,
        content_type: String,
        headers: Vec<(String, String)>,
        body: String,
    }

    struct TestServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl TestServer {
        fn new(responses: Vec<TestResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let routes: Arc<HashMap<String, TestResponse>> = Arc::new(
                responses
                    .into_iter()
                    .map(|response| (response.path.clone(), response))
                    .collect(),
            );
            let requests = Arc::new(Mutex::new(Vec::new()));
            let server_routes = Arc::clone(&routes);
            let server_requests = Arc::clone(&requests);

            thread::spawn(move || {
                for stream in listener.incoming().flatten() {
                    handle_connection(stream, &server_routes, &server_requests);
                }
            });

            Self {
                base_url: format!("http://{address}"),
                requests,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("{}{}", self.base_url, path)
        }

        fn request_count(&self, path: &str) -> usize {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request_path| request_path.as_str() == path)
                .count()
        }
    }

    fn handle_connection(
        mut stream: TcpStream,
        routes: &HashMap<String, TestResponse>,
        requests: &Arc<Mutex<Vec<String>>>,
    ) {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).is_err() {
            return;
        }

        let path = request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .to_string();
        requests.lock().unwrap().push(path.clone());

        let mut header = String::new();
        while reader.read_line(&mut header).is_ok() {
            if header == "\r\n" || header == "\n" {
                break;
            }
            header.clear();
        }

        let response = routes.get(&path).cloned().unwrap_or_else(|| TestResponse {
            path,
            status: 404,
            content_type: "text/plain".to_string(),
            headers: Vec::new(),
            body: "not found".to_string(),
        });
        let reason = match response.status {
            200 => "OK",
            300 => "Multiple Choices",
            301 => "Moved Permanently",
            302 => "Found",
            303 => "See Other",
            304 => "Not Modified",
            307 => "Temporary Redirect",
            308 => "Permanent Redirect",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => "OK",
        };
        let body = response.body.as_bytes();
        let mut headers = format!(
            "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            response.status,
            response.content_type,
            body.len()
        );
        for (name, value) in response.headers {
            headers.push_str(&format!("{name}: {value}\r\n"));
        }
        headers.push_str("\r\n");

        stream.write_all(headers.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        let _ = stream.flush();
    }
}
