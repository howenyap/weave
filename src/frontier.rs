use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::ops::{Index, IndexMut};
use std::time::Instant;

use rand::distr::{Distribution, weighted::WeightedIndex};

use crate::traits::{
    BackQueueIndex, CrawlUrl, FetchCompletion, FrontierEntry, FrontierLease, NextUrl, Priority,
    UrlFrontier, UrlPrioritizer,
};

#[derive(Debug)]
pub struct MercatorFrontier<P> {
    front_queues: FrontQueues,
    back_queues: BackQueues,
    host_to_back_queue: HashMap<String, BackQueueIndex>,
    ready_heap: BinaryHeap<Reverse<QueueHandle>>,
    prioritizer: P,
    priority_levels: Priority,
    politeness_multiplier: u32,
}

#[derive(Debug)]
pub struct FrontQueues(Vec<VecDeque<FrontierEntry>>);

#[derive(Debug)]
struct BackQueues(Vec<BackQueue>);

#[derive(Debug, Default)]
enum BackQueue {
    #[default]
    Inactive,
    Assigned {
        host: String,
        urls: VecDeque<FrontierEntry>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct QueueHandle {
    ready_at: Instant,
    queue_index: BackQueueIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrontQueueIndex(usize);

impl FrontQueueIndex {
    fn new(index: usize) -> Self {
        Self(index)
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl From<Priority> for FrontQueueIndex {
    fn from(priority: Priority) -> Self {
        Self::new(usize::from(priority.get() - 1))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefillBackQueueResult {
    Assigned,
    NoUrlAvailable,
}

impl<P: UrlPrioritizer> MercatorFrontier<P> {
    pub fn new(
        prioritizer: P,
        priority_levels: Priority,
        back_queue_count: usize,
        politeness_multiplier: u32,
    ) -> Self {
        assert!(
            back_queue_count > 0,
            "back_queue_count must be greater than 0"
        );

        let front_queues = FrontQueues(
            (0..priority_levels.get())
                .map(|_| VecDeque::new())
                .collect(),
        );
        let back_queues = BackQueues(
            (0..back_queue_count)
                .map(|_| BackQueue::default())
                .collect(),
        );

        Self {
            front_queues,
            back_queues,
            host_to_back_queue: HashMap::new(),
            ready_heap: BinaryHeap::new(),
            prioritizer,
            priority_levels,
            politeness_multiplier,
        }
    }

    fn activate_inactive_back_queues(&mut self) {
        for queue_index in (0..self.back_queues.len()).map(BackQueueIndex::new) {
            if !self.back_queues[queue_index].is_inactive() {
                continue;
            }

            match self.refill_back_queue(queue_index) {
                RefillBackQueueResult::Assigned => {
                    self.ready_heap.push(Reverse(QueueHandle {
                        ready_at: Instant::now(),
                        queue_index,
                    }));
                }
                RefillBackQueueResult::NoUrlAvailable => break,
            }
        }
    }

    fn refill_back_queue(&mut self, queue_index: BackQueueIndex) -> RefillBackQueueResult {
        assert!(
            !self.back_queues[queue_index].is_assigned(),
            "cannot refill an already assigned back queue"
        );

        while let Some(candidate) = self.get_front_queue_entry() {
            if let Some(existing_queue_index) =
                self.host_to_back_queue.get(candidate.url.host()).copied()
            {
                self.back_queues[existing_queue_index].push_assigned_url(candidate);
            } else {
                let host = candidate.url.host().to_owned();

                self.host_to_back_queue.insert(host.clone(), queue_index);
                self.back_queues[queue_index] = BackQueue::Assigned {
                    host,
                    urls: VecDeque::from([candidate]),
                };

                return RefillBackQueueResult::Assigned;
            }
        }

        RefillBackQueueResult::NoUrlAvailable
    }

    fn get_front_queue_entry(&mut self) -> Option<FrontierEntry> {
        let queue_index = self.front_queues.pick_weighted_random()?;

        self.front_queues[queue_index].pop_front()
    }
}

impl<P: UrlPrioritizer> UrlFrontier for MercatorFrontier<P> {
    fn add(&mut self, url: CrawlUrl) {
        let priority = self.prioritizer.get_priority(&url);

        assert!(
            priority <= self.priority_levels,
            "prioritizer returned priority {priority} outside 1..={}",
            self.priority_levels
        );

        let entry = FrontierEntry { url, priority };
        let front_queue_index = FrontQueueIndex::from(priority);

        self.front_queues[front_queue_index].push_back(entry);
    }

    fn next(&mut self) -> Option<NextUrl> {
        self.activate_inactive_back_queues();

        let Some(Reverse(queue_handle)) = self.ready_heap.pop() else {
            return None;
        };

        if queue_handle.ready_at > Instant::now() {
            self.ready_heap.push(Reverse(queue_handle));

            return Some(NextUrl::WaitUntil(queue_handle.ready_at));
        }

        let entry = self.back_queues[queue_handle.queue_index]
            .front()
            .cloned()
            .expect("ready_heap contained a handle for an empty back queue");

        Some(NextUrl::Ready(FrontierLease {
            queue_index: queue_handle.queue_index,
            entry,
        }))
    }

    fn complete(&mut self, completion: FetchCompletion) {
        let back_queue_index = completion.lease.queue_index;
        assert!(
            self.back_queues.contains_index(back_queue_index),
            "lease referenced an invalid back queue index"
        );

        let back_queue = &mut self.back_queues[back_queue_index];
        back_queue.pop_front_queue_with_url(&completion.lease.entry.url);

        let ready_at = if back_queue.is_assigned_but_empty() {
            let released_host = back_queue.host().to_owned();
            back_queue.deactivate();

            self.host_to_back_queue.remove(&released_host);

            match self.refill_back_queue(back_queue_index) {
                RefillBackQueueResult::Assigned => Instant::now(),
                RefillBackQueueResult::NoUrlAvailable => return,
            }
        } else {
            let delay = completion
                .fetch_duration
                .saturating_mul(self.politeness_multiplier);

            Instant::now() + delay
        };

        self.ready_heap.push(Reverse(QueueHandle {
            ready_at,
            queue_index: back_queue_index,
        }));
    }
}

impl FrontQueues {
    fn iter(&self) -> impl Iterator<Item = &VecDeque<FrontierEntry>> {
        self.0.iter()
    }

    fn pick_weighted_random(&self) -> Option<FrontQueueIndex> {
        let non_empty_queue_indexes: Vec<_> = self
            .iter()
            .enumerate()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(queue_index, _)| queue_index)
            .collect();

        if non_empty_queue_indexes.is_empty() {
            return None;
        }

        let weights = non_empty_queue_indexes
            .iter()
            .map(|queue_index| queue_index + 1);
        let distribution =
            WeightedIndex::new(weights).expect("non-empty queues have valid weights");
        let sampled_index = distribution.sample(&mut rand::rng());
        let sampled_queue_index = non_empty_queue_indexes[sampled_index];

        Some(FrontQueueIndex::new(sampled_queue_index))
    }
}

impl Index<FrontQueueIndex> for FrontQueues {
    type Output = VecDeque<FrontierEntry>;

    fn index(&self, index: FrontQueueIndex) -> &Self::Output {
        &self.0[index.get()]
    }
}

impl IndexMut<FrontQueueIndex> for FrontQueues {
    fn index_mut(&mut self, index: FrontQueueIndex) -> &mut Self::Output {
        &mut self.0[index.get()]
    }
}

impl BackQueues {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn contains_index(&self, index: BackQueueIndex) -> bool {
        index.get() < self.0.len()
    }
}

impl Index<BackQueueIndex> for BackQueues {
    type Output = BackQueue;

    fn index(&self, index: BackQueueIndex) -> &Self::Output {
        &self.0[index.get()]
    }
}

impl IndexMut<BackQueueIndex> for BackQueues {
    fn index_mut(&mut self, index: BackQueueIndex) -> &mut Self::Output {
        &mut self.0[index.get()]
    }
}

impl BackQueue {
    fn is_inactive(&self) -> bool {
        matches!(self, Self::Inactive)
    }

    fn is_assigned(&self) -> bool {
        matches!(self, Self::Assigned { .. })
    }

    fn front(&self) -> Option<&FrontierEntry> {
        match self {
            Self::Inactive => None,
            Self::Assigned { urls, .. } => urls.front(),
        }
    }

    fn push_assigned_url(&mut self, entry: FrontierEntry) {
        match self {
            Self::Inactive => panic!("cannot push a URL into an inactive back queue"),
            Self::Assigned { urls, .. } => urls.push_back(entry),
        }
    }

    fn pop_front_queue_with_url(&mut self, url: &CrawlUrl) {
        let Self::Assigned { urls, .. } = self else {
            panic!("cannot pop from an inactive back queue");
        };

        let head = urls
            .front()
            .expect("cannot complete a lease for an empty back queue");

        assert_eq!(&head.url, url, "lease did not match the queue head");
        urls.pop_front();
    }

    fn is_assigned_but_empty(&self) -> bool {
        matches!(self, Self::Assigned { urls, .. } if urls.is_empty())
    }

    fn host(&self) -> &str {
        match self {
            Self::Inactive => panic!("cannot get the host of an inactive back queue"),
            Self::Assigned { host, .. } => host,
        }
    }

    fn deactivate(&mut self) {
        assert!(
            self.is_assigned(),
            "cannot deactivate an inactive back queue"
        );
        *self = Self::Inactive;
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Inactive => 0,
            Self::Assigned { urls, .. } => urls.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::prioritizer::ConstantPrioritizer;
    use crate::traits::CrawlUrlError;
    use url::Url;

    #[test]
    fn constant_prioritizer_returns_valid_bucket() {
        let prioritizer = ConstantPrioritizer::new(priority(1));

        assert_eq!(
            prioritizer.get_priority(&crawl_url("https://example.com")),
            priority(1)
        );
        assert_eq!(
            prioritizer.get_priority(&crawl_url("https://example.com")),
            priority(1)
        );
    }

    #[test]
    fn add_rejects_urls_without_hosts() {
        let mut frontier = frontier();

        assert!(matches!(
            CrawlUrl::new(url("mailto:test@example.com")),
            Err(CrawlUrlError::MissingHost { .. })
        ));
        assert_eq!(frontier.next(), None);
    }

    #[test]
    fn next_returns_empty_when_no_urls_exist() {
        let mut frontier = frontier();

        assert_eq!(frontier.next(), None);
    }

    #[test]
    fn different_hosts_can_occupy_different_back_queues() {
        let mut frontier = frontier();
        frontier.add(crawl_url("https://a.example/one"));
        frontier.add(crawl_url("https://b.example/one"));

        let first = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        let second = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        assert_ne!(first.entry.url.host(), second.entry.url.host());
        assert_ne!(first.queue_index, second.queue_index);
    }

    #[test]
    fn same_host_urls_are_routed_to_same_back_queue() {
        let mut frontier = frontier();
        frontier.add(crawl_url("https://example.com/one"));
        frontier.add(crawl_url("https://example.com/two"));

        let first = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        frontier.complete(FetchCompletion {
            lease: first.clone(),
            fetch_duration: Duration::ZERO,
        });

        let second = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        assert_eq!(first.queue_index, second.queue_index);
        assert_eq!(first.entry.url.url(), &url("https://example.com/one"));
        assert_eq!(second.entry.url.url(), &url("https://example.com/two"));
    }

    #[test]
    fn complete_applies_politeness_multiplier() {
        let mut frontier = frontier();
        frontier.add(crawl_url("https://example.com/one"));
        frontier.add(crawl_url("https://example.com/two"));

        let first = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        frontier.complete(FetchCompletion {
            lease: first,
            fetch_duration: Duration::from_millis(50),
        });

        match frontier.next() {
            Some(NextUrl::WaitUntil(ready_at)) => {
                assert!(ready_at > Instant::now());
            }
            other => panic!("expected wait, got {other:?}"),
        }
    }

    #[test]
    fn refill_assigns_one_url_to_new_back_queue() {
        let mut frontier =
            MercatorFrontier::new(ConstantPrioritizer::new(priority(1)), priority(2), 1, 0);
        frontier.add(crawl_url("https://example.com/one"));
        frontier.add(crawl_url("https://example.com/two"));

        let first = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        assert_eq!(frontier.back_queues[first.queue_index].len(), 1);
    }

    #[test]
    fn refill_routes_owned_hosts_to_existing_back_queue() {
        let mut frontier =
            MercatorFrontier::new(ConstantPrioritizer::new(priority(1)), priority(2), 2, 0);
        frontier.add(crawl_url("https://example.com/one"));
        frontier.add(crawl_url("https://example.com/two"));
        frontier.add(crawl_url("https://other.example/one"));

        let first = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        assert_eq!(frontier.back_queues[first.queue_index].len(), 2);
    }

    #[test]
    fn refilled_queue_is_immediately_ready_for_new_host() {
        let mut frontier =
            MercatorFrontier::new(ConstantPrioritizer::new(priority(1)), priority(2), 1, 10);
        frontier.add(crawl_url("https://a.example/one"));
        frontier.add(crawl_url("https://b.example/one"));

        let first = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        frontier.complete(FetchCompletion {
            lease: first,
            fetch_duration: Duration::from_millis(50),
        });

        let second = match frontier.next() {
            Some(NextUrl::Ready(lease)) => lease,
            other => panic!("expected ready URL, got {other:?}"),
        };

        assert_eq!(second.entry.url.host(), "b.example");
    }

    fn frontier() -> MercatorFrontier<ConstantPrioritizer> {
        MercatorFrontier::new(ConstantPrioritizer::new(priority(1)), priority(4), 3, 10)
    }

    fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    fn crawl_url(raw: &str) -> CrawlUrl {
        CrawlUrl::new(url(raw)).unwrap()
    }

    fn priority(value: u8) -> Priority {
        Priority::new(value).expect("test priority is non-zero")
    }
}
