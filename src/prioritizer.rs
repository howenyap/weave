use crate::traits::{CrawlUrl, Priority, UrlPrioritizer};

#[derive(Clone, Copy, Debug)]
pub struct ConstantPrioritizer {
    priority: Priority,
}

impl ConstantPrioritizer {
    pub fn new(priority: Priority) -> Self {
        Self { priority }
    }
}

impl Default for ConstantPrioritizer {
    fn default() -> Self {
        Self::new(Priority::new(1).expect("default priority is non-zero"))
    }
}

impl UrlPrioritizer for ConstantPrioritizer {
    fn get_priority(&self, _url: &CrawlUrl) -> Priority {
        self.priority
    }
}
