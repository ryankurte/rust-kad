use std::sync::{Arc, Mutex};

/// Synchronous structure used for testing
#[derive(Debug, Clone)]
pub struct MockSync(Arc<Mutex<u64>>);

impl MockSync {
    pub fn new(v: u64) -> Self {
        MockSync(Arc::new(Mutex::new(v)))
    }
}

impl PartialEq for MockSync {
    fn eq(&self, o: &Self) -> bool {
        let v1 = { *self.0.lock().unwrap() };
        let v2 = { *o.0.lock().unwrap() };
        v1 == v2
    }
}

unsafe impl Sync for MockSync {}
