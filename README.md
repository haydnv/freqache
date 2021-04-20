# freqache
A weighted, thread-safe, futures-aware Rust LFU cache which supports a custom eviction policy.

Example:
```rust
use freqache::LFUCache;

#[derive(Clone)]
struct Entry;

impl freqache::Entry for Entry {
    fn weight(&self) -> u64 {
        1
    }
}

struct Policy;

#[async_trait]
impl freqache::Policy<String, Entry> for Policy {
    fn can_evict(&self, value: &Entry) -> bool {
        true
    }

    async fn evict(&self, key: String, value: &Entry) {
        // maybe backup the entry contents here
    }
}

let mut cache = LFUCache::new(1, Policy);
cache.insert("key".to_string(), Entry);

if cache.is_full() {
    cache.evict().await;
}
```
