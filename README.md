# freqache
A weighted, thread-safe, futures-aware Rust LFU cache which supports a custom eviction policy.

Example:
```rust
use std::time::Duration;
use futures::executor::block_on;
use freqache::{Entry, LFUCache};

#[derive(Clone)]
struct Item;

impl Entry for Item {
    fn weight() -> u64 {
        1
    }
}

struct Evict;

let (tx, rx) = std::sync::mpsc::channel();
let mut cache = LFUCache::new(100, || { tx.send(Evict); });
cache.insert("key", Item);

while rx.recv_timeout(Duration::default()).is_ok() {
    cache.evict(|key, value| {
        // maybe backup the contents of the entry here
        futures::future::ready(Result::<bool, String>::Ok(true))
    });
}
```
