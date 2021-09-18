# freqache
A thread-safe Rust LFU cache which supports iteration.

Example:
```rust
use freqache::LFUCache;

let mut cache = LFUCache::new();
cache.insert("key1");
cache.insert("key2");
cache.insert("key3");
cache.insert("key2");

for key in cache.iter() {
    println!("key: {}", key);
}
```
