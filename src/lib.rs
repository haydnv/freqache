//! A weighted, thread-safe, futures-aware least-frequently-used cache.
//!
//! [`LFUCache`] does not provide a default eviction policy, but a callback and traversal method
//! which allow the developer to implement their own.
//!
//! Example:
//! ```
//! # use async_trait::async_trait;
//! # use futures::executor::block_on;
//! use freqache::LFUCache;
//!
//! #[derive(Clone)]
//! struct Entry;
//!
//! impl freqache::Entry for Entry {
//!     fn weight(&self) -> u64 {
//!         1
//!     }
//! }
//!
//! struct Policy;
//!
//! #[async_trait]
//! impl freqache::Policy<String, Entry> for Policy {
//!     fn can_evict(&self, value: &Entry) -> bool {
//!         true
//!     }
//!
//!     async fn evict(&self, key: String, value: &Entry) {
//!         // maybe backup the entry contents here
//!     }
//! }
//!
//! let mut cache = LFUCache::new(1, Policy);
//! cache.insert("key".to_string(), Entry);
//!
//! if cache.is_full() {
//!     block_on(cache.evict());
//! }
//! ```

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;

use async_trait::async_trait;
use futures::join;
use uplock::RwLock;

/// An [`LFUCache`] entry
pub trait Entry: Clone {
    /// The weight of this item in the cache.
    /// This value must be stable over the lifetime of the item.
    fn weight(&self) -> u64;
}

/// A cache eviction policy.
#[async_trait]
pub trait Policy<K, V>: Sized + Send {
    fn can_evict(&self, value: &V) -> bool;

    async fn evict(&self, key: K, value: &V);
}

struct Item<K, V> {
    key: K,
    value: V,
    prev: Option<RwLock<Self>>,
    next: Option<RwLock<Self>>,
}

impl<K, V> Deref for Item<K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        &self.value
    }
}

/// A weighted, thread-safe, futures-aware least-frequently-used cache
pub struct LFUCache<K, V, P> {
    cache: HashMap<K, RwLock<Item<K, V>>>,
    first: Option<RwLock<Item<K, V>>>,
    last: Option<RwLock<Item<K, V>>>,
    occupied: i64,
    capacity: i64,
    policy: P,
}

impl<K: Clone + Eq + Hash, V: Entry, P: Policy<K, V>> LFUCache<K, V, P> {
    /// Construct a new `LFUCache`.
    pub fn new(capacity: u64, policy: P) -> Self {
        Self {
            cache: HashMap::new(),
            first: None,
            last: None,
            occupied: 0,
            capacity: capacity as i64,
            policy,
        }
    }

    /// Return `true` if the cache contains the given key.
    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.cache.contains_key(key)
    }

    /// Borrow the value of the cache entry with the given key.
    pub async fn get<Q: ?Sized>(
        &mut self,
        key: &Q,
    ) -> Option<impl Deref<Target = impl Deref<Target = V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(item) = self.cache.get(key) {
            let (last, first) = bump(item).await;

            if last.is_some() {
                self.last = last;
            }

            if first.is_some() {
                self.first = first;
            }

            Some(item.read().await)
        } else {
            None
        }
    }

    /// Add a new entry to the cache.
    pub async fn insert(&mut self, key: K, value: V) -> bool {
        if let Some(item) = self.cache.get(&key) {
            let (last, first) = bump(item).await;

            if last.is_some() {
                self.last = last;
            }

            if first.is_some() {
                self.first = first;
            }

            let mut lock = item.write().await;
            lock.value = value;

            true
        } else {
            let mut last = None;
            mem::swap(&mut self.last, &mut last);

            self.occupied += value.weight() as i64;

            let item = RwLock::new(Item {
                key: key.clone(),
                value,
                prev: None,
                next: last,
            });

            if let Some(next) = &item.write().await.next {
                next.write().await.prev = Some(item.clone());
            }

            self.cache.insert(key, item.clone());
            self.last = Some(item);
            false
        }
    }

    /// Return `true` if the cache is full.
    pub fn is_full(&self) -> bool {
        self.occupied >= self.capacity
    }

    /// Return the number of entries in this cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Remove an entry from the cache, and clone and return its value if present.
    pub async fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(item) = self.cache.remove(key) {
            let mut item_lock = item.write().await;

            if item_lock.prev.is_none() && item_lock.next.is_none() {
                self.last = None;
                self.first = None;
            } else if item_lock.prev.is_none() {
                self.last = item_lock.next.clone();

                let mut next = item_lock.next.as_ref().unwrap().write().await;
                mem::swap(&mut next.prev, &mut item_lock.prev);
            } else if item_lock.next.is_none() {
                self.first = item_lock.prev.clone();

                let mut prev = item_lock.prev.as_ref().unwrap().write().await;
                mem::swap(&mut prev.next, &mut item_lock.next);
            } else {
                let (mut prev, mut next) = join!(
                    item_lock.prev.as_ref().unwrap().write(),
                    item_lock.next.as_ref().unwrap().write()
                );

                mem::swap(&mut next.prev, &mut item_lock.prev);
                mem::swap(&mut prev.next, &mut item_lock.next);
            }

            self.occupied -= item_lock.value.weight() as i64;

            Some(item_lock.value.clone())
        } else {
            None
        }
    }

    /// Traverse the cache values beginning with the least-frequent.
    pub async fn traverse<C: FnMut(&V) -> () + Send>(&self, mut f: C) {
        let mut next = self.last.clone();
        while let Some(item) = next {
            let lock = item.read().await;
            f(&lock.value);
            next = lock.next.clone();
        }
    }

    /// Traverse the cache values beginning with the least-frequent, and evict values from the
    /// cache according to this its [`Policy`].
    pub async fn evict(&mut self) {
        let mut next = self.last.clone();
        while let Some(item) = next {
            let lock = item.read().await;
            next = lock.next.clone();

            if self.policy.can_evict(&lock.value) {
                let (key, value) = self.cache.remove_entry(&lock.key).unwrap();
                let lock = value.read().await;
                self.policy.evict(key, &lock.value).await;
            }

            if !self.is_full() {
                break;
            }
        }
    }
}

async fn bump<K, V>(
    item: &RwLock<Item<K, V>>,
) -> (Option<RwLock<Item<K, V>>>, Option<RwLock<Item<K, V>>>) {
    let mut item_lock = item.write().await;

    let last = if item_lock.next.is_none() {
        return (None, None); // nothing to update
    } else if item_lock.prev.is_none() && item_lock.next.is_some() {
        let mut next_lock = item_lock.next.as_ref().unwrap().write().await;

        mem::swap(&mut next_lock.prev, &mut item_lock.prev); // set next.prev
        mem::swap(&mut item_lock.next, &mut next_lock.next); // set item.next
        mem::swap(&mut item_lock.prev, &mut next_lock.next); // set item.prev & next.next

        item_lock.prev.clone()
    } else {
        let (mut prev_lock, mut next_lock) = join!(
            item_lock.prev.as_ref().unwrap().write(),
            item_lock.next.as_ref().unwrap().write()
        );

        let next = item_lock.next.clone();

        mem::swap(&mut prev_lock.next, &mut item_lock.next); // set prev.next
        mem::swap(&mut item_lock.next, &mut next_lock.next); // set item.next
        mem::swap(&mut next_lock.prev, &mut item_lock.prev); // set next.prev

        item_lock.prev = next; // set item.prev

        None
    };

    let first = if item_lock.next.is_some() {
        let mut skip_lock = item_lock.next.as_ref().unwrap().write().await;
        skip_lock.prev = Some(item.clone());
        None
    } else {
        Some(item.clone())
    };

    (last, first)
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use rand::{thread_rng, Rng};

    use super::*;

    impl Entry for i32 {
        fn weight(&self) -> u64 {
            2
        }
    }

    struct Evict;

    #[async_trait]
    impl Policy<i32, i32> for Evict {
        fn can_evict(&self, _value: &i32) -> bool {
            true
        }

        async fn evict(&self, _key: i32, _value: &i32) {
            // no-op
        }
    }

    async fn print_debug<K, V: fmt::Display, P>(cache: &LFUCache<K, V, P>) {
        let mut next = cache.last.clone();
        while let Some(item) = next {
            let lock = item.read().await;

            if let Some(item) = lock.prev.as_ref() {
                print!("{}-", item.read().await.value);
            }

            print!("{}", lock.value);

            next = lock.next.clone();
            if let Some(item) = &next {
                print!("-{}", item.read().await.value);
            }

            print!(" ");
        }

        println!();
    }

    async fn validate<K, V: Copy + Eq + fmt::Debug, P>(cache: &LFUCache<K, V, P>) {
        let mut last = None;
        let mut next = cache.last.clone();
        while let Some(item) = next {
            let lock = item.read().await;

            if let Some(last) = last {
                assert_eq!(lock.prev.as_ref().unwrap().read().await.value, last);
            }

            last = Some(lock.value);
            next = lock.next.clone();
        }
    }

    #[tokio::test]
    async fn test_order() {
        let mut cache = LFUCache::new(100, Evict);
        let expected: Vec<i32> = (0..10).collect();

        for i in expected.iter().rev() {
            cache.insert(*i, *i).await;
        }

        let mut actual = Vec::with_capacity(expected.len());
        cache.traverse(|i| actual.push(*i)).await;

        assert_eq!(actual, expected)
    }

    #[tokio::test]
    async fn test_access() {
        let mut cache = LFUCache::new(100, Evict);

        let mut rng = thread_rng();
        for _ in 0..100_000 {
            let i: i32 = rng.gen_range(0..10);
            println!("insert {}", i);
            cache.insert(i, i).await;

            print_debug(&cache).await;
            validate(&cache).await;

            let mut size = 0;
            cache.traverse(|_| size += 1).await;

            assert_eq!(cache.len(), size);

            let i: i32 = rng.gen_range(0..10);
            println!("remove {}", i);
            cache.remove(&i).await;

            print_debug(&cache).await;
            validate(&cache).await;
            assert!(!cache.contains_key(&i));

            let mut size = 0;
            cache.traverse(|_| size += 1).await;

            assert_eq!(cache.len(), size);
        }
    }
}
