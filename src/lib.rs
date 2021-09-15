//! A weighted, futures-aware least-frequently-used cache.
//!
//! [`LFUCache`] by itself is not thread-safe; for thread safety you can use a `tokio::sync::Mutex`.
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
//! #[derive(Clone, Debug, Eq, PartialEq)]
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
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;

use async_trait::async_trait;

/// An [`LFUCache`] entry
pub trait Entry {
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

struct Inner<K> {
    prev: Option<K>,
    next: Option<K>,
}

struct Item<K, V> {
    value: V,
    inner: RefCell<Inner<K>>,
}

impl<K, V> Deref for Item<K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        &self.value
    }
}

/// A weighted, thread-safe, futures-aware least-frequently-used cache
pub struct LFUCache<K, V, P> {
    cache: HashMap<K, Item<K, V>>,
    first: Option<K>,
    last: Option<K>,
    occupied: u64,
    capacity: u64,
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
            capacity,
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
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if self.cache.contains_key(key) {
            let (last, first) = self.bump(key.clone());

            if last.is_some() {
                self.last = last;
            }

            if first.is_some() {
                self.first = first;
            }

            self.cache.get(key).map(|item| item.deref())
        } else {
            None
        }
    }

    /// Add a new entry to the cache.
    pub fn insert(&mut self, key: K, value: V) -> bool {
        let present = if self.cache.contains_key(&key) {
            {
                let mut item = self.cache.get_mut(&key).expect("item");
                item.value = value;
            }

            let (last, first) = self.bump(key);

            if last.is_some() {
                self.last = last;
            }

            if first.is_some() {
                self.first = first;
            }

            true
        } else {
            let prev = None;
            let mut next = Some(key.clone());
            mem::swap(&mut self.last, &mut next);

            self.occupied += value.weight();

            if let Some(next_key) = &next {
                let mut next = self
                    .cache
                    .get(next_key)
                    .expect("next item")
                    .inner
                    .borrow_mut();

                next.prev = Some(key.clone());
            }

            if self.first.is_none() {
                self.first = Some(key.clone());
            }

            let inner = RefCell::new(Inner { prev, next });
            let item = Item { value, inner };
            self.cache.insert(key, item);

            false
        };

        present
    }

    /// Return the unoccupied capacity of this cache.
    pub fn available(&self) -> u64 {
        if self.capacity > self.occupied {
            self.capacity - self.occupied
        } else {
            0
        }
    }

    /// Return the capacity of this cache.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Return `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Return `true` if the cache is full.
    pub fn is_full(&self) -> bool {
        self.occupied >= self.capacity
    }

    /// Return the number of entries in this cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Return the currently occupied capacity of this cache.
    pub fn occupied(&self) -> u64 {
        self.occupied
    }

    /// Remove an entry from the cache, and clone and return its value if present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(item) = self.cache.remove(key) {
            let mut inner = item.inner.borrow_mut();

            if inner.prev.is_none() && inner.next.is_none() {
                // there was only one item and now the cache is empty
                self.last = None;
                self.first = None;
            } else if inner.prev.is_none() {
                // the last item has been removed
                self.last = inner.next.clone();
                let next_key = inner.next.as_ref().expect("next key");
                let mut next = self
                    .cache
                    .get(next_key)
                    .expect("next item")
                    .inner
                    .borrow_mut();

                mem::swap(&mut next.prev, &mut inner.prev);
            } else if inner.next.is_none() {
                // the first item has been removed
                self.first = inner.prev.clone();
                let prev_key = inner.prev.as_ref().expect("previous key");
                let mut prev = self
                    .cache
                    .get(prev_key)
                    .expect("previous item")
                    .inner
                    .borrow_mut();

                mem::swap(&mut prev.next, &mut inner.next);
            } else {
                // an item in the middle has been removed
                let prev_key = inner.prev.as_ref().expect("previous key");
                let mut prev = self
                    .cache
                    .get(prev_key)
                    .expect("previous item")
                    .inner
                    .borrow_mut();

                let next_key = inner.next.as_ref().expect("next key");
                let mut next = self
                    .cache
                    .get(next_key)
                    .expect("next item")
                    .inner
                    .borrow_mut();

                mem::swap(&mut next.prev, &mut inner.prev);
                mem::swap(&mut prev.next, &mut inner.next);
            }

            if self.occupied > item.value.weight() {
                self.occupied -= item.value.weight();
            } else {
                self.occupied = 0;
            }

            Some(item.value)
        } else {
            None
        }
    }

    /// Traverse the cache values beginning with the least-frequent.
    pub fn traverse<C: FnMut(&V) -> () + Send>(&self, mut f: C) {
        let mut next = self.last.clone();
        while let Some(key) = next {
            let item = self.cache.get(&key).expect("next item");
            f(&item.value);
            next = item.inner.borrow().next.clone();
        }
    }

    /// Traverse the cache entries beginning with the least-frequent, and evict entries from the
    /// cache according to this its [`Policy`].
    pub async fn evict(&mut self) {
        let mut current_key = self.last.clone();
        while let Some(item_key) = current_key {
            {
                let item = self.cache.get(&item_key).expect("next item");
                if !self.policy.can_evict(&item.value) {
                    current_key = item.inner.borrow().next.clone();
                    continue;
                }
            }

            let (item_key_tmp, item) = self.cache.remove_entry(&item_key).expect("cache key");

            self.policy.evict(item_key_tmp, &item.value).await;

            let value = item.value;
            let mut item = item.inner.borrow_mut();
            let next_key = item.next.clone();

            if let Some(prev_key) = &item.prev {
                let mut prev = self
                    .cache
                    .get(prev_key)
                    .expect("previous item")
                    .inner
                    .borrow_mut();

                mem::swap(&mut item.next, &mut prev.next);
            } else {
                self.last = item.next.clone();
            }

            if let Some(next_key) = &next_key {
                let mut next = self
                    .cache
                    .get(next_key)
                    .expect("next item")
                    .inner
                    .borrow_mut();

                mem::swap(&mut item.prev, &mut next.prev);
            } else {
                self.first = item.prev.clone();
            }

            if self.occupied > value.weight() {
                self.occupied -= value.weight();
            } else {
                self.occupied = 0;
            }

            if self.is_full() {
                current_key = next_key;
            } else {
                break;
            }
        }
    }

    fn bump(&mut self, key: K) -> (Option<K>, Option<K>) {
        let mut item = if let Some(item) = self.cache.get(&key) {
            item.inner.borrow_mut()
        } else {
            return (None, None);
        };

        let last = if item.next.is_none() {
            // can't bump the first item
            return (None, None);
        } else if item.prev.is_none() && item.next.is_some() {
            // bump the last item

            let next_key = item.next.as_ref().expect("next key");
            let mut next = self
                .cache
                .get(next_key)
                .expect("next item")
                .inner
                .borrow_mut();

            mem::swap(&mut next.prev, &mut item.prev); // set next.prev
            mem::swap(&mut item.next, &mut next.next); // set item.next
            mem::swap(&mut item.prev, &mut next.next); // set item.prev & next.next

            item.prev.clone()
        } else {
            // bump an item in the middle

            let prev_key = item.prev.as_ref().expect("previous key");
            let mut prev = self
                .cache
                .get(prev_key)
                .expect("previous item")
                .inner
                .borrow_mut();

            let next_key = item.next.as_ref().expect("next key").clone();
            let mut next = self
                .cache
                .get(&next_key)
                .expect("next item")
                .inner
                .borrow_mut();

            mem::swap(&mut prev.next, &mut item.next); // set prev.next
            mem::swap(&mut item.next, &mut next.next); // set item.next
            mem::swap(&mut next.prev, &mut item.prev); // set next.prev

            item.prev = Some(next_key); // set item.prev

            None
        };

        let first = if let Some(next_key) = &item.next {
            let skip = self.cache.get(next_key).expect("skipped item");
            let mut inner = skip.inner.borrow_mut();
            inner.prev = Some(key);
            None
        } else {
            Some(key)
        };

        (last, first)
    }
}
#[cfg(test)]
mod tests {
    use std::fmt;

    use rand::{thread_rng, Rng};

    use super::*;

    #[allow(dead_code)]
    async fn print_debug<K: Clone + Eq + Hash, V: fmt::Display, P>(cache: &LFUCache<K, V, P>) {
        let mut next = cache.last.clone();
        while let Some(next_key) = next {
            let item = cache.cache.get(&next_key).expect("item");
            let inner = item.inner.borrow();

            if let Some(prev_key) = inner.prev.as_ref() {
                let prev = cache.cache.get(prev_key).expect("previous item");
                print!("{}-", prev.value);
            }

            print!("{}", item.value);

            next = inner.next.clone();
            if let Some(next_key) = &next {
                let next = cache.cache.get(next_key).expect("next item");
                print!("-{}", next.value);
            }

            print!(" ");
        }

        println!();
    }

    fn validate<K: fmt::Debug + Clone + Eq + Hash, V: Entry, P: Policy<K, V>>(
        cache: &LFUCache<K, V, P>,
    ) {
        if cache.is_empty() {
            assert!(cache.first.is_none());
            assert!(cache.last.is_none());
        } else {
            let first_key = cache.first.as_ref().expect("first key");
            let first = cache
                .cache
                .get(first_key)
                .expect("first item")
                .inner
                .borrow();
            assert!(first.next.is_none());

            let last_key = cache.last.as_ref().expect("last key");
            let last = cache.cache.get(last_key).expect("last item").inner.borrow();
            assert!(last.prev.is_none());
        }

        let mut last = None;
        let mut next = cache.last.clone();
        while let Some(key) = next {
            let item = cache.cache.get(&key).expect("item").inner.borrow();

            if let Some(last_key) = &last {
                let prev_key = item.prev.as_ref().expect("previous key");
                assert_eq!(last_key, prev_key);
            }

            last = Some(key);
            next = item.next.clone();
        }
    }

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

    #[tokio::test]
    async fn test_order() {
        let mut cache = LFUCache::new(100, Evict);
        let expected: Vec<i32> = (0..10).collect();

        for i in expected.iter().rev() {
            cache.insert(*i, *i);
        }

        let mut actual = Vec::with_capacity(expected.len());
        cache.traverse(|i| actual.push(*i));

        assert_eq!(actual, expected)
    }

    #[tokio::test]
    async fn test_access() {
        let mut cache = LFUCache::new(100, Evict);

        let mut rng = thread_rng();
        for _ in 0..100_000 {
            let i: i32 = rng.gen_range(0..10);
            cache.insert(i, i);
            validate(&mut cache);

            let i: i32 = rng.gen_range(0..10);
            cache.remove(&i);
            validate(&mut cache);

            if rng.gen_bool(0.01) {
                if cache.is_full() {
                    cache.evict().await;
                }

                assert!(!cache.is_full());
                validate(&mut cache);
            }

            let mut size = 0;
            cache.traverse(|_| size += 1);

            assert_eq!(cache.len(), size);
        }
    }
}
