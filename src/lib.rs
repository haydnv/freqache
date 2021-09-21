//! A thread-safe least-frequently-used cache which provides an `Iterator`.
//!
//! Example:
//! ```
//! use freqache::LFUCache;
//!
//! let mut cache = LFUCache::new();
//! cache.insert("key1");
//! cache.insert("key2");
//! cache.insert("key3");
//! cache.insert("key2");
//!
//! for key in cache.iter() {
//!     println!("key: {}", key);
//! }
//! ```

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

struct Inner<K> {
    prev: Option<K>,
    next: Option<K>,
}

struct Item<K> {
    inner: RwLock<Inner<K>>,
}

impl<K> Item<K> {
    fn read(&self) -> RwLockReadGuard<Inner<K>> {
        self.inner.read().expect("item")
    }

    fn write(&self) -> RwLockWriteGuard<Inner<K>> {
        self.inner.write().expect("item")
    }
}

pub struct State<K> {
    cache: HashMap<K, Item<K>>,
    first: Option<K>,
    last: Option<K>,
}

impl<K: Clone + Eq + Hash> State<K> {
    fn bump(&mut self, key: K) {
        let mut item = if let Some(item) = self.cache.get(&key) {
            item.write()
        } else {
            return;
        };

        let last = if item.next.is_none() {
            // can't bump the first item
            return;
        } else if item.prev.is_none() && item.next.is_some() {
            // bump the last item

            let next_key = item.next.as_ref().expect("next key");
            let mut next = self.cache.get(next_key).expect("next item").write();

            mem::swap(&mut next.prev, &mut item.prev); // set next.prev
            mem::swap(&mut item.next, &mut next.next); // set item.next
            mem::swap(&mut item.prev, &mut next.next); // set item.prev & next.next

            item.prev.clone()
        } else {
            // bump an item in the middle

            let prev_key = item.prev.as_ref().expect("previous key");
            let mut prev = self.cache.get(prev_key).expect("previous item").write();

            let next_key = item.next.as_ref().expect("next key").clone();
            let mut next = self.cache.get(&next_key).expect("next item").write();

            mem::swap(&mut prev.next, &mut item.next); // set prev.next
            mem::swap(&mut item.next, &mut next.next); // set item.next
            mem::swap(&mut next.prev, &mut item.prev); // set next.prev

            item.prev = Some(next_key); // set item.prev

            None
        };

        let first = if let Some(next_key) = &item.next {
            let mut skip = self.cache.get(next_key).expect("skipped item").write();
            skip.prev = Some(key);
            None
        } else {
            Some(key)
        };

        match (last, first) {
            (Some(last), None) => self.last = Some(last),
            (None, Some(first)) => self.first = Some(first),
            (Some(last), Some(first)) => {
                self.last = Some(last);
                self.first = Some(first);
            }
            (None, None) => {}
        }
    }
}

/// A weighted, thread-safe least-frequently-used cache
pub struct LFUCache<K> {
    state: RwLock<State<K>>,
}

impl<K: Clone + Eq + Hash> LFUCache<K> {
    /// Construct a new `LFUCache`.
    pub fn new() -> Self {
        let state = State {
            cache: HashMap::new(),
            first: None,
            last: None,
        };

        Self {
            state: RwLock::new(state),
        }
    }

    /// Return `true` if the cache contains the given key.
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let state = self.state.read().expect("LFU read lock");
        state.cache.contains_key(key)
    }

    /// Add a new key to the cache and return `true` is it was already present, otherwise `false`.
    ///
    /// If already present, the key's priority is increased by one.
    pub fn insert(&self, key: K) -> bool {
        let mut state = self.state.write().expect("LFU write lock");

        if state.cache.contains_key(&key) {
            state.bump(key);
            true
        } else {
            let prev = None;
            let mut next = Some(key.clone());
            mem::swap(&mut state.last, &mut next);

            if let Some(next_key) = &next {
                let mut next = state.cache.get(next_key).expect("next item").write();

                next.prev = Some(key.clone());
            }

            if state.first.is_none() {
                state.first = Some(key.clone());
            }

            let item = Item {
                inner: RwLock::new(Inner { prev, next }),
            };
            state.cache.insert(key, item);

            false
        }
    }

    /// Return `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.state.read().expect("LFU cache state").cache.is_empty()
    }

    /// Return the number of entries in this cache.
    pub fn len(&self) -> usize {
        self.state.read().expect("LFU cache state").cache.len()
    }

    /// Remove an entry from the cache and return `true` if it was present or `false` if not.
    pub fn remove(&self, key: &K) -> bool {
        let mut state = self.state.write().expect("LFU cache state");

        if let Some(item) = state.cache.remove(key.borrow()) {
            let mut inner = item.write();

            if inner.prev.is_none() && inner.next.is_none() {
                // there was only one item and now the cache is empty
                state.last = None;
                state.first = None;
            } else if inner.prev.is_none() {
                // the last item has been removed
                state.last = inner.next.clone();

                let next_key = inner.next.as_ref().expect("next key");
                let mut next = state.cache.get(&*next_key).expect("next item").write();

                mem::swap(&mut next.prev, &mut inner.prev);
            } else if inner.next.is_none() {
                // the first item has been removed
                state.first = inner.prev.clone();
                let prev_key = inner.prev.as_ref().expect("previous key");
                let mut prev = state.cache.get(prev_key).expect("previous item").write();

                mem::swap(&mut prev.next, &mut inner.next);
            } else {
                // an item in the middle has been removed
                let prev_key = inner.prev.as_ref().expect("previous key");
                let mut prev = state.cache.get(prev_key).expect("previous item").write();

                let next_key = inner.next.as_ref().expect("next key");
                let mut next = state.cache.get(&*next_key).expect("next item").write();

                mem::swap(&mut next.prev, &mut inner.prev);
                mem::swap(&mut prev.next, &mut inner.next);
            }

            true
        } else {
            false
        }
    }

    /// Iterate over the keys in the cache, beginning with the least-frequently used.
    ///
    /// Calling `insert` or `remove` while iterating will result in a deadlock.
    pub fn iter(&self) -> Iter<K> {
        let state = self.state.read().expect("LFU cache");
        let current = state.last.clone();
        Iter { state, current }
    }
}

pub struct Iter<'a, K> {
    state: RwLockReadGuard<'a, State<K>>,
    current: Option<K>,
}

impl<'a, K: Clone + Eq + Hash> Iterator for Iter<'a, K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(key) = &mut self.current {
            let item = self.state.cache.get(key).expect("LFU cache item");
            let mut next = item.read().next.clone();
            mem::swap(&mut self.current, &mut next);
            next
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use rand::{thread_rng, Rng};

    use super::*;

    #[allow(dead_code)]
    fn print_debug<K: fmt::Display + Clone + Eq + Hash>(cache: &LFUCache<K>) {
        let state = cache.state.read().expect("LFU cache state");

        let mut next = state.last.clone();
        while let Some(next_key) = next {
            let item = state.cache.get(&next_key).expect("item").read();

            if let Some(prev_key) = item.prev.as_ref() {
                print!("{}-", prev_key);
            }

            next = item.next.clone();
            if let Some(next_key) = &next {
                print!("-{}", next_key);
            }

            print!(" ");
        }

        println!();
    }

    fn validate<K: fmt::Debug + Clone + Eq + Hash>(cache: &LFUCache<K>) {
        let state = cache.state.read().expect("LFU cache state");

        if state.cache.is_empty() {
            assert!(state.first.is_none());
            assert!(state.last.is_none());
        } else {
            let first_key = state.first.as_ref().expect("first key");
            let first = state.cache.get(first_key).expect("first item").read();

            assert!(first.next.is_none());

            let last_key = state.last.as_ref().expect("last key");
            let last = state.cache.get(last_key).expect("last item").read();
            assert!(last.prev.is_none());
        }

        let mut last = None;
        let mut next = state.last.clone();
        while let Some(key) = next {
            let item = state.cache.get(&key).expect("item").read();

            if let Some(last_key) = &last {
                let prev_key = item.prev.as_ref().expect("previous key");
                assert_eq!(last_key, prev_key);
            }

            last = Some(key);
            next = item.next.clone();
        }
    }

    #[test]
    fn test_order() {
        let cache = LFUCache::new();
        let expected: Vec<i32> = (0..10).collect();

        for i in expected.iter().rev() {
            cache.insert(i);
        }

        let mut actual = Vec::with_capacity(expected.len());
        for i in cache.iter() {
            actual.push(*i);
        }

        assert_eq!(actual, expected)
    }

    #[test]
    fn test_access() {
        let mut cache = LFUCache::new();

        let mut rng = thread_rng();
        for _ in 0..100_000 {
            let i: i32 = rng.gen_range(0..10);
            cache.insert(i);
            validate(&mut cache);

            let i: i32 = rng.gen_range(0..10);
            cache.remove(&i);
            validate(&mut cache);

            let mut size = 0;
            for _ in cache.iter() {
                size += 1;
            }

            assert_eq!(cache.len(), size);
        }
    }
}
