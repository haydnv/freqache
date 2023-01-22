//! A thread-safe set which provides an `Iterator` ordered by access frequency.
//!
//! Example:
//! ```
//! use freqache::LFUCache;
//!
//! const CACHE_SIZE: usize = 10;
//!
//! let mut cache = LFUCache::new();
//! cache.insert("key1");
//! cache.insert("key2");
//! cache.insert("key3");
//! cache.insert("key2");
//! // ...
//!
//! for key in cache.iter() {
//!     println!("key: {}", key);
//! }
//!
//! while cache.len() > CACHE_SIZE {
//!     cache.pop();
//! }
//!
//! ```

use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{fmt, mem};

struct ItemState<K> {
    prev: Option<Arc<K>>,
    next: Option<Arc<K>>,
}

struct Item<K> {
    key: Arc<K>,
    state: Arc<RwLock<ItemState<K>>>,
}

impl<K> Item<K> {
    fn read(&self) -> RwLockReadGuard<ItemState<K>> {
        self.state.read().expect("item")
    }

    fn write(&self) -> RwLockWriteGuard<ItemState<K>> {
        self.state.write().expect("item")
    }
}

impl<K: PartialEq> PartialEq<K> for Item<K> {
    fn eq(&self, other: &K) -> bool {
        self.key.deref() == other
    }
}

impl<K: PartialEq> PartialEq<Item<K>> for Item<K> {
    fn eq(&self, other: &Item<K>) -> bool {
        self.key == other.key
    }
}

impl<K: PartialEq> Eq for Item<K> {}

impl<K> Borrow<K> for Item<K> {
    fn borrow(&self) -> &K {
        self.key.deref()
    }
}

impl<K: Hash> Hash for Item<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state)
    }
}

struct CacheState<K> {
    cache: HashSet<Item<K>>,
    first: Option<Arc<K>>,
    last: Option<Arc<K>>,
}

impl<K: Eq + Hash> CacheState<K> {
    fn bump(&mut self, key: &K) -> bool {
        let item = if let Some(item) = self.cache.get(key) {
            item
        } else {
            return false;
        };

        let mut item_state = item.write();

        let last = if item_state.next.is_none() {
            // can't bump the first item
            return true;
        } else if item_state.prev.is_none() && item_state.next.is_some() {
            // bump the last item

            let next_key = item_state.next.as_ref().expect("next key");
            let mut next = self.cache.get::<K>(next_key).expect("next item").write();

            mem::swap(&mut next.prev, &mut item_state.prev); // set next.prev
            mem::swap(&mut item_state.next, &mut next.next); // set item.next
            mem::swap(&mut item_state.prev, &mut next.next); // set item.prev & next.next

            item_state.prev.clone()
        } else {
            // bump an item in the middle

            let prev_key = item_state.prev.as_ref().expect("previous key");
            let mut prev = self
                .cache
                .get::<K>(prev_key)
                .expect("previous item")
                .write();

            let next_key = item_state.next.as_ref().expect("next key").clone();
            let mut next = self.cache.get::<K>(&next_key).expect("next item").write();

            mem::swap(&mut prev.next, &mut item_state.next); // set prev.next
            mem::swap(&mut item_state.next, &mut next.next); // set item.next
            mem::swap(&mut next.prev, &mut item_state.prev); // set next.prev

            item_state.prev = Some(next_key); // set item.prev

            None
        };

        let first = if let Some(next_key) = &item_state.next {
            let mut skip = self.cache.get::<K>(next_key).expect("skipped item").write();

            skip.prev = Some(item.key.clone());
            None
        } else {
            Some(item.key.clone())
        };

        match (last, first) {
            (Some(last), None) => self.last = Some(last),
            (None, Some(first)) => self.first = Some(first),
            (Some(last), Some(first)) => {
                self.last = Some(last);
                self.first = Some(first);
            }
            (None, None) => {}
        };

        true
    }

    fn insert(&mut self, key: K) {
        let key = Arc::new(key);
        let prev = None;
        let mut next = Some(key.clone());
        mem::swap(&mut self.last, &mut next);

        if let Some(next_key) = &next {
            let mut next = self.cache.get::<K>(next_key).expect("next item").write();
            next.prev = Some(key.clone());
        }

        if self.first.is_none() {
            self.first = Some(key.clone());
        }

        let item = Item {
            key: key.clone(),
            state: Arc::new(RwLock::new(ItemState { prev, next })),
        };

        assert!(self.cache.insert(item));
    }

    fn pop(&mut self) -> Option<Arc<K>> {
        let last = self.last.as_ref()?;
        let item = self.cache.take::<K>(last).expect("last entry");
        Some(self.remove_inner(item))
    }

    fn remove_inner(&mut self, item: Item<K>) -> Arc<K> {
        let mut item_state = item.write();

        if item_state.prev.is_none() && item_state.next.is_none() {
            // there was only one item and now the cache is empty
            self.last = None;
            self.first = None;
        } else if item_state.prev.is_none() {
            // the last item has been removed
            self.last = item_state.next.clone();

            let next_key = item_state.next.as_ref().expect("next key");
            let mut next = self.cache.get::<K>(next_key).expect("next item").write();

            mem::swap(&mut next.prev, &mut item_state.prev);
        } else if item_state.next.is_none() {
            // the first item has been removed
            self.first = item_state.prev.clone();
            let prev_key = item_state.prev.as_ref().expect("previous key");
            let mut prev = self
                .cache
                .get::<K>(prev_key)
                .expect("previous item")
                .write();

            mem::swap(&mut prev.next, &mut item_state.next);
        } else {
            // an item in the middle has been removed
            let prev_key = item_state.prev.as_ref().expect("previous key");
            let mut prev = self
                .cache
                .get::<K>(prev_key)
                .expect("previous item")
                .write();

            let next_key = item_state.next.as_ref().expect("next key");
            let mut next = self.cache.get::<K>(next_key).expect("next item").write();

            mem::swap(&mut next.prev, &mut item_state.prev);
            mem::swap(&mut prev.next, &mut item_state.next);
        }

        mem::drop(item_state);
        item.key
    }

    fn remove(&mut self, key: &K) -> Option<Arc<K>> {
        if let Some(item) = self.cache.take(key) {
            Some(self.remove_inner(item))
        } else {
            None
        }
    }
}

/// A weighted, thread-safe least-frequently-used cache
pub struct LFUCache<K> {
    state: RwLock<CacheState<K>>,
}

impl<K: Eq + Hash> LFUCache<K> {
    /// Construct a new `LFUCache`.
    pub fn new() -> Self {
        let state = CacheState {
            cache: HashSet::new(),
            first: None,
            last: None,
        };

        Self {
            state: RwLock::new(state),
        }
    }

    /// Return `true` if the cache contains the given key.
    pub fn contains(&self, key: &K) -> bool {
        let state = self.state.read().expect("LFU read lock");
        state.cache.contains(key)
    }

    /// Increase the given `key`'s priority and return `true` if present, otherwise `false`.
    pub fn bump<Q>(&self, key: &K) -> bool {
        let mut state = self.state.write().expect("LFU read lock");
        state.bump(key)
    }

    /// Add a new key to the cache and return `true` is it was already present, otherwise `false`.
    ///
    /// If already present, the key's priority is increased by one.
    pub fn insert(&self, key: K) -> bool {
        let mut state = self.state.write().expect("LFU write lock");

        if state.bump(&key) {
            true
        } else {
            state.insert(key);
            false
        }
    }

    /// Return `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.state.read().expect("LFU cache state").cache.is_empty()
    }

    /// Iterate over the keys in the cache, beginning with the least-frequently used.
    ///
    /// Calling `insert` or `remove` while iterating will result in a deadlock.
    pub fn iter(&self) -> Iter<K> {
        let state = self.state.read().expect("LFU cache");
        Iter::new(state)
    }

    /// Return the number of entries in this cache.
    pub fn len(&self) -> usize {
        self.state.read().expect("LFU cache state").cache.len()
    }
}

impl<K: Eq + Hash + fmt::Debug> LFUCache<K> {
    /// Remove and return the last element in the cache, if any.
    pub fn pop(&self) -> Option<K> {
        let mut state = self.state.write().expect("LFU cache state");
        let key = state.pop()?;
        let key = Arc::try_unwrap(key).expect("key");
        Some(key)
    }

    /// Remove the given `key` from the cache and return it, if it was present.
    pub fn remove(&self, key: &K) -> Option<K> {
        let mut state = self.state.write().expect("LFU cache state");
        let key = state.remove(key)?;
        let key = Arc::try_unwrap(key).expect("key");
        Some(key)
    }
}

pub struct Iter<'a, K> {
    state: RwLockReadGuard<'a, CacheState<K>>,
    current: Option<Arc<K>>,
}

impl<'a, K: Eq + Hash> Iter<'a, K> {
    fn new(state: RwLockReadGuard<'a, CacheState<K>>) -> Self {
        let current = state.last.clone();
        Self { state, current }
    }
}

impl<'a, K: Eq + Hash> Iterator for Iter<'a, K> {
    type Item = Arc<K>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current.as_ref() {
            let item = self.state.cache.get::<K>(current).expect("next item");
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
    fn print_debug<K: fmt::Display + Eq + Hash>(cache: &LFUCache<K>) {
        let state = cache.state.read().expect("LFU cache state");

        let mut next = state.last.clone();
        while let Some(next_key) = next {
            let item = state.cache.get::<K>(&next_key).expect("item").read();

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

    fn validate<K: fmt::Debug + Eq + Hash>(cache: &LFUCache<K>) {
        let state = cache.state.read().expect("LFU cache state");

        if state.cache.is_empty() {
            assert!(state.first.is_none(), "first item is {:?}", state.first);
            assert!(state.last.is_none(), "last item is {:?}", state.last);
        } else {
            let first_key = state.first.as_ref().expect("first key");
            let first = state.cache.get::<K>(first_key).expect("first item").read();

            assert!(first.next.is_none());

            let last_key = state.last.as_ref().expect("last key");
            let last = state.cache.get::<K>(last_key).expect("last item").read();
            assert!(last.prev.is_none());
        }

        let mut last = None;
        let mut next = state.last.clone();
        while let Some(key) = next {
            let item = state.cache.get::<K>(&key).expect("item").read();

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
            cache.insert(*i);
            validate(&cache);
        }

        let mut actual = Vec::with_capacity(expected.len());
        for i in cache.iter() {
            actual.push(i);
        }

        assert_eq!(actual.len(), expected.len());
        assert!(actual.iter().zip(expected).all(|(l, r)| **l == r))
    }

    #[test]
    fn test_access() {
        let mut cache = LFUCache::new();
        validate(&mut cache);

        let mut rng = thread_rng();
        for _ in 1..100_000 {
            let i: i32 = rng.gen_range(0..1000);
            cache.insert(i);
            validate(&mut cache);

            let mut size = 0;
            for _ in cache.iter() {
                size += 1;
            }

            assert_eq!(cache.len(), size);
            assert!(!cache.is_empty());

            let i: i32 = rng.gen_range(0..1000);
            cache.remove(&i);
            validate(&mut cache);

            let mut size = 0;
            for _ in cache.iter() {
                size += 1;
            }

            while !cache.is_empty() {
                cache.pop();
                validate(&mut cache);
                size -= 1;
                assert_eq!(cache.len(), size);
            }

            assert_eq!(cache.len(), 0);
        }
    }
}
