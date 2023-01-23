//! A hash map ordered by access frequency.
//!
//! Example:
//! ```
//! use freqache::LFUCache;
//!
//! const CACHE_SIZE: usize = 10;
//!
//! let mut cache = LFUCache::new();
//! cache.insert("one", 1);
//! cache.insert("two", 2);
//! // ...
//!
//! for (key, value) in cache.iter() {
//!     println!("{}: {}", key, value);
//! }
//!
//! while cache.len() > CACHE_SIZE {
//!     cache.pop();
//! }
//!
//! ```

use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::sync::Arc;

struct ItemState<K> {
    prev: Option<Arc<K>>,
    next: Option<Arc<K>>,
}

struct Item<K, V> {
    key: Arc<K>,
    value: V,
    state: RefCell<ItemState<K>>,
}

impl<K, V> Item<K, V> {
    #[inline]
    fn state(&self) -> RefMut<ItemState<K>> {
        self.state.borrow_mut()
    }
}

/// A hash set whose keys are ordered by frequency of access
pub struct LFUCache<K, V> {
    cache: HashMap<Arc<K>, Item<K, V>>,
    first: Option<Arc<K>>,
    last: Option<Arc<K>>,
}

impl<K: Eq + Hash, V> LFUCache<K, V> {
    /// Construct a new `LFUCache`.
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            first: None,
            last: None,
        }
    }

    /// Return `true` if the cache contains the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.cache.contains_key(key)
    }

    /// Increase the given `key`'s priority and return `true` if present, otherwise `false`.
    pub fn bump(&mut self, key: &K) -> bool {
        let item = if let Some(item) = self.cache.get(key) {
            item
        } else {
            return false;
        };

        let mut item_state = item.state();

        let last = if item_state.next.is_none() {
            // can't bump the first item
            return true;
        } else if item_state.prev.is_none() && item_state.next.is_some() {
            // bump the last item

            let next_key = item_state.next.as_ref().expect("next key");
            let mut next = self.cache.get::<K>(next_key).expect("next item").state();

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
                .state();

            let next_key = item_state.next.as_ref().expect("next key").clone();
            let mut next = self.cache.get::<K>(&next_key).expect("next item").state();

            mem::swap(&mut prev.next, &mut item_state.next); // set prev.next
            mem::swap(&mut item_state.next, &mut next.next); // set item.next
            mem::swap(&mut next.prev, &mut item_state.prev); // set next.prev

            item_state.prev = Some(next_key); // set item.prev

            None
        };

        let first = if let Some(next_key) = &item_state.next {
            let mut skip = self.cache.get::<K>(next_key).expect("skipped item").state();
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

    /// Add a new value to the cache, and return the old value at `key`, if any.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let old_value = self.remove(&key);

        let key = Arc::new(key);
        let prev = None;
        let mut next = Some(key.clone());
        mem::swap(&mut self.last, &mut next);

        if let Some(next_key) = &next {
            let mut next = self.cache.get::<K>(next_key).expect("next item").state();
            next.prev = Some(key.clone());
        }

        if self.first.is_none() {
            self.first = Some(key.clone());
        }

        let item = Item {
            key: key.clone(),
            value,
            state: RefCell::new(ItemState { prev, next }),
        };

        assert!(self.cache.insert(key, item).is_none());

        old_value
    }

    /// Return `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Iterate over the keys in the cache, beginning with the least-frequently used.
    pub fn iter(&self) -> Iter<K, V> {
        let next = if let Some(key) = &self.last {
            let item = self.cache.get(key).expect("last item");
            Some((key, item))
        } else {
            None
        };

        Iter {
            cache: &self.cache,
            next,
        }
    }

    /// Return the number of entries in this cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    fn remove_inner(&mut self, item: Item<K, V>) -> V {
        let mut item_state = item.state();

        if item_state.prev.is_none() && item_state.next.is_none() {
            // there was only one item and now the cache is empty
            self.last = None;
            self.first = None;
        } else if item_state.prev.is_none() {
            // the last item has been removed
            self.last = item_state.next.clone();

            let next_key = item_state.next.as_ref().expect("next key");
            let mut next = self.cache.get::<K>(next_key).expect("next item").state();

            mem::swap(&mut next.prev, &mut item_state.prev);
        } else if item_state.next.is_none() {
            // the first item has been removed
            self.first = item_state.prev.clone();
            let prev_key = item_state.prev.as_ref().expect("previous key");
            let mut prev = self
                .cache
                .get::<K>(prev_key)
                .expect("previous item")
                .state();

            mem::swap(&mut prev.next, &mut item_state.next);
        } else {
            // an item in the middle has been removed
            let prev_key = item_state.prev.as_ref().expect("previous key");
            let mut prev = self
                .cache
                .get::<K>(prev_key)
                .expect("previous item")
                .state();

            let next_key = item_state.next.as_ref().expect("next key");
            let mut next = self.cache.get::<K>(next_key).expect("next item").state();

            mem::swap(&mut next.prev, &mut item_state.prev);
            mem::swap(&mut prev.next, &mut item_state.next);
        }

        std::mem::drop(item_state);
        item.value
    }

    /// Remove and return the last element in the cache, if any.
    pub fn pop(&mut self) -> Option<V> {
        let last = self.last.as_ref()?;
        let item = self.cache.remove(last).expect("last entry");
        Some(self.remove_inner(item))
    }

    /// Remove the given `key` from the cache and return it, if it was present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let item = self.cache.remove(key)?;
        Some(self.remove_inner(item))
    }
}

pub struct Iter<'a, K, V> {
    cache: &'a HashMap<Arc<K>, Item<K, V>>,
    next: Option<(&'a Arc<K>, &'a Item<K, V>)>,
}

impl<'a, K: Eq + Hash, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let (_key, item) = self.next?;
        let mut next = if let Some(next_key) = &item.state().next {
            self.cache.get_key_value(next_key)
        } else {
            None
        };

        mem::swap(&mut self.next, &mut next);

        if let Some((key, item)) = next {
            Some((&**key, &item.value))
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
    fn print_debug<K: fmt::Display + Eq + Hash, V>(cache: &LFUCache<K, V>) {
        let mut next = cache.last.clone();
        while let Some(next_key) = next {
            let item = cache.cache.get::<K>(&next_key).expect("item").state();

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

    fn validate<K: fmt::Debug + Eq + Hash, V>(cache: &LFUCache<K, V>) {
        if cache.cache.is_empty() {
            assert!(cache.first.is_none(), "first item is {:?}", cache.first);
            assert!(cache.last.is_none(), "last item is {:?}", cache.last);
        } else {
            let first_key = cache.first.as_ref().expect("first key");
            let first = cache.cache.get::<K>(first_key).expect("first item");

            assert!(first.state.borrow().next.is_none());

            let last_key = cache.last.as_ref().expect("last key");
            let last = cache.cache.get::<K>(last_key).expect("last item");
            assert!(last.state.borrow().prev.is_none());
        }

        let mut last = None;
        let mut next = cache.last.clone();
        while let Some(key) = next {
            let item = cache.cache.get::<K>(&key).expect("item");

            if let Some(last_key) = &last {
                let item_state = item.state.borrow();
                let prev_key = item_state.prev.as_ref().expect("previous key");
                assert_eq!(last_key, prev_key);
            }

            last = Some(key);
            next = item.state.borrow().next.clone();
        }
    }

    #[test]
    fn test_order() {
        let mut cache = LFUCache::new();
        let expected: Vec<i32> = (0..10).collect();

        for i in expected.iter().rev() {
            cache.insert(*i, i.to_string());
            validate(&cache);
        }

        let mut actual = Vec::with_capacity(expected.len());
        for (i, s) in cache.iter() {
            assert_eq!(&i.to_string(), s);
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
            cache.insert(i, i.to_string());
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
