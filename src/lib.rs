use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;
use std::ops::Deref;

use async_trait::async_trait;
use futures::{join, FutureExt};
use uplock::{RwLock, RwLockReadGuard};

#[async_trait]
pub trait Entry {
    fn weight(&self) -> u64;
}

struct Item<V> {
    value: V,
    prev: Option<RwLock<Self>>,
    next: Option<RwLock<Self>>,
}

struct LFUCache<K, V> {
    cache: HashMap<K, RwLock<Item<V>>>,
    first: Option<RwLock<Item<V>>>,
    last: Option<RwLock<Item<V>>>,
    capacity: u64,
}

impl<K: Eq + Hash, V: Clone> LFUCache<K, V> {
    pub fn new(capacity: u64) -> Self {
        Self {
            cache: HashMap::new(),
            first: None,
            last: None,
            capacity,
        }
    }

    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.cache.contains_key(key)
    }

    pub async fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        if let Some(item) = self.cache.get(key) {
            if bump(item).await {
                self.first = Some(item.clone());
            }

            Some(item.read().map(|lock| lock.value.clone()).await)
        } else {
            None
        }
    }

    pub async fn insert(&mut self, key: K, value: V) -> bool {
        if let Some(item) = self.cache.get(&key) {
            if bump(item).await {
                self.first = Some(item.clone());
            }

            true
        } else {
            let mut last = None;
            mem::swap(&mut self.last, &mut last);

            let item = RwLock::new(Item {
                value,
                prev: None,
                next: last,
            });

            self.cache.insert(key, item.clone());
            self.last = Some(item);
            false
        }
    }

    // TODO: could this return a Stream of &V?
    pub async fn values(&self) -> Vec<V> {
        let mut keys = Vec::with_capacity(self.cache.len());

        let mut next = self.last.clone();
        while let Some(item) = next {
            let lock = item.read().await;
            keys.push(lock.deref().value.clone());
            next = lock.next.clone();
        }

        keys
    }
}

async fn bump<V>(item: &RwLock<Item<V>>) -> bool {
    let mut item_lock = item.write().await;

    if item_lock.next.is_none() {
        false
    } else if item_lock.prev.is_none() && item_lock.next.is_some() {
        let mut next_lock = item_lock.next.as_ref().unwrap().write().await;

        mem::swap(&mut next_lock.prev, &mut item_lock.prev); // set next.prev
        mem::swap(&mut item_lock.next, &mut next_lock.next); // set item.next

        next_lock.next = Some(item.clone()); // set next.next
        item_lock.prev = Some(item_lock.next.as_ref().unwrap().clone()); // set item.prev

        item_lock.next.is_none()
    } else {
        let (mut prev_lock, mut next_lock) = join!(
            item_lock.prev.as_ref().unwrap().write(),
            item_lock.next.as_ref().unwrap().write()
        );

        mem::swap(&mut prev_lock.next, &mut item_lock.next); // set prev.next
        mem::swap(&mut next_lock.prev, &mut item_lock.prev); // set next.prev
        mem::swap(&mut item_lock.next, &mut next_lock.next); // set item.next

        next_lock.next = Some(item.clone()); // set next.next
        item_lock.prev = Some(item_lock.next.as_ref().unwrap().clone()); // set item.prev

        item_lock.next.is_none()
    }
}
