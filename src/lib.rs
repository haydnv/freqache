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

pub struct LFUCache<K, V> {
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
            let (last, first) = bump(item).await;

            if last.is_some() {
                self.last = last;
            }

            if first.is_some() {
                self.first = first;
            }

            Some(item.read().map(|lock| lock.value.clone()).await)
        } else {
            None
        }
    }

    pub async fn insert(&mut self, key: K, value: V) -> bool {
        if let Some(item) = self.cache.get(&key) {
            let (last, first) = bump(item).await;

            if last.is_some() {
                self.last = last;
            }

            if first.is_some() {
                self.first = first;
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

            if let Some(next) = &item.write().await.next {
                next.write().await.prev = Some(item.clone());
            }

            self.cache.insert(key, item.clone());
            self.last = Some(item);
            false
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
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

async fn bump<V>(item: &RwLock<Item<V>>) -> (Option<RwLock<Item<V>>>, Option<RwLock<Item<V>>>) {
    let mut item_lock = item.write().await;

    if item_lock.next.is_none() {
        (None, None) // nothing to update
    } else if item_lock.prev.is_none() && item_lock.next.is_some() {
        let mut next_lock = item_lock.next.as_ref().unwrap().write().await;

        mem::swap(&mut next_lock.prev, &mut item_lock.prev); // set next.prev
        mem::swap(&mut item_lock.next, &mut next_lock.next); // set item.next
        mem::swap(&mut item_lock.prev, &mut next_lock.next); // set item.prev & next.next

        let first = if item_lock.next.is_none() {
            Some(item.clone())
        } else {
            None
        };

        (item_lock.prev.clone(), first)
    } else {
        let (mut prev_lock, mut next_lock) = join!(
            item_lock.prev.as_ref().unwrap().write(),
            item_lock.next.as_ref().unwrap().write()
        );

        let next = item_lock.next.clone();

        mem::swap(&mut prev_lock.next, &mut item_lock.next); // set prev.next
        mem::swap(&mut item_lock.next, &mut next_lock.next); // set item.next
        mem::swap(&mut next_lock.prev, &mut item_lock.prev); // set next.prev

        item_lock.prev = next;

        let first = if item_lock.next.is_none() {
            Some(item.clone())
        } else {
            None
        };

        (None, first)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};

    #[tokio::test]
    async fn test_order() {
        let mut cache = LFUCache::new(100);
        let values: Vec<i32> = (0..10).collect();

        for i in values.iter().rev() {
            cache.insert(i, *i).await;
        }

        assert_eq!(cache.values().await, values)
    }

    #[tokio::test]
    async fn test_access() {
        let mut cache = LFUCache::new(100);

        let mut rng = thread_rng();
        for _ in 0..100_000 {
            let i: i32 = rng.gen_range(10..20);
            println!("insert {}", i);
            cache.insert(i, i).await;

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
            assert_eq!(cache.len(), cache.values().await.len());
        }
    }
}
