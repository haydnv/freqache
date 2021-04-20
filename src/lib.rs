use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::mem;

use async_trait::async_trait;
use futures::{join, FutureExt};
use uplock::RwLock;

#[async_trait]
pub trait Entry: Clone {
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

impl<K: Eq + Hash, V: Entry> LFUCache<K, V> {
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

    pub async fn remove<Q>(&mut self, key: &Q) -> bool
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

            true
        } else {
            false
        }
    }

    pub async fn traverse<F: FnMut(&V) -> () + Send>(&self, mut f: F) {
        let mut next = self.last.clone();
        while let Some(item) = next {
            let lock = item.read().await;
            f(&lock.value);
            next = lock.next.clone();
        }
    }
}

async fn bump<V>(item: &RwLock<Item<V>>) -> (Option<RwLock<Item<V>>>, Option<RwLock<Item<V>>>) {
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

    async fn print_debug<K, V: fmt::Display>(cache: &LFUCache<K, V>) {
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

    async fn validate<K, V: Copy + Eq + fmt::Debug>(cache: &LFUCache<K, V>) {
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

    #[async_trait]
    impl Entry for i32 {
        fn weight(&self) -> u64 {
            2
        }
    }

    #[tokio::test]
    async fn test_order() {
        let mut cache = LFUCache::new(100);
        let expected: Vec<i32> = (0..10).collect();

        for i in expected.iter().rev() {
            cache.insert(i, *i).await;
        }

        let mut actual = Vec::with_capacity(expected.len());
        cache.traverse(|i| actual.push(*i)).await;
        assert_eq!(actual, expected)
    }

    #[tokio::test]
    async fn test_access() {
        let mut cache = LFUCache::new(100);

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
