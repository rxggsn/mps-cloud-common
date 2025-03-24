use crossbeam_skiplist::{map::Entry, SkipMap};

pub struct LocalConcurrentCache<K, V> {
    cache: SkipMap<K, V>,
}

impl<K, V> LocalConcurrentCache<K, V> {
    pub fn new() -> Self {
        Self {
            cache: SkipMap::new(),
        }
    }
}

impl<K, V> LocalConcurrentCache<K, V>
where
    K: Ord + Send + 'static,
    V: Send + 'static,
{
    pub fn insert(&self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    pub fn get<'a>(&'a self, key: &K) -> Option<Entry<'a, K, V>> {
        self.cache.get(key)
    }
}
