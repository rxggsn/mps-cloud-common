use std::collections::HashMap;

pub fn index_for_each<T, F: FnMut(usize, &T)>(iter: &Vec<T>, mut process: F) {
    let mut idx = 0;
    iter.iter().for_each(|elem| {
        process(idx, elem);
        idx += 1;
    })
}

pub fn group<T, F: Fn(&T) -> K, K: Eq + std::hash::Hash>(
    iter: &Vec<T>,
    key_fn: F,
) -> HashMap<K, Vec<&T>> {
    let mut map = HashMap::new();
    iter.iter().for_each(|elem| {
        let key = key_fn(elem);
        map.entry(key).or_insert_with(Vec::new).push(elem);
    });
    map
}

pub fn map_self_mut<T, F: Fn(&mut T) -> K, K: Eq + std::hash::Hash>(
    iter: &mut Vec<T>,
    key_fn: F,
) -> HashMap<K, &mut T> {
    let mut map = HashMap::new();
    iter.iter_mut().for_each(|elem| {
        let key = key_fn(elem);
        map.insert(key, elem);
    });
    map
}

pub fn map_self<T, F: Fn(&T) -> K, K: Eq + std::hash::Hash>(
    iter: &Vec<T>,
    key_fn: F,
) -> HashMap<K, &T> {
    let mut map = HashMap::new();
    iter.iter().for_each(|elem| {
        let key = key_fn(elem);
        map.insert(key, elem);
    });
    map
}
