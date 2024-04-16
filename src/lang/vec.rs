use std::collections::BTreeMap;

pub fn index_for_each<T, F: FnMut(usize, &T)>(iter: &Vec<T>, mut process: F) {
    let mut idx = 0;
    iter.iter().for_each(|elem| {
        process(idx, elem);
        idx += 1;
    })
}

pub fn group<K: Ord, V, F: FnMut(&V) -> K>(iter: &Vec<V>, mut key: F) -> BTreeMap<K, Vec<&V>> {
    let mut map = BTreeMap::new();
    iter.iter().for_each(|elem| {
        let k = key(elem);
        let v = map.entry(k).or_insert(Vec::new());
        v.push(elem);
    });
    map
}

pub fn group_mut<K: Ord, V, F: FnMut(&mut V) -> K>(
    iter: &mut Vec<V>,
    mut key: F,
) -> BTreeMap<K, Vec<&mut V>> {
    let mut map = BTreeMap::new();
    iter.iter_mut().for_each(|elem| {
        let k = key(elem);
        let v = map.entry(k).or_insert(Vec::new());
        v.push(elem);
    });
    map
}
