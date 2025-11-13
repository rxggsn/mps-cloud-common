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

pub fn group_mut<T, F: Fn(&T) -> K, K: Eq + std::hash::Hash>(
    iter: &mut Vec<T>,
    key_fn: F,
) -> HashMap<K, Vec<&mut T>> {
    let mut map: HashMap<K, Vec<&mut T>> = HashMap::new();
    iter.iter_mut().for_each(|elem| {
        let key = key_fn(elem);
        if let Some(vec) = map.get_mut(&key) {
            vec.push(elem);
        } else {
            map.insert(key, vec![elem]);
        }
    });
    map
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_vec_tuples_serde() {
        let v = vec![(1u32, "a".to_string()), (2u32, "b".to_string())];
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, r#"[[1,"a"],[2,"b"]]"#);
        let d: Vec<(u32, String)> = serde_json::from_str(&s).unwrap();
        assert_eq!(d, v);
    }
}
