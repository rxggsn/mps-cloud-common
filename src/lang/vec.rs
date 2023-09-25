pub fn index_for_each<T, F: FnMut(usize, &T)>(iter: &Vec<T>, mut process: F) {
    let mut idx = 0;
    iter.iter().for_each(|elem| {
        process(idx, elem);
        idx += 1;
    })
}
