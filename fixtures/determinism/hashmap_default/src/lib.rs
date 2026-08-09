/// Builds a map on the default hasher and reads its keys back in iteration
/// order — the order the operating system's hasher seed happens to produce.
pub fn violation() -> Vec<u32> {
    let map: std::collections::HashMap<u32, &'static str> =
        [(1, "a"), (2, "b"), (3, "c")].into_iter().collect();
    map.keys().copied().collect()
}
