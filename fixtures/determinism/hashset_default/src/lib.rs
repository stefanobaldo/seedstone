/// Builds a set on the default hasher and reads it back in iteration order —
/// the order the operating system's hasher seed happens to produce.
pub fn violation() -> Vec<u32> {
    let set: std::collections::HashSet<u32> = [1, 2, 3].into_iter().collect();
    set.into_iter().collect()
}
