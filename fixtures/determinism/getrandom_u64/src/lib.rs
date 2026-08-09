pub fn violation() -> u64 {
    getrandom::u64().expect("entropy unavailable")
}
