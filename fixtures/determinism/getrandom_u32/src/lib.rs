pub fn violation() -> u32 {
    getrandom::u32().expect("entropy unavailable")
}
