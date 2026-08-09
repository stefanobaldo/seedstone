pub fn violation() -> [u8; 8] {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).expect("entropy unavailable");
    bytes
}
