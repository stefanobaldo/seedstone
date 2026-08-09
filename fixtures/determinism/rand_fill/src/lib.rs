pub fn violation() -> [u8; 8] {
    let mut bytes = [0u8; 8];
    rand::fill(&mut bytes[..]);
    bytes
}
