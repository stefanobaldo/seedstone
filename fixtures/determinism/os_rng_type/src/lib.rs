use rand_core::OsRng;
use rand_core::TryRngCore;

pub fn violation() -> u32 {
    let mut source = OsRng;
    source.try_next_u32().expect("entropy unavailable")
}
