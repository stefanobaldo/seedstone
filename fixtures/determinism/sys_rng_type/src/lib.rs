use rand::rngs::SysRng;
use rand_core::TryRng;

pub fn violation() -> u32 {
    let mut source = SysRng;
    source.try_next_u32().expect("entropy unavailable")
}
