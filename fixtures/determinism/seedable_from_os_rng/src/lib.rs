use rand::SeedableRng;

pub fn violation() -> rand_chacha::ChaCha8Rng {
    rand_chacha::ChaCha8Rng::from_os_rng()
}
