use rand::SeedableRng;

pub fn violation() -> rand_chacha::ChaCha8Rng {
    rand_chacha::ChaCha8Rng::try_from_os_rng().expect("entropy unavailable")
}
