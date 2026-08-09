use std::mem::MaybeUninit;

pub fn violation() {
    let mut bytes = [MaybeUninit::<u8>::uninit(); 8];
    getrandom::fill_uninit(&mut bytes).expect("entropy unavailable");
}
