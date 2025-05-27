pub mod models;
pub mod repository;
pub mod errors;
pub mod use_case;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

