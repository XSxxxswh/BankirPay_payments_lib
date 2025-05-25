pub mod models;
pub mod repository;
pub mod errors;
pub mod use_case;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use bankirpay_lib::models::payments::requests::GetPaymentsRequest;
    use tokio_postgres::NoTls;
    use super::*;

    #[test]
     fn it_works() {
        assert_eq!(2 + 2, 4);
     }
}
