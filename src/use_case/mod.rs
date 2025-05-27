use bankirpay_lib::errors::LibError;
use tracing::{error, warn};
use crate::errors::payment_error::PaymentError;

pub mod payment;
pub(crate) mod merchant;
pub(crate) mod trader;
mod requisite;
mod exchange_rate;
pub mod kafka;

fn from_lib_to_pe(err: LibError) -> PaymentError {
    match err {
        LibError::TraderNotFound => PaymentError::NotFound,
        LibError::Forbidden => PaymentError::InternalServerError,
        LibError::Unauthorized => PaymentError::NotFound,
        LibError::InternalError => PaymentError::InternalServerError,
        LibError::MerchantNotFound => PaymentError::NotFound,
        LibError::NotFound => PaymentError::NotFound,
        LibError::NoAvailableRequisites => PaymentError::NoAvailableRequisites,
        LibError::InsufficientFunds => PaymentError::InsufficientFunds,
        LibError::InvalidAmount => PaymentError::InvalidAmount,
        LibError::Conflict => PaymentError::Conflict,
    }
}

pub async fn get_db_conn(pool: &deadpool_postgres::Pool)
-> Result<deadpool_postgres::Object, PaymentError>
{
    let mut attempts = 0;
    loop {
        attempts += 1;
        match pool.get().await {
            Ok(client) => return Ok(client),
            Err(e) if attempts < 3 => {
                warn!(err=e.to_string(), "Error getting db connection. Retrying...");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            Err(e) => {
                error!(err=e.to_string(), "Error getting db connection");
                return Err(PaymentError::InternalServerError)
            },
        }
    }
}

pub async fn get_rdb_conn(pool: &deadpool_redis::Pool)
                         -> Result<deadpool_redis::Connection, PaymentError>
{
    let mut attempts = 0;
    loop {
        attempts += 1;
        match pool.get().await {
            Ok(client) => return Ok(client),
            Err(e) if attempts < 3 => {
                warn!(err=e.to_string(), "Error getting redis connection. Retrying...");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            Err(e) => {
                error!(err=e.to_string(), "Error getting redis connection");
                return Err(PaymentError::InternalServerError)
            },
        }
    }
}
