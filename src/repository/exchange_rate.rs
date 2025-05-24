use tracing::error;
use bankirpay_lib::map_err_with_log;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::InternalServerError;

pub async fn get_exchange_rate_from_redis(conn: &mut MultiplexedConnection)
                                          ->Result<Option<Decimal>, PaymentError>
{
    let i = 1;
    let rate: Option<f64> = map_err_with_log!(conn.get("exchange_rate").await, 
        "Error get exchange rate from redis", 
        InternalServerError, i)?;
    Ok(rate.map(|rate| Decimal::from_f64(rate).unwrap_or_default()))
    
}