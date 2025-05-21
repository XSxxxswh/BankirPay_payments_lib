use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tracing::error;
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::{InternalServerError, InvalidAmount};

pub async fn get_trader_margin_from_redis(conn: &mut MultiplexedConnection, trader_id:&str)
                                          -> Result<Option<Decimal>, PaymentError>
{
    let key = format!("trader:{}:margin", trader_id);
    let margin: Option<f64> = conn.get(key).await.map_err(|e| {
        error!(trader_id=trader_id, err=e.to_string(), "Error getting trader margin from redis");
        InternalServerError
    })?;
    if let Some(margin) = margin {
        return Ok(Decimal::from_f64(margin))
    }
    Ok(None)
}

pub async fn set_trader_margin_to_redis(conn: &mut MultiplexedConnection, trader_id:&str, margin:f64) -> Result<(), PaymentError> {
    let key = format!("trader:{}:margin", trader_id);
    let _ : () = conn.set_ex(key, margin, 300).await.map_err(|e|{
        error!(trader_id=trader_id, err=e.to_string(), "Error setting trader margin to redis");
        InternalServerError
    })?;
    Ok(())
}

