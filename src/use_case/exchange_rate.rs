use std::sync::Arc;
use rust_decimal::Decimal;
use tracing::error;
use crate::errors::payment_error::PaymentError;
use crate::{models, repository};
use crate::use_case::get_rdb_conn;

pub async fn get_exchange_rate(state: Arc<models::State>)
                               -> Result<Decimal, PaymentError>
{
    let mut conn = match get_rdb_conn(&state.rdb).await { 
        Ok(conn) => conn,
        Err(_) => {
            error!("Error get redis connection");
            return state.exchange_api.clone().get_exchange_rate().await.map_err(PaymentError::from);
        }
    };
    match repository::exchange_rate::get_exchange_rate_from_redis(&mut conn).await { 
        Ok(Some(rate)) => Ok(rate),
        Err(e) => {
            error!(err=e.to_string(),"Error getting exchange rate from redis");
            state.exchange_api.clone().get_exchange_rate().await.map_err(PaymentError::from)
        }
        _ => state.exchange_api.clone().get_exchange_rate().await.map_err(PaymentError::from),
    }
}