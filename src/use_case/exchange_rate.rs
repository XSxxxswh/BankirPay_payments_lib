use std::sync::Arc;
use futures::future::err;
use rust_decimal::Decimal;
use tracing::error;
use crate::errors::payment_error::PaymentError;
use crate::{models, repository};

pub async fn get_exchange_rate(state: Arc<models::State>)
-> Result<Decimal, PaymentError>
{
    let mut conn = match state.rdb.get().await { 
        Ok(conn) => conn,
        Err(e) => {
            error!(err=e.to_string(),"Error get redis connection");
            return state.exchange_api.clone().get_exchange_rate().await.map_err(PaymentError::from);
        }
    };
    match repository::exchange_rate::get_exchange_rate_from_redis(&mut conn).await { 
        Ok(Some(rate)) => Ok(rate),
        _ => state.exchange_api.clone().get_exchange_rate().await.map_err(PaymentError::from),
    }
}