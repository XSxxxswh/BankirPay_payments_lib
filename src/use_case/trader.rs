use std::sync::Arc;
use redis::aio::MultiplexedConnection;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tracing::{debug, error, warn};
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::InternalServerError;
use crate::{models, repository};
use crate::use_case::from_lib_to_pe;

pub async fn get_trader_margin(conn: &mut MultiplexedConnection, mut trader_api: bankirpay_lib::services::traders::trader_service::TraderService, trader_id: &str)
                               -> Result<Decimal, PaymentError>
{

    let margin = match repository::trader::get_trader_margin_from_redis(conn, trader_id).await {
        Ok(Some(margin)) => margin,
        Ok(None) => {
            warn!(trader_id=trader_id, "trader margin not found in redis");
            let margin_raw = trader_api.get_trader_margin(trader_id.to_string()).await.map_err(from_lib_to_pe)?.margin;
            let _ = repository::trader::set_trader_margin_to_redis(conn, trader_id, margin_raw).await;
            Decimal::from_f64(margin_raw).ok_or(InternalServerError)?
        },
        Err(_e) => {
            let margin_raw = trader_api.get_trader_margin(trader_id.to_string()).await.map_err(from_lib_to_pe)?.margin;
            Decimal::from_f64(margin_raw).ok_or(InternalServerError)?
        }
    };
    Ok(margin)
}