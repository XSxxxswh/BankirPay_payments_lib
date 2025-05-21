use bankirpay_lib::merchant_proto::PaymentMethod;
use prost::Message;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use tracing::{debug, error};
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::InternalServerError;

pub async fn get_merchant_margin_from_redis(conn: &mut MultiplexedConnection, merchant_id: &str, pm_id: &str)
                                            -> Result<Option<bankirpay_lib::merchant_proto::PaymentMethod>, PaymentError>
{
    debug!(merchant_id=merchant_id, pm_id=pm_id, "Getting payment method from redis");
    let key = format!("mpm:{}:{}", merchant_id, pm_id);
    let res : Vec<u8> = conn.get(key).await.map_err(|e|{
        error!(merchant_id=merchant_id, pm_id=pm_id, err=e.to_string(), "Error getting merchant from redis");
        InternalServerError
    })?;
    if res.is_empty() {
        return Ok(None)
    }
    Ok(Some(PaymentMethod::decode(res.as_slice()).map_err(|e|{
        error!(merchant_id=merchant_id, pm_id=pm_id, err=e.to_string(), "Error deserializing payment method");
        InternalServerError
    })?))
}

pub async fn set_merchant_margin_to_redis(conn: &mut MultiplexedConnection, merchant_id: &str, pm: &PaymentMethod)
-> Result<(), PaymentError>
{
    debug!(merchant_id=merchant_id, pm_id=pm.payment_method_id.as_str(), "Setting payment method to redis");
    let key = format!("mpm:{}:{}", merchant_id, pm.payment_method_id.clone());
    let g = pm.encode_to_vec();
    let _ : () = conn.set_ex(key, g, 300).await.map_err(|e|{
        error!(merchant_id=merchant_id, pm_id=pm.payment_method_id.clone(), err=e.to_string(), "Error setting merchant to redis");
        InternalServerError
    })?;
    Ok(())
}