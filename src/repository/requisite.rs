use bankirpay_lib::repository::is_connection_err;
use bankirpay_lib::retry;
use bigdecimal::num_traits::abs;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands};
use rust_decimal::{dec, Decimal};
use tokio_postgres::types::Type;
use tracing::{debug, error, warn};
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::InternalServerError;
use std::time::Duration;

pub async fn check_amount_is_available_in_db(client: &tokio_postgres::Client, requisite_id: & str, amount: Decimal, limit: i32)
                                             -> Result<bool, PaymentError>
{
    debug!(requisite_id=requisite_id,"checking for available amount");
    let amounts = retry!(client.query_typed(
        "SELECT fiat_amount
            FROM payments
            WHERE requisite_id = $1 AND payment_side = 'BUY' AND status IN ('UNPAID', 'PAID')",
        &[(&requisite_id, Type::VARCHAR)]
    ), 3).map_err(|e| {
        error!(requisite_id=requisite_id, amount=amount.to_string(), err=e, "get amounts error");
        InternalServerError
    })?;
    if amounts.len() >= limit as usize {
        return Ok(false)
    }
    let amounts = amounts.iter().map(|row| row.get::<_, Decimal>(0)).collect::<Vec<Decimal>>();
    for p_amount in &amounts {
        if abs(amount - p_amount) <= amount * dec!(0.001) + dec!(5)  {
            return Ok(false)
        }
    }
    debug!(requisite_id=requisite_id, "AMOUNT is available");
    Ok(true)
}



pub async fn try_lock(conn: &mut MultiplexedConnection, requisite_id : &str) -> Result<bool, PaymentError> {
    let key = format!("lock:{}", requisite_id);
    let result: Option<String> = retry!(redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(5)
        .query_async(conn), 3)
        .map_err(|e| {
            error!(requisite_id=requisite_id,"Failed to lock requisite checking: {}", e);
            InternalServerError
        })?;

    if result.as_deref() == Some("OK") {
        debug!(requisite_id=requisite_id,"lock requisite OK");
        Ok(true)
    } else {
        Ok(false)
    }
}

pub async fn release_lock(conn: &mut MultiplexedConnection, requisite_id: &str) -> Result<(), PaymentError> {
    let key = format!("lock:{}", requisite_id);
    let _ : () =  retry!(conn.del(key.as_str()),3).map_err(|e| {
        error!(requisite_id=requisite_id,"Failed to release requisite state: {}", e);
        InternalServerError
    })?;
    debug!(requisite_id=requisite_id,"Release requisite state OK");
    Ok(())
}

pub async fn check_amount_available(conn: &mut MultiplexedConnection, requisite_id: &str, amount: Decimal)
-> Result<bool, PaymentError>
{
    let key = format!("unavailable:{}:{}", requisite_id, amount);
    let unavailable: bool = conn.exists(key.as_str()).await.map_err(|e| {
        error!(requisite_id=requisite_id,err=e.to_string(),"Failed to check unavailable state");
        InternalServerError
    })?;
    Ok(!unavailable)
}

pub async fn set_amount_unavailable(conn: &mut MultiplexedConnection,requisite_id: &str ,amount: Decimal)
-> Result<(), PaymentError>
{
    let key = format!("unavailable:{}:{}", requisite_id, amount);
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(5)
        .query_async(conn)
        .await
        .map_err(|e| {
            error!(requisite_id=requisite_id,"Failed to lock requisite checking: {}", e);
            InternalServerError
        })?;
    Ok(())
}

pub async fn lock_and_check_amount(conn: &mut MultiplexedConnection, requisite_id: &str, amount: Decimal)
-> Result<bool, PaymentError>
{
    let key = format!("lock:{}", requisite_id);
    let amount_key = format!("unavailable:{}:{}", requisite_id, amount);
    let result: [Option<bool>; 2] = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(5)
        .exists(amount_key.as_str())
        .query_async(conn)
        .await
        .map_err(|e| {
            error!(requisite_id=requisite_id,"Failed to lock requisite checking: {}", e);
            InternalServerError
        })?;
    if let Some(val)  = result[0] {
        if val {
            if let Some(unavailable) = result[1] {
                if !unavailable {
                    return Ok(true)
                }
            }
        }
    }
    Ok(false)
}