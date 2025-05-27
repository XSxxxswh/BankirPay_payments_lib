use redis::aio::MultiplexedConnection;
use rust_decimal::Decimal;
use tokio::time::Instant;
use tracing::{warn};
use crate::errors::payment_error::PaymentError;
use crate::{repository};
use crate::models::{LockGuard};
use crate::repository::requisite::{release_lock, set_amount_unavailable};
pub async fn check_amount_available(client: &tokio_postgres::Client, conn: &mut MultiplexedConnection, requisite_id: &str, amount: Decimal, limit: i32)
                                    -> Result<Option<LockGuard>, PaymentError>
{
    let start = Instant::now();
   if repository::requisite::lock_and_check_amount(conn, requisite_id, amount).await? {
       if repository::requisite::check_amount_is_available_in_db(client, requisite_id,amount ,limit).await? {
           if start.elapsed().as_millis() >= 10 {
               warn!("SLOW CHECK DB {:?}", start.elapsed());
           }
           return Ok(Some(LockGuard::new(conn, requisite_id)));
       }
      let _ = set_amount_unavailable(conn, requisite_id, amount).await;
       let _ = release_lock(conn, requisite_id).await;
   }
    if start.elapsed().as_millis() >= 10 {
        warn!("SLOW CHECK {:?}", start.elapsed());
    }
    Ok(None)
}


