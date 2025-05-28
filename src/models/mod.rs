use rdkafka::producer::FutureProducer;
use redis::aio::MultiplexedConnection;
use tokio_postgres::Row;
use uuid::Uuid;
use crate::repository::requisite::release_lock;



#[derive(Clone)]
pub struct State {
    pub pool: deadpool_postgres::Pool,
    pub rdb: deadpool_redis::Pool,
    pub trader_api: bankirpay_lib::services::traders::trader_service::TraderService,
    pub merchant_api: bankirpay_lib::services::merchants::merchant_service::MerchantService,
    pub requisite_api: bankirpay_lib::services::requisites::requisite_service::RequisitesService,
    pub exchange_api: bankirpay_lib::services::exchange::exchange_service::ExchangeService,
    pub kafka_producer: FutureProducer
}

pub struct LockGuard {
    conn: MultiplexedConnection,
    requisite_id: String,
    released: bool,
}

impl LockGuard {
    pub fn new(conn: &mut MultiplexedConnection, requisite_id: &str) -> Self {
        LockGuard {
            conn: conn.clone(),
            requisite_id: requisite_id.to_string(),
            released: false,
        }
    }

    pub async fn release(mut self) {
        let _ = release_lock(&mut self.conn, self.requisite_id.as_str()).await;
        self.released = true;
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.released {
            let mut conn = self.conn.clone();
            let id = self.requisite_id.clone();
            tokio::spawn(async move {
                let _ = release_lock(&mut conn, id.as_str()).await;
            });
        }
    }
}

#[derive(Debug)]
pub struct OutboxMessage {
    pub id: Uuid,
    pub topic: String,
    pub payload: Vec<u8>,
    pub aggregate_id: String,
}

impl From<&Row> for OutboxMessage {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("id"),
            topic: row.get("topic"),
            payload: row.get("payload"),
            aggregate_id: row.get("aggregate_id"),
        }
    }
}