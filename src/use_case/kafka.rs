use bankirpay_lib::repository::is_connection_err;
use std::sync::Arc;
use std::time::Duration;
use bankirpay_lib::{map_err_with_log, retry, trader_proto};
use bankirpay_lib::models::payments::payment::FullPayment;
use bankirpay_lib::models::payments::payment_proto::PaymentProto;
use futures::StreamExt;
use prost::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message as RdkMessage;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;
use tracing::{error, warn};
use uuid::Uuid;
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::{InternalServerError, NotFound};
use crate::{models, repository};
use crate::models::OutboxMessage;
use crate::use_case::get_db_conn;
use crate::use_case::payment::{close_payment_by_notification};

pub async fn kafka_worker_start(consumer: StreamConsumer, state: Arc<models::State>)
{
    loop {
        if let Err(e) = consumer.subscribe(&["BANK_EVENTS"]) {
            error!(err=e.to_string(), "Error subscribing to kafka events");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        let mut stream = consumer.stream();
        loop {
            match tokio::time::timeout(Duration::from_secs(60), stream.next()).await {
                Ok(Some(Ok(message))) => {
                    handle_kafka_notification_event(state.clone(), &message).await;
                    let _ = map_err_with_log!(
                        consumer.commit_message(&message, CommitMode::Async),
                        "Kafka commit error", InternalServerError, false
                    );
                }
                Ok(Some(Err(e))) => {
                    error!(err = %e, "Kafka stream error");
                    break; // переподключение
                }
                Ok(None) => {
                    warn!("Kafka stream ended");
                    break;
                }
                Err(_) => {
                    warn!("Kafka stream timeout after 60s");
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub(crate) async fn send_trader_change_balance_request(state: Arc<models::State>, trader_id:String, amount: Decimal,
                                                       balance_action_type: trader_proto::BalanceActionType)
                                                       -> Result<(), PaymentError>
{
    let request = trader_proto::ChangeBalanceRequest{
        trader_id: trader_id.clone(),
        amount: amount.to_f64().ok_or(PaymentError::InvalidAmount)?,
        action_type: balance_action_type.into(),
        idempotent_key: Uuid::now_v7().to_string(),
    };
    let payload = request.encode_to_vec();
    let client = map_err_with_log!(get_db_conn(&state.pool).await, "Error get DB connection", InternalServerError, false)?;
    if repository::payment::add_new_kafka_message(&client, "trader_change_balance", 
                                                          &payload, trader_id.as_str()).await.is_err() {
        error!(trader_id=trader_id, amount=?amount, action_type=?balance_action_type,
            "Kafka send trader balance request error retry count exceeded");
        return Err(InternalServerError)
    }
    Ok(())
}

pub async fn send_payment_event_to_kafka(producer: &FutureProducer, payment: FullPayment) {
    let id = payment.id.clone();
    let payload = PaymentProto::from(payment).encode_to_vec();
    if bankirpay_lib::use_case::kafka::send_kafka_message(producer, "PAYMENT_EVENTS", id.as_str(),  payload.as_slice()).await.is_err() 
    {
        error!(payment_id=id, "Kafka error send event to kafka");
    }
}

async fn handle_kafka_notification_event(state: Arc<models::State>, message: &BorrowedMessage<'_>)
{
    if let Some(payload) = message.payload() {
        match bankirpay_lib::device_proto::Notification::decode(payload) {
            Ok(notification) => {
                if let Err(e) = close_payment_by_notification(state.clone(), &notification).await {
                    if e == NotFound {
                        warn!(bank_id=notification.id, "payment by notification not found");
                    }else {
                        error!(err=e.to_string(), "Error closing payment");
                        let _ = bankirpay_lib::use_case::kafka::send_kafka_message(&state.kafka_producer, "HANDLE_NOTIFICATION_ERRORS",
                                           notification.id.as_str(), payload).await;
                    }
                }
            },
            Err(e) => {
                error!(err=e.to_string(), "Error decode payload from kafka");
                let _ = bankirpay_lib::use_case::kafka::send_kafka_message(&state.kafka_producer, "HANDLE_NOTIFICATION_ERRORS",
                                   Uuid::now_v7().to_string().as_str(), payload).await;
            }
        };
    }
}


pub async fn process_outbox_messages(
    pool: &deadpool_postgres::Pool,
    producer: &rdkafka::producer::FutureProducer,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut client = match get_db_conn(pool).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to get DB connection: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let tx = match client.transaction().await {
            Ok(tx) => tx,
            Err(e) => {
                error!("Failed to start DB transaction: {}", e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        let rows = match tx
            .query_typed(
                "SELECT id, topic, payload, aggregate_id FROM outbox_messages WHERE processed_at IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED",
                &[],
            )
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to query outbox messages: {}", e);
                let _ = tx.rollback().await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        if rows.is_empty() {
            if let Err(e) = tx.commit().await {
                error!("Failed to commit empty transaction: {}", e);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        for row in &rows {
            let msg = OutboxMessage::from(row);

            match bankirpay_lib::use_case::kafka::send_kafka_message(
                producer,
                msg.topic.as_str(),
                msg.aggregate_id.as_str(),
                msg.payload.as_slice(),
            )
                .await
            {
                Ok(_) => {
                    let _ = retry!(tx.query_typed(
                        "UPDATE outbox_messages SET processed_at = NOW() WHERE id = $1",
                        &[(&msg.id, Type::UUID)]
                    ), 3)
                        .map_err(|e| {
                            error!("Failed to update processed_at for id {}: {}", msg.id, e);
                        });
                }
                Err(_) => {
                    warn!("Failed to send message id {} to Kafka", msg.id);
                }
            }
        }

        if let Err(e) = tx.commit().await {
            error!("Failed to commit transaction: {}", e);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}