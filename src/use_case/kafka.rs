use std::sync::Arc;
use std::time::Duration;
use bankirpay_lib::{map_err_with_log, trader_proto};
use bankirpay_lib::models::payments::payment::FullPayment;
use bankirpay_lib::models::payments::payment_proto::PaymentProto;
use futures::StreamExt;
use prost::Message;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::Message as RdkMessage;
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{debug, error, warn};
use uuid::Uuid;
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::{InternalServerError, NotFound};
use crate::models;
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
        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    handle_kafka_notification_event(state.clone(), &message).await;
                    let _ = map_err_with_log!(consumer.commit_message(&message, CommitMode::Async),
                        "Error committing message", InternalServerError, false);
                },
                Err(e) => {
                    error!(err=e.to_string(), "Error receiving message");
                    break;
                    // скорее всего ошибка соединения поэтому прерываем цикл while и идем на повторное подключение
                }
            }
        }
    }
}

pub(crate) async fn send_trader_change_balance_request(producer: &FutureProducer, trader_id:String, amount: Decimal,
                                                       balance_action_type: trader_proto::BalanceActionType)
                                                       -> Result<(), PaymentError>
{
    let request = trader_proto::ChangeBalanceRequest{
        trader_id: trader_id.clone(),
        amount: amount.to_f64().ok_or(PaymentError::InvalidAmount)?,
        action_type: balance_action_type.into(),
        idempotent_key: Uuid::now_v7().to_string(),
    };
    let buff = request.encode_to_vec();
    for i in 0..3 {
        if i > 0 {
            warn!("Trader change balance kafka send retry attempt {}", i);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let record = FutureRecord::to("trader_change_balance")
            .key(&trader_id)
            .payload(&buff);
        match producer.send(record, Duration::from_secs(5)).await {
            Ok(f) => {
                debug!("Sent trader change balance kafka {:?}", f);
                return Ok(())
            },
            Err((e, _)) => {
                error!(err=e.to_string(), "Error sending trader change balance kafka");
                continue
            }
        }
    }
    error!("retry count exceeded");
    Err(InternalServerError)
}

pub async fn send_payment_event_to_kafka(producer: &FutureProducer, payment: FullPayment) {
    let id = payment.id.clone();
    let payload = PaymentProto::from(payment).encode_to_vec();
    for i in 0..3 {
        if i > 0 {
            warn!("Kafka retry send attempt {}", i);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let record = FutureRecord::to("PAYMENT_EVENTS")
            .key(&id)
            .payload(&payload);
        match producer.send(record, Duration::from_secs(5)).await {
            Ok(f) => {
                debug!("Sent kafka message {:?}", f);
                break
            },
            Err((e, _)) => {
                warn!(err=e.to_string(), "Error sending kafka message");
                continue
            }
        }
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
                        bankirpay_lib::use_case::kafka::send_kafka_message(&state.kafka_producer, "HANDLE_NOTIFICATION_ERRORS",
                                           notification.id.as_str(), payload).await;
                    }
                }
            },
            Err(e) => {
                error!(err=e.to_string(), "Error decode payload from kafka");
                bankirpay_lib::use_case::kafka::send_kafka_message(&state.kafka_producer, "HANDLE_NOTIFICATION_ERRORS",
                                   Uuid::now_v7().to_string().as_str(), payload).await;
            }
        };
    }
}