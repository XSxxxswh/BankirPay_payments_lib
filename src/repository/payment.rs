use bankirpay_lib::{retry};
use bankirpay_lib::repository::is_connection_err;
use bankirpay_lib::device_proto::notification::Notification;
use bankirpay_lib::{map_err_with_log};
use bankirpay_lib::models::payments::payment::{FullPayment, ToSQL};
use bankirpay_lib::models::payments::requests::GetPaymentsRequest;
use rust_decimal::Decimal;
use tokio_postgres::{Client, Transaction};
use tokio_postgres::types::{ToSql, Type};
use tonic::codegen::tokio_stream::StreamExt;
use tracing::{debug, error, warn};
use crate::errors::payment_error::PaymentError;
use crate::errors::payment_error::PaymentError::{InternalServerError, NotFound};
use rust_decimal::prelude::FromPrimitive;
use simd_json::prelude::ArrayTrait;
use std::time::Duration;

pub async fn insert_payment_to_db(client: &Client, payment: &FullPayment) -> Result<(), PaymentError>{
    debug!(payment_id=%payment.id, "Inserting new payment to DB");
    let start = std::time::Instant::now();
    let _result = retry!(client.query_typed(
        "INSERT INTO payments (
         id, external_id, merchant_id, client_id,
         trader_id, requisite_id, bank_id,
         status, payment_side, currency,
         target_amount, fiat_amount, crypto_amount,
         trader_crypto_amount, exchange_rate, fee_type,
         crypto_fee, fiat_fee, trader_crypto_fee,
         trader_fiat_fee, holder_name,
         holder_account, method, margin, trader_margin,
         earnings, created_at, updated_at, deadline, bank_name, last_four, card_last_four)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
         $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32)",
        &[(&payment.id, Type::VARCHAR), (&payment.external_id, Type::VARCHAR), (&payment.merchant_id, Type::VARCHAR), (&payment.client_id, Type::VARCHAR),
            (&payment.trader_id, Type::VARCHAR), (&payment.requisite_id, Type::VARCHAR), (&payment.bank_id, Type::VARCHAR),
            (&payment.status.to_string(),Type::VARCHAR), (&payment.payment_side.to_string(), Type::VARCHAR), (&payment.currency,Type::VARCHAR),
            (&payment.target_amount,Type::NUMERIC), (&payment.fiat_amount, Type::NUMERIC), (&payment.crypto_amount,Type::NUMERIC),
            (&payment.trader_crypto_amount, Type::NUMERIC), (&payment.exchange_rate, Type::NUMERIC), (&payment.fee_type.to_string(),Type::VARCHAR),
            (&payment.crypto_fee, Type::NUMERIC), (&payment.fiat_fee, Type::NUMERIC), (&payment.trader_crypto_fee, Type::NUMERIC),
            (&payment.trader_fiat_fee, Type::NUMERIC), (&payment.holder_name, Type::VARCHAR),
            (&payment.holder_account, Type::VARCHAR), (&payment.method, Type::VARCHAR), (&payment.margin, Type::NUMERIC), (&payment.trader_margin, Type::NUMERIC),
            (&payment.earnings, Type::NUMERIC), (&payment.created_at, Type::TIMESTAMP), (&payment.updated_at,Type::TIMESTAMP),
            (&payment.deadline, Type::TIMESTAMP), (&payment.bank_name, Type::VARCHAR), (&payment.last_four, Type::VARCHAR), (&payment.card_last_four, Type::VARCHAR),]
    ), 3).map_err(|e| {
        error!(err=e,"Error save payment");
        InternalServerError
    })?;
    if start.elapsed().as_millis() > 10 {
        warn!("[SQL] SLOW SAVE REQUISITE {:?}", start.elapsed());
    }
    Ok(())
}


pub async fn get_payment_by_ex_id<T>(client: &tokio_postgres::Client, external_id: &str, request: GetPaymentsRequest)
                                        -> Result<T, PaymentError>
where T: From<tokio_postgres::Row> + ToSQL
{
    
    let mut query = T::sql();
    let mut query_params: Vec<(&(dyn ToSql + Sync), Type)> = Vec::with_capacity(2);
    
    let (query_cond, id) = request.single_query(None);
    // если у пользователя есть id, то WHERE уже в query cond
    if let Some(id) = id.as_ref() {
        query_params.push((id, Type::VARCHAR));
        query = query + &query_cond + " AND external_id=$2";
    }else {
        query += " WHERE external_id=$1";
    }
    query_params.push((&external_id, Type::VARCHAR));
    debug!("executing query {}", query);
    let rows = retry!(client.query_typed(
            &query, &query_params
    ), 3);
    let result = map_err_with_log!(rows, "Error get payment by external_id", InternalServerError, external_id)?;
    Ok(T::from(result.into_iter().next().ok_or(NotFound)?))
}

pub async fn get_payment_by_id<T>(client: &tokio_postgres::Client, payment_id: &str, request: GetPaymentsRequest)
                                     -> Result<T, PaymentError>
where T: From<tokio_postgres::Row> + ToSQL
{
    let mut query = T::sql();
    let mut query_params: Vec<(&(dyn ToSql + Sync), Type)> = Vec::with_capacity(2);
    let (query_cond, id) = request.single_query(None);
    if let Some(id) = id.as_ref() {
        query_params.push((id, Type::VARCHAR));
        query = query + &query_cond + " AND id=$2";
    }else {
        query = query + " WHERE id=$1";
    }
    debug!("executing query {}", query);
    query_params.push((&payment_id, Type::VARCHAR));
    let rows = retry!(client.query_typed(
        &query, &query_params
    ),3);
    let result = map_err_with_log!(rows, "Error get payment by external_id", InternalServerError, payment_id)?;
    Ok(T::from(result.into_iter().next().ok_or(NotFound)?))
}


pub async fn get_payments<T>(client: &Client, request: GetPaymentsRequest)
-> Result<Vec<T>, PaymentError>
where T: From<tokio_postgres::Row> + ToSQL
{
    let mut query = T::sql();
    let (query_conditions, params) = request.to_sql();
    query.push_str(&query_conditions);
    debug!("executing query {}", query);
    let stream = retry!(client.query_typed_raw(&query, params.clone()), 3).map_err(|e|{
        error!(err=e,"Error getting payments from DB");
        InternalServerError
    })?;
    let mut payments = Vec::<T>::with_capacity(request.get_requested_limit());
    tokio::pin!(stream);
    while let Some(Ok(row)) = stream.as_mut().next().await {
        payments.push(T::from(row));
    }
    Ok(payments)
}


pub async fn close_payment(client: &mut Client, notify: &bankirpay_lib::device_proto::Notification)
                           -> Result<FullPayment, PaymentError>
{
    if let Some(Notification::Event(f)) = notify.notification.as_ref() {
        let amount = Decimal::from_f64(f.amount).unwrap();
        let mut query = String::from("SELECT id FROM payments WHERE fiat_amount = $1 AND bank_id = $2 AND status IN ('UNPAID', 'PAID')");
        let mut query_params: Vec<(&(dyn ToSql + Sync), Type)> = vec![
            (&amount, Type::NUMERIC),
            (&notify.id, Type::VARCHAR),
        ];

        match f.search_by.as_str() {
            "C2C" => {
                query.push_str(" AND card_last_four = $3");
                query_params.push((&f.requisite, Type::VARCHAR));
            }
            "SBP" => {
                query.push_str(" AND method = $3");
                query_params.push((&"SBP", Type::VARCHAR));
            }
            "ACCT" => {
                query.push_str(" AND last_four = $3");
                query_params.push((&f.requisite, Type::VARCHAR));
            }
            _ => {}
        }
        query.push_str(" FOR UPDATE");
        let tx = client.transaction().await.map_err(|e|{
            error!(err=e.to_string(),"Error get DB tx payment");
            InternalServerError
        })?;
        let ids = tx.query_typed(&query, &query_params).await.map_err(|e|{
            error!(err=e.to_string(),"Error get DB tx payment");
            InternalServerError
        })?.iter().map(|r| r.get(0)).collect::<Vec<String>>();
        if ids.is_empty() || ids.len() > 1 {
            return Err(NotFound);
        }
        
        let row = tx.query_typed("UPDATE payments SET status = 'COMPLETED', close_by = 'AUTO', updated_at = NOW() WHERE id = $1 RETURNING *",
        &[(&ids[0], Type::VARCHAR)]).await.map_err(|e|{
            error!(err=e.to_string(),"Error update payment status");
            InternalServerError
        })?;
        tx.commit().await.map_err(|e|{
            error!(err=e.to_string(),"Error commit transaction");
            InternalServerError
        })?;
        return Ok(FullPayment::from(row.first().ok_or(NotFound)?));
    }
    Err(NotFound)
}

pub async fn close_payment_by_hand(client: &Client, issuer: &GetPaymentsRequest, payment_id: &str)
-> Result<FullPayment, PaymentError>
{
    let mut query = String::from("UPDATE payments SET status = 'COMPLETED', close_by = $1, updated_at = NOW()");
    let mut query_params: Vec<(&(dyn ToSql + Sync), Type)> = vec![];
    let mut param_index = 2;
    query_params.push(get_close_by_for_sql(issuer));
    let (query_conditions, params) = issuer.single_query(Some(&mut param_index)); // where возвращается когда это не админ,
    // когда админ то все пусто
    query.push_str(&query_conditions);
    if let Some(id) = params.as_ref() {
        query_params.push((id, Type::VARCHAR));
        query.push_str(format!(" AND id = ${}", param_index).as_str());
        query_params.push((&payment_id, Type::VARCHAR));
    }else {
        query_params.push((&payment_id, Type::VARCHAR));
        query.push_str(format!(" WHERE id= ${}", param_index).as_str());
    }
    query.push_str(" AND status IN ('UNPAID', 'PAID', 'CANCELLED_BY_TIMEOUT', \
    'CANCELLED_BY_ADMIN', 'CANCELLED_BY_ADMIN', 'CANCELLED_BY_TRADER', \
    'CANCELLED_BY_MERCHANT', 'CANCELLED_BY_CUSTOMER') RETURNING *");
    debug!("executing query {}", query);
    let rows = map_err_with_log!(retry!(client.query_typed(&query, &query_params), 3), 
        "Error start SQL transaction to close payment", 
        InternalServerError, 
        payment_id)?;
    if rows.is_empty() || rows.len() > 1 {
        return Err(NotFound);
    }
    Ok(FullPayment::from(rows.first().ok_or(NotFound)?))
}

// получение платежа для перерасчета (открывается транзакция и она возвращается)
pub async fn get_payment_for_recalculate<'a>(client: &'a mut Client, payment_id: &str, request: &GetPaymentsRequest)
-> Result<(Transaction<'a>,FullPayment), PaymentError>
{
    let mut query = String::from("SELECT * FROM payments");
    let mut query_params: Vec<(&(dyn ToSql + Sync), Type)> = vec![];
    let mut param_index = 1;
    let (query_conditions, params) = request.single_query(Some(&mut param_index)); // where возвращается когда это не админ,
    // когда админ то все пусто
    query.push_str(&query_conditions);
    if let Some(id) = params.as_ref() {
        query_params.push((id, Type::VARCHAR));
        query.push_str(format!(" AND id = ${}", param_index).as_str());
        query_params.push((&payment_id, Type::VARCHAR));
    }else {
        query_params.push((&payment_id, Type::VARCHAR));
        query.push_str(format!(" WHERE id= ${}", param_index).as_str());
    }
    query.push_str(" FOR UPDATE");
    println!("executing query {}", query);
    let tx = map_err_with_log!(client.transaction().await,
        "Error create payment transaction", InternalServerError, payment_id)?;
    let rows = map_err_with_log!(tx.query_typed(&query, &query_params).await,
        "Error start SQL transaction to close payment",InternalServerError,payment_id)?;
    if rows.is_empty() {
        return Err(NotFound);
    }
    Ok((tx, FullPayment::from(rows.first().unwrap())))
}

pub async fn close_recalculated_payment(tx: Transaction<'_>, payment: &FullPayment, request: &GetPaymentsRequest)
-> Result<FullPayment, PaymentError>
{
    let mut query = String::from(r#"
    UPDATE payments SET
    status = 'COMPLETED',
    updated_at = NOW(),
    target_amount = $1,
    fiat_amount = $2,
    crypto_amount = $3,
    trader_crypto_amount = $4,
    crypto_fee = $5,
    fiat_fee = $6,
    trader_crypto_fee = $7,
    trader_fiat_fee = $8,
    close_by = $9"#);
    let mut query_params: Vec<(&(dyn ToSql + Sync), Type)> = vec![
        (&payment.target_amount, Type::NUMERIC),
        (&payment.fiat_amount, Type::NUMERIC),
        (&payment.crypto_amount, Type::NUMERIC),
        (&payment.trader_crypto_amount, Type::NUMERIC),
        (&payment.crypto_fee, Type::NUMERIC),
        (&payment.fiat_fee, Type::NUMERIC),
        (&payment.trader_crypto_fee, Type::NUMERIC),
        (&payment.trader_fiat_fee, Type::NUMERIC),
    ];

    query_params.push(get_close_by_for_sql(request));
    let mut param_index = 10;
    let (query_conditions, params) = request.single_query(Some(&mut param_index)); // where возвращается когда это не админ,
    // когда админ то все пусто
    query.push_str(&query_conditions);
    if let Some(id) = params.as_ref() {
        query_params.push((id, Type::VARCHAR));
        query.push_str(format!(" AND id = ${}", param_index).as_str());
        query_params.push((&payment.id, Type::VARCHAR));
    }else {
        query_params.push((&payment.id, Type::VARCHAR));
        query.push_str(format!(" WHERE id= ${}", param_index).as_str());
    }
    query.push_str(" AND status IN ('UNPAID', 'PAID', 'CANCELLED_BY_TIMEOUT', \
    'CANCELLED_BY_ADMIN', 'CANCELLED_BY_ADMIN', 'CANCELLED_BY_TRADER', \
    'CANCELLED_BY_MERCHANT', 'CANCELLED_BY_CUSTOMER') RETURNING *");
    debug!("executing query {}", query);
    let payment_id = payment.id.clone();
    let rows = map_err_with_log!(tx.query_typed(&query, &query_params).await,
        "Error update payment in DB", InternalServerError, payment_id)?;
    if rows.is_empty() {
        return Err(NotFound);
    }
    map_err_with_log!(tx.commit().await, "Error committing transaction to payment", InternalServerError, payment_id)?;
    Ok(FullPayment::from(rows.first().unwrap()))
}
pub async fn cancel_payment_auto(client: &Client)
                                 -> Result<Vec<FullPayment>, PaymentError>
{
    let result = retry!(client.query_typed(
        "UPDATE payments SET status = 'CANCELLED_BY_TIMEOUT', updated_at= NOW() WHERE deadline < NOW() AND payment_side = 'BUY' AND status IN ('UNPAID', 'PAID') RETURNING *",
        &[]
    ),3).map_err(|e| {
        error!(err=e, "Error cancel payments in DB");
        InternalServerError
    })?;
    Ok(result.into_iter().map(FullPayment::from).collect::<Vec<FullPayment>>())
}

fn get_close_by_for_sql(request: &GetPaymentsRequest)
-> (&(dyn ToSql + Sync), Type)
{
    match request {
        GetPaymentsRequest::Trader(_) => {
            (&"HAND_TRADER", Type::VARCHAR)
        },
        GetPaymentsRequest::Admin(_) => {
            (&"HAND_ADMIN", Type::VARCHAR)
        },
        GetPaymentsRequest::Merchant(_) => {
            (&"HAND_MERCHANT", Type::VARCHAR)
        }
    }
}

