use std::ops::{Deref};
use std::sync::Arc;
use bankirpay_lib::errors::LibError;
use bankirpay_lib::models::payments::merchant::MerchantPayment;
use bankirpay_lib::models::payments::payment::{FeeTypes, FullPayment, NewPaymentRequest, PaymentSides, ToSQL};
use bankirpay_lib::models::payments::requests::GetPaymentsRequest;
use bankirpay_lib::models::payments::trader::TraderPaymentBuilder;
use bankirpay_lib::requisites_proto::Requisite;
use bankirpay_lib::services::traders::trader_service::TraderService;
use bankirpay_lib::trader_proto::BalanceActionType;
use bigdecimal::num_traits::abs;
use futures::stream::FuturesUnordered;
use redis::aio::MultiplexedConnection;
use rust_decimal::{dec, Decimal};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::{debug, error};
use uuid::Uuid;
use crate::errors::payment_error::PaymentError;
use crate::models::{LockGuard, State};
use crate::{models, repository, use_case};
use crate::repository::requisite::release_lock;
use crate::use_case::{from_lib_to_pe, get_db_conn, get_rdb_conn, kafka};

pub async fn new_payment (state: Arc<State>, merchant_id: String, request: NewPaymentRequest) -> Result<MerchantPayment, PaymentError> {
    let (merchant_margin, exchange_rate) = tokio::try_join!(use_case::merchant::get_merchant_margin(state.clone(), merchant_id.as_str(), request.method_id.deref()), use_case::exchange_rate::get_exchange_rate(state.clone()))?;
    if merchant_margin.currency.to_lowercase() != request.currency.to_lowercase() {
        return Err(PaymentError::InvalidCurrency)
    }
    let cb_allow = match (merchant_margin.cb_allow, merchant_margin.cross_border) {
        (true, true) => Some(true),
        (true, false) => None,
        _ => Some(false),
    };
    let mut conn = get_rdb_conn(&state.rdb).await?;
    let pg = Arc::new(get_db_conn(&state.pool).await?);
    let mut payment = calculate_fee_for_merchant(request, &merchant_margin, exchange_rate, merchant_id)?;
    let requisites = state.requisite_api.clone().get_requisites_for_payment(merchant_margin.method_type, payment.fiat_amount.to_f64().unwrap(), payment.currency.to_string(), merchant_margin.bank, cb_allow).await.map_err(from_lib_to_pe)?;
    let chunks = requisites.chunks((requisites.len() / 4).max(1));
    let cancellation_token = CancellationToken::new();
    let payment_side = Arc::new(payment.payment_side);
    let mut futures = FuturesUnordered::new();
    for requisite_chunk in chunks.into_iter() {
        let trader_api = state.trader_api.clone();
        let conn = conn.clone();
        let pg = pg.clone();
        let cancellation_token = cancellation_token.clone();
        let payment_side = payment_side.clone();
        let requisite_chunk = requisite_chunk.to_vec();
        futures.push(tokio::spawn(async move {
            process_requisite_chunk(
                conn,
                pg,
                trader_api,
                requisite_chunk,
                exchange_rate,
                payment.fiat_amount,
                &payment_side,
                cancellation_token,
            ).await
        }));
    }
    while let Some(Ok(result)) = futures.next().await {
        if let Some((requisite, trader_builder, _guard)) = result? {
            if let Err(err) = state.trader_api.clone().change_balance(requisite.trader_id.clone(),
                                                        trader_builder.trader_crypto_amount.to_f64().unwrap(),
                                                        BalanceActionType::FrozeSoft).await
            {
                if err == LibError::Conflict || err == LibError::InsufficientFunds || err == LibError::NotFound {
                    continue;
                }else {
                    error!(err=?err, "Error withdrawal trader balance");
                    return Err(PaymentError::InternalServerError)
                }
            }
            payment.trader_margin = trader_builder.trader_margin;
            payment.trader_fiat_fee = trader_builder.trader_fiat_fee;
            payment.trader_crypto_fee = trader_builder.trader_crypto_fee;
            payment.trader_crypto_amount = trader_builder.trader_crypto_amount;
            payment.trader_id = requisite.trader_id;
            payment.requisite_id = requisite.id.clone();
            payment.bank_id = requisite.bank_id;
            payment.bank_name = requisite.bank_name;
            payment.holder_name = requisite.holder_name;
            payment.holder_account = requisite.holder_account;
            payment.last_four = requisite.last_four;
            payment.card_last_four = requisite.card_last_four;
            payment.earnings = payment.crypto_fee - payment.trader_crypto_fee;
            payment.method = requisite.method;
            if repository::payment::insert_payment_to_db(&pg, &payment).await.is_err() {
                error!("Error adding payment to payment db");
                let _ = use_case::kafka::send_trader_change_balance_request(state, 
                                                                            payment.trader_id.clone(), 
                                                                            payment.trader_crypto_amount,
                                                                            BalanceActionType::Unfroze).await;
            }
            cancellation_token.cancel();
            let _ = release_lock(&mut conn, requisite.id.as_str()).await;
            return Ok(MerchantPayment::from(payment));
        }
    }
    Err(PaymentError::NoAvailableRequisites)
}



async fn process_requisite_chunk(
    conn: MultiplexedConnection,
    pg: Arc<deadpool_postgres::Object>,
    trader_api: TraderService,
    requisites: Vec<Requisite>,
    exchange_rate: Decimal,
    fiat_amount: Decimal,
    payment_side: &PaymentSides,
    cancelled: CancellationToken
) -> Result<Option<(Requisite, TraderPaymentBuilder, LockGuard)>, PaymentError>
{
    for requisite in requisites.into_iter() {
        if cancelled.is_cancelled() {
            return Ok(None);
        }
        let mut conn = conn.clone();
        let mut conn2 = conn.clone();
        let pg = pg.clone();
        let trader_api = trader_api.clone();
        let result  = tokio::try_join!(
        use_case::trader::get_trader_margin(&mut conn, trader_api.clone(), requisite.trader_id.as_str()),
        use_case::requisite::check_amount_available(&pg, &mut conn2, requisite.id.as_str(), fiat_amount, requisite.max_payments_limit));
        let (margin, available) = result?;
        if let Some(lock) = available {
            let builder = calculate_trader_fee(fiat_amount, margin, exchange_rate, payment_side)?;
            return Ok(Some((requisite, builder, lock)));
        }
    }
    Ok(None)
}



fn calculate_fee_for_merchant(
    request: NewPaymentRequest,
    fee: &bankirpay_lib::merchant_proto::PaymentMethod,
    exchange_rate: Decimal,
    merchant_id: String,
) -> Result<FullPayment, PaymentError> {
    if request.side != PaymentSides::Buy {
        return Err(PaymentError::SellPaymentsUnavailable);
    }

    let margin = Decimal::from_f64(fee.buy_margin).ok_or(PaymentError::InvalidAmount)?;
    let fee_percent = margin / dec!(100); // Используем dec! из rust_decimal_macros

    let fiat_fee = (request.target_amount * fee_percent).round_dp(2);
    let crypto_amount_base = request.target_amount / exchange_rate;
    let crypto_fee = (crypto_amount_base * fee_percent).round_dp(2);

    let (fiat_amount, crypto_amount) = match request.fee_type {
        FeeTypes::ChargeCustomer => {
            let fiat_amount = (request.target_amount + fiat_fee).round_dp(2);
            let crypto_amount = crypto_amount_base.round_dp(2);
            (fiat_amount, crypto_amount)
        }
        FeeTypes::ChargeMerchant => {
            let fiat_amount = request.target_amount.round_dp(2);
            let crypto_amount = (crypto_amount_base - crypto_fee).round_dp(2);
            (fiat_amount, crypto_amount)
        }
    };

    Ok(FullPayment {
        id: Uuid::now_v7().to_string(),
        external_id: request.external_id,
        merchant_id,
        client_id: request.client_id,
        payment_side: request.side,
        currency: request.currency,
        target_amount: request.target_amount,
        fiat_amount,
        crypto_amount,
        exchange_rate,
        fee_type: request.fee_type,
        crypto_fee,
        fiat_fee,
        margin,
        created_at: chrono::Utc::now().naive_utc(),
        deadline: chrono::Utc::now().naive_utc() + chrono::Duration::seconds(fee.payment_exp),
        ..Default::default()
    })
}



fn calculate_trader_fee(
    fiat_amount: Decimal,
    margin: Decimal,
    exchange_rate: Decimal,
    payment_side: &PaymentSides,
) -> Result<TraderPaymentBuilder, PaymentError> 
{
    if !matches!(payment_side, PaymentSides::Buy) {
        return Err(PaymentError::SellPaymentsUnavailable);
    }

    let hundred = Decimal::from_f64(100.0).unwrap();
    let fee_rate = margin / hundred;
    let fiat_fee = fiat_amount * fee_rate;
    let crypto_fee = fiat_fee / exchange_rate;
    let crypto_amount = (fiat_amount / exchange_rate) - crypto_fee;

    Ok(TraderPaymentBuilder {
        trader_fiat_fee: fiat_fee.round_dp(2),
        trader_margin: margin,
        trader_crypto_fee: crypto_fee.round_dp(2),
        trader_crypto_amount: crypto_amount.round_dp(2),
    })
}


pub async fn get_payments<T>(state: Arc<models::State>, request: GetPaymentsRequest)
-> Result<Vec<T>, PaymentError>                             
where T: From<tokio_postgres::Row> + ToSQL
{
    debug!("Getting payments");
    let pg = get_db_conn(&state.pool).await?;
    repository::payment::get_payments(&pg, request).await
}

pub async fn get_payment_by_id<T>(state: Arc<models::State>, payment_id: &str, request: GetPaymentsRequest)
                                  -> Result<T, PaymentError>
where T: From<tokio_postgres::Row> + ToSQL
{
    debug!(payment_id=payment_id, "Getting payment by id");
    let pg = get_db_conn(&state.pool).await?;
    let payment = repository::payment::get_payment_by_id(&pg, payment_id, request).await?;
    Ok(payment)
}


pub async fn get_payment_by_external_id<T>(state: Arc<models::State>, ex_payment_id: &str, request: GetPaymentsRequest)
                                           -> Result<T, PaymentError>
where T: From<tokio_postgres::Row> + ToSQL
{
    debug!(external_id=ex_payment_id, "Getting payment by ex id");
    let pg = get_db_conn(&state.pool).await?;
    let payment = repository::payment::get_payment_by_ex_id(&pg, ex_payment_id, request).await?;
    Ok(payment)
}


pub async fn close_payment_by_notification(state: Arc<models::State>, notification: &bankirpay_lib::device_proto::Notification)
-> Result<(), PaymentError>
{
    let mut pg = get_db_conn(&state.pool).await?;
    repository::payment::close_payment(&mut pg, notification).await?;
    Ok(())
}

pub async fn close_payment_by_hand<T>(state: Arc<models::State>, issuer: GetPaymentsRequest, payment_id: &str, final_amount: Option<Decimal>)
-> Result<T, PaymentError>
where T: From<FullPayment>
{
    debug!(payment_id=payment_id,issuer=?issuer,"Closing payment by hand");
    let mut pg = get_db_conn(&state.pool).await?;
    let payment = match final_amount {
        Some(final_amount) => {
            let (tx, mut payment) = repository::payment::get_payment_for_recalculate(&mut pg, payment_id, &issuer).await?;
            let previous_amount = payment.trader_crypto_amount;
            recalculate_payment(&mut payment, final_amount).await?;
            let final_payment = repository::payment::close_recalculated_payment(tx, &payment, &issuer).await?;
            match payment.status {
                // если заявка еще в процессе значит баланс трейдера еще заморожен
                c if !c.is_final() => {
                    let amount_to_froze = final_payment.trader_crypto_amount - previous_amount;
                    let _ = kafka::send_trader_change_balance_request(state,
                                                       payment.trader_id,
                                                       abs(amount_to_froze),
                                                       if amount_to_froze > dec!(0) {BalanceActionType::FrozeHard}else{BalanceActionType::Unfroze}).await;
                    // если финальная сумма больше чем исходная, то замораживаем разницу, если меньше, то размораживаем
                }
                _ => {}
            }
            final_payment
        },
        None => repository::payment::close_payment_by_hand(&mut pg, &issuer, payment_id).await?
    };
    Ok(T::from(payment))
}

pub async fn auto_cancel_worker(state: Arc<State>)
                                       -> Result<(), PaymentError>
{
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        let mut pg = match get_db_conn(&state.pool).await {
            Ok(pg) => pg,
            Err(_) => {
                error!("Error get DB conn");
                continue;
            }
        };
        let _ = repository::payment::cancel_payment_auto(&mut pg).await;
    }
}


async fn recalculate_payment(payment: &mut FullPayment, amount: Decimal)
-> Result<(), PaymentError>
{
    payment.fiat_amount = amount;
    let fee_percent = payment.margin / dec!(100);
    let builder = calculate_trader_fee(amount,
                                       payment.trader_margin,
                                       payment.exchange_rate,
                                       &payment.payment_side)?;
    payment.trader_crypto_fee = builder.trader_crypto_fee;
    payment.trader_fiat_fee = builder.trader_fiat_fee;
    payment.trader_crypto_amount = builder.trader_crypto_amount;
    match &payment.fee_type {
        FeeTypes::ChargeCustomer => {
            payment.target_amount = (amount / (dec!(1) + fee_percent)).round_dp(2);
            payment.fiat_fee = (payment.target_amount * fee_percent).round_dp(2);
            let crypto_amount_base = payment.target_amount / payment.exchange_rate;
            payment.crypto_amount = crypto_amount_base.round_dp(2);
            payment.crypto_fee = (crypto_amount_base * fee_percent).round_dp(2);
        }
        FeeTypes::ChargeMerchant => {
            payment.target_amount = amount;
            payment.fiat_fee = (payment.target_amount * fee_percent).round_dp(2);
            let crypto_amount_base = payment.target_amount / payment.exchange_rate;
            payment.crypto_fee = (crypto_amount_base * fee_percent).round_dp(2);
            payment.crypto_amount = (crypto_amount_base - payment.crypto_fee).round_dp(2);
        }
    }
    Ok(())
}



