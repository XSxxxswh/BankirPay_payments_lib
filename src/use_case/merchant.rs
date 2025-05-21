use std::sync::Arc;
use tracing::{debug, error, warn};
use crate::errors::payment_error::PaymentError;
use crate::{models, repository};
use crate::errors::payment_error::PaymentError::InternalServerError;

use crate::use_case::from_lib_to_pe;

pub async fn get_merchant_margin(state: Arc<models::State>, merchant_id: &str, pm_id: &str)
                                 -> Result<bankirpay_lib::merchant_proto::PaymentMethod, PaymentError>
{
    let mut conn = match state.rdb.get().await {
        Ok(conn) => conn,
        Err(e) => {
            error!(merchant_id=merchant_id, pm_id=pm_id, err=e.to_string(), "Error getting redis conn");
            return state.merchant_api.clone().get_payment_method(merchant_id.to_string(), pm_id.to_string()).await.map_err(from_lib_to_pe)
        }
    };
    let pm = match repository::merchant::get_merchant_margin_from_redis(&mut conn, merchant_id, pm_id).await{
        Ok(Some(pm)) => pm,
        Ok(None) => {
            warn!(merchant_id=merchant_id, pm_id=pm_id, "No pm found in redis");
            let pm = state.merchant_api.clone().get_payment_method(merchant_id.to_string(), pm_id.to_string()).await.map_err(from_lib_to_pe)?;
            let _ = repository::merchant::set_merchant_margin_to_redis(&mut conn, merchant_id, &pm).await;
            pm
        },
        Err(_e) => {
            state.merchant_api.clone().get_payment_method(merchant_id.to_string(), pm_id.to_string()).await.map_err(from_lib_to_pe)?
        }
    };
    Ok(pm)
}





