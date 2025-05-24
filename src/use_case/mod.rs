use bankirpay_lib::errors::LibError;
use crate::errors::payment_error::PaymentError;

pub mod payment;
pub(crate) mod merchant;
pub(crate) mod trader;
mod requisite;
mod exchange_rate;

fn from_lib_to_pe(err: LibError) -> PaymentError {
    match err {
        LibError::TraderNotFound => PaymentError::NotFound,
        LibError::Forbidden => PaymentError::InternalServerError,
        LibError::Unauthorized => PaymentError::NotFound,
        LibError::InternalError => PaymentError::InternalServerError,
        LibError::MerchantNotFound => PaymentError::NotFound,
        LibError::NotFound => PaymentError::NotFound,
        LibError::NoAvailableRequisites => PaymentError::NoAvailableRequisites,
        LibError::InsufficientFunds => PaymentError::InsufficientFunds,
        LibError::InvalidAmount => PaymentError::InvalidAmount,
        LibError::Conflict => PaymentError::Conflict,
    }
}