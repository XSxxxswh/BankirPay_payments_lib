use axum::http::{Response};
use axum::response::IntoResponse;
use bankirpay_lib::errors::LibError;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PaymentError {
    InvalidPaymentSide,
    InvalidFeeType,
    InvalidCurrencyType,
    NoAvailableRequisites,
    InternalServerError,
    InsufficientFunds,
    AccessDenied(String),
    NotFound,
    Conflict,
    InvalidAmount,
    InvalidCurrency,
    SellPaymentsUnavailable,
}

#[derive(serde::Serialize)]
struct ErrorMsg {
    status: u16,
    message: &'static str,
}

macro_rules! payment_error_build {
    ($status:expr, $message:literal) => {
        Response::builder()
        .status($status)
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(simd_json::to_vec(&ErrorMsg {
            status: $status,
            message: $message,
        }).unwrap()))
        .unwrap()
    };
}

impl IntoResponse for PaymentError {
    fn into_response(self) -> axum::response::Response {
        match self {
            PaymentError::InvalidPaymentSide => payment_error_build!(400, "Invalid Payment Side"),
            PaymentError::InvalidFeeType => payment_error_build!(400, "Invalid Payment Type"),
            PaymentError::InvalidCurrencyType => payment_error_build!(400, "Invalid Payment Currency"),
            PaymentError::NoAvailableRequisites => payment_error_build!(406, "No available requisites"),
            PaymentError::InternalServerError => payment_error_build!(500, "Internal server error"),
            PaymentError::InsufficientFunds => payment_error_build!(400, "Insufficient funds"),
            PaymentError::AccessDenied(_) => payment_error_build!(403, "Access denied"),
            PaymentError::NotFound => payment_error_build!(404, "Not found"),
            PaymentError::Conflict => payment_error_build!(409, "Conflict"),
            PaymentError::InvalidAmount => payment_error_build!(400, "Invalid amount"),
            PaymentError::InvalidCurrency => payment_error_build!(400, "Invalid currency"),
            PaymentError::SellPaymentsUnavailable => payment_error_build!(400, "Sell Payments Unavailable"),

        }
    }
}
impl std::fmt::Display for PaymentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPaymentSide => write!(f, "Invalid payment side"),
            Self::InvalidFeeType => write!(f, "Invalid payment type"),
            Self::InvalidCurrencyType => write!(f, "Invalid currency"),
            Self::NoAvailableRequisites => write!(f, "No available requisites"),
            Self::InternalServerError => write!(f, "Internal server error"),
            Self::InsufficientFunds => write!(f, "Insufficient funds"),
            Self::AccessDenied(reason) => write!(f, "Access denied: {}", reason),
            Self::NotFound => write!(f, "Not found"),
            Self::Conflict => write!(f, "Conflict"),
            Self::InvalidAmount => write!(f, "Invalid amount"),
            Self::InvalidCurrency => write!(f, "Invalid currency"),
            Self::SellPaymentsUnavailable => write!(f, "Sell Payments Unavailable"),

        }
    }
}

impl From<LibError> for PaymentError {
    fn from(error: LibError) -> Self {
        match error {
            LibError::TraderNotFound => Self::NotFound,
            LibError::Forbidden => Self::InternalServerError,
            LibError::Unauthorized => Self::AccessDenied(String::from("Unauthorized")),
            LibError::InternalError => Self::InternalServerError,
            LibError::MerchantNotFound => Self::NotFound,
            LibError::NotFound => Self::NotFound,
            LibError::NoAvailableRequisites => Self::NoAvailableRequisites,
            LibError::InsufficientFunds => Self::InsufficientFunds,
            LibError::InvalidAmount => Self::InvalidAmount,
            LibError::Conflict => Self::Conflict,
        }
    }
}