//! The DIPs gRPC server for the gateway.
//!
//! This module contains the generated code to implement the gateway's DIPs gRPC server.

use dipper_core::ids::IndexingAgreementId;
use thegraph_core::{
    alloy::sol,
    signed_message::{SignedMessage, ToSolStruct},
};

// Include the tonic-generated code
include!("gen/gateway.mod.rs");

use self::graphprotocol::gateway::dips::{CancelAgreementRequest, ReportProgressRequest};

pub struct CancelAgreementRequestMessage {
    pub agreement_id: IndexingAgreementId,
}

sol! {
    struct CancelAgreementRequestMessageSol {
        bytes16 agreement_id;
    }
}

impl ToSolStruct<CancelAgreementRequestMessageSol> for CancelAgreementRequestMessage {
    fn to_sol_struct(&self) -> CancelAgreementRequestMessageSol {
        CancelAgreementRequestMessageSol {
            agreement_id: self.agreement_id.as_bytes().into(),
        }
    }
}

impl TryFrom<CancelAgreementRequest> for SignedMessage<CancelAgreementRequestMessage> {
    type Error = anyhow::Error;

    fn try_from(value: CancelAgreementRequest) -> Result<Self, Self::Error> {
        let message = CancelAgreementRequestMessage {
            agreement_id: value.agreement_id.as_slice().try_into()?,
        };
        let signature = value.signature.as_slice().try_into()?;

        Ok(SignedMessage { message, signature })
    }
}

pub struct ReportProgressRequestMessage {
    pub agreement_id: IndexingAgreementId,
}

sol! {
    struct ReportProgressRequestMessageSol {
        bytes16 agreement_id;
    }
}

impl ToSolStruct<ReportProgressRequestMessageSol> for ReportProgressRequestMessage {
    fn to_sol_struct(&self) -> ReportProgressRequestMessageSol {
        ReportProgressRequestMessageSol {
            agreement_id: self.agreement_id.as_bytes().into(),
        }
    }
}

impl TryFrom<ReportProgressRequest> for SignedMessage<ReportProgressRequestMessage> {
    type Error = anyhow::Error;

    fn try_from(value: ReportProgressRequest) -> Result<Self, Self::Error> {
        let message = ReportProgressRequestMessage {
            agreement_id: value.agreement_id.as_slice().try_into()?,
        };
        let signature = value.signature.as_slice().try_into()?;

        Ok(SignedMessage { message, signature })
    }
}
