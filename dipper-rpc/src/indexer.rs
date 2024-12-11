//! DIPs gRPC API for the DIPs Gateway.
//!
//! This module contains the generated code to implement the DIPs Gateway's gRPC API:
//! - [`rpc::gateway_server`]: The tonic gRPC service implementation.
//! - [`rpc::indexer_client`]: The indexer's DIPs gRPC client.

pub mod rpc {
    /// The DIPs gRPC server for the gateway.
    ///
    /// This module contains the generated code to implement the gateway's DIPs gRPC server.
    pub mod gateway_server {
        use dipper_core::ids::IndexingAgreementId;
        use thegraph_core::{
            alloy::sol,
            signed_message::{SignedMessage, ToSolStruct},
        };

        use self::graphprotocol::gateway::dips::{CancelAgreementRequest, ReportProgressRequest};

        include!("indexer/gen/gateway.mod.rs");

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
    }

    /// The RPC client for the indexer's DIPs gRPC API.
    ///
    /// This module contains the generated code to interact with the indexer's DIPs gRPC server.
    pub mod indexer_client {
        include!("indexer/gen/indexer.mod.rs");
    }
}
