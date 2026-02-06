//! DIPs gRPC API for the DIPs Gateway.
//!
//! This module contains the generated code to implement the DIPs Gateway's gRPC API:
//! - [`gateway_server`]: The tonic gRPC service implementation.
//! - [`indexer_client`]: The indexer's DIPs gRPC client.

// Re-export the indexer-rs crate types
pub mod gateway_server {
    pub mod rpc {
        #[doc(inline)]
        pub use indexer_dips::proto::gateway::graphprotocol::gateway::dips::{
            CancelAgreementRequest, CancelAgreementResponse, CollectPaymentRequest,
            CollectPaymentResponse, CollectPaymentStatus,
            gateway_dips_service_server::{GatewayDipsService, GatewayDipsServiceServer},
        };
    }

    pub mod sol {
        #[doc(inline)]
        pub use indexer_dips::{
            CancellationRequest, CollectionRequest, SignedCancellationRequest,
            SignedCollectionRequest,
        };
    }

    #[doc(inline)]
    pub use indexer_dips::{dips_cancellation_eip712_domain, dips_collection_eip712_domain};
}

// Re-export the indexer-rs crate types
pub mod indexer_client {
    pub mod rpc {
        #[doc(inline)]
        pub use indexer_dips::proto::indexer::graphprotocol::indexer::dips::{
            CancelAgreementRequest, SubmitAgreementProposalRequest,
            indexer_dips_service_client::IndexerDipsServiceClient,
        };
    }

    pub mod sol {
        #[doc(inline)]
        pub use indexer_dips::{
            CancellationRequest, IndexingAgreementVoucher, SignedCancellationRequest,
            SignedIndexingAgreementVoucher, SubgraphIndexingVoucherMetadata,
        };
    }

    #[doc(inline)]
    pub use indexer_dips::{dips_agreement_eip712_domain, dips_cancellation_eip712_domain};
}
