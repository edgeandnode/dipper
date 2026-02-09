//! DIPs gRPC API for the DIPs Gateway.
//!
//! This module contains the generated code to implement the DIPs Gateway's gRPC API:
//! - [`gateway_server`]: The tonic gRPC service implementation.
//! - [`indexer_client`]: The indexer's DIPs gRPC client.

use thegraph_core::alloy::{
    primitives::Address,
    sol_types::{Eip712Domain, eip712_domain},
};

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
        pub use indexer_dips::{CancellationRequest, SignedCancellationRequest};
    }

    #[doc(inline)]
    pub use indexer_dips::dips_cancellation_eip712_domain;
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

    /// Solidity types for RCA-based indexing agreements.
    ///
    /// The `RecurringCollectionAgreement` and related types match the on-chain
    /// contract types from `IRecurringCollector` and `IndexingAgreement.sol`.
    pub mod sol {
        thegraph_core::alloy::sol! {
            /// The on-chain RecurringCollectionAgreement type.
            ///
            /// Matches `IRecurringCollector.RecurringCollectionAgreement` exactly.
            struct RecurringCollectionAgreement {
                bytes16 agreementId;
                uint64 deadline;
                uint64 endsAt;
                address payer;
                address dataService;
                address serviceProvider;
                uint256 maxInitialTokens;
                uint256 maxOngoingTokensPerSecond;
                uint32 minSecondsPerCollection;
                uint32 maxSecondsPerCollection;
                bytes metadata;
            }

            /// Wrapper pairing an RCA with its EIP-712 signature.
            struct SignedRecurringCollectionAgreement {
                RecurringCollectionAgreement agreement;
                bytes signature;
            }

            /// Metadata for indexing agreement acceptance, ABI-encoded into
            /// `RecurringCollectionAgreement.metadata`.
            struct AcceptIndexingAgreementMetadata {
                bytes32 subgraphDeploymentId;
                uint8 version;
                bytes terms;
            }

            /// V1 pricing terms, ABI-encoded into
            /// `AcceptIndexingAgreementMetadata.terms`.
            struct IndexingAgreementTermsV1 {
                uint256 tokensPerSecond;
                uint256 tokensPerEntityPerSecond;
            }
        }

        // Cancellation types are unchanged -- keep from indexer_dips
        #[doc(inline)]
        pub use indexer_dips::{CancellationRequest, SignedCancellationRequest};
    }

    #[doc(inline)]
    pub use indexer_dips::dips_cancellation_eip712_domain;
}

/// EIP-712 domain for the RecurringCollector contract.
///
/// Used to sign `RecurringCollectionAgreement` messages. The `verifying_contract`
/// is the deployed RecurringCollector address.
pub fn rca_eip712_domain(chain_id: u64, recurring_collector: Address) -> Eip712Domain {
    eip712_domain! {
        name: "RecurringCollector",
        version: "1",
        chain_id: chain_id,
        verifying_contract: recurring_collector,
    }
}
