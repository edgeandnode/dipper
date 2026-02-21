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
            CancelAgreementRequest, ProposalResponse, RejectReason, SubmitAgreementProposalRequest,
            SubmitAgreementProposalResponse, indexer_dips_service_client::IndexerDipsServiceClient,
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
                // NB: The on-chain struct declares these as uint64 for storage efficiency,
                // but the EIP-712 typehash uses uint256. We must match the typehash.
                uint256 deadline;
                uint256 endsAt;
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

#[cfg(test)]
mod tests {
    use thegraph_core::alloy::{primitives::keccak256, sol_types::SolStruct};

    use super::*;

    #[test]
    fn test_rca_eip712_typehash() {
        use thegraph_core::alloy::primitives::{FixedBytes, U256};

        // The canonical EIP-712 type string for RecurringCollectionAgreement.
        // This matches the hardcoded typehash in RecurringCollector.sol at line 27-30.
        // If this test fails, it means the sol! struct definition has drifted from
        // the on-chain contract's EIP-712 typehash.
        const EXPECTED_TYPE_STRING: &[u8] = b"RecurringCollectionAgreement(bytes16 agreementId,uint256 deadline,uint256 endsAt,address payer,address dataService,address serviceProvider,uint256 maxInitialTokens,uint256 maxOngoingTokensPerSecond,uint32 minSecondsPerCollection,uint32 maxSecondsPerCollection,bytes metadata)";

        let expected_typehash = keccak256(EXPECTED_TYPE_STRING);

        // Create a dummy RCA to call the instance method
        let dummy_rca = indexer_client::sol::RecurringCollectionAgreement {
            agreementId: FixedBytes::default(),
            deadline: U256::ZERO,
            endsAt: U256::ZERO,
            payer: Address::ZERO,
            dataService: Address::ZERO,
            serviceProvider: Address::ZERO,
            maxInitialTokens: U256::ZERO,
            maxOngoingTokensPerSecond: U256::ZERO,
            minSecondsPerCollection: 0,
            maxSecondsPerCollection: 0,
            metadata: Default::default(),
        };

        let actual_typehash = dummy_rca.eip712_type_hash();

        assert_eq!(
            actual_typehash, expected_typehash,
            "RecurringCollectionAgreement EIP-712 typehash mismatch. \
             This likely means the sol! struct definition does not match the on-chain contract. \
             Verify that all field types (especially deadline and endsAt as uint256) match the contract's typehash."
        );
    }
}
