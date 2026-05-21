//! DIPs gRPC API for the DIPs Gateway.
//!
//! This module re-exports the indexer-side DIPs gRPC types for use by dipper's
//! [`indexer_client`]. Only the proposal-submission RPC remains; cancellation
//! and collection are handled on-chain via the RecurringCollector and
//! SubgraphService contracts.

use thegraph_core::alloy::{
    primitives::Address,
    sol_types::{Eip712Domain, eip712_domain},
};

// Re-export the indexer-rs crate types
pub mod indexer_client {
    pub mod rpc {
        #[doc(inline)]
        pub use indexer_dips::proto::indexer::graphprotocol::indexer::dips::{
            ProposalResponse, RejectReason, SubmitAgreementProposalRequest,
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
            /// The agreement ID is derived on-chain via
            /// `bytes16(keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce)))`.
            struct RecurringCollectionAgreement {
                uint64 deadline;
                uint64 endsAt;
                address payer;
                address dataService;
                address serviceProvider;
                uint256 maxInitialTokens;
                uint256 maxOngoingTokensPerSecond;
                uint32 minSecondsPerCollection;
                uint32 maxSecondsPerCollection;
                uint16 conditions;
                uint256 nonce;
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
    }
}

/// Derive the on-chain agreement ID from the RCA fields.
///
/// The contract computes:
///   `bytes16(keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce)))`
///
/// This replicates that derivation so dipper can predict the agreement ID
/// without waiting for an on-chain event.
pub fn derive_agreement_id(rca: &indexer_client::sol::RecurringCollectionAgreement) -> [u8; 16] {
    use thegraph_core::alloy::{primitives::keccak256, sol_types::SolValue};

    let encoded = (
        rca.payer,
        rca.dataService,
        rca.serviceProvider,
        rca.deadline,
        rca.nonce,
    )
        .abi_encode();
    let hash = keccak256(&encoded);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hash[..16]);
    id
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
    fn test_derive_agreement_id() {
        use thegraph_core::alloy::{
            primitives::{U256, address, keccak256},
            sol_types::SolValue,
        };

        let rca = indexer_client::sol::RecurringCollectionAgreement {
            deadline: 1000,
            endsAt: 2000,
            payer: address!("0000000000000000000000000000000000000001"),
            dataService: address!("0000000000000000000000000000000000000002"),
            serviceProvider: address!("0000000000000000000000000000000000000003"),
            maxInitialTokens: U256::from(100),
            maxOngoingTokensPerSecond: U256::from(10),
            minSecondsPerCollection: 60,
            maxSecondsPerCollection: 3600,
            conditions: 0,
            nonce: U256::from(42),
            metadata: Default::default(),
        };

        let id = derive_agreement_id(&rca);

        // Verify it matches the on-chain derivation:
        // bytes16(keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce)))
        let expected_hash = keccak256(
            (
                rca.payer,
                rca.dataService,
                rca.serviceProvider,
                rca.deadline,
                rca.nonce,
            )
                .abi_encode(),
        );
        assert_eq!(id, expected_hash[..16]);
    }

    /// Shared test vector with indexer-rs (crates/dips/src/lib.rs).
    /// Both repos must produce the same bytes16 for this input.
    /// If this test fails, the derivation has drifted from the on-chain
    /// contract and/or from indexer-rs -- cancellations and agreement
    /// matching will break silently.
    #[test]
    fn test_derive_agreement_id_shared_vector() {
        use thegraph_core::alloy::primitives::{U256, address};

        let rca = indexer_client::sol::RecurringCollectionAgreement {
            deadline: 1700000300,
            endsAt: 1700086400,
            payer: address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266"),
            dataService: address!("Cf7Ed3AccA5a467e9e704C703E8D87F634fB0Fc9"),
            serviceProvider: address!("f4EF6650E48d099a4972ea5B414daB86e1998Bd3"),
            maxInitialTokens: U256::from(1_000_000_000_000_000_000u64),
            maxOngoingTokensPerSecond: U256::from(1_000_000_000_000_000u64),
            minSecondsPerCollection: 3600,
            maxSecondsPerCollection: 86400,
            conditions: 0,
            nonce: U256::from(0x019d44a86ac97e938672e2501fe630f2u128),
            metadata: Default::default(),
        };

        let id = derive_agreement_id(&rca);

        // Pinned expected value. If this fails, check:
        // 1. indexer-rs: crates/dips/src/lib.rs test_derive_agreement_id_shared_vector
        // 2. Solidity: RecurringCollector._generateAgreementId()
        let expected: [u8; 16] = [
            0x55, 0x79, 0x42, 0xae, 0xfa, 0xb6, 0x16, 0x09, 0xcf, 0xb9, 0xee, 0x14, 0xd3, 0x09,
            0xa1, 0x7e,
        ];
        assert_eq!(
            id,
            expected,
            "derive_agreement_id output does not match pinned shared vector. \
             Actual: 0x{} -- update this test AND the matching test in \
             indexer-rs (crates/dips/src/lib.rs)",
            id.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
    }

    #[test]
    fn test_rca_eip712_typehash() {
        use thegraph_core::alloy::primitives::U256;

        // The canonical EIP-712 type string for RecurringCollectionAgreement.
        // This matches the hardcoded typehash in RecurringCollector.sol.
        // If this test fails, it means the sol! struct definition has drifted from
        // the on-chain contract's EIP-712 typehash.
        const EXPECTED_TYPE_STRING: &[u8] = b"RecurringCollectionAgreement(uint64 deadline,uint64 endsAt,address payer,address dataService,address serviceProvider,uint256 maxInitialTokens,uint256 maxOngoingTokensPerSecond,uint32 minSecondsPerCollection,uint32 maxSecondsPerCollection,uint16 conditions,uint256 nonce,bytes metadata)";

        let expected_typehash = keccak256(EXPECTED_TYPE_STRING);

        // Create a dummy RCA to call the instance method
        let dummy_rca = indexer_client::sol::RecurringCollectionAgreement {
            deadline: 0,
            endsAt: 0,
            payer: Address::ZERO,
            dataService: Address::ZERO,
            serviceProvider: Address::ZERO,
            maxInitialTokens: U256::ZERO,
            maxOngoingTokensPerSecond: U256::ZERO,
            minSecondsPerCollection: 0,
            maxSecondsPerCollection: 0,
            conditions: 0,
            nonce: U256::ZERO,
            metadata: Default::default(),
        };

        let actual_typehash = dummy_rca.eip712_type_hash();

        assert_eq!(
            actual_typehash, expected_typehash,
            "RecurringCollectionAgreement EIP-712 typehash mismatch. \
             This likely means the sol! struct definition does not match the on-chain contract."
        );
    }
}
