//! Contract ABI definitions for on-chain interactions.

use thegraph_core::alloy::sol;

sol! {
    /// SubgraphService contract interface (minimal subset for agreement cancellation).
    ///
    /// The full contract is at `contracts/SubgraphService.sol` on branch
    /// `ma/indexing-payments-003`. This interface only includes the methods
    /// needed for dipper's on-chain operations.
    #[allow(missing_docs)]
    interface ISubgraphService {
        /// Cancel an indexing agreement as the payer.
        ///
        /// This caps the collectible fees at the cancellation timestamp and
        /// emits an `IndexingAgreementCanceled` event.
        ///
        /// Can only be called by the original payer of the agreement.
        function cancelIndexingAgreementByPayer(bytes16 agreementId) external;
    }

    /// RecurringCollector contract interface (minimal subset for offer-based RCA authorization).
    ///
    /// The full contract is at
    /// `packages/horizon/contracts/payments/collectors/RecurringCollector.sol`
    /// on branch `indexing-payments-management-audit-fix-reduced`.
    ///
    /// The `offer` function stores an RCA offer on-chain keyed by agreement ID.
    /// Indexers later call `accept(rca, "")` with an empty signature, and the
    /// contract verifies the stored offer hash matches `hashRCA(rca)`.
    /// `msg.sender` of `offer()` must equal `rca.payer`.
    ///
    /// The stored offer mapping lives inside an ERC-7201 namespaced storage
    /// struct and has no public getter, so there is no RPC-level idempotency
    /// check available. Dipper queries the indexing-payments subgraph's
    /// Offer entity instead.
    #[allow(missing_docs)]
    interface IRecurringCollector {
        /// Agreement details returned from `offer()`.
        struct AgreementDetails {
            bytes16 agreementId;
            address payer;
            address dataService;
            address serviceProvider;
            bytes32 versionHash;
            uint8 state;
        }

        /// Store a new or updated RCA offer on-chain.
        ///
        /// `offerType` = 0 for OFFER_TYPE_NEW, 1 for OFFER_TYPE_UPDATE.
        /// `data` is the ABI-encoded `RecurringCollectionAgreement` struct.
        /// `options` is a reserved parameter, pass 0.
        function offer(uint8 offerType, bytes calldata data, uint16 options)
            external
            returns (AgreementDetails memory details);

        /// Emitted when `offer()` stores a new or updated RCA offer.
        event OfferStored(
            bytes16 indexed agreementId,
            address indexed payer,
            uint8 indexed offerType,
            bytes32 offerHash
        );
    }
}
