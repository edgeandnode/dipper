//! Contract ABI definitions for on-chain interactions.

use thegraph_core::alloy::sol;

sol! {
    /// SubgraphService contract interface (minimal subset for agreement cancellation).
    ///
    /// The full contract is at
    /// `packages/subgraph-service/contracts/SubgraphService.sol` in
    /// `graphprotocol/contracts`. This interface only includes the methods
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

        /// Reverted by SubgraphService when the agreement is not in an active
        /// state at the moment of the call. The cancel path treats this as an
        /// idempotent no-op: the agreement is already canceled (or settled or
        /// expired) on-chain, so resubmission is unnecessary. Dipper matches
        /// the 4-byte selector to drop into the success branch.
        error IndexingAgreementNotActive(bytes16 agreementId);
    }

    /// RecurringCollector contract interface (minimal subset for offer-based RCA authorization).
    ///
    /// The full contract is at
    /// `packages/horizon/contracts/payments/collectors/RecurringCollector.sol`
    /// in `graphprotocol/contracts`.
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
        ///
        /// `state` is a bitmask of flags defined in `IAgreementCollector.sol`
        /// (REGISTERED=1, ACCEPTED=2, NOTICE_GIVEN=4, SETTLED=8, BY_PAYER=16,
        /// BY_PROVIDER=32, UPDATE=128); it is `uint16` on-chain because the
        /// flag values exceed `uint8`. Dipper does not currently decode this
        /// return value, but the layout must match for future use.
        struct AgreementDetails {
            bytes16 agreementId;
            address payer;
            address dataService;
            address serviceProvider;
            bytes32 versionHash;
            uint16 state;
        }

        /// Store a new or updated RCA offer on-chain.
        ///
        /// `offerType` = 1 for OFFER_TYPE_NEW, 2 for OFFER_TYPE_UPDATE
        /// (0 = OFFER_TYPE_NONE, reserved sentinel — submitting 0 reverts).
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

        /// EIP-5267 report of the EIP-712 domain the contract verifies
        /// signatures under. Inherited from OpenZeppelin's EIP712Upgradeable;
        /// dipper fetches it at startup (see `chain_client::eip5267`).
        function eip712Domain()
            external
            view
            returns (
                bytes1 fields,
                string memory name,
                string memory version,
                uint256 chainId,
                address verifyingContract,
                bytes32 salt,
                uint256[] memory extensions
            );
    }
}
