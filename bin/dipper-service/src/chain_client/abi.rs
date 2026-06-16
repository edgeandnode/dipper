//! Contract ABI definitions for on-chain interactions.

use thegraph_core::alloy::sol;

sol! {
    /// RecurringCollector contract interface (the EIP-712 domain read dipper
    /// needs). Full contract: `RecurringCollector.sol` in `graphprotocol/contracts`.
    #[allow(missing_docs)]
    interface IRecurringCollector {
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

    /// RecurringAgreementManager (RAM) contract interface (minimal subset). In
    /// protocol-managed mode the manager is the RCA payer; dipper drives it as an
    /// unsigned operator while the manager funds escrow.
    #[allow(missing_docs)]
    interface IRecurringAgreementManager {
        /// Offer an agreement through the manager. `offerType` mirrors
        /// `RecurringCollector` (1 = OFFER_TYPE_NEW); `offerData` is the
        /// ABI-encoded `RecurringCollectionAgreement` whose payer is this manager.
        function offerAgreement(address collector, uint8 offerType, bytes calldata offerData)
            external
            returns (bytes16 agreementId);

        /// Cancel an agreement through the manager. `versionHash` is the EIP-712
        /// terms hash the collector stored; `options` selects the cancel scope
        /// (1 = active, 2 = pending).
        function cancelAgreement(address collector, bytes16 agreementId, bytes32 versionHash, uint16 options)
            external;

        /// Emitted when the manager stores a new agreement.
        event AgreementAdded(
            bytes16 indexed agreementId,
            address indexed collector,
            address indexed dataService,
            address provider
        );

        /// Emitted when the manager cancels an agreement.
        event AgreementRemoved(bytes16 indexed agreementId);
    }
}
