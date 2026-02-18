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
}
