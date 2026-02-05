use super::handlers::{
    ProcessIndexingAgreementCancellation, ProcessIndexingRequestCancellation,
    ProcessNewIndexingRequest, ReassessIndexingRequest, SendIndexingAgreementCancellation,
    SendIndexingAgreementProposal,
};

/// The queue worker message enum
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    /// Process a new indexing request.
    ///
    /// Given an indexing request, run the IISA and get a list of indexers that
    /// can index the subgraph deployment.
    ///
    /// See [`ProcessNewIndexingRequest`] for more details.
    ProcessNewIndexingRequest(ProcessNewIndexingRequest),

    /// Process indexing request cancellation.
    ///
    /// When a customer cancels an indexing request, this message is sent to the
    /// queue worker to notify it that the indexing request has been cancelled.
    ///
    /// This should trigger the queue worker to cancel any associated indexing
    /// agreements and notify the indexers.
    ///
    /// See [`ProcessIndexingRequestCancellation`] for more details.
    ProcessIndexingRequestCancellation(ProcessIndexingRequestCancellation),

    /// Reassess an indexing request against the current IISA target state.
    ///
    /// Periodically re-evaluates open indexing requests by diffing the IISA target
    /// group against current active agreements, canceling stale assignments and
    /// creating new ones as needed.
    ///
    /// See [`ReassessIndexingRequest`] for more details.
    ReassessIndexingRequest(ReassessIndexingRequest),

    /// Send an indexing agreement proposal to the indexer.
    ///
    /// See [`SendIndexingAgreementProposal`] for more details.
    SendIndexingAgreementProposal(SendIndexingAgreementProposal),

    /// Send an indexing agreement cancellation to the indexer.
    ///
    /// See [`SendIndexingAgreementCancellation`] for more details.
    SendIndexingAgreementCancellation(SendIndexingAgreementCancellation),

    /// Process indexing agreement cancellation triggered by the indexer.
    ///
    /// When an indexer cancels an indexing agreement, a new indexer must be selected
    /// to fulfill the indexing request.
    ///
    /// See [`ProcessIndexingAgreementCancellation`] for more details.
    ProcessIndexingAgreementIndexerCancellation(ProcessIndexingAgreementCancellation),

    /// Process an indexing agreement cancellation triggered by the customer.
    ///
    /// When a customer cancels an indexing agreement, the queue worker must notify
    /// the indexer that the agreement has been cancelled. Additionally, a new indexer
    /// must be selected to fulfill the indexing request.
    ///
    /// See [`ProcessIndexingAgreementCancellation`] for more details.
    ProcessIndexingAgreementRequesterCancellation(ProcessIndexingAgreementCancellation),
}
