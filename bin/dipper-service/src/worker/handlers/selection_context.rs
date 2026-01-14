//! Shared utilities for gathering IISA selection context.

use dipper_iisa::{Indexer as IndexerCandidate, SelectionContext};
use thegraph_core::{DeploymentId, IndexerId};

use crate::{
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Number of days to look back for declined indexers.
///
/// Indexers that declined an agreement within this period will be excluded
/// from selection for that deployment.
const DECLINED_INDEXER_LOOKBACK_DAYS: i32 = 30;

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What other deployments each candidate indexer is currently working on
/// - Which indexers have recently declined agreements (within 30 days)
pub async fn gather_selection_context<R>(
    registry: &R,
    deployment_id: &DeploymentId,
    candidates: &[IndexerCandidate],
) -> JobResult<SelectionContext>
where
    R: AgreementRegistry,
{
    // Get indexers that already have active agreements for this deployment
    let existing_indexers = registry
        .get_indexing_agreements_by_deployment_id(deployment_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
        .into_iter()
        .filter(|a| is_active_agreement(&a.status))
        .map(|a| a.indexer.id)
        .collect::<Vec<_>>();

    // Get pending agreements using database-side aggregation
    // This returns deployment -> [indexers] directly, avoiding full object transfer
    let candidate_ids: Vec<IndexerId> = candidates.iter().map(|c| c.id).collect();

    let pending_agreements = registry
        .get_pending_agreement_indexers_by_deployment(&candidate_ids)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Get indexers that declined agreements within the lookback period
    let declined_indexers = registry
        .get_declined_indexers_by_deployment(DECLINED_INDEXER_LOOKBACK_DAYS)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    Ok(SelectionContext {
        existing_indexers,
        pending_agreements,
        declined_indexers,
        ..Default::default()
    })
}

/// Check if an agreement status represents an active agreement.
///
/// Active agreements are those that are either pending acceptance (Created)
/// or currently in effect (Accepted).
fn is_active_agreement(status: &IndexingAgreementStatus) -> bool {
    matches!(
        status,
        IndexingAgreementStatus::Created | IndexingAgreementStatus::Accepted { .. }
    )
}
