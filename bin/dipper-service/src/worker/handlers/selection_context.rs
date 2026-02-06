//! Shared utilities for gathering IISA selection context.

use dipper_iisa::SelectionContext;
use thegraph_core::DeploymentId;

use crate::{
    registry::{AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementStatus},
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
/// - What pending agreements exist across all deployments
/// - Which indexers have recently declined agreements (within 30 days)
/// - Which indexers are on the denylist and should be excluded entirely
pub async fn gather_selection_context<R>(
    registry: &R,
    deployment_id: &DeploymentId,
) -> JobResult<SelectionContext>
where
    R: AgreementRegistry + IndexerDenylistRegistry,
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

    // Get pending agreements across all deployments.
    // Since IISA handles candidate filtering internally, we pass all existing indexer IDs
    // from active agreements (the existing_indexers we just computed) as the filter.
    let pending_agreements = registry
        .get_pending_agreement_indexers_by_deployment(&existing_indexers)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Get indexers that declined agreements within the lookback period
    let declined_indexers = registry
        .get_declined_indexers_by_deployment(DECLINED_INDEXER_LOOKBACK_DAYS)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Get denied indexers that should be excluded from selection
    let indexer_denylist = registry
        .get_indexer_denylist()
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    Ok(SelectionContext {
        existing_indexers,
        pending_agreements,
        declined_indexers,
        indexer_denylist,
    })
}

/// Check if an agreement status represents an active agreement.
///
/// Active agreements are those that are either pending on-chain acceptance (Created)
/// or confirmed on-chain (AcceptedOnChain).
fn is_active_agreement(status: &IndexingAgreementStatus) -> bool {
    matches!(
        status,
        IndexingAgreementStatus::Created | IndexingAgreementStatus::AcceptedOnChain
    )
}
