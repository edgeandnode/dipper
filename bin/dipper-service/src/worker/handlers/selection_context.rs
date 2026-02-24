//! Shared utilities for gathering IISA selection context.

use dipper_iisa::SelectionContext;
use thegraph_core::DeploymentId;

use crate::{
    registry::{AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What pending agreements exist across all deployments
/// - Which indexers have recently declined agreements (within lookback windows)
/// - Which indexers are on the denylist and should be excluded entirely
///
/// # Parameters
///
/// - `declined_indexer_lookback_days`: Standard exclusion window for declined indexers
///   (CanceledByIndexer, Expired, Rejected with OTHER/UNSPECIFIED reason)
/// - `price_rejection_lookback_days`: Shorter window for PRICE_TOO_LOW rejections
///   (allows retry after IISA price refresh)
pub async fn gather_selection_context<R>(
    registry: &R,
    deployment_id: &DeploymentId,
    declined_indexer_lookback_days: i32,
    price_rejection_lookback_days: i32,
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

    // Get indexers that declined agreements within their respective lookback periods:
    // - PRICE_TOO_LOW: price_rejection_lookback_days (until next IISA price refresh)
    // - Other rejections: declined_indexer_lookback_days (standard exclusion)
    let declined_indexers = registry
        .get_declined_indexers_by_deployment(
            declined_indexer_lookback_days,
            price_rejection_lookback_days,
        )
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
        // chain_id and max_grt_per_30_days are set by the caller after gathering
        // the base context, since they depend on the deployment's chain ID.
        ..Default::default()
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
