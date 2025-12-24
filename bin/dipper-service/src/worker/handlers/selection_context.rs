//! Shared utilities for gathering IISA selection context.

use std::collections::HashMap;

use dipper_iisa::{Indexer as IndexerCandidate, SelectionContext};
use thegraph_core::{DeploymentId, IndexerId};

use crate::{
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What other deployments each candidate indexer is currently working on
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

    // Build pending agreements map for all candidates in a single batch query
    // This tells IISA what other work each candidate is currently handling
    let candidate_ids: Vec<IndexerId> = candidates.iter().map(|c| c.id).collect();

    let all_agreements = registry
        .get_active_indexing_agreements_by_indexer_ids(&candidate_ids)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Group agreements by indexer ID
    let mut pending_agreements: HashMap<IndexerId, Vec<DeploymentId>> = HashMap::new();
    for agreement in all_agreements {
        pending_agreements
            .entry(agreement.indexer.id)
            .or_default()
            .push(agreement.voucher.metadata.subgraph_deployment_id);
    }

    Ok(SelectionContext {
        existing_indexers,
        pending_agreements,
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
