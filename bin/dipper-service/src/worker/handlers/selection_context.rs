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

    // Build pending agreements map for each candidate
    // This tells IISA what other work each candidate is currently handling
    let mut pending_agreements: HashMap<IndexerId, Vec<DeploymentId>> = HashMap::new();
    for candidate in candidates {
        let agreements = registry
            .get_indexing_agreements_by_indexer_id(&candidate.id)
            .await
            .map_err(|err| JobError::Fatal(err.into()))?;

        let deployment_ids: Vec<DeploymentId> = agreements
            .into_iter()
            .filter(|a| is_active_agreement(&a.status))
            .map(|a| a.voucher.metadata.subgraph_deployment_id)
            .collect();

        if !deployment_ids.is_empty() {
            pending_agreements.insert(candidate.id, deployment_ids);
        }
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
