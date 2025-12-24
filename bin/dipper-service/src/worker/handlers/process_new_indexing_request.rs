use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, Indexer as IndexerCandidate, SelectionContext};
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};

use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::NetworkProvider,
    registry::{
        AgreementRegistry, IndexingAgreementStatus, IndexingAgreementVoucher,
        IndexingAgreementVoucherMetadata, IndexingRequestRegistry,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::{
        result::{JobError, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, N, W, I> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub chain_price: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    pub registry: R,
    pub network: N,
    pub queue: W,
    pub iisa: I,
}

/// Given a new indexing request, run the IISA and get a list of indexers that
/// can index the subgraph deployment.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the subgraph deployment
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph deployment
    pub deployment_chain_id: ChainId,
    /// The maximum number of indexers to select
    pub num_candidates: usize,
}

pub async fn handle<R, N, W, I>(
    ctx: Ctx<R, N, W, I>,
    Message {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    }: &Message,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Get the indexers that are not indexing the deployment and treat it as the raw candidate list
    // and pass it to the IISA to get the final list of candidates
    let indexers = ctx
        .network
        .get_indexers_not_indexing_a_deployment_id(deployment_id)
        .into_iter()
        .map(|indexer| IndexerCandidate {
            id: indexer.id,
            url: indexer.url,
        })
        .collect::<Vec<_>>();
    if indexers.is_empty() {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            "No indexers available to fulfill the indexing request"
        );
        return Ok(());
    }

    // Gather load balancing context for IISA
    let context = gather_selection_context(&ctx.registry, deployment_id, &indexers).await?;

    let candidates = ctx
        .iisa
        .select(*deployment_id, indexers, *num_candidates, &context)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    if candidates.is_empty() {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            "No candidates selected to fulfill the indexing request"
        );
        return Ok(());
    }

    // Create indexing agreements for the selected indexers and register them in the registry
    for candidate in candidates {
        let voucher_metadata = {
            let prices = match ctx.chain_price.get(deployment_chain_id) {
                Some(prices) => prices,
                None => {
                    tracing::warn!(
                        indexing_request_id=%indexing_request_id,
                        deployment_id=%deployment_id,
                        chain_id=%deployment_chain_id,
                        "Chain prices not found"
                    );
                    return Err(JobError::Fatal(anyhow::anyhow!(
                        "Chain prices not found for chain_id"
                    )));
                }
            };

            IndexingAgreementVoucherMetadata {
                base_price_per_epoch: prices.base_price_per_epoch,
                price_per_entity: prices.price_per_entity,
                subgraph_deployment_id: *deployment_id,
                protocol_network: ctx.signer.chain_id(),
                chain_id: *deployment_chain_id,
            }
        };

        let voucher = IndexingAgreementVoucher {
            payer: ctx.signer.address(),
            recipient: candidate.id.into_inner(),
            service: ctx.agreement_conf.service(),
            duration_epochs: ctx.agreement_conf.duration_epochs(),
            max_initial_amount: ctx.agreement_conf.max_initial_amount(),
            max_ongoing_amount_per_epoch: ctx.agreement_conf.max_ongoing_amount_per_epoch(),
            min_epochs_per_collection: ctx.agreement_conf.min_epochs_per_collection(),
            max_epochs_per_collection: ctx.agreement_conf.max_epochs_per_collection(),
            deadline: Default::default(), // TODO(v2): add the deadline
            metadata: voucher_metadata,
        };

        let agreement_id = ctx
            .registry
            .register_new_indexing_agreement(
                *indexing_request_id,
                *deployment_id,
                candidate.id,
                candidate.url.clone(),
                voucher,
            )
            .await
            .map_err(|err| JobError::Fatal(err.into()))?;

        if let Err(err) = ctx
            .queue
            .send_indexing_agreement_proposal(
                candidate.url,
                agreement_id,
                *indexing_request_id,
                *deployment_id,
                *deployment_chain_id,
            )
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_proposal'");
            return Err(JobError::Fatal(err));
        }
    }

    Ok(())
}

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What other deployments each candidate indexer is currently working on
async fn gather_selection_context<R>(
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
