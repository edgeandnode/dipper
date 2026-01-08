use std::{collections::BTreeMap, sync::Arc, time::Duration};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, Indexer as IndexerCandidate, SelectionError};
use rand::seq::IndexedRandom;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};

use super::selection_context::gather_selection_context;
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::NetworkProvider,
    registry::{
        AgreementRegistry, IndexingAgreementVoucher, IndexingAgreementVoucherMetadata,
        IndexingRequestRegistry,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::{
        result::{JobError, JobMeta, JobResult},
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

/// Duration after which random fallback is used if IISA remains unavailable
const FALLBACK_THRESHOLD: time::Duration = time::Duration::hours(6);

pub async fn handle<R, N, W, I>(
    ctx: Ctx<R, N, W, I>,
    Message {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    }: &Message,
    job_meta: JobMeta,
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

    // Try IISA selection, with random fallback if IISA has been unavailable for too long
    let candidates = match ctx
        .iisa
        .select(*deployment_id, indexers.clone(), *num_candidates, &context)
        .await
    {
        Ok(candidates) => candidates,
        Err(SelectionError::IisaServiceUnavailable) => {
            if job_meta.age_exceeds(FALLBACK_THRESHOLD) {
                // IISA unavailable for 6+ hours, fall back to random selection
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    age_hours=%((time::OffsetDateTime::now_utc() - job_meta.created_at).whole_hours()),
                    "IISA unavailable for 6+ hours, using random selection fallback"
                );
                let mut rng = rand::rng();
                indexers
                    .choose_multiple(&mut rng, *num_candidates)
                    .cloned()
                    .collect()
            } else {
                tracing::warn!("IISA service unavailable, will retry");
                return Err(JobError::Retryable(
                    SelectionError::IisaServiceUnavailable.into(),
                    Duration::from_secs(5),
                ));
            }
        }
        Err(SelectionError::Error(e)) => return Err(JobError::Fatal(e)),
    };
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
