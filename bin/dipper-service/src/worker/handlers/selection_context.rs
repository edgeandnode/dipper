//! Shared utilities for gathering IISA selection context.

use std::{collections::HashMap, time::Duration};

use dipper_iisa::SelectionContext;
use thegraph_core::{DeploymentId, IndexerId};
use url::Url;

use crate::{
    network::service::entity_counts::fetch_entity_counts,
    registry::{AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Seconds in 30 days.
const SECONDS_PER_30_DAYS: f64 = 86400.0 * 30.0;

/// 1 GRT = 10^18 wei.
const WEI_PER_GRT: f64 = 1e18;

/// Timeout for entity count subgraph queries.
const ENTITY_COUNT_QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What pending agreements exist across all deployments
/// - Which indexers have recently declined agreements (within lookback windows)
/// - Which indexers are on the denylist and should be excluded entirely
/// - Optimistic DIPs fees from accepted agreement vouchers
///
/// # Parameters
///
/// - `declined_indexer_lookback_days`: Standard exclusion window for declined indexers
///   (CanceledByIndexer, Expired, Rejected with OTHER/UNSPECIFIED reason)
/// - `price_rejection_lookback_days`: Shorter window for PRICE_TOO_LOW rejections
///   (allows retry after IISA price refresh)
/// - `signer_rejection_lookback_minutes`: Very short window for SIGNER_NOT_AUTHORISED
///   rejections (transient escrow signer configuration issue)
pub async fn gather_selection_context<R>(
    registry: &R,
    deployment_id: &DeploymentId,
    declined_indexer_lookback_days: i32,
    price_rejection_lookback_days: i32,
    signer_rejection_lookback_minutes: i32,
    entity_count_subgraph_url: Option<&Url>,
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
    // - SIGNER_NOT_AUTHORISED: signer_rejection_lookback_minutes (transient auth issue)
    // - Other rejections: declined_indexer_lookback_days (standard exclusion)
    let declined_indexers = registry
        .get_declined_indexers_by_deployment(
            declined_indexer_lookback_days,
            price_rejection_lookback_days,
            signer_rejection_lookback_minutes,
        )
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Get denied indexers that should be excluded from selection
    let indexer_denylist = registry
        .get_indexer_denylist()
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Compute optimistic DIPs fees from active agreement vouchers. When
    // the entity count subgraph URL is available, entity fees are included.
    let optimistic_dips_fees =
        compute_optimistic_dips_fees(registry, entity_count_subgraph_url).await?;

    Ok(SelectionContext {
        existing_indexers,
        pending_agreements,
        declined_indexers,
        indexer_denylist,
        optimistic_dips_fees,
        // chain_id and max_grt_per_30_days are set by the caller after gathering
        // the base context, since they depend on the deployment's chain ID.
        ..Default::default()
    })
}

/// Compute optimistic DIPs fees per indexer in GRT per 30 days.
///
/// For each active agreement, computes the expected fee rate:
/// - If entity counts are available from the subgraph:
///   `fee_tps = base_tps + entity_tps * entities`
/// - Otherwise: `fee_tps = base_tps` (base rate only)
///
/// Sums per indexer and converts wei/second to GRT/30d.
async fn compute_optimistic_dips_fees<R>(
    registry: &R,
    entity_count_subgraph_url: Option<&Url>,
) -> JobResult<HashMap<IndexerId, f64>>
where
    R: AgreementRegistry,
{
    let rates = registry
        .get_agreement_fee_rates()
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Fetch entity counts from the subgraph if available.
    let entity_counts = if let Some(url) = entity_count_subgraph_url {
        let deployment_ids: Vec<DeploymentId> = rates.iter().map(|r| r.deployment_id).collect();
        fetch_entity_counts(url, &deployment_ids, ENTITY_COUNT_QUERY_TIMEOUT).await
    } else {
        HashMap::new()
    };

    let optimistic_dips_fees = sum_fee_rates(&rates, &entity_counts);

    if !optimistic_dips_fees.is_empty() {
        let enriched = rates
            .iter()
            .filter(|r| entity_counts.contains_key(&r.deployment_id))
            .count();
        tracing::debug!(
            indexer_count = optimistic_dips_fees.len(),
            agreement_count = rates.len(),
            enriched_with_entities = enriched,
            "computed optimistic DIPs fees for IISA"
        );
    }

    Ok(optimistic_dips_fees)
}

/// Sum fee rates per indexer and convert to GRT per 30 days.
///
/// When entity counts are available for a deployment, includes the
/// entity component: `fee_tps = base_tps + entity_tps * entities`.
/// Otherwise uses base rate only.
fn sum_fee_rates(
    rates: &[crate::registry::AgreementFeeRate],
    entity_counts: &HashMap<DeploymentId, u64>,
) -> HashMap<IndexerId, f64> {
    let mut fees: HashMap<IndexerId, f64> = HashMap::new();
    for rate in rates {
        let fee_tps = if let Some(&entities) = entity_counts.get(&rate.deployment_id) {
            rate.tokens_per_second + rate.tokens_per_entity_per_second * entities as f64
        } else {
            rate.tokens_per_second
        };
        *fees.entry(rate.indexer_id).or_default() += fee_tps;
    }
    fees.into_iter()
        .map(|(id, tps_wei)| (id, wei_per_second_to_grt_per_30d(tps_wei)))
        .collect()
}

/// Convert wei/second to GRT per 30 days.
fn wei_per_second_to_grt_per_30d(wei_per_second: f64) -> f64 {
    wei_per_second * SECONDS_PER_30_DAYS / WEI_PER_GRT
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::AgreementFeeRate;

    #[test]
    fn test_wei_per_second_to_grt_per_30d() {
        // 1 GRT/second = 2,592,000 GRT/30d
        let one_grt_per_sec = 1e18; // 1 GRT in wei
        let result = wei_per_second_to_grt_per_30d(one_grt_per_sec);
        assert!((result - 2_592_000.0).abs() < 0.01);

        // ~3.858 wei/second ~ 10 GRT/30d
        // 10 GRT/30d = 10 * 1e18 / (86400 * 30) = 3_858_024_691_358.025 wei/sec
        let wei_per_sec = 10.0 * 1e18 / (86400.0 * 30.0);
        let result = wei_per_second_to_grt_per_30d(wei_per_sec);
        assert!((result - 10.0).abs() < 1e-6);

        // Zero in, zero out
        assert_eq!(wei_per_second_to_grt_per_30d(0.0), 0.0);
    }

    #[test]
    fn test_sum_fee_rates_base_only() {
        let indexer_a: IndexerId = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_b: IndexerId = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .unwrap();
        let deployment: DeploymentId =
            "0x0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .unwrap();

        let rates = vec![
            AgreementFeeRate {
                indexer_id: indexer_a,
                deployment_id: deployment,
                tokens_per_second: 1e18,
                tokens_per_entity_per_second: 5e14,
            },
            AgreementFeeRate {
                indexer_id: indexer_a,
                deployment_id: deployment,
                tokens_per_second: 2e18,
                tokens_per_entity_per_second: 0.0,
            },
            AgreementFeeRate {
                indexer_id: indexer_b,
                deployment_id: deployment,
                tokens_per_second: 0.5e18,
                tokens_per_entity_per_second: 1e15,
            },
        ];

        // No entity counts — base rate only
        let fees = sum_fee_rates(&rates, &HashMap::new());

        // indexer_a: (1 + 2) GRT/sec * 2,592,000 sec = 7,776,000 GRT/30d
        assert!((fees[&indexer_a] - 7_776_000.0).abs() < 1.0);
        // indexer_b: 0.5 GRT/sec * 2,592,000 sec = 1,296,000 GRT/30d
        assert!((fees[&indexer_b] - 1_296_000.0).abs() < 1.0);
    }

    #[test]
    fn test_sum_fee_rates_with_entity_counts() {
        let indexer_a: IndexerId = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let deployment: DeploymentId =
            "0x0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .unwrap();

        let rates = vec![AgreementFeeRate {
            indexer_id: indexer_a,
            deployment_id: deployment,
            tokens_per_second: 1e18,            // 1 GRT/sec base
            tokens_per_entity_per_second: 1e15, // 0.001 GRT/entity/sec
        }];

        let mut entity_counts = HashMap::new();
        entity_counts.insert(deployment, 1000u64);

        let fees = sum_fee_rates(&rates, &entity_counts);

        // fee_tps = 1e18 + 1e15 * 1000 = 1e18 + 1e18 = 2e18
        // 2 GRT/sec * 2,592,000 = 5,184,000 GRT/30d
        assert!((fees[&indexer_a] - 5_184_000.0).abs() < 1.0);
    }

    #[test]
    fn test_sum_fee_rates_empty() {
        let fees = sum_fee_rates(&[], &HashMap::new());
        assert!(fees.is_empty());
    }
}
