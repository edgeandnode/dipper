//! Shared utilities for gathering IISA selection context.

use std::collections::HashMap;

use dipper_iisa::SelectionContext;
use thegraph_core::{DeploymentId, IndexerId};

use crate::{
    network::service::entity_count_cache::EntityCountCache,
    registry::{AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobResult},
};

/// Seconds in 30 days.
const SECONDS_PER_30_DAYS: f64 = 86400.0 * 30.0;

/// 1 GRT = 10^18 wei.
const WEI_PER_GRT: f64 = 1e18;

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What pending agreements exist across all deployments
/// - Which indexers have recently declined agreements (within lookback windows)
/// - Which indexers are on the denylist and should be excluded entirely
/// - Optimistic DIPs fees from accepted agreement vouchers, enriched with
///   entity counts from the shared cache when available
pub async fn gather_selection_context<R>(
    registry: &R,
    deployment_id: &DeploymentId,
    declined_indexer_lookback_days: i32,
    price_rejection_lookback_days: i32,
    signer_rejection_lookback_minutes: i32,
    entity_count_cache: &EntityCountCache,
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

    // Get pending agreements across all deployments
    let pending_agreements = registry
        .get_pending_agreement_indexers_by_deployment(&existing_indexers)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Get indexers that declined within their respective lookback periods
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

    // Compute optimistic DIPs fees from active agreements, enriched with
    // entity counts from the shared cache when available.
    let optimistic_dips_fees = compute_optimistic_dips_fees(registry, entity_count_cache).await?;

    Ok(SelectionContext {
        existing_indexers,
        pending_agreements,
        declined_indexers,
        indexer_denylist,
        optimistic_dips_fees,
        ..Default::default()
    })
}

/// Compute optimistic DIPs fees per indexer in GRT per 30 days.
///
/// For each active agreement, computes the expected fee rate:
/// - If entity counts are available in the cache:
///   `fee_rate = base_rate + entity_rate * entities`
/// - Otherwise: `fee_rate = base_rate` (base rate only)
///
/// Sums per indexer and converts wei/second to GRT/30d.
async fn compute_optimistic_dips_fees<R>(
    registry: &R,
    entity_count_cache: &EntityCountCache,
) -> JobResult<HashMap<IndexerId, f64>>
where
    R: AgreementRegistry,
{
    let rates = registry
        .get_agreement_fee_rates()
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    let cache = entity_count_cache.read().await;
    let optimistic_dips_fees = sum_fee_rates(&rates, &cache);
    let enriched = rates
        .iter()
        .filter(|r| cache.contains_key(&(r.indexer_id, r.deployment_id)))
        .count();
    drop(cache);

    if !optimistic_dips_fees.is_empty() {
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
/// When the cache has entity counts for an (indexer, deployment) pair,
/// includes the entity component:
/// `fee_rate = base_rate + entity_rate * claimed_entities`.
/// Otherwise uses base rate only.
fn sum_fee_rates(
    rates: &[crate::registry::AgreementFeeRate],
    entity_counts: &HashMap<(IndexerId, DeploymentId), u64>,
) -> HashMap<IndexerId, f64> {
    let mut fees: HashMap<IndexerId, f64> = HashMap::new();
    for rate in rates {
        let fee_rate =
            if let Some(&entities) = entity_counts.get(&(rate.indexer_id, rate.deployment_id)) {
                rate.tokens_per_second + rate.tokens_per_entity_per_second * entities as f64
            } else {
                rate.tokens_per_second
            };
        *fees.entry(rate.indexer_id).or_default() += fee_rate;
    }
    fees.into_iter()
        .map(|(id, wei_per_sec)| (id, wei_per_second_to_grt_per_30d(wei_per_sec)))
        .collect()
}

/// Convert wei/second to GRT per 30 days.
fn wei_per_second_to_grt_per_30d(wei_per_second: f64) -> f64 {
    wei_per_second * SECONDS_PER_30_DAYS / WEI_PER_GRT
}

/// Check if an agreement status represents an active agreement.
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

    fn test_agreement_id(n: u8) -> dipper_core::ids::IndexingAgreementId {
        dipper_core::ids::IndexingAgreementId::from_bytes([n; 16])
    }

    #[test]
    fn test_wei_per_second_to_grt_per_30d() {
        let one_grt_per_sec = 1e18;
        let result = wei_per_second_to_grt_per_30d(one_grt_per_sec);
        assert!((result - 2_592_000.0).abs() < 0.01);

        let wei_per_sec = 10.0 * 1e18 / (86400.0 * 30.0);
        let result = wei_per_second_to_grt_per_30d(wei_per_sec);
        assert!((result - 10.0).abs() < 1e-6);

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
                agreement_id: test_agreement_id(1),
                indexer_id: indexer_a,
                deployment_id: deployment,
                tokens_per_second: 1e18,
                tokens_per_entity_per_second: 5e14,
            },
            AgreementFeeRate {
                agreement_id: test_agreement_id(2),
                indexer_id: indexer_a,
                deployment_id: deployment,
                tokens_per_second: 2e18,
                tokens_per_entity_per_second: 0.0,
            },
            AgreementFeeRate {
                agreement_id: test_agreement_id(3),
                indexer_id: indexer_b,
                deployment_id: deployment,
                tokens_per_second: 0.5e18,
                tokens_per_entity_per_second: 1e15,
            },
        ];

        let fees = sum_fee_rates(&rates, &HashMap::new());

        assert!((fees[&indexer_a] - 7_776_000.0).abs() < 1.0);
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
            agreement_id: test_agreement_id(1),
            indexer_id: indexer_a,
            deployment_id: deployment,
            tokens_per_second: 1e18,
            tokens_per_entity_per_second: 1e15,
        }];

        let mut entity_counts = HashMap::new();
        entity_counts.insert((indexer_a, deployment), 1000u64);

        let fees = sum_fee_rates(&rates, &entity_counts);

        // fee_rate = 1e18 + 1e15 * 1000 = 2e18
        // 2 GRT/sec * 2,592,000 = 5,184,000 GRT/30d
        assert!((fees[&indexer_a] - 5_184_000.0).abs() < 1.0);
    }

    #[test]
    fn test_sum_fee_rates_empty() {
        let fees = sum_fee_rates(&[], &HashMap::new());
        assert!(fees.is_empty());
    }
}
