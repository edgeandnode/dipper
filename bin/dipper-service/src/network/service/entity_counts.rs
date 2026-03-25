//! Fetch latest entity counts per deployment from the indexing-payments subgraph.
//!
//! Used to enrich optimistic DIPs fee estimates with the entity component.
//! The subgraph's `DeploymentLatestEntityCount` mutable entity tracks the
//! most recent entity count reported by any indexer for each deployment.

use std::{collections::HashMap, time::Duration};

use serde::Deserialize;
use thegraph_core::DeploymentId;
use url::Url;

/// Shared HTTP client for subgraph queries, constructed once.
static CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(|| reqwest::Client::new());

/// Fetch latest entity counts for a set of deployments.
///
/// Queries the indexing-payments subgraph's `DeploymentLatestEntityCount`
/// entities. Returns a map from deployment ID to entity count. Deployments
/// with no collection events are absent from the result.
///
/// Returns an empty map on any failure (network, parse, timeout) so the
/// caller can fall back to base-rate-only fee estimation.
pub async fn fetch_entity_counts(
    endpoint: &Url,
    deployment_ids: &[DeploymentId],
    timeout: Duration,
) -> HashMap<DeploymentId, u64> {
    if deployment_ids.is_empty() {
        return HashMap::new();
    }

    let hex_ids: Vec<String> = deployment_ids.iter().map(|d| format!("{:#x}", d)).collect();

    let body = serde_json::json!({
        "query": QUERY,
        "variables": { "deployments": hex_ids },
    });

    let response = match CLIENT
        .post(endpoint.as_str())
        .timeout(timeout)
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "failed to fetch entity counts from subgraph"
            );
            return HashMap::new();
        }
    };

    let json: GraphQLResponse = match response.json().await {
        Ok(j) => j,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "failed to parse entity count response"
            );
            return HashMap::new();
        }
    };

    let Some(data) = json.data else {
        if let Some(errors) = json.errors {
            tracing::warn!(
                errors = ?errors,
                "subgraph returned errors for entity count query"
            );
        }
        return HashMap::new();
    };

    parse_entity_count_response(data.deployment_latest_entity_counts)
}

const QUERY: &str = r#"
    query LatestEntityCounts($deployments: [Bytes!]!) {
        deploymentLatestEntityCounts(
            where: { subgraphDeploymentId_in: $deployments }
            first: 1000
        ) {
            subgraphDeploymentId
            entities
        }
    }
"#;

/// Parse entity count entries from the subgraph response.
///
/// The subgraph returns `subgraphDeploymentId` as `0x`-prefixed hex
/// (from AssemblyScript `Bytes.toHexString()`). `DeploymentId::from_str`
/// accepts this format. Entries that fail to parse are silently skipped.
fn parse_entity_count_response(entries: Vec<EntityCountEntity>) -> HashMap<DeploymentId, u64> {
    let mut result = HashMap::new();
    for entry in entries {
        let Ok(deployment_id) = entry.subgraph_deployment_id.parse::<DeploymentId>() else {
            tracing::debug!(
                raw = %entry.subgraph_deployment_id,
                "skipping unparseable deployment ID in entity count response"
            );
            continue;
        };
        let Ok(entities) = entry.entities.parse::<u64>() else {
            continue;
        };
        result.insert(deployment_id, entities);
    }

    tracing::debug!(
        deployment_count = result.len(),
        "fetched entity counts from subgraph"
    );

    result
}

#[derive(Debug, Deserialize)]
struct GraphQLResponse {
    data: Option<EntityCountData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntityCountData {
    deployment_latest_entity_counts: Vec<EntityCountEntity>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntityCountEntity {
    subgraph_deployment_id: String,
    entities: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_id_hex_round_trip() {
        // DeploymentId must accept the 0x-prefixed hex format that the
        // subgraph returns from Bytes.toHexString().
        let cid = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9";
        let deployment: DeploymentId = cid.parse().unwrap();
        let hex = format!("{:#x}", deployment);

        // hex should be 0x-prefixed, 66 chars (0x + 64 hex digits)
        assert!(hex.starts_with("0x"), "expected 0x prefix, got: {hex}");
        assert_eq!(hex.len(), 66);

        // Round-trip: hex -> DeploymentId should succeed
        let parsed: DeploymentId = hex
            .parse()
            .expect("DeploymentId::from_str must accept 0x-prefixed hex");
        assert_eq!(deployment, parsed);
    }

    #[test]
    fn test_parse_entity_count_response_valid() {
        let cid = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9";
        let deployment: DeploymentId = cid.parse().unwrap();
        let hex = format!("{:#x}", deployment);

        let entries = vec![EntityCountEntity {
            subgraph_deployment_id: hex,
            entities: "5000".to_string(),
        }];

        let result = parse_entity_count_response(entries);
        assert_eq!(result.len(), 1);
        assert_eq!(result[&deployment], 5000);
    }

    #[test]
    fn test_parse_entity_count_response_invalid_deployment_skipped() {
        let entries = vec![EntityCountEntity {
            subgraph_deployment_id: "not-a-valid-id".to_string(),
            entities: "100".to_string(),
        }];

        let result = parse_entity_count_response(entries);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_entity_count_response_invalid_entities_skipped() {
        let cid = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9";
        let hex = format!("{:#x}", cid.parse::<DeploymentId>().unwrap());

        let entries = vec![EntityCountEntity {
            subgraph_deployment_id: hex,
            entities: "not-a-number".to_string(),
        }];

        let result = parse_entity_count_response(entries);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_entity_count_response_empty() {
        let result = parse_entity_count_response(vec![]);
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_entity_counts_empty_deployments() {
        let url: Url = "http://localhost:9999/subgraphs/name/test".parse().unwrap();
        let result = fetch_entity_counts(&url, &[], Duration::from_secs(1)).await;
        assert!(result.is_empty());
    }
}
