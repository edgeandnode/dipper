//! Fetch latest entity counts per deployment from the indexing-payments subgraph.
//!
//! Used to enrich optimistic DIPs fee estimates with the entity component.
//! The subgraph's `DeploymentLatestEntityCount` mutable entity tracks the
//! most recent entity count reported by any indexer for each deployment.

use std::{collections::HashMap, time::Duration};

use serde::Deserialize;
use thegraph_core::DeploymentId;
use url::Url;

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

    let query = r#"
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

    let variables = serde_json::json!({ "deployments": hex_ids });
    let body = serde_json::json!({ "query": query, "variables": variables });

    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .unwrap_or_default();

    let response = match client.post(endpoint.as_str()).json(&body).send().await {
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

    let mut result = HashMap::new();
    for entry in data.deployment_latest_entity_counts {
        let Ok(deployment_id) = entry.subgraph_deployment_id.parse::<DeploymentId>() else {
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
