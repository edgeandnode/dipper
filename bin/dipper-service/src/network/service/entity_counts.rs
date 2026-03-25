//! Fetch latest claimed entity counts per agreement from the indexing-payments subgraph.
//!
//! Used to enrich optimistic DIPs fee estimates with the entity component.
//! The subgraph's `AgreementLatestCollection` mutable entity tracks the
//! most recent collection per agreement, including the entity count the
//! indexer claimed.

use std::{collections::HashMap, time::Duration};

use dipper_core::ids::IndexingAgreementId;
use serde::Deserialize;
use url::Url;

/// Shared HTTP client for subgraph queries, constructed once.
static CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(reqwest::Client::new);

/// Fetch latest claimed entity counts for a set of agreements.
///
/// Queries the indexing-payments subgraph's `AgreementLatestCollection`
/// entities. Returns a map from agreement ID to entity count. Agreements
/// with no collection events are absent from the result.
///
/// Returns an empty map on any failure (network, parse, timeout) so the
/// caller can fall back to base-rate-only fee estimation.
pub async fn fetch_entity_counts(
    endpoint: &Url,
    agreement_ids: &[IndexingAgreementId],
    timeout: Duration,
) -> HashMap<IndexingAgreementId, u64> {
    if agreement_ids.is_empty() {
        return HashMap::new();
    }

    let hex_ids: Vec<String> = agreement_ids
        .iter()
        .map(|id| {
            let bytes = id.as_bytes();
            format!(
                "0x{}",
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            )
        })
        .collect();

    let body = serde_json::json!({
        "query": QUERY,
        "variables": { "agreementIds": hex_ids },
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

    parse_entity_count_response(data.agreement_latest_collections)
}

const QUERY: &str = r#"
    query LatestEntityCounts($agreementIds: [Bytes!]!) {
        agreementLatestCollections(
            where: { agreementId_in: $agreementIds }
            first: 1000
        ) {
            agreementId
            entities
        }
    }
"#;

/// Parse entity count entries from the subgraph response.
///
/// The subgraph returns `agreementId` as `0x`-prefixed hex (bytes16).
/// Entries that fail to parse are skipped with a debug log.
fn parse_entity_count_response(
    entries: Vec<EntityCountEntity>,
) -> HashMap<IndexingAgreementId, u64> {
    let mut result = HashMap::new();
    for entry in entries {
        let Some(agreement_id) = parse_agreement_id(&entry.agreement_id) else {
            tracing::debug!(
                raw = %entry.agreement_id,
                "skipping unparseable agreement ID in entity count response"
            );
            continue;
        };
        let Ok(entities) = entry.entities.parse::<u64>() else {
            continue;
        };
        result.insert(agreement_id, entities);
    }

    tracing::debug!(
        agreement_count = result.len(),
        "fetched entity counts from subgraph"
    );

    result
}

/// Parse a hex-encoded agreement ID (bytes16) from the subgraph.
fn parse_agreement_id(hex_str: &str) -> Option<IndexingAgreementId> {
    let hex = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    if hex.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_digit(chunk[0])?;
        let lo = hex_digit(chunk[1])?;
        arr[i] = (hi << 4) | lo;
    }
    Some(IndexingAgreementId::from_bytes(arr))
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct GraphQLResponse {
    data: Option<EntityCountData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntityCountData {
    agreement_latest_collections: Vec<EntityCountEntity>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EntityCountEntity {
    agreement_id: String,
    entities: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_agreement_id() -> IndexingAgreementId {
        IndexingAgreementId::from_bytes([1u8; 16])
    }

    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn test_parse_agreement_id_valid() {
        let id = test_agreement_id();
        let hex = format!("0x{}", bytes_to_hex(id.as_bytes()));
        let parsed = parse_agreement_id(&hex).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_parse_agreement_id_no_prefix() {
        let id = test_agreement_id();
        let hex = bytes_to_hex(id.as_bytes());
        let parsed = parse_agreement_id(&hex).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_parse_agreement_id_invalid() {
        assert!(parse_agreement_id("not-hex").is_none());
        assert!(parse_agreement_id("0x0102").is_none()); // too short
        assert!(parse_agreement_id("").is_none());
    }

    #[test]
    fn test_parse_entity_count_response_valid() {
        let id = test_agreement_id();
        let hex = format!("0x{}", bytes_to_hex(id.as_bytes()));

        let entries = vec![EntityCountEntity {
            agreement_id: hex,
            entities: "5000".to_string(),
        }];

        let result = parse_entity_count_response(entries);
        assert_eq!(result.len(), 1);
        assert_eq!(result[&id], 5000);
    }

    #[test]
    fn test_parse_entity_count_response_invalid_id_skipped() {
        let entries = vec![EntityCountEntity {
            agreement_id: "bad".to_string(),
            entities: "100".to_string(),
        }];

        let result = parse_entity_count_response(entries);
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_entity_count_response_invalid_entities_skipped() {
        let id = test_agreement_id();
        let hex = format!("0x{}", bytes_to_hex(id.as_bytes()));

        let entries = vec![EntityCountEntity {
            agreement_id: hex,
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
    async fn test_fetch_entity_counts_empty_agreements() {
        let url: Url = "http://localhost:9999/subgraphs/name/test".parse().unwrap();
        let result = fetch_entity_counts(&url, &[], Duration::from_secs(1)).await;
        assert!(result.is_empty());
    }
}
