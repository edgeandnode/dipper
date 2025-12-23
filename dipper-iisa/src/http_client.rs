//! HTTP Client for IISA (Indexing Indexer Selection Algorithm)
//!
//! This module implements a Rust HTTP client for communicating with the IISA container service.
//! The client sends indexer selection requests and receives the selected indexer IDs.
//!
//! The IISA container handles:
//! - Fetching performance data from BigQuery
//! - GeoIP resolution for geographic diversity
//! - Calculating weighted scores for each candidate
//! - Running the selection algorithm

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thegraph_core::{DeploymentId, IndexerId};

use crate::api::{CandidateSelection, Indexer, SelectionError};

/// HTTP client for the IISA container service.
#[derive(Clone)]
pub struct HttpIisaClient {
    client: Client,
    endpoint: String,
}

/// A candidate indexer with ID and URL for the selection request.
#[derive(Debug, Clone, Serialize)]
struct CandidateIndexer {
    /// Indexer ID as hex string (0x...)
    id: String,
    /// Indexer URL endpoint
    url: String,
}

/// Request body for indexer selection endpoints.
#[derive(Debug, Serialize)]
struct SelectionRequest {
    /// The deployment ID to select indexers for
    deployment_id: String,

    /// List of candidate indexers with their URLs
    #[serde(skip_serializing_if = "Option::is_none")]
    candidates: Option<Vec<CandidateIndexer>>,

    /// List of existing indexer IDs already assigned to this deployment
    #[serde(skip_serializing_if = "Option::is_none")]
    existing_indexers: Option<Vec<String>>,

    /// Pending agreements: indexer ID -> list of deployment IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_agreements: Option<HashMap<String, Vec<String>>>,

    /// Number of indexers to select (for select-many)
    #[serde(skip_serializing_if = "Option::is_none")]
    num_candidates: Option<usize>,
}

/// Response from the /select-one endpoint.
#[derive(Debug, Deserialize)]
struct SingleSelectionResponse {
    /// The selected indexer ID, or None if no selection was made
    indexer_id: Option<String>,
}

/// Response from the /select-many endpoint.
#[derive(Debug, Deserialize)]
struct MultiSelectionResponse {
    /// List of selected indexer IDs
    indexer_ids: Vec<String>,
}

impl HttpIisaClient {
    /// Create a new HTTP client for the IISA service.
    ///
    /// # Arguments
    /// * `endpoint` - Base URL of the IISA service (e.g., "http://iisa-service:8080")
    pub fn new(endpoint: String) -> Self {
        let endpoint = if endpoint.ends_with('/') {
            endpoint
        } else {
            format!("{}/", endpoint)
        };

        Self {
            client: Client::new(),
            endpoint,
        }
    }

    /// Check if the IISA service is healthy.
    pub async fn health_check(&self) -> Result<bool, SelectionError> {
        let url = format!("{}health", self.endpoint);

        let response =
            self.client.get(&url).send().await.map_err(|e| {
                SelectionError::Error(anyhow::anyhow!("Health check failed: {}", e))
            })?;

        Ok(response.status().is_success())
    }

    /// Convert Indexer to CandidateIndexer for serialization.
    fn to_candidate(indexer: &Indexer) -> CandidateIndexer {
        CandidateIndexer {
            id: format!("{:#x}", indexer.id),
            url: indexer.url.to_string(),
        }
    }
}

#[async_trait]
impl CandidateSelection for HttpIisaClient {
    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
    ) -> Result<Option<Indexer>, SelectionError> {
        if candidates.is_empty() {
            return Ok(None);
        }

        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            candidates: Some(candidates.iter().map(Self::to_candidate).collect()),
            existing_indexers: None,
            pending_agreements: None,
            num_candidates: None,
        };

        let url = format!("{}select-one", self.endpoint);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("IISA request failed: {}", e);
                SelectionError::IisaServiceUnavailable
            })?;

        if !response.status().is_success() {
            tracing::error!("IISA returned error status: {}", response.status());
            return Err(SelectionError::IisaServiceUnavailable);
        }

        let result: SingleSelectionResponse = response.json().await.map_err(|e| {
            SelectionError::Error(anyhow::anyhow!("Failed to parse response: {}", e))
        })?;

        // Find the selected indexer in the original candidates list
        if let Some(id_str) = result.indexer_id {
            let id: IndexerId = id_str
                .parse()
                .map_err(|e| SelectionError::Error(anyhow::anyhow!("Invalid indexer ID: {}", e)))?;

            Ok(candidates.into_iter().find(|i| i.id == id))
        } else {
            Ok(None)
        }
    }

    async fn select(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        num_candidates: usize,
    ) -> Result<Vec<Indexer>, SelectionError> {
        if candidates.is_empty() || num_candidates == 0 {
            return Ok(Vec::new());
        }

        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            candidates: Some(candidates.iter().map(Self::to_candidate).collect()),
            existing_indexers: None,
            pending_agreements: None,
            num_candidates: Some(num_candidates),
        };

        let url = format!("{}select-many", self.endpoint);

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("IISA request failed: {}", e);
                SelectionError::IisaServiceUnavailable
            })?;

        if !response.status().is_success() {
            tracing::error!("IISA returned error status: {}", response.status());
            return Err(SelectionError::IisaServiceUnavailable);
        }

        let result: MultiSelectionResponse = response.json().await.map_err(|e| {
            SelectionError::Error(anyhow::anyhow!("Failed to parse response: {}", e))
        })?;

        // Find selected indexers in the original candidates list
        let mut selected = Vec::with_capacity(result.indexer_ids.len());
        for id_str in result.indexer_ids {
            let id: IndexerId = match id_str.parse() {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!("Failed to parse indexer ID '{}': {}", id_str, e);
                    continue;
                }
            };

            if let Some(indexer) = candidates.iter().find(|i| i.id == id) {
                selected.push(indexer.clone());
            }
        }

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_normalization() {
        let client = HttpIisaClient::new("http://localhost:8080".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080/");

        let client = HttpIisaClient::new("http://localhost:8080/".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080/");
    }
}
