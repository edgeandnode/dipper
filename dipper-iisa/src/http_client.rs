/*
 * HTTP Client for IISA (Indexing Indexer Selection Algorithm)
 * 
 * This file implements a Rust HTTP client for communicating with the IISA docker container.
 * The client provides an interface for passing necessary inputs:
 * 
 * - 1. Subgraph deployment ID that we need to (re)assign indexers to.
 *     - e.g. QmABC
 * - 2. List of existing indexers assigned to this subgraph deployment.
 *     - e.g. [0x123, 0x456, 0x789]
 * - 3. List of pre-filtered indexers eligible for selection - refered to as candidates.
 *     - Candidates are indexers that are:
 *         - Serving >0 subgraph(s) on the network where this subgraph is deployed.
 *         - Not blocked from receiving indexing agreements. TODO: Track blocked indexers in the DB.
 *         - Have not declined indexing agreements on this subgraph deployment. TODO: Within last x days.
 *     - e.g. [0x123, 0x456, 0x789, 0x321, 0x654, 0x987]
 * - 4. Pending agreements dict from the database.
 *     - We need pending agreements seperate from candidates, because we might otherwise drop all agreements for an indexer that has a pending agreement.
 *     - e.g. {"0x123": ["QmDEF"], "0x456": ["QmGHI"]}
 * - 5. Indexer base pricing dict from the database.
 *     - Indexers will elect to use different base prices per epoch for different networks to absorb archive node costs variations
 *     - e.g. {"0x123": {"mainnet": 100.0, "arbitrum": 200.0}, "0x456": {"bsc": 100.0, "matic": 100.0}}
 * - 6. Indexer entity pricing dict from the database.
 *     - Indexers will use the same entity price per epoch for all networks.
 *     - e.g. {"0x123": 100, "0x456": 200, "0x789": 300}
 * 
 * The IISA container handles indexer performance metric calculations, weighting, and selection algorithm.
 * - Fetching performance data from BigQuery
 * - Calculating indexer scoring metrics:
 *     - Stake to fees ratio score,
 *     - Base price score,
 *     - Query response latency score,
 *     - Uptime score,
 *     - Success rate score,
 *     - Price per entity score,
 *     - Sync speed score.
 * - Applying weights to the above metrics to create an overall score for each candidate.
 * - Running the selection algorithm to determine the best indexers for the deployment.
 * 
 * This client implements the `CandidateSelection` interface, allowing other Rust code to 
 * interact with the IISA Docker container without needing to understand the complexity
 * of the selection process.
 */


// --- Import Statements ---

use async_trait::async_trait;                                  // enables async functions in traits
use serde::{Deserialize, Serialize};                           // converts between Rust data and JSON
use thegraph_core::{DeploymentId, IndexerId};                  // imports specific types from thegraph_core
use crate::api::{CandidateSelection, Indexer, SelectionError}; // imports from api.rs module in this directory


// --- Client Struct ---

// Define a rust struct `HttpIsaClient` for making requests to the IISA docker container
// This struct stores the client for HTTP requests and the base URL of the IISA service
#[derive(Clone)]              // Allows creating copies of this struct, so we can make multiple requests at once
pub struct HttpIisaClient {
    client: reqwest::Client,  // Object that handles HTTP requests
    endpoint: String,         // Stores the base URL of the IISA service
}


// --- Request and Response Structs ---

// Define struct `SelectionRequest` that's converted to a JSON request body when sent to the IISA service
#[derive(Serialize)]          // Allows this struct to be converted to JSON
struct SelectionRequest {
    deployment_id: String,    // The ID of the deployment to select indexers for
    candidates: Vec<String>,  // List of candidate indexer ID's to select indexers from
    #[serde(skip_serializing_if = "Option::is_none")]  // Only include num_candidates if it's not None
    num_candidates: Option<usize>,  // Optional number of indexers to select
}

// Define struct `SingleSelectionResponse` for passing JSON response from select-one endpoint from IISA service
#[derive(Deserialize, Debug)] // Allows struct to be converted from JSON and printed for debugging
struct SingleSelectionResponse {
    indexer_id: Option<String>,  // The ID of the selected indexer, or None if no selection was made
}

// Define struct `MultiSelectionResponse` for passing JSON response from select-many endpoint from IISA service
#[derive(Deserialize, Debug)] // Allows struct to be converted from JSON and printed for debugging
struct MultiSelectionResponse {
    indexer_ids: Vec<String>,  // A list of selected indexer IDs
}


// --- Client Methods ---

// Define methods to add to the `HttpIisaClient` struct
impl HttpIisaClient {

    // Constructor to create a new HTTP client for the IISA service
    pub fn new(endpoint: String) -> Self {

        // Ensure the endpoint ends with a slash for URL consistency
        let endpoint = if endpoint.ends_with('/') {
            endpoint
        } else {
            format!("{}/", endpoint)
        };

        // Create a new client instance with the formatted endpoint
        Self {
            client: reqwest::Client::new(),
            endpoint,
        }
    }

    // Health check method - checks if the IISA service is running
    pub async fn health_check(&self) -> Result<(), SelectionError> {
        let url = format!("{}health", self.endpoint);
        
        // Send a GET request to the health endpoint and handle errors
        self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                SelectionError::Error(anyhow::anyhow!("Health check failed: {}", e))
            })?
            .error_for_status()
            .map_err(|e| {
                SelectionError::Error(anyhow::anyhow!("Health check returned error status: {}", e))
            })?;
        
        Ok(())
    }
}

// --- Candidate Selection Implementation ---

// Define the `CandidateSelection` trait to add to the `HttpIisaClient` struct
// Allows the `HttpIisaClient` client to select indexers by making HTTP requests to the IISA service
#[async_trait]                // Enables async functions in traits
impl CandidateSelection for HttpIisaClient {

    // Define the `select_one` method for the `HttpIisaClient` struct
    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
    ) -> Result<Option<Indexer>, SelectionError> {

        // Create a selection request
        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            candidates: candidates.iter().map(|i| format!("{:#x}", i.id)).collect(),
            num_candidates: None,
        };

        // Create a URL for the select-one endpoint
        let url = format!("{}select-one", self.endpoint);
        
        // Call our REST API
        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| SelectionError::Error(anyhow::anyhow!("HTTP request failed: {}", e)))?
            .json::<SingleSelectionResponse>()
            .await
            .map_err(|e| SelectionError::Error(anyhow::anyhow!("Failed to parse response: {}", e)))?;

        // Check if an indexer was selected
        if let Some(id_str) = response.indexer_id {

            // Parse the indexer ID
            let id: IndexerId = id_str
                .parse()
                .map_err(|e| SelectionError::Error(anyhow::anyhow!("Failed to parse indexer ID: {}", e)))?;
            
            // Find the indexer in the candidates
            let indexer = candidates.iter().find(|i| i.id == id).cloned();

            // Return the selected indexer
            Ok(indexer)
        } else {
            Ok(None)
        }
    }

    // Define the `select` method for the `HttpIisaClient` struct
    async fn select(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        num_candidates: usize,
    ) -> Result<Vec<Indexer>, SelectionError> {

        // Create a selection request
        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            candidates: candidates.iter().map(|i| format!("{:#x}", i.id)).collect(),
            num_candidates: Some(num_candidates),
        };

        // Create a URL for the select-many endpoint
        let url = format!("{}select-many", self.endpoint);
        
        // Call our REST API
        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| SelectionError::Error(anyhow::anyhow!("HTTP request failed: {}", e)))?
            .json::<MultiSelectionResponse>()
            .await
            .map_err(|e| SelectionError::Error(anyhow::anyhow!("Failed to parse response: {}", e)))?;

        // Create a list to store results
        let mut selected = Vec::with_capacity(response.indexer_ids.len());
        for id_str in response.indexer_ids {

            // Parse the indexer ID
            let id: IndexerId = id_str
                .parse()
                .map_err(|e| SelectionError::Error(anyhow::anyhow!("Failed to parse indexer ID: {}", e)))?;
            
            // Find the indexer in the candidates
            if let Some(indexer) = candidates.iter().find(|i| i.id == id) {
                selected.push(indexer.clone());
            }
        }

        // Return the selected indexers
        Ok(selected)
    }
} 