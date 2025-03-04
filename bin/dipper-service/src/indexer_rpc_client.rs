//! # Indexer client
//!
//! The indexer client is responsible for communicating directly with the indexers.

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use dashmap::{DashMap, Entry};
use dipper_core::ids::IndexingAgreementId;
use dipper_rpc::indexer::indexer_client::{
    dips_agreement_eip712_domain, dips_cancellation_eip712_domain, rpc, sol,
};
use thegraph_core::alloy::sol_types::SolValue;
use url::Url;

use crate::{registry::IndexingAgreementVoucher, signing::eip712::PrivateKeyEip712Signer};

/// The indexer client error type for DIPs endpoint
#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum DipsError {
    #[error("Error connecting to the indexer: {0}")]
    ConnectionError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Error sending the request to the indexer: {0}")]
    RequestError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Invalid response: {0}")]
    ResponseError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Request signing failed: {0}")]
    SigningError(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug)]
pub enum AgreementProposalResponse {
    Accepted,
    Rejected,
}

impl std::fmt::Display for AgreementProposalResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            AgreementProposalResponse::Accepted => "ACCEPTED",
            AgreementProposalResponse::Rejected => "REJECTED",
        };
        f.write_str(repr)
    }
}

/// Indexer client's DIPs trait
#[async_trait]
pub trait IndexerClient {
    /// Send an indexing agreement proposal request to the indexer
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
        voucher: IndexingAgreementVoucher,
    ) -> Result<AgreementProposalResponse, DipsError>;

    /// Send an indexing agreement cancel request to the indexer
    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError>;
}

#[derive(Clone)]
pub struct DipsIndexerClient {
    signer: Arc<PrivateKeyEip712Signer>,
    pool: Arc<DashMap<Url, rpc::IndexerDipsServiceClient<tonic::transport::Channel>>>,
}

impl DipsIndexerClient {
    /// Create a new indexer client
    pub fn new(signer: Arc<PrivateKeyEip712Signer>) -> Self {
        Self {
            signer,
            pool: Default::default(),
        }
    }

    /// Get a client for the given indexer URL
    ///
    /// If the client is not in the pool, create a new instance.
    fn get_client(
        &self,
        indexer_url: Url,
    ) -> Result<
        impl std::ops::DerefMut<Target = rpc::IndexerDipsServiceClient<tonic::transport::Channel>> + '_,
        DipsError,
    > {
        // If the client is not in the pool, create a new one
        let entry = match self.pool.entry(indexer_url) {
            Entry::Vacant(entry) => {
                let indexer_url = entry.key().as_str();

                let channel = tonic::transport::Endpoint::from_str(indexer_url)
                    .map_err(|err| DipsError::ConnectionError(err.into()))?
                    .connect_lazy();
                let client = rpc::IndexerDipsServiceClient::new(channel);

                entry.insert_entry(client)
            }
            Entry::Occupied(entry) => entry,
        };

        Ok(entry.into_ref())
    }
}

#[async_trait]
impl IndexerClient for DipsIndexerClient {
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
        voucher: IndexingAgreementVoucher,
    ) -> Result<AgreementProposalResponse, DipsError> {
        // Convert to the solidity voucher data structure
        let sol_voucher = into_sol_voucher(indexing_agreement_id, voucher);

        // Sign the solidity voucher with the appropriate domain
        let signed = self
            .signer
            .sign_with_domain(&dips_agreement_eip712_domain(), sol_voucher)
            .map_err(|err| DipsError::SigningError(err.into()))?;

        // Serialize the Solidity signed voucher to bytes (ABI encoding)
        let sol_signed_voucher_bytes: Vec<u8> = sol::SignedIndexingAgreementVoucher {
            voucher: signed.message,
            signature: signed.signature.as_bytes().into(),
        }
        .abi_encode();

        // Build the SubmitAgreementProposalRequest RPC request message
        // For now, the MVP, we are using version 0
        let request = tonic::Request::new(rpc::SubmitAgreementProposalRequest {
            version: 0, // MVP version
            signed_voucher: sol_signed_voucher_bytes,
        });

        // Send the proposal request to the indexer
        let mut client = self.get_client(indexer)?;
        let response = client
            .submit_agreement_proposal(request)
            .await
            .map_err(|err| DipsError::RequestError(err.into()))?;

        // Check the proposal response
        let resp = response.into_inner();
        if resp.response == rpc::ProposalResponse::Accept as i32 {
            Ok(AgreementProposalResponse::Accepted)
        } else if resp.response == rpc::ProposalResponse::Reject as i32 {
            Ok(AgreementProposalResponse::Rejected)
        } else {
            Err(DipsError::ResponseError(
                format!("Invalid response decision value: {}", resp.response).into(),
            ))
        }
    }

    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError> {
        // Convert to the solidity cancellation request data structure
        let sol_cancellation_request = into_sol_cancellation_request(indexing_agreement_id);

        // Sign the solidity cancellation request with the appropriate domain
        let signed = self
            .signer
            .sign_with_domain(&dips_cancellation_eip712_domain(), sol_cancellation_request)
            .map_err(|err| DipsError::SigningError(err.into()))?;

        // Serialize the Solidity signed cancellation request to bytes (ABI encoding)
        let sol_signed_cancellation_request_bytes: Vec<u8> = sol::SignedCancellationRequest {
            request: signed.message,
            signature: signed.signature.as_bytes().into(),
        }
        .abi_encode();

        // Build the CancelAgreementRequest RPC request message
        let request = tonic::Request::new(rpc::CancelAgreementRequest {
            version: 0,
            signed_cancellation: sol_signed_cancellation_request_bytes,
        });

        // Send the cancellation request to the indexer
        let mut client = self.get_client(indexer)?;
        client
            .cancel_agreement(request)
            .await
            .map_err(|err| DipsError::RequestError(err.into()))?;

        Ok(())
    }
}

#[inline]
fn into_sol_voucher(
    agreement_id: IndexingAgreementId,
    voucher: IndexingAgreementVoucher,
) -> sol::IndexingAgreementVoucher {
    sol::IndexingAgreementVoucher {
        agreement_id: agreement_id.as_bytes().into(),
        payer: voucher.payer,
        recipient: voucher.recipient,
        service: voucher.service,
        durationEpochs: voucher.duration_epochs,
        maxInitialAmount: voucher.max_initial_amount,
        maxOngoingAmountPerEpoch: voucher.max_ongoing_amount_per_epoch,
        minEpochsPerCollection: voucher.min_epochs_per_collection,
        maxEpochsPerCollection: voucher.max_epochs_per_collection,
        deadline: voucher.deadline,
        metadata: sol::SubgraphIndexingVoucherMetadata {
            basePricePerEpoch: voucher.metadata.base_price_per_epoch,
            pricePerEntity: voucher.metadata.price_per_entity,
            subgraphDeploymentId: voucher.metadata.subgraph_deployment_id.to_string(),
            protocolNetwork: format!("eip155:{}", voucher.metadata.protocol_network),
            chainId: format!("eip155:{}", voucher.metadata.chain_id),
        }
        .abi_encode()
        .into(),
    }
}

#[inline]
fn into_sol_cancellation_request(agreement_id: IndexingAgreementId) -> sol::CancellationRequest {
    sol::CancellationRequest {
        agreement_id: agreement_id.as_bytes().into(),
    }
}
