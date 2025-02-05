//! # Indexer client
//!
//! The indexer client is responsible for communicating directly with the indexers.

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use dashmap::{DashMap, Entry};
use dipper_core::ids::IndexingAgreementId;
use dipper_registry::IndexingAgreementVoucher;
use dipper_rpc::indexer::indexer_client::{
    graphprotocol::indexer::dips::{dips_service_client::DipsServiceClient, Decision},
    CancelAgreementRequestMessage, IndexingAgreementVoucher as VoucherRpc,
    IndexingAgreementVoucherMetadata as VoucherMetadataRpc, SubmitAgreementProposalRequestMessage,
};
use url::Url;

use crate::signer::PrivateKeyEip712Signer;

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
pub trait DipsClient {
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
    pool: Arc<DashMap<Url, DipsServiceClient<tonic::transport::Channel>>>,
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
        impl std::ops::DerefMut<Target = DipsServiceClient<tonic::transport::Channel>> + '_,
        DipsError,
    > {
        // If the client is not in the pool, create a new one
        let entry = match self.pool.entry(indexer_url) {
            Entry::Vacant(entry) => {
                let indexer_url = entry.key().as_str();

                let channel = tonic::transport::Endpoint::from_str(indexer_url)
                    .map_err(|err| DipsError::ConnectionError(err.into()))?
                    .connect_lazy();
                let client = DipsServiceClient::new(channel);

                entry.insert_entry(client)
            }
            Entry::Occupied(entry) => entry,
        };

        Ok(entry.into_ref())
    }
}

#[async_trait]
impl DipsClient for DipsIndexerClient {
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
        voucher: IndexingAgreementVoucher,
    ) -> Result<AgreementProposalResponse, DipsError> {
        let message = self
            .signer
            .sign(SubmitAgreementProposalRequestMessage {
                agreement_id: indexing_agreement_id,
                voucher: into_rpc_indexing_agreement_voucher(voucher),
            })
            .map_err(|err| DipsError::SigningError(err.into()))?;

        let request = tonic::Request::new(message.into());

        let mut client = self.get_client(indexer)?;
        let response = client
            .submit_agreement_proposal(request)
            .await
            .map_err(|err| DipsError::RequestError(err.into()))?;

        // Check the proposal response
        let response = response.into_inner();
        if response.decision == Decision::Accept as i32 {
            Ok(AgreementProposalResponse::Accepted)
        } else if response.decision == Decision::Reject as i32 {
            Ok(AgreementProposalResponse::Rejected)
        } else {
            Err(DipsError::ResponseError(
                format!("Invalid response decision value: {}", response.decision).into(),
            ))
        }
    }

    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError> {
        let message = self
            .signer
            .sign(CancelAgreementRequestMessage {
                agreement_id: indexing_agreement_id,
            })
            .map_err(|err| DipsError::SigningError(err.into()))?;

        let request = tonic::Request::new(message.into());

        let mut client = self.get_client(indexer)?;
        client
            .cancel_agreement(request)
            .await
            .map_err(|err| DipsError::RequestError(err.into()))?;

        Ok(())
    }
}

#[inline]
fn into_rpc_indexing_agreement_voucher(voucher: IndexingAgreementVoucher) -> VoucherRpc {
    VoucherRpc {
        payer: voucher.payer,
        recipient: voucher.recipient,
        service: voucher.service,
        duration_epochs: voucher.duration_epochs,
        max_initial_amount: voucher.max_initial_amount,
        max_ongoing_amount_per_epoch: voucher.max_ongoing_amount_per_epoch,
        max_epochs_per_collection: voucher.max_epochs_per_collection,
        min_epochs_per_collection: voucher.min_epochs_per_collection,
        metadata: VoucherMetadataRpc {
            deployment_id: voucher.metadata.deployment_id,
            price_per_block: voucher.metadata.price_per_block,
            price_per_entity_per_epoch: voucher.metadata.price_per_entity_per_epoch,
        },
    }
}
