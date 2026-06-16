//! Payer-mode-aware cancel dispatch. Every on-chain cancel goes through
//! [`cancel_agreement_on_chain`] so the external-payer and protocol-managed
//! paths are chosen in exactly one place.

use thegraph_core::alloy::primitives::B256;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::{IndexingAgreementConfig, PayerMode},
    registry::{IndexingAgreement, IndexingAgreementStatus},
};

/// Cancel-scope option for an agreement already accepted on-chain.
const SCOPE_ACTIVE: u16 = 1;
/// Cancel-scope option for an agreement not yet accepted on-chain.
const SCOPE_PENDING: u16 = 2;

/// Cancel an agreement on-chain using the configured payer mode. Mirrors
/// [`ChainClient::cancel_indexing_agreement_by_payer`]'s contract. In
/// protocol-managed mode a missing stored hash is a `ConfigError`, not a guess.
pub async fn cancel_agreement_on_chain<T: ChainClient>(
    chain_client: &T,
    agreement: &IndexingAgreement,
    config: &IndexingAgreementConfig,
) -> Result<Option<B256>, ChainClientError> {
    match config.payer_mode() {
        PayerMode::ExternalPayer => {
            chain_client
                .cancel_indexing_agreement_by_payer(agreement.id.as_bytes())
                .await
        }
        PayerMode::AgreementManager => {
            let version_hash = agreement
                .terms_version_hash
                .as_deref()
                .filter(|h| h.len() == 32)
                .map(B256::from_slice)
                .ok_or_else(|| {
                    ChainClientError::ConfigError(format!(
                        "agreement {} has no 32-byte terms_version_hash for manager cancel",
                        agreement.id
                    ))
                })?;
            let options = if agreement.status == IndexingAgreementStatus::AcceptedOnChain {
                SCOPE_ACTIVE
            } else {
                SCOPE_PENDING
            };
            chain_client
                .cancel_via_manager(
                    config.recurring_collector(),
                    agreement.id.as_bytes(),
                    version_hash,
                    options,
                )
                .await
        }
    }
}
