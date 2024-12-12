//! The RPC client for the indexer's DIPs gRPC API.
//!
//! This module contains the generated code to interact with the indexer's DIPs gRPC server.

use dipper_core::ids::IndexingAgreementId;
use thegraph_core::{
    alloy::{
        primitives::{Address, U256},
        sol,
    },
    signed_message::{SignedMessage, ToSolStruct},
    DeploymentId,
};

// Include the tonic-generated code
include!("gen/indexer.mod.rs");

use self::graphprotocol::indexer::dips::{
    CancelAgreementRequest, SubmitAgreementProposalRequest, Voucher, VoucherMetadata,
};

pub struct SubmitAgreementProposalRequestMessage {
    pub agreement_id: IndexingAgreementId,
    pub voucher: IndexingAgreementVoucher,
}

pub struct IndexingAgreementVoucher {
    /// The agreement payer.
    ///
    /// It should coincide with the voucher signer address.
    pub payer: Address,
    /// The voucher recipient address. The indexer ID.
    pub recipient: Address,
    /// Data service that will initiate the payment collection.
    pub service: Address,

    /// The duration of the agreement in epochs.
    pub duration_epochs: u32,

    /// The maximum amount, in _wei GRT_, that can be collected for the initial subgraph sync.
    pub max_initial_amount: U256,
    /// The maximum amount, in _wei GRT_, that can be collected per epoch (after the initial sync).
    pub max_ongoing_amount_per_epoch: U256,

    /// The maximum number of epochs that can be collected at once.
    pub max_epochs_per_collection: u32,
    /// The minimum number of epochs that can be collected at once.
    pub min_epochs_per_collection: u32,

    /// The voucher metadata
    pub metadata: IndexingAgreementVoucherMetadata,
}

pub struct IndexingAgreementVoucherMetadata {
    /// The Subgraph deployment ID to index.
    pub deployment_id: DeploymentId,

    /// The amount to pay per indexed block in _wei GRT per block_.
    pub price_per_block: U256,
    /// The amount to pay per indexed and stored entity in _wei GRT per entity per epoch_.
    pub price_per_entity_per_epoch: U256,
}

sol! {
    struct SubmitAgreementProposalRequestMessageSol {
        bytes16 agreement_id;
        IndexingAgreementVoucherSol voucher;
    }

    struct IndexingAgreementVoucherSol {
        address payer;
        address recipient;
        address service;

        uint32 durationEpochs;

        uint256 maxInitialAmount;
        uint256 minOngoingAmountPerEpoch;

        uint32 maxEpochsPerCollection;
        uint32 minEpochsPerCollection;

        VoucherMetadataSol metadata;
    }

    struct VoucherMetadataSol {
        bytes32 deploymentId;

        uint256 pricePerBlock;
        uint256 pricePerEntityPerEpoch;
    }
}

impl ToSolStruct<SubmitAgreementProposalRequestMessageSol>
    for SubmitAgreementProposalRequestMessage
{
    fn to_sol_struct(&self) -> SubmitAgreementProposalRequestMessageSol {
        SubmitAgreementProposalRequestMessageSol {
            agreement_id: self.agreement_id.as_bytes().into(),
            voucher: self.voucher.to_sol_struct(),
        }
    }
}

impl ToSolStruct<IndexingAgreementVoucherSol> for IndexingAgreementVoucher {
    fn to_sol_struct(&self) -> IndexingAgreementVoucherSol {
        IndexingAgreementVoucherSol {
            payer: self.payer,
            recipient: self.recipient,
            service: self.service,
            durationEpochs: self.duration_epochs,
            maxInitialAmount: self.max_initial_amount,
            minOngoingAmountPerEpoch: self.max_ongoing_amount_per_epoch,
            maxEpochsPerCollection: self.max_epochs_per_collection,
            minEpochsPerCollection: self.min_epochs_per_collection,
            metadata: self.metadata.to_sol_struct(),
        }
    }
}

impl ToSolStruct<VoucherMetadataSol> for IndexingAgreementVoucherMetadata {
    fn to_sol_struct(&self) -> VoucherMetadataSol {
        VoucherMetadataSol {
            deploymentId: self.deployment_id.into(),
            pricePerBlock: self.price_per_block,
            pricePerEntityPerEpoch: self.price_per_entity_per_epoch,
        }
    }
}

impl From<SignedMessage<SubmitAgreementProposalRequestMessage>> for SubmitAgreementProposalRequest {
    fn from(value: SignedMessage<SubmitAgreementProposalRequestMessage>) -> Self {
        SubmitAgreementProposalRequest {
            agreement_id: value.message.agreement_id.as_bytes().to_vec(),
            voucher: Some(value.message.voucher.into()),
            signature: value.signature.as_bytes().to_vec(),
        }
    }
}

impl From<IndexingAgreementVoucher> for Voucher {
    fn from(value: IndexingAgreementVoucher) -> Self {
        Voucher {
            payer: value.payer.as_slice().to_vec(),
            recipient: value.recipient.as_slice().to_vec(),
            service: value.service.as_slice().to_vec(),
            duration_epochs: value.duration_epochs,
            max_initial_amount: value.max_initial_amount.to_be_bytes::<32>().to_vec(),
            min_ongoing_amount_per_epoch: value
                .max_ongoing_amount_per_epoch
                .to_be_bytes::<32>()
                .to_vec(),
            max_epochs_per_collection: value.max_epochs_per_collection,
            min_epochs_per_collection: value.min_epochs_per_collection,
            metadata: Some(value.metadata.into()),
        }
    }
}

impl From<IndexingAgreementVoucherMetadata> for VoucherMetadata {
    fn from(value: IndexingAgreementVoucherMetadata) -> Self {
        VoucherMetadata {
            deployment_id: value.deployment_id.as_slice().to_vec(),
            price_per_block: value.price_per_block.to_be_bytes::<32>().to_vec(),
            price_per_entity_per_epoch: value
                .price_per_entity_per_epoch
                .to_be_bytes::<32>()
                .to_vec(),
        }
    }
}

pub struct CancelAgreementRequestMessage {
    pub agreement_id: IndexingAgreementId,
}

sol! {
    struct CancelAgreementRequestMessageSol {
        bytes16 agreement_id;
    }
}

impl ToSolStruct<CancelAgreementRequestMessageSol> for CancelAgreementRequestMessage {
    fn to_sol_struct(&self) -> CancelAgreementRequestMessageSol {
        CancelAgreementRequestMessageSol {
            agreement_id: self.agreement_id.as_bytes().into(),
        }
    }
}

impl From<SignedMessage<CancelAgreementRequestMessage>> for CancelAgreementRequest {
    fn from(value: SignedMessage<CancelAgreementRequestMessage>) -> Self {
        CancelAgreementRequest {
            agreement_id: value.message.agreement_id.as_bytes().to_vec(),
            signature: value.signature.as_bytes().to_vec(),
        }
    }
}
