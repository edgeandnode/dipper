use std::{collections::BTreeSet, sync::Arc};

use alloy_signer_local::PrivateKeySigner;
use axum::{extract::State, http::StatusCode, Json};
use dipper_core::{
    ids::IndexingRequestId, signed_message::Eip712Signer, signed_message_serde::SignedMessage,
};
use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use serde::{Deserialize, Serialize};
use thegraph_core::{alloy_primitives::B256, Address, DeploymentId};

use crate::{
    http_server::context::Ctx,
    worker::messages::{Message, ProcessNewIndexingRequest},
};

/// The substate for the new indexing request handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct NewIndexingRequestCtx<R, W> {
    signer: Arc<Eip712Signer<PrivateKeySigner>>,
    allowlist: Arc<BTreeSet<Address>>,
    registry: R,
    worker: W,
    max_candidates: usize,
}

impl<R, W> axum::extract::FromRef<Ctx<R, W>> for NewIndexingRequestCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_ref(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            allowlist: ctx.allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
            max_candidates: ctx.max_candidates,
        }
    }
}

alloy_sol_types::sol! {
    /// The new indexing request message (Solidity version)
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct NewIndexingRequestSol {
        /// The deployment ID of the subgraph that should be indexed
        #[serde(
            serialize_with = "serialize_as_deployment_id",
            deserialize_with = "deserialize_as_deployment_id"
        )]
        bytes32 deployment_id;
    }
}

fn serialize_as_deployment_id<S>(value: &B256, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    DeploymentId::from(*value).serialize(serializer)
}

fn deserialize_as_deployment_id<'de, D>(deserializer: D) -> Result<B256, D::Error>
where
    D: serde::Deserializer<'de>,
{
    DeploymentId::deserialize(deserializer).map(Into::into)
}

/// The new indexing request message (Rust version)
#[derive(Debug)]
pub struct NewIndexingRequest {
    deployment_id: DeploymentId,
}

impl From<NewIndexingRequestSol> for NewIndexingRequest {
    fn from(value: NewIndexingRequestSol) -> Self {
        Self {
            deployment_id: value.deployment_id.into(),
        }
    }
}

impl From<NewIndexingRequest> for NewIndexingRequestSol {
    fn from(value: NewIndexingRequest) -> Self {
        Self {
            deployment_id: value.deployment_id.into(),
        }
    }
}

pub async fn register_new_indexing_request<R, W>(
    State(ctx): State<NewIndexingRequestCtx<R, W>>,
    Json(payload): Json<SignedMessage<NewIndexingRequestSol>>,
) -> Result<Json<IndexingRequestId>, StatusCode>
where
    R: Registry,
    W: Queue<Message>,
{
    // Check if the signer is authorized to make this request
    let requested_by = match ctx.signer.recover_signer(&payload) {
        Ok(requested_by) => requested_by,
        Err(err) => {
            tracing::debug!(error=?err, "Failed to recover signer");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };
    if !ctx.allowlist.contains(&requested_by) {
        return Err(StatusCode::FORBIDDEN);
    }

    let NewIndexingRequest { deployment_id } = payload.into_message();

    // Register the new indexing request
    let indexing_request_id = match ctx
        .registry
        .register_new_indexing_request(requested_by, deployment_id)
        .await
    {
        Ok(indexing_request_id) => indexing_request_id,
        Err(err) => {
            tracing::error!(error=?err, "Failed to register new indexing request");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Process the new indexing request
    if let Err(err) = ctx
        .worker
        .push(Message::ProcessNewIndexingRequest(
            ProcessNewIndexingRequest {
                indexing_request_id,
                deployment_id,
                num_candidates: ctx.max_candidates,
            },
        ))
        .await
    {
        tracing::error!(error=?err, "Failed to post 'ProcessNewIndexingRequest' message to worker");
    };

    Ok(Json(indexing_request_id))
}

#[cfg(test)]
mod tests {
    use alloy_signer_local::PrivateKeySigner;
    use alloy_sol_types::{eip712_domain, Eip712Domain};
    use dipper_core::{
        signed_message::Eip712Signer, signed_message_serde::SignedMessage as SignedMessageSerde,
    };
    use thegraph_core::{address, alloy_primitives::b256, deployment_id};

    use super::{NewIndexingRequest, NewIndexingRequestSol};

    /// A test EIP-712 domain
    const EIP712_DOMAIN: Eip712Domain = eip712_domain! {
        name: "Test domain",
        version: "1",
        chain_id: 1,
        verifying_contract: address!("a83682bbe91c0d2d48a13fd751b2da8e989fe421"),
        salt: b256!("66eb090e6dbb9668c7d32c0ee7ba5e8f08d84385804485d316dd5f5692273593")
    };

    #[test]
    fn serialize_new_indexing_request_signed_message() {
        //* Given
        // EIP-712 signer
        let signer = PrivateKeySigner::random();
        let signer_address = signer.address();
        let eip712_signer = Eip712Signer::new(signer, signer_address, EIP712_DOMAIN);

        // Message
        let deployment_id = deployment_id!("QmZTy9EJHu8rfY9QbEk3z1epmmvh5XHhT2Wqhkfbyt8k9Z");
        let request = NewIndexingRequest { deployment_id };

        let request_sol: NewIndexingRequestSol = request.into();

        //* When
        let signed_message: SignedMessageSerde<NewIndexingRequestSol> = eip712_signer
            .sign(request_sol)
            .expect("signing failed")
            .into();

        let serialized = serde_json::to_string(&signed_message).expect("serialization failed");
        let deserialized =
            serde_json::from_str::<SignedMessageSerde<NewIndexingRequestSol>>(&serialized)
                .expect("deserialization failed");

        //* Then
        // Assert the signer address is the same after deserialization
        let deserialized_signer_address = eip712_signer
            .recover_signer(&deserialized)
            .expect("recovering signer failed");
        assert_eq!(signer_address, deserialized_signer_address);

        // Assert the message is the same after deserialization
        let deserialized_message: NewIndexingRequest = deserialized.into_message();
        assert_eq!(deployment_id, deserialized_message.deployment_id);
    }
}
