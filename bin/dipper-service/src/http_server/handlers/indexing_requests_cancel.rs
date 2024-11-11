use std::{collections::BTreeSet, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dipper_core::{
    api::admin::indexing_requests::CancelIndexingRequest, ids::IndexingRequestId,
    signed_message::serde::SignedMessage,
};
use dipper_pgmq::queue::Queue;
use dipper_registry::{Error, Registry};
use thegraph_core::alloy::primitives::Address;

use crate::{
    http_server::context::Ctx,
    signer::PrivateKeyEip712Signer,
    worker::messages::{Message, ProcessIndexingRequestCancellation},
};

/// The substate for the `cancel_indexing_request` handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct CancelIndexingRequestCtx<R, W> {
    signer: Arc<PrivateKeyEip712Signer>,
    allowlist: Arc<BTreeSet<Address>>,
    registry: R,
    worker: W,
}

impl<R, W> axum::extract::FromRef<Ctx<R, W>> for CancelIndexingRequestCtx<R, W>
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
        }
    }
}

// TODO: Review error reporting
pub async fn cancel_indexing_request<R, W>(
    State(ctx): State<CancelIndexingRequestCtx<R, W>>,
    Path(path_indexing_request_id): Path<IndexingRequestId>,
    Json(payload): Json<SignedMessage<CancelIndexingRequest>>,
) -> Result<(), StatusCode>
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

    let CancelIndexingRequest {
        id: indexing_request_id,
    } = payload.into_message();

    // Check if the indexing request ID in the path matches the one in the payload
    // TODO: Review this check. Shall we remove the payload ID and use only the path ID?
    if indexing_request_id != path_indexing_request_id {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Register the new indexing request
    if let Err(Error::DbError(err)) = ctx
        .registry
        .mark_indexing_request_as_canceled(&indexing_request_id)
        .await
    {
        tracing::error!(error=?err, "Failed to cancel indexing request");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    // Process the indexing request cancellation
    if let Err(err) = ctx
        .worker
        .push(Message::ProcessIndexingRequestCancellation(
            ProcessIndexingRequestCancellation {
                indexing_request_id,
            },
        ))
        .await
    {
        tracing::error!(error=?err, "Failed to post 'ProcessIndexingRequestCancellation' message");
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use dipper_core::signed_message::serde::SignedMessage as SignedMessageSerde;
    use thegraph_core::alloy::{
        primitives::{address, b256},
        signers::local::PrivateKeySigner,
        sol_types::{eip712_domain, Eip712Domain},
    };
    use uuid::uuid;

    use super::CancelIndexingRequest;
    use crate::signer::Eip712Signer;

    /// A test EIP-712 domain
    const EIP712_DOMAIN: Eip712Domain = eip712_domain! {
        name: "Test domain",
        version: "1",
        chain_id: 1,
        verifying_contract: address!("a83682bbe91c0d2d48a13fd751b2da8e989fe421"),
        salt: b256!("66eb090e6dbb9668c7d32c0ee7ba5e8f08d84385804485d316dd5f5692273593")
    };

    #[test]
    fn serialize_cancel_indexing_request_signed_message() {
        //* Given
        // EIP-712 signer
        let signer = PrivateKeySigner::random();
        let signer_address = signer.address();
        let eip712_signer = Eip712Signer::new(signer, signer_address, EIP712_DOMAIN);

        // Message
        let indexing_request_id = uuid!("91eef387-eec6-4189-8498-8acc8de7de9f").into();
        let request = CancelIndexingRequest {
            id: indexing_request_id,
        };

        //* When
        let signed_message: SignedMessageSerde<CancelIndexingRequest> =
            eip712_signer.sign(request).expect("signing failed").into();

        let serialized = serde_json::to_string(&signed_message).expect("serialization failed");
        let deserialized =
            serde_json::from_str::<SignedMessageSerde<CancelIndexingRequest>>(&serialized)
                .expect("deserialization failed");

        //* Then
        // Assert the signer address is the same after deserialization
        let deserialized_signer_address = eip712_signer
            .recover_signer(&deserialized)
            .expect("recovering signer failed");
        assert_eq!(signer_address, deserialized_signer_address);

        // Assert the message is the same after deserialization
        let deserialized_message: CancelIndexingRequest = deserialized.into_message();
        assert_eq!(indexing_request_id, deserialized_message.id);
    }
}
