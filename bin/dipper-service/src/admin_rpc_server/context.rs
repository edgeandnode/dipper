use std::{collections::BTreeSet, sync::Arc};

use dipper_core::state::FromState;
use thegraph_core::alloy::primitives::Address;

use super::handlers::{IndexingAgreementsCtx, IndexingRequestsCtx};
use crate::signing::eip712::PrivateKeyEip712Signer;

/// Shared context for the gateway operator API.
#[derive(Clone)]
pub struct Ctx<R, W> {
    /// EIP-712 signer for response authentication.
    pub signer: Arc<PrivateKeyEip712Signer>,

    /// Authorized gateway operator addresses (e.g., Graph Studio).
    pub gateway_operator_allowlist: Arc<BTreeSet<Address>>,

    /// The maximum number of candidates to select
    pub max_candidates: usize,

    /// The DIPs registry
    pub registry: R,

    /// The message queue worker
    pub worker: W,
}

impl<R, W> FromState<Ctx<R, W>> for IndexingRequestsCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            gateway_operator_allowlist: ctx.gateway_operator_allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
            max_candidates: ctx.max_candidates,
        }
    }
}

impl<R, W> FromState<Ctx<R, W>> for IndexingAgreementsCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            gateway_operator_allowlist: ctx.gateway_operator_allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
        }
    }
}
