use std::{collections::BTreeSet, sync::Arc};

use dipper_core::state::FromState;
use thegraph_core::alloy::primitives::Address;

use super::handlers::{IndexingAgreementsCtx, IndexingRequestsCtx};
use crate::signing::eip712::PrivateKeyEip712Signer;

/// The context shared across all requests.
#[derive(Clone)]
pub struct Ctx<R, W> {
    /// The EIP-712 signer
    pub signer: Arc<PrivateKeyEip712Signer>,

    /// The allowlist of addresses that are allowed to make requests to the DIPs gateway Admin API
    pub admin_allowlist: Arc<BTreeSet<Address>>,

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
            allowlist: ctx.admin_allowlist.clone(),
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
            allowlist: ctx.admin_allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
        }
    }
}
