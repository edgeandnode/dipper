//! Shared test utilities for the dipper-service crate.
//!
//! Currently provides [`CapturingEventsProducer`], a test double for
//! [`SubgraphIndexingAgreementEventsProducer`] that records every produced event
//! so handler/service tests can assert exactly which lifecycle events were emitted
//! (and with which payloads) without a Kafka backend.

use std::sync::{Arc, Mutex};

use dipper_producer::{events::SubgraphIndexingAgreementEventsProducer, proto};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};

/// One captured lifecycle event: the routing metadata plus the typed payload.
#[derive(Clone, Debug, PartialEq)]
pub enum CapturedEvent {
    RequestReceived {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    },
    Proposed {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    },
    Accepted {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    },
    RequestExpired {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    },
    NIndexersUnavailable {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    },
    Terminated {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    },
}

/// A [`SubgraphIndexingAgreementEventsProducer`] that records produced events into
/// a shared buffer for assertions. Clone shares the same buffer.
#[derive(Clone, Default)]
pub struct CapturingEventsProducer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl CapturingEventsProducer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of all events captured so far, in production order.
    pub fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().expect("events lock poisoned").clone()
    }

    fn push(&self, event: CapturedEvent) {
        self.events
            .lock()
            .expect("events lock poisoned")
            .push(event);
    }
}

impl SubgraphIndexingAgreementEventsProducer for CapturingEventsProducer {
    fn produce_subgraph_indexing_agreement_request_received(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    ) {
        self.push(CapturedEvent::RequestReceived {
            deployment,
            chain_id,
            event,
        });
    }

    fn produce_subgraph_indexing_agreement_proposed(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    ) {
        self.push(CapturedEvent::Proposed {
            deployment,
            chain_id,
            event,
        });
    }

    fn produce_subgraph_indexing_agreement_accepted(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    ) {
        self.push(CapturedEvent::Accepted {
            deployment,
            chain_id,
            event,
        });
    }

    fn produce_subgraph_indexing_agreement_request_expired(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    ) {
        self.push(CapturedEvent::RequestExpired {
            deployment,
            chain_id,
            event,
        });
    }

    fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    ) {
        self.push(CapturedEvent::NIndexersUnavailable {
            deployment,
            chain_id,
            event,
        });
    }

    fn produce_subgraph_indexing_agreement_terminated(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    ) {
        self.push(CapturedEvent::Terminated {
            deployment,
            chain_id,
            event,
        });
    }
}
