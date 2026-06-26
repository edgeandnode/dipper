//! Subgraph Indexing Agreement event emitter to Kafka topic utilities

use std::sync::Arc;

use prost::Message;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use tokio::sync::mpsc;

use crate::{kafka::KafkaProducer, proto};

/// CAIP-2 identifier for an EVM (`eip155`) chain.
///
/// Wraps a numeric [`ChainId`] and renders to its CAIP-2 string form
/// `eip155:{chain_id}` (e.g. `eip155:42161`) when written to the wire. Construct
/// one from a chain id via [`From`] / `ChainId::into`, so call sites pass the raw
/// chain id without repeating the `eip155:` formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caip2ChainId(ChainId);

impl From<ChainId> for Caip2ChainId {
    fn from(chain_id: ChainId) -> Self {
        Self(chain_id)
    }
}

impl std::fmt::Display for Caip2ChainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "eip155:{}", self.0)
    }
}

/// Produces Subgraph Indexing Agreement lifecycle events.
///
/// Abstracts [`SubgraphIndexingAgreementsEventsEmitter`] so callers can depend on
/// the behavior rather than the concrete Kafka emitter, and tests can substitute a
/// capturing double. `the_graph_network` is the protocol network's numeric
/// [`ChainId`]; implementors render it to its CAIP-2 form (`eip155:{id}`) on the wire.
pub trait SubgraphIndexingAgreementEventsProducer: Send + Sync {
    /// Produces a subgraph.indexing.agreement.request.received event
    fn produce_subgraph_indexing_agreement_request_received(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    );

    /// Produces a subgraph.indexing.agreement.proposed event
    fn produce_subgraph_indexing_agreement_proposed(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    );

    /// Produces a subgraph.indexing.agreement.accepted event
    fn produce_subgraph_indexing_agreement_accepted(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    );

    /// Produces a subgraph.indexing.agreement.request.expired event
    fn produce_subgraph_indexing_agreement_request_expired(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    );

    /// Produces a subgraph.indexing.agreement.n_indexers_unavailable event
    fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    );

    /// Produces a subgraph.indexing.agreement.terminated event
    fn produce_subgraph_indexing_agreement_terminated(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    );
}

/// Kafka producer wrapper for Subgraph Indexing agreements lifecycle events
///
/// When the queue is `None`, Kafka production is disabled and all produce methods return immediately.
pub struct SubgraphIndexingAgreementsEventsEmitter {
    queue: Option<mpsc::Sender<QueuedSubgraphIndexingAgreementEvent>>,
}

impl SubgraphIndexingAgreementsEventsEmitter {
    /// Creates a disabled producer.
    pub fn disabled() -> Self {
        Self { queue: None }
    }

    /// Creates a producer backed by a KafkaProducer instance for sending Subgraph Indexing agreement lifecycle events.
    pub fn enabled(client: Arc<KafkaProducer>, capacity: usize) -> Self {
        let (tx, mut rx) = mpsc::channel::<QueuedSubgraphIndexingAgreementEvent>(capacity);

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let (event_type, key, envelope) = Self::prepare_event(event);
                // `send_event` already bounds the produce attempt via `KafkaProducer`'s
                // internal produce timeout, so no additional timeout is layered here.
                Self::send_event(&client, event_type, &key, envelope).await;
            }
        });

        Self { queue: Some(tx) }
    }
}

impl SubgraphIndexingAgreementEventsProducer for SubgraphIndexingAgreementsEventsEmitter {
    fn produce_subgraph_indexing_agreement_request_received(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::RequestReceived(event),
        );
    }

    fn produce_subgraph_indexing_agreement_proposed(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::Proposed(event),
        );
    }

    fn produce_subgraph_indexing_agreement_accepted(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::Accepted(event),
        );
    }

    fn produce_subgraph_indexing_agreement_request_expired(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::RequestExpired(event),
        );
    }

    fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::NIndexersUnavailable(event),
        );
    }

    fn produce_subgraph_indexing_agreement_terminated(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    ) {
        self.enqueue(
            subgraph_deployment_qm_hash,
            the_graph_network.into(),
            EventPayload::Terminated(event),
        );
    }
}

impl SubgraphIndexingAgreementsEventsEmitter {
    fn enqueue(
        &self,
        subgraph_deployment_qm_hash: DeploymentId,
        the_graph_network: Caip2ChainId,
        payload: EventPayload,
    ) {
        let Some(queue) = &self.queue else {
            return;
        };

        let event = QueuedSubgraphIndexingAgreementEvent {
            metadata: EventMetadata {
                subgraph_deployment_qm_hash,
                the_graph_network,
            },
            payload,
        };

        let event_type = event.payload.event_type();
        if let Err(err) = queue.try_send(event) {
            match err {
                mpsc::error::TrySendError::Full(_) => {
                    tracing::warn!(event_type = %event_type, "event queue full; dropping event");
                }
                mpsc::error::TrySendError::Closed(_) => {
                    tracing::warn!(event_type = %event_type, "event queue closed; dropping event");
                }
            }
        }
    }

    fn prepare_event(
        event: QueuedSubgraphIndexingAgreementEvent,
    ) -> (
        SubgraphIndexingAgreementEventType,
        String,
        proto::SubgraphIndexingAgreementEvent,
    ) {
        let event_type = event.payload.event_type();
        let key = event.metadata.partition_key();
        let envelope =
            Self::create_event_envelope(event_type, &event.metadata, event.payload.into_proto());

        (event_type, key, envelope)
    }

    /// Sends the event to the Kafka topic
    ///
    /// Errors are logged, but do not fail as it is a best-effort
    async fn send_event(
        producer: &Arc<KafkaProducer>,
        event_type: SubgraphIndexingAgreementEventType,
        key: &str,
        event: proto::SubgraphIndexingAgreementEvent,
    ) {
        let mut buf = Vec::with_capacity(event.encoded_len());
        if let Err(e) = event.encode(&mut buf) {
            tracing::error!(
                event_type = %event_type,
                error = %e,
                "failed to encode Subgraph Indexing Agreement event"
            );
            return;
        }

        if let Err(e) = producer.send(key, &buf).await {
            tracing::warn!(
                event_type = %event_type,
                key,
                error = %e,
                "failed to send Subgraph Indexing Agreement event to producer (event dropped)"
            );
        }
    }

    /// Creates the event envelope with common metadata
    fn create_event_envelope(
        event_type: SubgraphIndexingAgreementEventType,
        metadata: &EventMetadata,
        payload: proto::subgraph_indexing_agreement_event::Payload,
    ) -> proto::SubgraphIndexingAgreementEvent {
        proto::SubgraphIndexingAgreementEvent {
            event_id: uuid::Uuid::now_v7().to_string(),
            event_type: event_type.to_string(),
            event_version: "1.0".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            subgraph_deployment_qm_hash: metadata.subgraph_deployment_qm_hash.to_string(),
            the_graph_network: metadata.the_graph_network.to_string(),
            payload: Some(payload),
        }
    }
}

/// A queued event: shared routing metadata plus its type-specific payload.
struct QueuedSubgraphIndexingAgreementEvent {
    metadata: EventMetadata,
    payload: EventPayload,
}

/// Routing metadata common to every Subgraph Indexing Agreement event.
struct EventMetadata {
    /// The Subgraph deployment. Rendered to its `Qm...` hash representation
    /// when building the envelope and partition key.
    subgraph_deployment_qm_hash: DeploymentId,
    /// The Graph protocol network. Rendered to its CAIP-2 form
    /// (e.g. `eip155:42161`) when building the envelope and partition key.
    the_graph_network: Caip2ChainId,
}

impl EventMetadata {
    /// Creates the partition key for the Subgraph Indexing Agreement events
    ///
    /// Format: `{the_graph_network}/{subgraph_deployment_qm_hash}`
    /// e.g. `eip155:42161/QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9`
    fn partition_key(&self) -> String {
        format!(
            "{}/{}",
            self.the_graph_network, self.subgraph_deployment_qm_hash
        )
    }
}

/// The type-specific payload of a Subgraph Indexing Agreement event.
enum EventPayload {
    RequestReceived(proto::SubgraphIndexingAgreementRequestReceived),
    Proposed(proto::SubgraphIndexingAgreementProposed),
    Accepted(proto::SubgraphIndexingAgreementAccepted),
    RequestExpired(proto::SubgraphIndexingAgreementRequestExpired),
    NIndexersUnavailable(proto::SubgraphIndexingAgreementNIndexersUnavailable),
    Terminated(proto::SubgraphIndexingAgreementTerminated),
}

impl EventPayload {
    fn event_type(&self) -> SubgraphIndexingAgreementEventType {
        match self {
            Self::RequestReceived(_) => SubgraphIndexingAgreementEventType::RequestReceived,
            Self::Proposed(_) => SubgraphIndexingAgreementEventType::Proposed,
            Self::Accepted(_) => SubgraphIndexingAgreementEventType::Accepted,
            Self::RequestExpired(_) => SubgraphIndexingAgreementEventType::RequestExpired,
            Self::NIndexersUnavailable(_) => {
                SubgraphIndexingAgreementEventType::NIndexersUnavailable
            }
            Self::Terminated(_) => SubgraphIndexingAgreementEventType::Terminated,
        }
    }

    fn into_proto(self) -> proto::subgraph_indexing_agreement_event::Payload {
        use proto::subgraph_indexing_agreement_event::Payload;
        match self {
            Self::RequestReceived(e) => Payload::SubgraphIndexingAgreementRequestReceived(e),
            Self::Proposed(e) => Payload::SubgraphIndexingAgreementProposed(e),
            Self::Accepted(e) => Payload::SubgraphIndexingAgreementAccepted(e),
            Self::RequestExpired(e) => Payload::SubgraphIndexingAgreementRequestExpired(e),
            Self::NIndexersUnavailable(e) => {
                Payload::SubgraphIndexingAgreementNIndexersUnavailable(e)
            }
            Self::Terminated(e) => Payload::SubgraphIndexingAgreementTerminated(e),
        }
    }
}

/// Subgraph Indexing Agreement Event type discriminator for events
#[derive(Debug, Clone, Copy)]
enum SubgraphIndexingAgreementEventType {
    RequestReceived,
    Proposed,
    Accepted,
    RequestExpired,
    NIndexersUnavailable,
    Terminated,
}

impl std::fmt::Display for SubgraphIndexingAgreementEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::RequestReceived => "subgraph.indexing.agreement.request.received",
            Self::Proposed => "subgraph.indexing.agreement.proposed",
            Self::Accepted => "subgraph.indexing.agreement.accepted",
            Self::RequestExpired => "subgraph.indexing.agreement.request.expired",
            Self::NIndexersUnavailable => "subgraph.indexing.agreement.n_indexers_unavailable",
            Self::Terminated => "subgraph.indexing.agreement.terminated",
        };
        f.write_str(s)
    }
}

#[cfg(test)]
mod tests {
    use proto::subgraph_indexing_agreement_event::Payload;

    use super::*;

    const HASH: &str = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9";
    const CHAIN_ID: ChainId = 42161;
    const NETWORK: &str = "eip155:42161";

    fn deployment_id() -> DeploymentId {
        HASH.parse().expect("HASH is a valid deployment id")
    }

    fn metadata() -> EventMetadata {
        EventMetadata {
            subgraph_deployment_qm_hash: deployment_id(),
            the_graph_network: CHAIN_ID.into(),
        }
    }

    /// Every payload variant paired with the event-type string and proto payload
    /// variant it is contractually required to map to. Adding a new event variant
    /// without updating this table is a compile error (non-exhaustive match below).
    fn all_payloads() -> Vec<(EventPayload, &'static str)> {
        vec![
            (
                EventPayload::RequestReceived(Default::default()),
                "subgraph.indexing.agreement.request.received",
            ),
            (
                EventPayload::Proposed(Default::default()),
                "subgraph.indexing.agreement.proposed",
            ),
            (
                EventPayload::Accepted(Default::default()),
                "subgraph.indexing.agreement.accepted",
            ),
            (
                EventPayload::RequestExpired(Default::default()),
                "subgraph.indexing.agreement.request.expired",
            ),
            (
                EventPayload::NIndexersUnavailable(Default::default()),
                "subgraph.indexing.agreement.n_indexers_unavailable",
            ),
            (
                EventPayload::Terminated(Default::default()),
                "subgraph.indexing.agreement.terminated",
            ),
        ]
    }

    #[test]
    fn caip2_chain_id_from_chain_id_renders_eip155() {
        let network: Caip2ChainId = CHAIN_ID.into();
        assert_eq!(network.to_string(), NETWORK);
        assert_eq!(network.to_string(), "eip155:42161");
    }

    #[test]
    fn partition_key_uses_network_slash_hash_format() {
        assert_eq!(metadata().partition_key(), format!("{NETWORK}/{HASH}"));
    }

    #[test]
    fn event_type_string_matches_payload_for_every_variant() {
        for (payload, expected) in all_payloads() {
            assert_eq!(payload.event_type().to_string(), expected);
        }
    }

    #[test]
    fn into_proto_preserves_the_variant_for_every_payload() {
        for (payload, expected) in all_payloads() {
            // Guards against a copy-paste swap between two payload arms.
            let mapped = match payload.into_proto() {
                Payload::SubgraphIndexingAgreementRequestReceived(_) => {
                    "subgraph.indexing.agreement.request.received"
                }
                Payload::SubgraphIndexingAgreementProposed(_) => {
                    "subgraph.indexing.agreement.proposed"
                }
                Payload::SubgraphIndexingAgreementAccepted(_) => {
                    "subgraph.indexing.agreement.accepted"
                }
                Payload::SubgraphIndexingAgreementRequestExpired(_) => {
                    "subgraph.indexing.agreement.request.expired"
                }
                Payload::SubgraphIndexingAgreementNIndexersUnavailable(_) => {
                    "subgraph.indexing.agreement.n_indexers_unavailable"
                }
                Payload::SubgraphIndexingAgreementTerminated(_) => {
                    "subgraph.indexing.agreement.terminated"
                }
            };
            assert_eq!(mapped, expected);
        }
    }

    #[test]
    fn create_event_envelope_populates_common_metadata() {
        let payload =
            EventPayload::RequestReceived(proto::SubgraphIndexingAgreementRequestReceived {
                agreements_requested: 2,
            });
        let event_type = payload.event_type();
        let envelope = SubgraphIndexingAgreementsEventsEmitter::create_event_envelope(
            event_type,
            &metadata(),
            payload.into_proto(),
        );

        assert_eq!(
            envelope.event_type,
            "subgraph.indexing.agreement.request.received"
        );
        assert_eq!(envelope.event_version, "1.0");
        assert_eq!(envelope.subgraph_deployment_qm_hash, HASH);
        assert_eq!(envelope.the_graph_network, NETWORK);
        assert!(matches!(
            envelope.payload,
            Some(Payload::SubgraphIndexingAgreementRequestReceived(_))
        ));

        let id = uuid::Uuid::parse_str(&envelope.event_id).expect("event_id is a valid uuid");
        assert_eq!(id.get_version_num(), 7, "event_id should be a UUIDv7");
        chrono::DateTime::parse_from_rfc3339(&envelope.timestamp)
            .expect("timestamp is valid rfc3339");
    }

    #[test]
    fn deployment_id_renders_to_qm_hash_in_envelope_and_partition_key() {
        // The emitter takes a strongly-typed DeploymentId and is responsible for
        // rendering it to its Qm-hash string on the wire. Assert that rendering is
        // the canonical Qm hash (round-trips back to the same DeploymentId) and that
        // the partition key uses the same rendering.
        let dep = deployment_id();
        let envelope = SubgraphIndexingAgreementsEventsEmitter::create_event_envelope(
            SubgraphIndexingAgreementEventType::RequestReceived,
            &metadata(),
            EventPayload::RequestReceived(Default::default()).into_proto(),
        );

        assert_eq!(envelope.subgraph_deployment_qm_hash, HASH);
        let parsed: DeploymentId = envelope
            .subgraph_deployment_qm_hash
            .parse()
            .expect("envelope qm hash parses back into a DeploymentId");
        assert_eq!(parsed, dep, "qm-hash rendering must be lossless");

        assert_eq!(
            metadata().partition_key(),
            format!("eip155:42161/{dep}"),
            "partition key must use the caip2 network and the Qm-hash rendering"
        );
    }

    #[test]
    fn prepare_event_builds_key_type_and_wire_encodable_envelope() {
        let event = QueuedSubgraphIndexingAgreementEvent {
            metadata: metadata(),
            payload: EventPayload::Terminated(proto::SubgraphIndexingAgreementTerminated {
                indexer: "0xabc".to_string(),
                terminated_at: 42,
                terminated_by: "0xdef".to_string(),
                terminated_tx: "0x123".to_string(),
                ..Default::default()
            }),
        };

        let (event_type, key, envelope) =
            SubgraphIndexingAgreementsEventsEmitter::prepare_event(event);

        assert_eq!(
            event_type.to_string(),
            "subgraph.indexing.agreement.terminated"
        );
        assert_eq!(key, format!("{NETWORK}/{HASH}"));

        // Round-trips through the same prost encode path `send_event` uses.
        let mut buf = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut buf).expect("envelope encodes");
        let decoded =
            proto::SubgraphIndexingAgreementEvent::decode(&buf[..]).expect("envelope decodes");
        assert_eq!(decoded, envelope);
    }
}
