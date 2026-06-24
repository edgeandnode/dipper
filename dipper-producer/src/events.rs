//! Subgraph Indexing agreement event streaming module.
//!
//! This module provides infrastructure for emitting Subgraph Indexing agreement lifecycle events:
//! - subgraph.indexing.agreement.request.receive
//! - subgraph.indexing.agreement.proposed
//! - subgraph.indexing.agreement.accepted
//! - subgraph.indexing.agreement.request.expired
//! - subgraph.indexing.agreement.n_indexers_unavailable
//! - subgraph.indexing.agreement.terminated
//!
//! # Example - disabled
//!
//! ```ignore
//! use dipper_producer::events::SubgraphIndexingAgreementsEventsEmitter;
//!
//! SubgraphIndexingAgreementsEventsEmitter::disabled();
//! ```
//!
//! # Example - enabled with Kafka config
//!
//! ```ignore
//! use dipper_producer::{
//!     events::SubgraphIndexingAgreementsEventsEmitter,
//!     kafka::{KafkaConfig, KafkaProducer, proto}
//! };
//!
//! let config = KafkaConfig {
//!     brokers: vec!["localhost:9092".to_string()],
//!     topic: "dipper.subgraph.indexing.agreement.events".to_string(),
//!     partitions: 16,
//! };
//!
//! let producer = match tokio::time::timeout(
//!     std::time::Duration::from_secs(30),
//!     KafkaProducer::new(&config),
//! )
//! .await
//! {
//!     Ok(Ok(producer)) => producer,
//!     Ok(Err(err)) => {
//!         tracing::warn!(error = %err, "failed to initialize KafkaProducer, events disabled");
//!         return std::sync::Arc::new(SubgraphIndexingAgreementsEventsEmitter::disabled());
//!     },
//!     Err(elapsed) => {
//!         tracing::warn!(error = %elapsed, "failed to initialize KafkaProducer, events disabled");
//!         return std::sync::Arc::new(SubgraphIndexingAgreementsEventsEmitter::disabled());
//!     }
//! };
//!
//! let emitter = std::sync::Arc::new(SubgraphIndexingAgreementsEventsEmitter::enabled(
//!     std::sync::Arc::new(producer),
//!     16,
//! ));
//!
//! emitter.produce_subgraph_indexing_agreement_request_received(
//!     "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9".to_string(),
//!     "arbitrum".to_string(),
//!     proto::SubgraphIndexingAgreementRequestReceived {
//!         agreements_requested: 2
//!     }
//! );
//! ```

mod subgraph_indexing_agreements_events_emitter;

pub use subgraph_indexing_agreements_events_emitter::SubgraphIndexingAgreementsEventsEmitter;
