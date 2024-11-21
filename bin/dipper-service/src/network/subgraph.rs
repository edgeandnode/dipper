//! The network subgraph client API of the network service.
//!
//! The network subgraph client API provides functionality for querying the network subgraph
//! for information about the indexers in the network. This includes information such as the
//! indexers' ID, URL, allocations, and more.
//!
//! The retrieved information is preprocessed and returned in a structured format. Further
//! processing should be done to verify the information and to use it in the application.
//! The client module provides a high-level client API to query subgraphs.

mod client;
pub mod snapshot;

pub use client::Client;
