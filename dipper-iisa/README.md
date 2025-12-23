dipper-iisa
===========

HTTP client library for communicating with the IISA (Indexing Indexer Selection Algorithm) service.

This crate provides the `HttpIisaClient` which implements the `CandidateSelection` trait for selecting indexers via HTTP requests to the containerized IISA service.

The IISA service is maintained in a separate repository: [edgeandnode/subgraph-dips-indexer-selection](https://github.com/edgeandnode/subgraph-dips-indexer-selection)
