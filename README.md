Dipper
------
[![ci](https://github.com/edgeandnode/dipper/actions/workflows/ci.yml/badge.svg)](https://github.com/edgeandnode/dipper/actions/workflows/ci.yml)

This repository contains the code for the DIPs Gateway service, a.k.a. _Dipper_. 

### Documentation

- [Admin CLI](docs/dipper-cli.md) - Documentation for the DIPs Admin CLI

### Operational notes

#### IISA dependency

Dipper depends on the Indexing Indexer Selection Algorithm (IISA) service
for picking which indexers should serve a given indexing request. When IISA
is unreachable, dipper's reassessment loop retries the selection request
with exponential backoff and surfaces the failure in logs but does not fall
back to an alternative selection mechanism. Newly registered indexing
requests will stall until IISA recovers; existing agreements continue to
operate normally. Operators should monitor IISA availability and treat a
prolonged IISA outage as a dipper outage for new-request scheduling
purposes.

### Contributing

Please refer to our [CONTRIBUTING.md docs](CONTRIBUTING.md) for more information on 
how contribute to this project.
