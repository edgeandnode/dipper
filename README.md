Dipper
------
[![ci](https://github.com/edgeandnode/dipper/actions/workflows/ci.yml/badge.svg)](https://github.com/edgeandnode/dipper/actions/workflows/ci.yml)

This repository contains the code for the DIPs Gateway service, a.k.a. _Dipper_. 

### Documentation

- [Admin CLI](docs/dipper-cli.md) - Documentation for the DIPs Admin CLI

### Operational notes

#### IISA dependency

Dipper depends on the Indexing Indexer Selection Algorithm (IISA) service
for picking which indexers should serve an indexing request. When IISA is
unreachable, the reassessment job retries the selection request with
exponential backoff and surfaces the failure in logs — there is no
fallback selection path. Any operation that requires a fresh target-set
decision blocks: new registrations, grow / shrink / zero-out calls on an
existing request, and the periodic reassignment sweep. Already-accepted
on-chain agreements are unaffected — indexers continue indexing and
collecting payment via the RecurringCollector contract independently of
IISA. Operators should monitor IISA availability and treat a prolonged
IISA outage as a dipper outage for any change to the target indexer set.

### Contributing

Please refer to our [CONTRIBUTING.md docs](CONTRIBUTING.md) for more information on 
how contribute to this project.
