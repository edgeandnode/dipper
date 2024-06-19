This repsitory contains the code for the dipper service. This service is responsible for implementing the indexer selection algorithm for direct indexer payments (DIPs).

To do this, we call out to python code that is responsible for running the indexer selection algorithm.

Directory structure:

- `assets/`: Contains the python code for the indexer selection algorithm.
- `crates/bin/dipper-service`: The main entry point for the dipper service.
- `crates/bin/dipper-cli`: Cli utility for manipulation of the service.
- `crates/dipper-common`: Common code shared between the service and the cli.


## Integration tests
To run the integration tests, you need to have local-network running, and you can invoke the integration tests crate like this:

An `xtask` script is provided to run the integration tests. You can run the integration tests like this:

```bash
cargo xtask integration-tests
```

Alternatively, you can run the integration tests directly like this:
```bash
cargo test -p integration-tests --feature "integration-tests" -- --nocapture
```
