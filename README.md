This repsitory contains the code for the dipper service. This service is responsible for implementing the indexer selection algorithm for direct indexer payments (DIPs).

To do this, we call out to python code that is responsible for running the indexer selection algorithm.

Directory structure:

- `assets/`: Contains the python code for the indexer selection algorithm.
- `crates/bin/dipper-service`: The main entry point for the dipper service.
- `crates/bin/dipper-cli`: Cli utility for manipulation of the service.
- `crates/dipper-common`: Common code shared between the service and the cli.
