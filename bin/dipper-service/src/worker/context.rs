/// Context is a struct that holds all the dependencies that a worker needs to run.
#[derive(Clone)]
pub struct Context<Q, N, R, C, I> {
    pub queue: Q,
    pub network: N,
    pub registry: R,
    pub indexer_client: C,
    pub iisa: I,
}
