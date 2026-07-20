//! A service providing information about the indexers in the network.

pub mod provider;
pub mod service;

#[cfg(test)]
mod tests {
    mod it_fetch_indexer_urls;
}
