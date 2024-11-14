use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use serde_with::{serde_as, DisplayFromStr};
use thegraph_core::alloy::{
    primitives::{Address, ChainId},
    signers::k256::SecretKey,
};
use url::Url;

/// The configuration for the DIPs CLI
#[serde_as]
#[derive(custom_debug::CustomDebug, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// The URL of the DIPs gateway server
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "DisplayFromStr")]
    pub server_url: Url,

    /// The signing key to use for authentication
    #[serde_as(as = "HiddenSecretKeyAsHexStr")]
    pub signing_key: Hidden<SecretKey>,

    /// The DIPs payment wallet chain ID
    pub chain_id: ChainId,

    /// The address of the DIPs payment wallet
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payer: Option<Address>,
}
