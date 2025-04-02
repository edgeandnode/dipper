use std::time::Duration;

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use serde_with::serde_as;
use url::Url;

use crate::{
    indexer_rpc_client::IndexerClient,
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::JobResult,
};

pub struct Ctx<R, C> {
    pub registry: R,
    pub indexer_client: C,
}

/// Send an indexing agreement cancellation to the indexer.
///
/// This message is sent to the indexers to notify them that an indexing agreement
/// has been cancelled.
#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub indexer_url: Url,
    pub indexing_request_id: IndexingRequestId,
    pub agreement_id: IndexingAgreementId,
}

/// Send an indexing agreement cancellation to the indexer.
///
/// This function sends an indexing agreement cancellation to the indexer. If the notification
/// fails, retry after 10 seconds.
pub async fn handle<R, C>(
    ctx: Ctx<R, C>,
    Message {
        indexer_url,
        indexing_request_id,
        agreement_id,
    }: &Message,
) -> anyhow::Result<JobResult<()>>
where
    R: AgreementRegistry,
    C: IndexerClient,
{
    // TODO: THIS IS A HACK
    let indexer_url = {
        let mut url = indexer_url.clone();
        url.set_port(Some(7602)).unwrap();
        url
    };

    // Check the status of the agreement before sending the cancellation
    let agreement = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await?;
    match agreement {
        None => {
            tracing::error!(
                %indexing_request_id,
                %agreement_id,
                "Indexing agreement not found"
            );
            return Ok(JobResult::Ok(()));
        }
        Some(agreement) => {
            // In debug builds, log an error if the agreement is not in the expected state
            #[cfg(debug_assertions)]
            if !matches!(
                agreement.status,
                IndexingAgreementStatus::CanceledByRequester
            ) {
                tracing::error!(
                    %indexing_request_id,
                    %agreement_id,
                    "Invalid agreement status: '{}'. Not sending cancellation notification",
                    agreement.status,
                );
                return Ok(JobResult::Ok(()));
            }
        }
    }

    tracing::debug!(
        %indexing_request_id,
        %agreement_id,
        %indexer_url,
        "Sending indexing agreement cancellation notification"
    );

    // If the notification fails, retry after 20 seconds
    if let Err(err) = ctx
        .indexer_client
        .send_indexing_agreement_cancellation_notification(&indexer_url, *agreement_id)
        .await
    {
        tracing::error!(
            %indexing_request_id,
            %agreement_id,
            error=?err,
            "Failed to send indexing agreement cancellation. Trying again in 20 seconds"
        );
        return Ok(JobResult::Retry(Duration::from_secs(20), err.into()));
    };

    tracing::debug!(
        %indexing_request_id,
        %agreement_id,
        %indexer_url,
        "Indexing agreement cancellation accepted by indexer"
    );

    Ok(JobResult::Ok(()))
}
