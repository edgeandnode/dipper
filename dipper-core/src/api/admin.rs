pub mod indexing_requests {
    use thegraph_core::DeploymentId;

    use crate::{ids::IndexingRequestId, signed_message::ToSolStruct};

    /// The new indexing request message
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct NewIndexingRequest {
        /// The deployment ID of the subgraph that should be indexed
        pub deployment_id: DeploymentId,
    }

    impl ToSolStruct<NewIndexingRequestSol> for NewIndexingRequest {
        fn to_sol_struct(&self) -> NewIndexingRequestSol {
            NewIndexingRequestSol {
                deployment_id: self.deployment_id.into(),
            }
        }
    }

    thegraph_core::alloy::sol! {
        /// The new indexing request message (Solidity version)
        ///
        /// See: [`NewIndexingRequest::to_sol_struct(...)`](struct.NewIndexingRequest.html#method.to_sol_struct)
        struct NewIndexingRequestSol {
            bytes32 deployment_id;
        }
    }

    /// The cancel indexing request message
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct CancelIndexingRequest {
        /// The deployment ID of the subgraph that should be indexed
        pub id: IndexingRequestId,
    }

    impl ToSolStruct<CancelIndexingRequestSol> for CancelIndexingRequest {
        fn to_sol_struct(&self) -> CancelIndexingRequestSol {
            CancelIndexingRequestSol {
                id: self.id.as_bytes().into(),
            }
        }
    }

    thegraph_core::alloy::sol! {
        /// The cancel indexing request message (Solidity version)
        ///
        /// See: [`CancelIndexingRequest::to_sol_struct(...)`](struct.CancelIndexingRequest.html#method.to_sol_struct)
        struct CancelIndexingRequestSol {
            bytes16 id;
        }
    }
}
