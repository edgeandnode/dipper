import random
import os
from typing import List, Optional
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


# --- Configuration ---

PORT = int(os.getenv("PORT", "8080"))
HOST = os.getenv("HOST", "0.0.0.0")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# --- Logging ---

# Configure logger with timestamp, name, level, and message
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Service-specific logger
logger = logging.getLogger("iisa-service")


# --- FastAPI App ---

# The IISA service needs to expose a HTTP API for selecting indexers to receive
# indexing agreements. FastAPI handles all the protocol details for us, managing
# passing HTTP requests, routing to the right function, and formatting the response.
app = FastAPI(title="IISA Service")


# --- Request and Response Structs ---


# The SelectionRequest defines what data clients must send to the API
class SelectionRequest(BaseModel):
    deployment_id: str
    candidates: List[str]
    num_candidates: Optional[int] = None


# The SingleSelectionResponse defines the response format for the select-one endpoint
class SingleSelectionResponse(BaseModel):
    indexer_id: Optional[str]


# The MultiSelectionResponse defines the response format for the select-many endpoint
class MultiSelectionResponse(BaseModel):
    indexer_ids: List[str]


# --- Routes ---


# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# Select-one endpoint
@app.post("/select-one", response_model=SingleSelectionResponse)
async def select_one(request: SelectionRequest):
    """
    Selects a single candidate indexer for indexing a Subgraph deployment.

    Currently implements a simple random selection algorithm.
    """
    logger.info(
        f"Selecting one indexer for deployment {request.deployment_id} from {len(request.candidates)} candidates"
    )

    # If no candidates are provided, return None
    if not request.candidates:
        return SingleSelectionResponse(indexer_id=None)

    # Select a random candidate
    selected = random.choice(request.candidates)
    return SingleSelectionResponse(indexer_id=selected)


# Select-many endpoint
@app.post("/select-many", response_model=MultiSelectionResponse)
async def select_many(request: SelectionRequest):
    """
    Selects multiple candidate indexers for indexing a Subgraph deployment.

    Currently implements a simple random selection algorithm.
    """
    # If num_candidates is not provided, raise an error
    if request.num_candidates is None:
        raise HTTPException(
            status_code=400, detail="num_candidates is required for select-many"
        )

    # Log the selection request
    logger.info(
        f"Selecting {request.num_candidates} indexers for deployment {request.deployment_id} from {len(request.candidates)} candidates"
    )

    # If no candidates are provided, return an empty list
    if not request.candidates or request.num_candidates <= 0:
        return MultiSelectionResponse(indexer_ids=[])

    # Select a random subset of candidates
    k = min(request.num_candidates, len(request.candidates))
    selected = random.choices(request.candidates, k=k)
    return MultiSelectionResponse(indexer_ids=selected)


# --- Main ---

if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting IISA service on {HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)
