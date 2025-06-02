---
title: RFC-001 - On-Chain SAFE-based Payments
name: RFC-001 - On-Chain SAFE-based Payments
slug: 001-on-chain-safe-payments
status: draft
tags: ["dipper-service"]
editor: "Lorenzo Delgado <lorenzo@edgeandnode.com>"
contributors: ["Lorenzo Delgado <lorenzo@edgeandnode.com>"]
---

## Abstract

This RFC proposes migrating the dipper service from the current TAP (Timeline Aggregation Protocol) based payment mechanism to an On-Chain SAFE-based payment system. The new system will replace immediate TAP receipt responses with Receipt IDs, implement asynchronous on-chain payment processing through worker tasks, and provide indexers with a polling mechanism to track payment status. This change addresses the limitations of the current synchronous payment model and provides better reliability, transparency, and on-chain verifiability for payment processing.

## Background

The decision to migrate from TAP was specifically motivated by significant allocation requirements that make TAP impractical for the dipper service's payment amounts. As identified during development:

> "There's gonna be a rather big problem with using TAP for indexing fees in the MVP. We're thinking of paying monthly amounts between 5 and 100 dollars, and that means exponential rebates will require indexers to allocate between 50 and 1000 dollars to be able to collect these payments."

This introduces several critical challenges:

1. **High Capital Requirements**: Indexers need to allocate $50-$1000 per subgraph for relatively small payments ($5-$100)
2. **Complex Allocation Management**: Variable allocation amounts create complexity in indexer agent configuration
3. **Allocation Calculation Difficulty**: Determining appropriate allocation amounts depends on chain pricing and expected query volumes
4. **Capital Efficiency**: Indexers must keep high amounts of stake free specifically for DIPs, reducing overall capital efficiency
5. **Missing Infrastructure**: TAP escrow management functionality is not yet implemented

### Alternative Approaches Considered

During the design process, three main approaches were evaluated:

**Option A: Continue with TAP** - Rejected due to the allocation requirements and complexity issues outlined above.

**Option B: Dummy Subgraph with Staking.collect** - Using a new allocation to a dummy subgraph at collection time and paying using `Staking.collect` (similar to StreamingFast's substreams approach). This was considered but adds unnecessary complexity around dummy subgraph management.

**Option C: Direct SAFE-based Payments** - Have a dedicated account for the dipper that transfers GRT directly to indexers using a SAFE multisig for batching and security. This approach includes a 1% burn mechanism to maintain protocol tax compliance.

This RFC implements **Option C** as it provides the best balance of simplicity, capital efficiency, and protocol compliance.

### Current TAP Workflow

The dipper service currently implements the following payment workflow:
- Indexer reports work via GRPC `collect_payment` endpoint
- Service validates the work and calculates fees
- TAP receipt is created and signed using `ReceiptSigner`
- Receipt is immediately returned to the indexer as serialized bytes

This RFC addresses the need for a more robust, transparent, and verifiable payment system that leverages on-chain SAFE transactions.

## Design

### Overview

Replace the TAP-based payment system with an on-chain SAFE-based approach that:

1. **Returns Receipt IDs** instead of TAP receipts for immediate response
2. **Implements asynchronous payment processing** using the existing worker system
3. **Provides status polling** for indexers to track payment progress
4. **Uses SAFE multisig contracts** for secure on-chain payment execution
5. **Maintains payment state** through a finite state machine

### Key Changes

#### 1. Workflow Modification
- **Current**: `report_work` → `validate` → `create_TAP_receipt` → `return_receipt_bytes`
- **Proposed**: `report_work` → `validate` → `create_receipt_record` → `queue_PAY_ON_CHAIN_job` → `return_receipt_ID`

#### 2. Payment Processing
- Move from synchronous TAP signing to asynchronous on-chain SAFE transactions
- Implement retry logic for failed payments
- Atomic status updates for receipt state management

#### 3. Status Tracking
- Introduce Receipt State Machine: `PENDING` → `SUBMITTED` → `COMPLETED` / `FAILED`
- Provide GRPC polling endpoint for indexers to check payment status

### Finite State Machine (FSM)

The receipt payment status will follow a well-defined finite state machine:

- **PENDING**: Payment not yet attempted or currently retrying
- **SUBMITTED**: Payment submitted to blockchain, transaction hash available
- **COMPLETED**: Payment confirmed on-chain with sufficient confirmations
- **FAILED**: Payment failed in a fatal, non-recoverable way

State transitions:
- `PENDING` → `SUBMITTED` (when payment is submitted to blockchain)
- `SUBMITTED` → `COMPLETED` (when transaction is confirmed)
- `PENDING` → `FAILED` (when fatal error occurs)
- `SUBMITTED` → `FAILED` (when transaction fails permanently)

```mermaid
stateDiagram-v2
    [*] --> PENDING : Receipt created
    
    PENDING --> SUBMITTED : Payment submitted\nto blockchain
    PENDING --> FAILED : Fatal error\noccurs
    
    SUBMITTED --> COMPLETED : Transaction\nconfirmed
    SUBMITTED --> FAILED : Transaction\nfails permanently
    
    COMPLETED --> [*]
    FAILED --> [*]
    
    note right of PENDING
        Initial state when receipt
        is created or retrying
    end note
    
    note right of SUBMITTED
        Transaction hash available,
        waiting for confirmation
    end note
    
    note right of COMPLETED
        Payment confirmed on-chain
        with sufficient confirmations
    end note
    
    note right of FAILED
        Payment failed in a fatal,
        non-recoverable way
    end note
```

### Indexer Workflow

This section describes the complete workflow from the indexer's perspective, illustrating how indexers interact with the new SAFE-based payment system.

#### Step 1: Collect Payment Request
The indexer sends a `collect_payment` request to the dipper service, reporting the amount of work performed. This request includes:
- **Agreement ID**: The indexing agreement identifier for which work is being reported
- **Allocation ID**: The Graph protocol allocation ID that the indexer used for indexing
- **Entity Count**: The absolute number of subgraph entities stored (not incremental since last collection)
- **Proof of Indexing (POI)**: Cryptographic proof that the indexer correctly indexed the deployment
- **Signature**: EIP-712 signature from the indexer's operator wallet to authenticate the request

#### Step 2: Dipper Processing and Response
Upon receiving the collect payment request, the dipper service:
1. **Performs validation checks** on the submitted work and ensures the indexer is eligible for payment
2. **Calculates the payment amount** based on the fee calculation formula: `(epochs_elapsed * base_price_per_epoch) + (entity_count * price_per_entity)`
3. **Creates a receipt record** in the registry with `PENDING` status, storing all payment details
4. **Submits a pay-on-chain job** to the worker queue for asynchronous processing
5. **Responds immediately** to the indexer with the unique Receipt ID

This immediate response ensures that the indexer's workflow is not blocked by the time required for on-chain payment processing.

#### Step 3: Asynchronous Payment Processing
While the indexer continues its operations, the dipper service processes payments asynchronously:
1. **Worker picks up the job** from the queue and begins payment processing
2. **SAFE transaction is created** and submitted to the blockchain
3. **Receipt status is updated** to `SUBMITTED` with the transaction hash once the payment is broadcast
4. **Transaction confirmation is monitored** until sufficient confirmations are received
5. **Receipt status is updated** to `COMPLETED` when the payment is fully confirmed on-chain

If any step fails, the worker implements retry logic or marks the payment as `FAILED` with appropriate error information.

#### Step 4: Status Polling and Verification
The indexer can poll the payment status at any time using the `get_receipt_by_id` RPC endpoint:
1. **Poll with Receipt ID** to retrieve current payment status and details
2. **Receive status updates** including current FSM state (`PENDING`, `SUBMITTED`, `COMPLETED`, `FAILED`)
3. **Access transaction hash** when the payment reaches `SUBMITTED` or `COMPLETED` status
4. **Verify payment on-chain** using the provided transaction hash for full transparency

This polling mechanism allows indexers to:
- Track payment progress in real-time
- Verify payments independently on the blockchain
- Integrate payment status into their own monitoring and accounting systems
- Handle any failed payments appropriately

#### Sequence Diagram

The following sequence diagram illustrates the complete payment workflow:

```mermaid
sequenceDiagram
    participant I as Indexer
    participant D as Dipper Service
    participant R as Registry
    participant W as Worker Queue
    participant S as SAFE Client
    participant B as Blockchain

    Note over I,B: Step 1: Collect Payment Request
    I->>D: collect_payment(agreement_id, allocation_id, entity_count, poi, signature)
    
    Note over I,B: Step 2: Dipper Processing and Response
    D->>D: Validate work and calculate payment amount
    D->>R: create_receipt_record(PENDING status)
    R-->>D: Receipt ID
    D->>W: queue_pay_on_chain_job(Receipt ID, amount, recipient)
    D->>I: CollectPaymentResponse(Receipt ID)
    
    Note over I,B: Step 3: Asynchronous Payment Processing
    W->>W: Pick up payment job
    W->>R: update_receipt_status(Receipt ID, SUBMITTED)
    W->>S: create_payment(recipient, amount)
    S->>B: Submit SAFE transaction
    B-->>S: Transaction hash
    S-->>W: Transaction hash
    W->>R: update_receipt_status(Receipt ID, SUBMITTED, tx_hash)
    
    Note over B: Transaction confirmation...
    
    W->>B: Check transaction confirmation
    B-->>W: Confirmed
    W->>R: update_receipt_status(Receipt ID, COMPLETED)
    
    Note over I,B: Step 4: Status Polling and Verification
    I->>D: get_receipt_by_id(Receipt ID)
    D->>R: get_receipt_by_id(Receipt ID)
    R-->>D: Receipt with status and tx_hash
    D->>I: ReceiptStatusResponse(COMPLETED, tx_hash, timestamps)
    
    I->>B: Verify transaction on-chain (optional)
    B-->>I: Transaction details confirmed
```

#### Benefits for Indexers
- **Non-blocking workflow**: Work reporting is not delayed by payment processing time
- **Full transparency**: Complete visibility into payment status and on-chain verification
- **Reliable tracking**: Persistent status that can be queried at any time
- **Verifiable payments**: On-chain transaction hashes provide cryptographic proof of payment

## Proposed Implementation

### Database Schema Changes

The existing indexing receipts table needs to be extended to support the new payment status tracking:

**Required new columns:**
- Payment status field: FSM state tracking (PENDING, SUBMITTED, COMPLETED, FAILED)
- Transaction hash field: On-chain transaction identifier when payment is submitted
- Payment submitted timestamp: When the payment was submitted to blockchain
- Payment completed timestamp: When the payment was confirmed on-chain
- Payment error field: Error message for failed payments
- Retry count field: Number of retry attempts for tracking

**Required indexes:**
- Index on payment status for efficient status-based queries
- Index on payment timestamps for time-based queries and cleanup

### Worker System Integration

#### New Message Type
Add a new payment message type to the existing worker message system alongside current indexing and agreement messages.

**Message structure requirements:**
- Include Receipt ID to identify which receipt to process
- Include payment amount for the payment value
- Include recipient address for the payment destination
- Follow existing worker message serialization patterns

#### Worker Handler Implementation
Create new payment handler following existing handler patterns:

**Handler functionality:**
- Update receipt status to SUBMITTED before attempting payment
- Execute SAFE transaction through SAFE client
- Handle successful payments by updating status to COMPLETED with transaction hash
- Implement retry logic for transient failures
- Mark permanently failed payments as FAILED status
- Ensure all status updates are atomic to prevent race conditions

### GRPC Interface Updates

#### Modified Payment Collection Response
Update the payment collection response structure:

**Changes required:**
- Replace TAP receipt bytes field with Receipt ID field
- Return the Receipt ID in the report work request response
- Maintain existing version and status fields for compatibility
- Ensure Receipt IDs are unique and suitable for polling

#### Update Existing Handlers
The payment collection handler needs major refactoring:
- Remove TAP receipt creation logic
- Change response from TAP receipt bytes to Receipt ID
- Add payment job queuing after receipt registration

#### New Polling Endpoint
Add new receipt status method to the existing GRPC service:

**Request structure:**
- Version field for API versioning
- Receipt ID field to identify the receipt to query

**Response structure:**
- Version field for API versioning  
- Status enum field (PENDING, SUBMITTED, COMPLETED, FAILED)
- Optional transaction hash field populated when status is SUBMITTED/COMPLETED
- Optional payment submitted timestamp field
- Optional payment completed timestamp field  
- Optional error message field populated when status is FAILED

### SAFE Client Implementation

Create new SAFE client module with trait-based architecture:

**SAFE Client interface requirements:**
- Submit payment method accepting recipient address and amount
- Return transaction hash on successful submission
- Proper error handling distinguishing retryable vs fatal errors
- Async implementation compatible with existing worker system

**SAFE Client implementation needs:**
- SAFE contract address configuration
- RPC client for blockchain interaction
- Private key signer for transaction signing
- Gas estimation and management functionality
- Transaction confirmation tracking

### Configuration Changes

#### Remove TAP Configuration
**Components to remove:**
- TAP signer configuration structure
- TAP signer field from main configuration
- TAP signer initialization code
- TAP signer from indexer RPC server context

#### Add SAFE Configuration
**New SAFE client configuration requirements:**
- SAFE contract address for payment operations
- RPC endpoint URL for blockchain connectivity
- Private key configuration for transaction signing
- Gas limit and pricing parameters
- Chain ID for network identification
- Secure handling of sensitive configuration data

### Registry Interface Extensions

Extend the existing receipt registry interface with new methods:

**New method requirements:**
- Update receipt payment status: Atomic status updates with optional transaction hash
- Get receipt by Receipt ID: Retrieve receipt with current payment status for polling
- Get pending receipts: Query for receipts in PENDING state (admin functionality)
- Get failed receipts: Query for receipts in FAILED state (admin functionality)

**New data structures:**
- Payment status enumeration with PENDING, SUBMITTED, COMPLETED, FAILED states
- Receipt with status structure combining receipt data with payment status
- Proper error handling for all registry operations

### Admin CLI Enhancements

The existing admin RPC server structure will be extended with new handlers for receipt management:

- List pending receipts: Query receipts in PENDING state
- List failed receipts: Query receipts in FAILED state  
- Retry failed receipt: Manually retry a failed receipt
- Get receipt details: Get detailed receipt information including payment history

## Migration Strategy

Since the service is under heavy development with no production deployment:

### 1. Direct Schema Updates
- Modify existing migration file directly
- Add payment status columns to receipts table
- No data preservation needed

### 2. Code Removal
Remove TAP-related components:
- TAP signing module
- TAP receipt creation in indexer RPC handlers
- Receipt signer initialization
- Receipt signer usage and initialization
- TAP configuration structures
- TAP-related imports and dependencies

### 3. Implementation Phases
1. Database schema changes and new registry methods
2. SAFE client implementation and worker handler
3. GRPC interface updates and polling endpoint
4. Remove TAP components
5. Integration and testing

## Monitoring and Observability

### 1. Metrics
- Track payment processing times
- Monitor success/failure rates
- Alert on stuck payments in PENDING state

### 2. Logging
- Log all state transitions
- Include transaction hashes in logs
- Add structured logging for payment events

## Testing Strategy

### 1. Unit Tests
- Test SAFE client functionality
- Test worker handler logic
- Test registry operations

### 2. Integration Tests
- Test end-to-end payment flow
- Test failure scenarios and retries
- Test GRPC endpoints

### 3. Load Testing
- Verify system performance under load
- Test worker queue capacity
- Validate database performance

## Alternatives Considered

### Synchronous SAFE Payments
**Rejected** due to potential delays in work reporting workflow and poor user experience during network congestion.

## Open Questions

1. **Gas Fee Management**: How should gas fees be handled? Fixed allocation or dynamic estimation?
2. **Transaction Batching**: Should multiple payments be batched into single SAFE transactions?
3. **Confirmation Requirements**: How many confirmations should be required before marking payments as completed?
4. **Error Recovery**: What should happen to receipts that fail repeatedly?

## Success Criteria

1. **Functional**: Indexers can successfully report work and receive payment confirmations
2. **Performance**: Payment processing doesn't significantly impact work reporting latency  
3. **Reliability**: 99.9% of payments are successfully processed within acceptable timeframes
4. **Transparency**: All payments are verifiable on-chain with transaction hashes

## Security/Privacy Considerations

### Private Key Management
- SAFE client private keys MUST be securely stored and managed
- Private keys MUST NOT be logged or exposed in configuration files

### Transaction Security
- All payment amounts and recipient addresses MUST be validated
- Proper gas estimation and limits MUST be implemented
- Transaction confirmation requirements MUST be enforced
- Payment data MUST be validated against expected ranges and formats

### State Consistency
- Receipt status updates MUST be atomic to prevent race conditions
- Error handling and rollback mechanisms MUST be implemented
- Monitoring for failed transactions MUST be added
- Database transactions MUST be used for all payment state changes

### Data Privacy
- Transaction hashes are public on-chain but don't expose sensitive user data
- Payment amounts and addresses follow existing privacy model
- No additional privacy implications beyond current system

## Copyright

Copyright and related rights waived [via CC0](https://creativecommons.org/publicdomain/zero/1.0/).

## References

### Normative References

- [RFC2119] Bradner, S., "Key words for use in RFCs to Indicate Requirement Levels", BCP 14, RFC 2119, DOI 10.17487/RFC2119, March 1997, <https://www.rfc-editor.org/rfc/rfc2119.html>.

### Informative References

- [SAFE Smart Account Documentation](https://docs.safe.global/)
- [Current TAP Implementation](bin/dipper-service/src/signing/tap.rs)
- [Worker System Architecture](bin/dipper-service/src/worker/)
- [GRPC Interface Definitions](dipper-rpc/src/indexer.rs)
