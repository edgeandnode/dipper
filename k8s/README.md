# Kubernetes Deployment

Kubernetes manifests for deploying the dipper service.

## Prerequisites

- PostgreSQL database accessible from the cluster
- Configuration secrets managed separately
- IISA service recommended (dipper falls back to random selection if unavailable)

## Files

- `deployment.yaml` - Dipper deployment with health probes and security context
- `service.yaml` - ClusterIP service exposing Admin RPC (8545) and Indexer RPC (50051)
- `configmap-example.yaml` - Example ConfigMap showing config.json structure (do not apply directly)

## Deployment

1. Create `dipper-config` ConfigMap with actual values
2. Apply dipper manifests
3. Deploy IISA service when ready (see subgraph-dips-indexer-selection repo)

Dipper can start without IISA - it gracefully degrades to random indexer selection until IISA becomes available.

## Configuration

The `configmap-example.yaml` shows the required config.json structure. Create a ConfigMap named `dipper-config` with your actual values:

```bash
kubectl create configmap dipper-config --from-file=config.json=your-config.json
```

Or apply a ConfigMap manifest with the values injected from your secrets management system.

## IISA Service Discovery

Dipper finds IISA via Kubernetes DNS. The config.json `iisa.endpoint` should be set to:

```
http://iisa:8080
```

This assumes IISA is deployed with a Service named `iisa` in the same namespace.

## Ports

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| Admin RPC | 8545 | TCP | JSON-RPC for admin operations |
| Indexer RPC | 50051 | TCP | gRPC for indexer interactions |
