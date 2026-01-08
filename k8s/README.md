# Kubernetes Deployment

Kubernetes manifests for deploying the dipper service.

## Prerequisites

- IISA service must be deployed and healthy before dipper starts
- PostgreSQL database accessible from the cluster
- Configuration secrets managed separately

## Files

- `deployment.yaml` - Dipper deployment with health probes
- `service.yaml` - ClusterIP service exposing Admin RPC (8545) and Indexer RPC (50051)
- `configmap-example.yaml` - Example ConfigMap showing config.json structure (do not apply directly)

## Deployment Order

1. Deploy IISA service (see subgraph-dips-indexer-selection repo)
2. Create `dipper-config` ConfigMap with actual values
3. Apply dipper manifests

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
