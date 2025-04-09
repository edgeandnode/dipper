# IISA Service

This is a containerized service for the Indexing Indexer Selection Algorithm (IISA).
The service provides a REST API for selecting indexers to receive indexing agreements.

## API Endpoints

- `GET /health` - Health check endpoint
- `POST /select-one` - Select a single indexer for a deployment
- `POST /select-many` - Select multiple indexers for a deployment

## Running the Service

### Using Docker Compose

```bash
cd iisa-service
docker-compose up --build
```

### Manually Building and Running

```bash
cd iisa-service
docker build -t iisa-service .
docker run -p 8080:8080 iisa-service
```

## Testing the Service

A test script is provided in the `scripts` directory:

```bash
cd iisa-service
pip install requests
python scripts/test_service.py
```

## API Documentation

Once the service is running, API documentation is available at:
- Swagger UI: http://localhost:8080/docs
- ReDoc: http://localhost:8080/redoc 