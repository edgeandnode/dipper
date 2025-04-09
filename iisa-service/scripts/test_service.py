#!/usr/bin/env python3
import json
import requests
import argparse
import random


def test_select_one(base_url, deployment_id=None):
    """Test the select-one endpoint"""
    if deployment_id is None:
        deployment_id = f"Qm{random.randint(10000000, 99999999)}"

    # Generate random indexer IDs
    candidates = [f"0x{random.randint(0, 0xFFFFFFFF):08x}" for _ in range(5)]

    # Prepare request
    url = f"{base_url}/select-one"
    payload = {"deployment_id": deployment_id, "candidates": candidates}

    # Send request
    print(f"Sending request to {url} with payload: {json.dumps(payload, indent=2)}")
    response = requests.post(url, json=payload)

    # Process response
    print(f"Response status: {response.status_code}")
    print(f"Response body: {json.dumps(response.json(), indent=2)}")

    return response.json()


def test_select_many(base_url, deployment_id=None, num_candidates=2):
    """Test the select-many endpoint"""
    if deployment_id is None:
        deployment_id = f"Qm{random.randint(10000000, 99999999)}"

    # Generate random indexer IDs
    candidates = [f"0x{random.randint(0, 0xFFFFFFFF):08x}" for _ in range(5)]

    # Prepare request
    url = f"{base_url}/select-many"
    payload = {
        "deployment_id": deployment_id,
        "candidates": candidates,
        "num_candidates": num_candidates,
    }

    # Send request
    print(f"Sending request to {url} with payload: {json.dumps(payload, indent=2)}")
    response = requests.post(url, json=payload)

    # Process response
    print(f"Response status: {response.status_code}")
    print(f"Response body: {json.dumps(response.json(), indent=2)}")

    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Test the IISA service")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8080",
        help="Base URL of the IISA service",
    )
    parser.add_argument("--deployment-id", help="Deployment ID to use for testing")
    parser.add_argument(
        "--num-candidates",
        type=int,
        default=2,
        help="Number of candidates to select for select-many",
    )
    args = parser.parse_args()

    # Test health endpoint
    try:
        health_response = requests.get(f"{args.base_url}/health")
        print(f"Health check: {health_response.status_code}")
        print(f"Health response: {health_response.json()}")
    except Exception as e:
        print(f"Health check failed: {e}")
        return

    # Test select-one
    print("\n=== Testing select-one ===")
    test_select_one(args.base_url, args.deployment_id)

    # Test select-many
    print("\n=== Testing select-many ===")
    test_select_many(args.base_url, args.deployment_id, args.num_candidates)


if __name__ == "__main__":
    main()
