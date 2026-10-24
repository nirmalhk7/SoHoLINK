#!/usr/bin/env python3
"""Test script for SoHoLINK HTTP API authentication and reputation endpoints."""

import base64
import json
import requests
import sys
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

REQUEST_TIMEOUT = 30

def load_private_key(key_path):
    """Load Ed25519 private key from PEM file."""
    with open(key_path, 'rb') as f:
        key_data = f.read()
    private_key = serialization.load_pem_private_key(
        key_data, password=None, backend=default_backend()
    )
    return private_key

def get_public_key_b64(private_key):
    """Get base64-encoded public key from private key."""
    public_key = private_key.public_key()
    public_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    )
    return base64.b64encode(public_bytes).decode('utf-8')

def authenticate(base_url, private_key_path, device_name="test-device"):
    """Authenticate and get device token."""
    # Step 1: Get a nonce
    challenge_url = f"{base_url}/api/auth/challenge"
    print(f"[*] Getting nonce from {challenge_url}")
    resp = requests.get(challenge_url, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        print(f"[!] Failed to get nonce: {resp.status_code} {resp.text}")
        return None

    nonce = resp.json()['nonce']
    print(f"[*] Nonce: {nonce}")

    # Step 2: Sign the nonce with private key
    private_key = load_private_key(private_key_path)
    public_key_b64 = get_public_key_b64(private_key)

    nonce_bytes = nonce.encode('utf-8')
    signature = private_key.sign(nonce_bytes)
    signature_b64 = base64.b64encode(signature).decode('utf-8')

    print(f"[*] Public key (b64): {public_key_b64[:50]}...")
    print(f"[*] Signature (b64): {signature_b64[:50]}...")

    # Step 3: Send signed nonce to /api/auth/connect
    connect_url = f"{base_url}/api/auth/connect"
    payload = {
        "nonce": nonce,
        "public_key": public_key_b64,
        "signature": signature_b64,
        "device_name": device_name
    }

    print(f"[*] Authenticating at {connect_url}")
    resp = requests.post(connect_url, json=payload, timeout=REQUEST_TIMEOUT)
    if resp.status_code != 200:
        print(f"[!] Authentication failed: {resp.status_code} {resp.text}")
        return None

    result = resp.json()
    token = result.get('token')
    print("[*] Authentication successful!")
    print(f"[*] Token: {token[:50]}..." if token else "[!] No token in response")

    return token

def query_api(base_url, endpoint, token):
    """Query a protected API endpoint with token."""
    url = f"{base_url}{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}

    print(f"\n[*] Querying {endpoint}")
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    print(f"    Status: {resp.status_code}")
    if resp.status_code == 200:
        try:
            data = resp.json()
            print(f"    Response (first 200 chars): {json.dumps(data, indent=2)[:200]}...")
            return data
        except ValueError:
            print(f"    Response: {resp.text[:200]}...")
    else:
        print(f"    Error: {resp.text}")

    return None

def print_available_files(config_dir):
    if not config_dir.exists():
        return
    for entry in config_dir.rglob("*"):
        if entry.is_file():
            print(f"    {entry.relative_to(config_dir)}")

def main():
    base_url = "http://localhost:8080"
    config_dir = Path.home() / ".soholink"
    private_key_path = config_dir / "identity" / "private.pem"

    if not private_key_path.exists():
        print(f"[!] Private key not found at {private_key_path}")
        print(f"[*] Available files in {config_dir}:")
        print_available_files(config_dir)
        return 1

    print("[*] SoHoLINK API Test")
    print(f"[*] Base URL: {base_url}")
    print(f"[*] Private key: {private_key_path}")

    # Authenticate
    token = authenticate(base_url, str(private_key_path))
    if not token:
        return 1

    # Query endpoints
    endpoints = [
        "/api/version",
        "/api/health",
        "/api/reputation/ledger",
        "/api/reputation/stats",
        "/api/topology/cluster/members",
    ]

    for endpoint in endpoints:
        if endpoint in ["/api/version", "/api/health"]:
            # These are public, don't need token
            print(f"\n[*] Querying {endpoint} (public)")
            resp = requests.get(f"{base_url}{endpoint}", timeout=REQUEST_TIMEOUT)
            print(f"    Status: {resp.status_code}")
            if resp.status_code == 200:
                print(f"    Response: {resp.text[:150]}...")
        else:
            # These need token
            query_api(base_url, endpoint, token)

    return 0

if __name__ == "__main__":
    sys.exit(main())
