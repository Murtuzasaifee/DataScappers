"""
Lambda Function: Intellect Platform Asset Invoker
Authenticates via API key/credentials, then POSTs to the asset invocation endpoint.

Environment Variables Required:
    AUTH_BASE_URL       - Base URL for authentication
    TENANT              - Tenant identifier for auth endpoint
    API_KEY             - Platform API key
    WORKSPACE_ID        - Platform workspace ID
    PLATFORM_USERNAME   - Login username
    PLATFORM_PASSWORD   - Login password
    PLATFORM_BASE_URL   - Base URL for the platform API
    ASSET_ID            - Asset UUID to invoke
"""

import os
import json
import requests


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token() -> dict:
    """
    Authenticate against the platform and return token data.

    Returns:
        dict with 'access_token' and 'refresh_token'

    Raises:
        Exception if authentication fails
    """
    auth_url = f"{os.environ['AUTH_BASE_URL']}/{os.environ['TENANT']}"

    headers = {
        "apikey":    os.environ["API_KEY"],
        "username":  os.environ["PLATFORM_USERNAME"],
        "password":  os.environ["PLATFORM_PASSWORD"],
        "Content-Type": "application/json"
    }

    response = requests.get(auth_url, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    access_token  = data.get("access_token")  or data.get("accessToken")
    refresh_token = data.get("refresh_token") or data.get("refreshToken")

    if not access_token:
        raise Exception("Authentication succeeded but no access_token in response.")

    return {"access_token": access_token, "refresh_token": refresh_token}


# ---------------------------------------------------------------------------
# API Call
# ---------------------------------------------------------------------------

def invoke_asset(body: dict, tokens: dict) -> dict:
    """
    POST the request body to the asset invocation endpoint.
    URL is constructed from PLATFORM_BASE_URL and ASSET_ID environment variables.

    Args:
        body:   Request payload (passed in from Lambda event)
        tokens: Auth tokens from get_access_token()

    Returns:
        Parsed JSON response from the platform
    """
    invoke_url = f"{os.environ['PLATFORM_BASE_URL']}/invokeasset/{os.environ['ASSET_ID']}/usecase"

    headers = {
        "Accept":                  "application/json",
        "Content-Type":            "application/json",
        "apikey":                  os.environ["API_KEY"],
        "authorization":           f"Bearer {tokens['access_token']}",
        "refreshtoken":            tokens["refresh_token"],
        "x-platform-workspaceid":  os.environ["WORKSPACE_ID"]
    }

    response = requests.post(invoke_url, headers=headers, json=body, timeout=30)
    response.raise_for_status()

    return response.json()


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Lambda entry point.

    Expected event body (passed as JSON string or dict):
        {
            "Date":     "2026-03-12",
            "last_run": "2026-03-12_00:00:00",
            "SE":       "IDFC"
        }

    Response includes SE from the input alongside the platform response.
    """
    try:
        # Parse body — supports both direct dict and JSON string (API Gateway)
        body = event.get("body", event)
        if isinstance(body, str):
            body = json.loads(body)

        # Step 1: Authenticate
        tokens = get_access_token()

        # Step 2: Invoke asset
        result = invoke_asset(body, tokens)

        # Step 3: Append SE from input to response
        result["SE"] = body.get("SE")

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except requests.HTTPError as e:
        return {
            "statusCode": e.response.status_code if e.response else 500,
            "body": json.dumps({"error": str(e)})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }