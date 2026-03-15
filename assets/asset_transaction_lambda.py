"""
Lambda Function: Intellect Platform Asset Invoker with S3 Checkpoint
Flow:
    1. Read checkpoint.json from S3 for the given SE
    2. Derive date & last_run from the latest processed file across socialmedia + news
    3. Authenticate and invoke the platform asset
    4. Return response with SE

Environment Variables:
    AUTH_BASE_URL       - Auth base URL
    TENANT              - Tenant identifier
    API_KEY             - Platform API key
    WORKSPACE_ID        - Platform workspace ID
    PLATFORM_USERNAME   - Login username
    PLATFORM_PASSWORD   - Login password
    PLATFORM_BASE_URL   - Platform API base URL
    ASSET_ID            - Asset UUID to invoke
    BUCKET_NAME         - S3 bucket (e.g. pf-gtm-general-purpose)
    CHECKPOINT_KEY      - Full S3 key to checkpoint.json (e.g. RBI-DIT/banking/checkpoint/checkpoint.json)

Input Event:
    { "SE": "IDFC" }
    { "SE": "IDFC", "Date": "2026-03-14", "last_run": "2026-03-14_00-34-50" } // Manually pass the input

checkpoint.json structure:
    {
        "IDFC": {
            "socialmedia": { "2026-03-12": ["2026-03-12_07-05-04.json", ...] },
            "news":        { "2026-03-12": ["2026-03-12_07-00-36.json", ...] }
        }
    }
"""

import os
import json
import boto3
import requests


s3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def get_latest_from_checkpoint(se_data: dict) -> tuple:
    """
    Derive date and last_run from the latest processed file across all sources and dates.

    Filenames are lexicographically sortable (e.g. 2026-03-13_04-34-46.json),
    so max() across all files gives the latest processed point.

    Args:
        se_data: checkpoint.json data for a specific SE
                 e.g. { "socialmedia": { "2026-03-12": ["file1.json", ...] }, "news": { ... } }

    Returns:
        (date, last_run) e.g. ("2026-03-13", "2026-03-13_04-34-46")
    """
    all_files = []
    for source_data in se_data.values():          # iterate socialmedia, news
        for file_list in source_data.values():    # iterate dates -> list of files
            all_files.extend(file_list)           # add individual filenames

    if not all_files:
        raise ValueError("No processed files found in checkpoint for this SE.")

    latest = max(all_files).replace(".json", "")  # e.g. "2026-03-13_04-34-46"
    date   = latest.split("_")[0]                 # e.g. "2026-03-13"

    print(f"All files count: {len(all_files)}, Latest: {latest}")
    return date, latest


def read_checkpoint(se: str) -> tuple:
    """Read checkpoint.json from S3 and return (date, last_run) for the given SE."""
    resp = s3.get_object(Bucket=os.environ["BUCKET_NAME"], Key=os.environ["CHECKPOINT_KEY"])
    checkpoint = json.loads(resp["Body"].read().decode("utf-8"))

    if se not in checkpoint:
        raise ValueError(f"No checkpoint found for SE: '{se}'")

    return get_latest_from_checkpoint(checkpoint[se])


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token() -> dict:
    """Authenticate and return access_token and refresh_token."""
    headers = {
        "apikey":       os.environ["API_KEY"],
        "username":     os.environ["PLATFORM_USERNAME"],
        "password":     os.environ["PLATFORM_PASSWORD"],
        "Content-Type": "application/json"
    }
    resp = requests.get(f"{os.environ['AUTH_BASE_URL']}/{os.environ['TENANT']}", headers=headers, timeout=30)
    resp.raise_for_status()

    data          = resp.json()
    access_token  = data.get("access_token")  or data.get("accessToken")
    refresh_token = data.get("refresh_token") or data.get("refreshToken")

    if not access_token:
        raise Exception("No access_token in auth response.")

    return {"access_token": access_token, "refresh_token": refresh_token}


# ---------------------------------------------------------------------------
# Asset Invocation
# ---------------------------------------------------------------------------

def invoke_asset(payload: dict, tokens: dict) -> dict:
    """POST payload to the platform asset endpoint."""
    url = f"{os.environ['PLATFORM_BASE_URL']}/invokeasset/{os.environ['ASSET_ID']}/usecase"

    headers = {
        "Accept":                 "application/json",
        "Content-Type":           "application/json",
        "apikey":                 os.environ["API_KEY"],
        "authorization":          f"Bearer {tokens['access_token']}",
        "refreshtoken":           tokens["refresh_token"],
        "x-platform-workspaceid": os.environ["WORKSPACE_ID"]
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    Entry point. Input: { "SE": "IDFC" }

    1. Derive date & last_run from latest file in checkpoint.json for the SE
    2. Authenticate
    3. Invoke asset with derived values
    4. Return response with SE
    """
    try:
        body = event.get("body", event)
        if isinstance(body, str):
            body = json.loads(body)

        se = body.get("SE")
        if not se:
            raise ValueError("Missing required field: 'SE'")

        # Step 1: Use input date & last_run if provided, otherwise derive from checkpoint
        input_date     = body.get("Date")
        input_last_run = body.get("last_run")

        if input_date and input_last_run:
            date, last_run = input_date, input_last_run
            print(f"Using input values for {se}: date={date}, last_run={last_run}")
        elif input_date or input_last_run:
            raise ValueError("Both 'Date' and 'last_run' are required if providing manual input.")
        else:
            date, last_run = read_checkpoint(se)
            print(f"Derived from checkpoint for {se}: date={date}, last_run={last_run}")

        # Step 2: Authenticate
        tokens = get_access_token()

        # Step 3: Invoke asset
        result = invoke_asset({"Date": date, "last_run": last_run, "SE": se}, tokens)

        # Step 4: Return response with SE
        result["SE"] = se
        result["Date"] = date
        result["last_run"] = last_run
        return {"statusCode": 200, "body": json.dumps(result)}

    except requests.HTTPError as e:
        return {"statusCode": e.response.status_code if e.response else 500, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}