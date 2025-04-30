import azure.functions as func
import logging
import json
import pandas as pd
import io
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from RealTimeMilitaryGovernmentGoodsChecker.functions.functions import transform_data
from AI_agents.Gemeni.adress_detector_mil_gov import AddressDetector

# === Key Vault and Blob Configuration ===
key_vault_url = "https://kv-functions-python.vault.azure.net"
blob_secret_name = "azure-storage-account-access-key2"
credential = DefaultAzureCredential()
kv_client = SecretClient(vault_url=key_vault_url, credential=credential)
api_key = kv_client.get_secret(blob_secret_name).value

# === Blob Storage Setup ===
CONNECTION_STRING = api_key
CONTAINER_NAME = "document-intelligence"
BLOB_NAME = "declarations-checker/adresses-checker/CHECKED_IDS.csv"
BLOB_NAME2 = "declarations-checker/adresses-checker/FOUND_ADDRESSES.csv"

def load_csv_from_blob():
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
    try:
        stream = blob_client.download_blob().readall()
        df = pd.read_csv(io.StringIO(stream.decode("utf-8")))
    except Exception:
        # If file doesn't exist or is empty
        df = pd.DataFrame(columns=["DECLARATIONID", "DATE"])
    return df, blob_client

def append_found_addresses_to_csv(matches: list):
    """
    Append matched address rows to FOUND_ADDRESSES.csv in Blob Storage.
    Adds 'checked' = False and empty 'checker' for each entry.
    """
    if not matches:
        return

    # Add 'checked' and 'checker' fields
    for obj in matches:
        obj["checked"] = False
        obj["checker"] = ""

    df_new = pd.DataFrame(matches)

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME2)

    try:
        stream = blob_client.download_blob().readall()
        df_existing = pd.read_csv(io.StringIO(stream.decode("utf-8")))
    except Exception:
        df_existing = pd.DataFrame()

    df_combined = pd.concat([df_existing, df_new], ignore_index=True)

    with io.StringIO() as output:
        df_combined.to_csv(output, index=False)
        output.seek(0)
        blob_client.upload_blob(output.getvalue(), overwrite=True)

# Function to clean the CSV file if it's arround 22:00
def clean_csv_file_if_needed():
    # Get current time
    current_time = datetime.now()

    # Check if the current hour is 22
    if current_time.hour == 22:
        # Load the CSV file from the Blob Storage
        df, blob_client = load_csv_from_blob()

        # Check if the DataFrame is empty
        if df.empty:
            logging.info("CSV file is already empty. No cleaning needed.")
        else:
            # If not empty, clear the content of the CSV file
            logging.info("Cleaning the CSV file...")

            # Create an empty DataFrame to overwrite the CSV
            empty_df = pd.DataFrame()

            # Upload the empty DataFrame back to the Blob Storage
            with io.StringIO() as output:
                empty_df.to_csv(output, index=False)
                output.seek(0)
                blob_client.upload_blob(output.getvalue(), overwrite=True)

            logging.info("CSV file cleaned successfully.")

# POST CHECKE
def handle_POST_CHECKER_REQ(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        queryData = body.get('data', {}).get("Table1", [])
        queryData = transform_data(queryData)
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON format"}),
            status_code=400,
            mimetype="application/json"
        )

    if not queryData:
        return func.HttpResponse(
            json.dumps({"error": "No data provided"}),
            status_code=400,
            mimetype="application/json"
        )
        
    # Load already checked IDs from blob
    checked_df, blob_client = load_csv_from_blob()
    checked_ids = set(checked_df["DECLARATIONID"].astype(str))

    # Init the detector
    detector = AddressDetector()

    today = datetime.utcnow().strftime("%Y-%m-%d")
    new_entries = []
    filtered_results = []
    
    # Process each object
    for obj in queryData:
        decl_id = str(obj.get("DECLARATIONID"))
        if decl_id in checked_ids:
            continue

        address = obj.get("ADDRESS", "")
        result = detector.parse_address(address)
        obj["MilitaryOrGovernment"] = result.strip()

        if result.strip() == "Yes":
            filtered_results.append(obj)

        # Add ID + date to save later
        new_entries.append({"DECLARATIONID": decl_id, "DATE": today})

    # Append matched addresses to FOUND_ADDRESSES.csv
    append_found_addresses_to_csv(filtered_results)

    # Update CSV with newly checked IDs
    if new_entries:
        new_df = pd.DataFrame(new_entries)
        updated_df = pd.concat([checked_df, new_df], ignore_index=True)
        csv_buffer = io.StringIO()
        updated_df.to_csv(csv_buffer, index=False)
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

    # Clean CSV if needed
    clean_csv_file_if_needed()

    return func.HttpResponse(
        json.dumps(filtered_results),
        mimetype="application/json",
        status_code=200
    )

# GET CHECKER
def handle_GET_MATCHED_ADDRESSES_REQ(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Connect to Blob and download FOUND_ADDRESSES.csv
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME2)

        stream = blob_client.download_blob().readall()
        df = pd.read_csv(io.StringIO(stream.decode("utf-8")))

        return func.HttpResponse(
            json.dumps(df.to_dict(orient="records")),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Failed to read matched addresses CSV: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "Unable to fetch matched addresses"}),
            mimetype="application/json",
            status_code=500
        )    
        
# PATCH CHECKER
def handle_PATCH_CHECKER_REQ(req: func.HttpRequest) -> func.HttpResponse:
    """
    Update 'checked' and 'checker' fields for a given DECLARATIONID in FOUND_ADDRESSES.csv.
    Expected body: { "DECLARATIONID": "some_id", "checker": "user_or_system" }
    """
    try:
        data = req.get_json()
        decl_id = str(data.get("DECLARATIONID"))
        checker_name = data.get("checker")

        if not decl_id or not checker_name:
            raise ValueError("Missing DECLARATIONID or checker")
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": "Invalid request body", "details": str(e)}),
            status_code=400,
            mimetype="application/json"
        )

    # Load existing data
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME2)

    try:
        stream = blob_client.download_blob().readall()
        df = pd.read_csv(io.StringIO(stream.decode("utf-8")))
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "Could not load FOUND_ADDRESSES.csv"}),
            status_code=500,
            mimetype="application/json"
        )

    # Update the row where DECLARATIONID matches
    match_found = False
    for idx, row in df.iterrows():
        if str(row.get("DECLARATIONID")) == decl_id:
            df.at[idx, "checked"] = True
            df.at[idx, "checker"] = checker_name
            match_found = True
            break

    if not match_found:
        return func.HttpResponse(
            json.dumps({"error": "DECLARATIONID not found"}),
            status_code=404,
            mimetype="application/json"
        )

    # Upload the updated DataFrame
    with io.StringIO() as output:
        df.to_csv(output, index=False)
        output.seek(0)
        blob_client.upload_blob(output.getvalue(), overwrite=True)

    return func.HttpResponse(
        json.dumps({"status": "Updated successfully"}),
        status_code=200,
        mimetype="application/json"
    )
       