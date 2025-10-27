from collections import defaultdict
from datetime import datetime, timedelta
import azure.functions as func
import logging
import json
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from performance.functions.functions import calculate_single_user_metrics_fast, count_user_file_creations_last_10_days, calculate_all_users_monthly_metrics

# --- Configuration ---
KEY_VAULT_URL = "https://kv-functions-python.vault.azure.net"
SECRET_NAME = "azure-storage-account-access-key2"

# --- Azure Services Initialization ---
try:
    credential = DefaultAzureCredential()
    kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
    connection_string = kv_client.get_secret(SECRET_NAME).value
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
except Exception as e:
    logging.critical(f"Failed to initialize Azure services: {e}")
    connection_string = None 
    blob_service_client = None
    
    

# --- Blob Storage Constants ---
CONTAINER_NAME = "document-intelligence"
PARQUET_BLOB_PATH = "statistics_check/all_data.parquet"
SUMMARY_BLOB_PATH = "statistics_check/cache/users_summary.json"
MONTHLY_SUMMARY_BLOB_PATH = "statistics_check/cache/monthly_report_cache.json"
USER_CACHE_PATH_PREFIX = "statistics_check/cache/users/"


# --- Helper Functions ---
def load_parquet_from_blob_lazy(columns=None, date_cutoff=None):
    """
    Lazy loads parquet with column filtering and date cutoff for memory efficiency.
    columns: list of column names to load (None = all)
    date_cutoff: datetime - only load rows where HISTORYDATETIME >= cutoff
    """
    if not blob_service_client: 
        raise ConnectionError("Blob service not initialized.")
    try:
        blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, PARQUET_BLOB_PATH)
        if not blob_client.exists():
            logging.warning("Parquet file not found at specified path.")
            return pd.DataFrame()
        
        # Load with column filtering if specified
        df = pd.read_parquet(io.BytesIO(blob_client.download_blob().readall()), columns=columns)
        
        # Apply date cutoff immediately to trim data
        if date_cutoff and 'HISTORYDATETIME' in df.columns:
            df['HISTORYDATETIME'] = pd.to_datetime(df['HISTORYDATETIME'], errors='coerce', format='mixed')
            df = df[df['HISTORYDATETIME'] >= date_cutoff]
            logging.info(f"Filtered parquet to {len(df)} rows after date cutoff")
        
        return df
    except Exception as e:
        logging.error(f"Could not load Parquet file. Error: {e}")
        return pd.DataFrame()


def load_parquet_from_blob():
    """
    Standard load for backward compatibility - uses lazy loading internally.
    """
    return load_parquet_from_blob_lazy()


def save_parquet_to_blob(df):
    """Saves a pandas DataFrame to a Parquet file in blob storage."""
    if not blob_service_client: 
        raise ConnectionError("Blob service not initialized.")
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, PARQUET_BLOB_PATH)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    blob_client.upload_blob(buffer.getvalue(), overwrite=True)
    logging.info(f"Successfully saved Parquet to {PARQUET_BLOB_PATH}")


def save_json_to_blob(data, blob_path):
    """Saves a dictionary as a JSON file in blob storage."""
    if not blob_service_client: 
        raise ConnectionError("Blob service not initialized.")
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, blob_path)
    blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)
    logging.info(f"Successfully saved JSON to {blob_path}")


# --- Main Function App ---
def main(req: func.HttpRequest) -> func.HttpResponse:
    if not connection_string:
        return func.HttpResponse(json.dumps({"error": "Backend service not configured."}), status_code=503, mimetype="application/json")

    try:
        method = req.method
        action = req.route_params.get('action')
        user_param = req.params.get('user')
        all_users_param = req.params.get('all_users', 'false').lower() == 'true'

        # --- Endpoint to add new raw data ---
        if method == "POST" and not action:
            body = req.get_json()
            new_df = pd.DataFrame(body.get("data", {}).get("Table1", []))
            if new_df.empty:
                return func.HttpResponse(json.dumps({"error": "No data provided in request body."}), status_code=400, mimetype="application/json")
            
            existing_df = load_parquet_from_blob()
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            save_parquet_to_blob(combined_df)
            return func.HttpResponse(json.dumps({"status": "success", "message": "Data stored successfully."}), status_code=200, mimetype="application/json")

        # --- NEW: Endpoint to refresh ALL individual user caches (CHUNKED) ---
        # Processes users in batches of 6 to avoid memory spikes
        elif method == "POST" and action == "refresh-users":
            logging.info("Starting full cache refresh for all individual users (chunked processing).")
            
            target_users = [
                'FADWA.ERRAZIKI', 'AYOUB.SOURISTE', 'AYMANE.BERRIOUA', 'SANA.IDRISSI', 'AMINA.SAISS',
                'KHADIJA.OUFKIR', 'ZOHRA.HMOUDOU', 'SIMO.ONSI', 'YOUSSEF.ASSABIR', 'ABOULHASSAN.AMINA',
                'MEHDI.OUAZIR', 'OUMAIMA.EL.OUTMANI', 'HAMZA.ALLALI', 'MUSTAPHA.BOUJALA', 'HIND.EZZAOUI',
                'MOHAMED.BOUIDAR', 'HOUDA.EZZAOUI', 'YAHYA.ANEJARN', 
                'IKRAM.OULHIANE', 'MOURAD.ELBAHAZ', 'MOHSINE.SABIL', 'AYA.HANNI',
                'ZAHIRA.OUHADDA', 'CHAIMAAE.EJJARI', 'HAFIDA.BOOHADDOU', 'KHADIJA.HICHAMI', 'FATIMA.ZAHRA.BOUGSIM'
            ]
            
            # OPTIMIZATION: Load once with lazy loading + early date filter
            cutoff = datetime.now() - timedelta(days=90)
            df = load_parquet_from_blob_lazy(date_cutoff=cutoff)
            
            if df.empty:
                return func.HttpResponse(json.dumps({"status": "skipped", "message": "Source data is empty."}), status_code=200, mimetype="application/json")

            if 'USERCODE' not in df.columns:
                return func.HttpResponse(json.dumps({"error": "'USERCODE' column not found in data."}), status_code=400, mimetype="application/json")
            
            all_users_in_df = df['USERCODE'].dropna().unique()
            target_users_upper = [tu.upper() for tu in target_users]
            users_to_process = [user for user in all_users_in_df if user.upper() in target_users_upper]

            logging.info(f"Found {len(users_to_process)} target users to process out of {len(all_users_in_df)} unique users.")
            
            processed_count = 0
            batch_size = 6
            
            # Process in batches to avoid memory spike
            for i in range(0, len(users_to_process), batch_size):
                batch = users_to_process[i:i + batch_size]
                logging.info(f"Processing batch {i//batch_size + 1}: users {batch}")
                
                for user in batch:
                    try:
                        user_metrics = calculate_single_user_metrics_fast(df, user)
                        user_blob_path = f"{USER_CACHE_PATH_PREFIX}{user}.json"
                        save_json_to_blob(user_metrics, user_blob_path)
                        logging.info(f"Successfully cached data for user: {user}")
                        processed_count += 1
                    except Exception as e:
                        logging.error(f"Failed to process and cache data for user {user}: {e}")
                
                # Clear batch from memory explicitly
                del batch
            
            return func.HttpResponse(json.dumps({"status": "success", "message": f"Cache refreshed for {processed_count}/{len(users_to_process)} target users."}), status_code=200, mimetype="application/json")

        # --- Endpoint to refresh the monthly report cache ---
        elif method == "POST" and action == "refresh-monthly":
            logging.info("Monthly report cache refresh process started.")
            
            # OPTIMIZATION: Load with early date filter for last 30 days
            cutoff = datetime.now() - timedelta(days=30)
            df = load_parquet_from_blob_lazy(date_cutoff=cutoff)
            
            if df.empty:
                return func.HttpResponse(json.dumps({"status": "skipped", "message": "No data available."}), status_code=200, mimetype="application/json")
            
            metrics = calculate_all_users_monthly_metrics(df)
            save_json_to_blob(metrics, MONTHLY_SUMMARY_BLOB_PATH)
            
            return func.HttpResponse(json.dumps({"status": "success", "message": "Monthly report cache refreshed."}), status_code=200, mimetype="application/json")

        # --- Endpoint for 10-day summary cache refresh ---
        elif method == "POST" and action == "refresh":
            logging.info("10-day summary cache refresh started.")
            
            # OPTIMIZATION: Load with early date filter for last 10 days
            cutoff = datetime.now() - timedelta(days=10)
            df = load_parquet_from_blob_lazy(date_cutoff=cutoff)
            
            if df.empty:
                return func.HttpResponse(json.dumps({"status": "skipped", "message": "No data available."}), status_code=200, mimetype="application/json")

            metrics = count_user_file_creations_last_10_days(df)
            save_json_to_blob(metrics, SUMMARY_BLOB_PATH)
            return func.HttpResponse(json.dumps({"status": "success", "message": "10-day summary cache refreshed."}), status_code=200, mimetype="application/json")

        # --- GET for single user (reads from cache) ---
        elif method == "GET" and user_param:
            user_blob_path = f"{USER_CACHE_PATH_PREFIX}{user_param}.json"
            logging.info(f"Request for cached user data from '{user_blob_path}'.")
            try:
                blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, user_blob_path)
                if not blob_client.exists():
                    return func.HttpResponse(json.dumps({"error": f"Cache for user '{user_param}' not found. Please trigger a user cache refresh."}), status_code=404, mimetype="application/json")
                
                blob_content = blob_client.download_blob().readall()
                return func.HttpResponse(body=blob_content, status_code=200, mimetype="application/json")
            except Exception as e:
                logging.error(f"Could not read cache file for {user_param}: {e}")
                return func.HttpResponse(json.dumps({"error": f"Could not read cache file: {e}"}), status_code=500, mimetype="application/json")

        # --- GET all_users reads from its own cache ---
        elif method == "GET" and all_users_param:
            logging.info(f"Request for cached monthly report from '{MONTHLY_SUMMARY_BLOB_PATH}'.")
            try:
                blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, MONTHLY_SUMMARY_BLOB_PATH)
                if not blob_client.exists():
                    return func.HttpResponse(json.dumps({"error": "Monthly report cache not found. Please trigger a refresh."}), status_code=404, mimetype="application/json")
                
                return func.HttpResponse(blob_client.download_blob().readall(), mimetype="application/json", status_code=200)
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": f"Could not read monthly cache file: {e}"}), status_code=500, mimetype="application/json")

        # --- GET for 10-day summary cache ---
        elif method == "GET" and not action:
            try:
                blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, SUMMARY_BLOB_PATH)
                if not blob_client.exists():
                    return func.HttpResponse(json.dumps({"error": "Cache file not found. Please trigger a refresh."}), status_code=404, mimetype="application/json")
                
                return func.HttpResponse(blob_client.download_blob().readall(), mimetype="application/json", status_code=200)
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": f"Could not read cache file: {e}"}), status_code=500, mimetype="application/json")
        
        else:
            return func.HttpResponse(json.dumps({"error": "Endpoint not found or method not allowed."}), status_code=404, mimetype="application/json")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return func.HttpResponse(json.dumps({"error": "An internal server error occurred."}), status_code=500, mimetype="application/json")