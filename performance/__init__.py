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
DAILY_PARQUET_PREFIX = "statistics_check/daily/"
SUMMARY_BLOB_PATH = "statistics_check/cache/users_summary.json"
MONTHLY_SUMMARY_BLOB_PATH = "statistics_check/cache/monthly_report_cache.json"
USER_CACHE_PATH_PREFIX = "statistics_check/cache/users/"


# --- Helper Functions ---
def get_today_parquet_path():
    """Returns today's daily parquet path: statistics_check/daily/2025-10-28.parquet"""
    today = datetime.now().strftime("%Y-%m-%d")
    return f"{DAILY_PARQUET_PREFIX}{today}.parquet"

def load_daily_parquet(date_str):
    """Load a specific daily parquet file by date string (YYYY-MM-DD format)"""
    if not blob_service_client:
        raise ConnectionError("Blob service not initialized.")
    try:
        parquet_path = f"{DAILY_PARQUET_PREFIX}{date_str}.parquet"
        blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, parquet_path)
        if not blob_client.exists():
            return pd.DataFrame()
        return pd.read_parquet(io.BytesIO(blob_client.download_blob().readall()))
    except Exception as e:
        logging.error(f"Could not load daily parquet {date_str}: {e}")
        return pd.DataFrame()


def load_last_n_days_parquets(days=90):
    """Load and UNION all daily parquets from last N days"""
    if not blob_service_client:
        raise ConnectionError("Blob service not initialized.")
    
    dfs = []
    cutoff_date = datetime.now() - timedelta(days=days)
    current_date = datetime.now()
    
    while current_date >= cutoff_date:
        date_str = current_date.strftime("%Y-%m-%d")
        df = load_daily_parquet(date_str)
        if not df.empty:
            dfs.append(df)
            logging.info(f"Loaded daily parquet: {date_str}")
        current_date -= timedelta(days=1)
    
    if not dfs:
        logging.warning(f"No daily parquets found in last {days} days")
        return pd.DataFrame()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"UNION complete: {len(combined_df)} total rows from {len(dfs)} daily parquets")
    return combined_df


def save_daily_parquet(df):
    """Saves today's daily parquet with metadata already calculated"""
    if not blob_service_client:
        raise ConnectionError("Blob service not initialized.")
    
    today_path = get_today_parquet_path()
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, today_path)
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression='snappy')
    blob_client.upload_blob(buffer.getvalue(), overwrite=True)
    logging.info(f"Successfully saved daily parquet to {today_path}")


def save_json_to_blob(data, blob_path):
    """Saves a dictionary as a JSON file in blob storage."""
    if not blob_service_client:
        raise ConnectionError("Blob service not initialized.")
    blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, blob_path)
    blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)
    logging.info(f"Successfully saved JSON to {blob_path}")


def add_file_metadata(df):
    """
    Pre-calculates file metadata at ingestion time.
    Adds columns: is_manual, is_automatic, classified_by_user, first_action_date, is_interface
    This preserves your exact counting logic without replaying it on every refresh.
    """
    if df.empty:
        return df
    
    df_meta = df.copy()
    
    # Clean columns
    for col in ["USERCREATE", "USERCODE", "HISTORY_STATUS"]:
        if col in df_meta.columns:
            df_meta[col] = df_meta[col].astype(str).str.strip().str.upper()
    
    df_meta["HISTORYDATETIME"] = pd.to_datetime(df_meta["HISTORYDATETIME"], errors="coerce", format="mixed")
    
    # Define manual statuses (your exact logic)
    manual_statuses = {"COPIED", "COPY", "NEW"}
    
    # Initialize metadata columns
    df_meta['is_interface'] = df_meta['HISTORY_STATUS'] == 'INTERFACE'
    df_meta['is_manual_status'] = df_meta['HISTORY_STATUS'].isin(manual_statuses)
    df_meta['is_modified'] = df_meta['HISTORY_STATUS'] == 'MODIFIED'
    df_meta['is_wrt_ent'] = df_meta['HISTORY_STATUS'] == 'WRT_ENT'
    
    return df_meta


# --- Main Function App ---
def main(req: func.HttpRequest) -> func.HttpResponse:
    if not connection_string:
        return func.HttpResponse(json.dumps({"error": "Backend service not configured."}), status_code=503, mimetype="application/json")

    try:
        method = req.method
        action = req.route_params.get('action')
        user_param = req.params.get('user')
        all_users_param = req.params.get('all_users', 'false').lower() == 'true'
        
        # --- NEW: Endpoint to migrate existing all_data.parquet into daily parquets ---
        # This is a one-time initialization endpoint
        # POST /api/migrate-to-daily will read all_data.parquet, split by date, save as daily parquets
        if method == "POST" and action == "migrate-to-daily":
            logging.info("Starting migration from all_data.parquet to daily parquets...")
            
            try:
                # Load the old all_data.parquet
                logging.info("Loading statistics_check/all_data.parquet...")
                blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, "statistics_check/all_data.parquet")
                
                if not blob_client.exists():
                    return func.HttpResponse(json.dumps({"error": "all_data.parquet not found at statistics_check/all_data.parquet"}), status_code=404, mimetype="application/json")
                
                df_all = pd.read_parquet(io.BytesIO(blob_client.download_blob().readall()))
                logging.info(f"Loaded all_data.parquet with {len(df_all)} rows")
                
                if df_all.empty:
                    return func.HttpResponse(json.dumps({"error": "all_data.parquet is empty"}), status_code=400, mimetype="application/json")
                
                # Clean datetime column
                df_all["HISTORYDATETIME"] = pd.to_datetime(df_all["HISTORYDATETIME"], errors="coerce", format="mixed")
                df_all = df_all.dropna(subset=["HISTORYDATETIME"])
                
                # Filter for last 90 days from 27/09/2025
                reference_date = datetime(2025, 9, 27)
                cutoff_date = reference_date - timedelta(days=90)
                
                df_filtered = df_all[(df_all["HISTORYDATETIME"].dt.date >= cutoff_date.date()) & 
                                    (df_all["HISTORYDATETIME"].dt.date <= reference_date.date())]
                
                logging.info(f"Filtered to {len(df_filtered)} rows between {cutoff_date.date()} and {reference_date.date()}")
                
                if df_filtered.empty:
                    return func.HttpResponse(json.dumps({"error": "No data found in the 90-day range"}), status_code=400, mimetype="application/json")
                
                # Add metadata
                df_filtered = add_file_metadata(df_filtered)
                
                # Group by date and save each day as a separate parquet
                df_filtered['DATE'] = df_filtered["HISTORYDATETIME"].dt.date
                
                daily_count = 0
                for day_date, group_df in df_filtered.groupby('DATE'):
                    day_str = day_date.strftime("%Y-%m-%d")
                    daily_path = f"{DAILY_PARQUET_PREFIX}{day_str}.parquet"
                    
                    # Save this day's data
                    buffer = io.BytesIO()
                    group_df.drop('DATE', axis=1).to_parquet(buffer, index=False, compression='snappy')
                    
                    blob_client_day = blob_service_client.get_blob_client(CONTAINER_NAME, daily_path)
                    blob_client_day.upload_blob(buffer.getvalue(), overwrite=True)
                    
                    logging.info(f"Created daily parquet: {day_str} with {len(group_df)} rows")
                    daily_count += 1
                
                return func.HttpResponse(json.dumps({
                    "status": "success", 
                    "message": f"Migration complete. Created {daily_count} daily parquets from {len(df_filtered)} rows."
                }), status_code=200, mimetype="application/json")
            
            except Exception as e:
                logging.error(f"Migration failed: {e}")
                return func.HttpResponse(json.dumps({"error": f"Migration failed: {str(e)}"}), status_code=500, mimetype="application/json")

        # --- Endpoint to add new raw data (with metadata pre-calculation) ---
        if method == "POST" and not action:
            body = req.get_json()
            new_df = pd.DataFrame(body.get("data", {}).get("Table1", []))
            if new_df.empty:
                return func.HttpResponse(json.dumps({"error": "No data provided in request body."}), status_code=400, mimetype="application/json")
            
            try:
                # Add metadata before saving
                new_df = add_file_metadata(new_df)
                
                # Get today's parquet path
                today_path = get_today_parquet_path()
                blob_client = blob_service_client.get_blob_client(CONTAINER_NAME, today_path)
                
                if blob_client.exists():
                    # If today's parquet exists, append to it
                    existing_df = pd.read_parquet(io.BytesIO(blob_client.download_blob().readall()))
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                else:
                    combined_df = new_df
                
                save_daily_parquet(combined_df)
                logging.info(f"Successfully appended {len(new_df)} rows with metadata to today's parquet")
                
                return func.HttpResponse(json.dumps({"status": "success", "message": "Data stored successfully with metadata."}), status_code=200, mimetype="application/json")
            except Exception as e:
                logging.error(f"Error saving data: {e}")
                return func.HttpResponse(json.dumps({"error": f"Failed to save data: {str(e)}"}), status_code=500, mimetype="application/json")

        # --- Endpoint to refresh ALL individual user caches ---
        elif method == "POST" and action == "refresh-users":
            logging.info("Starting full cache refresh for all individual users (using daily parquets).")
            
            target_users = [
                'FADWA.ERRAZIKI', 'AYOUB.SOURISTE', 'AYMANE.BERRIOUA', 'SANA.IDRISSI', 'AMINA.SAISS',
                'KHADIJA.OUFKIR', 'ZOHRA.HMOUDOU', 'SIMO.ONSI', 'YOUSSEF.ASSABIR', 'ABOULHASSAN.AMINA',
                'MEHDI.OUAZIR', 'OUMAIMA.EL.OUTMANI', 'HAMZA.ALLALI', 'MUSTAPHA.BOUJALA', 'HIND.EZZAOUI',
                'IKRAM.OULHIANE', 'MOURAD.ELBAHAZ', 'MOHSINE.SABIL', 'AYA.HANNI',
                'ZAHIRA.OUHADDA', 'CHAIMAAE.EJJARI', 'HAFIDA.BOOHADDOU', 'KHADIJA.HICHAMI', 'FATIMA.ZAHRA.BOUGSIM'
            ]
            
            try:
                # OPTIMIZATION: Load all daily parquets from last 90 days (UNION)
                logging.info("Loading last 90 days of daily parquets...")
                df_union = load_last_n_days_parquets(days=90)
                
                if df_union.empty:
                    return func.HttpResponse(json.dumps({"status": "skipped", "message": "No data available."}), status_code=200, mimetype="application/json")

                if 'USERCODE' not in df_union.columns:
                    return func.HttpResponse(json.dumps({"error": "'USERCODE' column not found in data."}), status_code=400, mimetype="application/json")
                
                # Find target users in data
                all_users_in_df = df_union['USERCODE'].dropna().unique()
                target_users_upper = [tu.upper() for tu in target_users]
                users_to_process = [user for user in all_users_in_df if str(user).upper() in target_users_upper]

                logging.info(f"Found {len(users_to_process)} target users to process.")
                
                processed_count = 0
                
                # Process each user - calculate_single_user_metrics_fast uses YOUR EXACT LOGIC
                for user in users_to_process:
                    try:
                        user_metrics = calculate_single_user_metrics_fast(df_union, user)
                        user_blob_path = f"{USER_CACHE_PATH_PREFIX}{user}.json"
                        save_json_to_blob(user_metrics, user_blob_path)
                        logging.info(f"Successfully cached data for user: {user}")
                        processed_count += 1
                    except Exception as e:
                        logging.error(f"Failed to process user {user}: {e}")
                
                return func.HttpResponse(json.dumps({"status": "success", "message": f"Cache refreshed for {processed_count}/{len(users_to_process)} users."}), status_code=200, mimetype="application/json")
            
            except Exception as e:
                logging.error(f"Refresh-users failed: {e}")
                return func.HttpResponse(json.dumps({"error": f"Refresh failed: {str(e)}"}), status_code=500, mimetype="application/json")

        # --- Endpoint to refresh the monthly report cache ---
        elif method == "POST" and action == "refresh-monthly":
            logging.info("Monthly report cache refresh process started.")
            
            try:
                # Load last 30 days of daily parquets
                df_union = load_last_n_days_parquets(days=30)
                
                if df_union.empty:
                    return func.HttpResponse(json.dumps({"status": "skipped", "message": "No data available."}), status_code=200, mimetype="application/json")
                
                # Uses YOUR EXACT LOGIC from calculate_all_users_monthly_metrics
                metrics = calculate_all_users_monthly_metrics(df_union)
                save_json_to_blob(metrics, MONTHLY_SUMMARY_BLOB_PATH)
                
                return func.HttpResponse(json.dumps({"status": "success", "message": "Monthly report cache refreshed."}), status_code=200, mimetype="application/json")
            except Exception as e:
                logging.error(f"Monthly refresh failed: {e}")
                return func.HttpResponse(json.dumps({"error": f"Monthly refresh failed: {str(e)}"}), status_code=500, mimetype="application/json")

        # --- Endpoint for 10-day summary cache refresh ---
        elif method == "POST" and action == "refresh":
            logging.info("10-day summary cache refresh started.")
            
            try:
                # Load last 10 days of daily parquets
                df_union = load_last_n_days_parquets(days=10)
                
                if df_union.empty:
                    return func.HttpResponse(json.dumps({"status": "skipped", "message": "No data available."}), status_code=200, mimetype="application/json")

                # Uses YOUR EXACT LOGIC from count_user_file_creations_last_10_days
                metrics = count_user_file_creations_last_10_days(df_union)
                save_json_to_blob(metrics, SUMMARY_BLOB_PATH)
                
                return func.HttpResponse(json.dumps({"status": "success", "message": "10-day summary cache refreshed."}), status_code=200, mimetype="application/json")
            except Exception as e:
                logging.error(f"10-day refresh failed: {e}")
                return func.HttpResponse(json.dumps({"error": f"10-day refresh failed: {str(e)}"}), status_code=500, mimetype="application/json")

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