import logging
import requests
import azure.functions as func
import json
import os
import base64
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from .services.data_transformer import transform_client_group
from .services.pdf_generator import generate_pdf
from .models.response_model import APIResponse, PDFResponse

# --- Configuration ---
# Must match the path used in DkmFiscdepetProcessor
CONTAINER_NAME = "document-intelligence"
QUEUE_FOLDER = "Bestemmingsrapport/Queue"
OUTPUT_FOLDER = "Bestemmingsrapport/Generated"

# Placeholder URL - User needs to add this to their App Settings
LOGIC_APP_URL = os.getenv("DATA_FETCHER_LOGIC_APP_URL", "https://prod-85.westeurope.logic.azure.com:443/workflows/9c70e08c39244c5e9bd1370c65e856c6/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=DTA47iRv1P5PW9Tye6VI_EWjsbIyJDJSAABxxrFKBVQ")

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Daily BestDoc Trigger (HTTP).
    1. Reads the daily queue of IDs.
    2. Calls Logic App to fetch full data for these IDs.
    3. Generates BestDoc PDFs.
    4. Saves PDFs to storage AND returns them in the JSON response.
    """
    logging.info("🚀 Daily BestDoc Trigger (HTTP) started")

    try:
        # 1. Connect to Blob Storage
        connect_str = os.getenv("AzureWebJobsStorage")
        if not connect_str:
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Missing AzureWebJobsStorage"}),
                status_code=500, mimetype="application/json"
            )
            
        blob_service = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service.get_container_client(CONTAINER_NAME)
        
        # 2. Determine Daily Queue Filename
        date_param = req.params.get('date')
        if date_param:
            today_str = date_param
        else:
            today_str = datetime.now().strftime("%Y%m%d")
            
        queue_filename = f"{QUEUE_FOLDER}/Queue_{today_str}.json"
        blob_client = container_client.get_blob_client(queue_filename)
        
        if not blob_client.exists():
            return func.HttpResponse(
                json.dumps({
                    "success": True, 
                    "message": f"No queue file found for date {today_str}",
                    "processed_count": 0,
                    "pdfs": []
                }),
                status_code=200, mimetype="application/json"
            )

        # 3. Read Queue (List of IDs)
        queue_data = blob_client.download_blob().readall().decode("utf-8")
        id_list = json.loads(queue_data)
        
        if not id_list:
             return func.HttpResponse(
                json.dumps({
                    "success": True, 
                    "message": "Queue file is empty",
                    "processed_count": 0,
                    "pdfs": []
                }),
                status_code=200, mimetype="application/json"
            )
            
        logging.info(f"Found {len(id_list)} IDs to process for {today_str}")
        
        # 4. Fetch Full Data from Logic App
        if not LOGIC_APP_URL:
            # Fallback/Error if URL not configured
            logging.error("Missing DATA_FETCHER_LOGIC_APP_URL")
            # If the queue actually contains full records (legacy support during transition), we might try to use them?
            # But the user explicitly said we refactor. So we error out.
            return func.HttpResponse(
                json.dumps({"success": False, "error": "Missing DATA_FETCHER_LOGIC_APP_URL configuration"}),
                status_code=500, mimetype="application/json"
            )

        try:
            logging.info(f"Fetching data from Logic App for {len(id_list)} IDs...")
            # Logic App expects a JSON body, likely with a key or just the array.
            # User said: "we store only the INTERNFACTUURNUMMER as an array then we send this array"
            # So we send the array directly or inside a wrapper. 
            # Usual Logic App 'When a HTTP request is received' often matches body.
            # We'll send {"ids": [...]} to be safe, or just [...] if configured.
            # Let's send a wrapper object to allow flexibility: {"ids": [...]}
            # WAIT: The SQL query says "WHERE ... IN @{outputs('Compose')}". 
            # If the Logic App expects the array directly, we send the array. 
            # Let's send the list directly as the body: [1, 2, 3]
            
            response = requests.post(LOGIC_APP_URL, json=id_list)
            response.raise_for_status()
            records = response.json()
            
            if not records:
                 logging.warning("Logic App returned no records.")
                 records = [] # Process nothing
                 
        except Exception as fetch_err:
             logging.error(f"Failed to fetch data from Logic App: {fetch_err}")
             return func.HttpResponse(
                json.dumps({"success": False, "error": f"Data fetch failed: {str(fetch_err)}"}),
                status_code=502, mimetype="application/json"
            )

        logging.info(f"Successfully retrieved {len(records)} records from Oracle.")
        
        # 4. Group Records by KLANT
        grouped_records = {}
        for record in records:
            # Normalize Client Name to create a robust key
            raw_klant = record.get("KLANT", "UNKNOWN")
            # If you want to group by exact string, use raw_klant.
            # If you want to be safer against spacing issues:
            klant_key = raw_klant.strip().upper()
            
            if klant_key not in grouped_records:
                grouped_records[klant_key] = []
            grouped_records[klant_key].append(record)
            
        logging.info(f"Grouped into {len(grouped_records)} unique clients.")

        # 5. Process Groups
        processed_pdfs = []
        errors = []
        processed_ids = []


        
        for klant_key, group_data in grouped_records.items():
            try:
                # Transform (Now passing the whole list of records for this client)
                bestemmings_data = transform_client_group(klant_key, group_data)
                
                # Generate PDF (PDF Generator already handles the nested line items logic)
                pdf_bytes = generate_pdf(bestemmings_data)
                
                # Filename logic: BS-{LANG}-{KLANT}-MULTI.pdf or similar
                # Since multiple IDs can be in one PDF, we can't put a single ID in the filename easily.
                # We can append the number of records or the first ID.
                lang = bestemmings_data.client.language.upper()
                safe_klant = bestemmings_data.client.naam.replace(" ", "").replace("-", "").replace("'", "").upper()[:20]
                
                # Example: BS-EN-CLIENTNAME-3RECS-20251217.pdf
                filename = f"BS-{lang}-{safe_klant}-{len(group_data)}RECS-{today_str}.pdf"
                output_filename = f"{OUTPUT_FOLDER}/{today_str}/{filename}"
                
                # Encode for Response
                pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
                
                # Collect all IDs in this group
                group_ids = [int(r.get("INTERNFACTUURNUMMER", 0)) for r in group_data]
                processed_ids.extend(group_ids)

                pdf_response = PDFResponse(
                    internfactuurnummer=group_ids[0] if group_ids else 0, # Representative ID
                    filename=filename,
                    pdf_base64=pdf_base64,
                    size_bytes=len(pdf_bytes),
                    metadata={
                        "klant": bestemmings_data.client.naam,
                        "date": today_str,
                        "included_ids": group_ids,
                        "declaration_guids": [record.declarationguid for record in bestemmings_data.records],
                        # Email Template Fields
                        "amount": f"{bestemmings_data.total_value:.2f}",
                        "currency": "EUR",
                        "c88": bestemmings_data.primary_record.mrn,
                        "datum": bestemmings_data.primary_record.datum, # Record date
                        "commercialreference": bestemmings_data.primary_record.reference,
                        "declaration_guid": bestemmings_data.primary_record.declarationguid # Primary/First GUID for tag
                    }
                )
                processed_pdfs.append(pdf_response)
                
                logging.info(f"✅ Generated Group PDF for {klant_key}: {output_filename}")
                
            except Exception as e:
                logging.error(f"❌ Failed to process group {klant_key}: {e}")
                errors.append({
                    "klant": klant_key,
                    "error": str(e)
                })
        
        # 5. Build Final Response
        response = APIResponse(
            success=True,
            timestamp=datetime.utcnow().isoformat() + "Z",
            processed_count=len(processed_pdfs),
            processed_ids=processed_ids,
            last_processed_id=0, # Not strictly tracked in this simple batch
            pdfs=[pdf.__dict__ for pdf in processed_pdfs],
            errors=errors
        )
        
        return func.HttpResponse(
            json.dumps(response.__dict__),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Critical error in Daily BestDoc Trigger: {str(e)}")
        return func.HttpResponse(
            json.dumps({"success": False, "error": str(e)}),
            status_code=500, mimetype="application/json"
        )
