import logging
import azure.functions as func
import json
from datetime import datetime
from typing import List

from .services.data_transformer import transform_row
from .services.pdf_generator import generate_pdf
from .services.state_manager import update_state, get_max_id
from .services.bestdoc_state_manager import add_to_daily_queue
from .models.response_model import APIResponse, PDFResponse

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main HTTP handler for debenote PDF generation
    Receives SQL data from Logic App, generates PDFs, returns base64 encoded PDFs
    """
    logging.info("DebenoteGenerator function triggered")
    
    if req.method != "POST":
        return func.HttpResponse(
            json.dumps({"success": False, "error": "Method not allowed"}),
            status_code=405,
            mimetype="application/json"
        )
    
    try:
        # 1. Parse request body
        body = req.get_json()
        rows = body.get("Table1", [])
        
        if not rows:
            logging.info("No data received from Logic App")
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": "No new data to process",
                    "processed_count": 0,
                    "pdfs": []
                }),
                status_code=200,
                mimetype="application/json"
            )
        
        logging.info(f"Received {len(rows)} records from Logic App")
        
        # 2. Process each row
        pdfs = []
        errors = []
        
        for row in rows:
            try:
                
                # Transform SQL row to DebenoteData object
                debenote_data = transform_row(row)
                               
                # Generate PDF
                pdf_bytes = generate_pdf(debenote_data)
                
                # Convert to base64
                import base64
                pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
                
                # Build filename
                filename = f"{debenote_data.internfactuurnummer}E-{debenote_data.btwnummer}-{row['DATUM']}.pdf"
                
                # Create PDF response object
                pdf_response = PDFResponse(
                    internfactuurnummer=debenote_data.internfactuurnummer,
                    filename=filename,
                    pdf_base64=pdf_base64,
                    size_bytes=len(pdf_bytes),
                    metadata={
                        "btw_nummer": debenote_data.btwnummer,
                        "klant": debenote_data.client.naam,
                        "leverancier": debenote_data.leverancier_naam,
                        "datum": debenote_data.datum,
                        "bedrag": debenote_data.factuurtotaal,
                        "munt": debenote_data.munt,
                        "c88": debenote_data.c88nummer,
                        "commercialreference": debenote_data.commercialreference,
                        "declarationGuid": debenote_data.DECLARATIONGUID,
                        "emails_to": debenote_data.emails_to,
                        "emails_cc": debenote_data.emails_cc
                    }
                )
                
                pdfs.append(pdf_response)
                add_to_daily_queue(row)
                logging.info(f"✅ Generated PDF for INTERNFACTUURNUMMER: {debenote_data.internfactuurnummer}")
                
            except Exception as e:
                error_msg = f"Failed to process INTERNFACTUURNUMMER {row.get('INTERNFACTUURNUMMER', 'unknown')}: {str(e)}"
                logging.error(error_msg)
                errors.append({
                    "internfactuurnummer": row.get('INTERNFACTUURNUMMER'),
                    "error": str(e)
                })
        
        # 3. Update state with max processed ID
        if pdfs:
            processed_ids = [p.internfactuurnummer for p in pdfs if getattr(p, "internfactuurnummer", None)]
            new_max_id = max(processed_ids)
            update_state(processed_ids, new_max_id)

            logging.info(f"Updated state: lastProcessedId = {new_max_id}")
        
        # 4. Build response
        response = APIResponse(
            success=True,
            timestamp=datetime.utcnow().isoformat() + "Z",
            processed_count=len(pdfs),
            processed_ids=processed_ids,
            last_processed_id=get_max_id(rows) if pdfs else 0,
            pdfs=[pdf.__dict__ for pdf in pdfs],
            errors=errors
        )
        
        logging.info(f"✅ Successfully processed {len(pdfs)} debenotes, {len(errors)} errors")
        
        return func.HttpResponse(
            json.dumps(response.__dict__),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"❌ Critical error in DebenoteGenerator: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "error": str(e),
                "processed_count": 0,
                "pdfs": []
            }),
            status_code=500,
            mimetype="application/json"
        )