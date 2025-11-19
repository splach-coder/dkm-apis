import logging
import azure.functions as func
import json
import base64
from datetime import datetime
from typing import Dict, List

from .services.state_manager import (
    add_records_to_state, get_unprocessed_pending_groups, update_after_processing, 
    filter_already_processed
)
from .services.data_transformer import transform_client_group, validate_group_consistency
from .services.pdf_generator import generate_pdf


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Main Azure Function handler for grouped bestemmings processing"""
    logging.info("🚀 BestemmingsGenerator - FLOW: Filter → Store → Group → Generate → Update")
    
    if req.method != "POST":
        return func.HttpResponse(
            json.dumps({"success": False, "error": "Method not allowed"}),
            status_code=405,
            mimetype="application/json"
        )
    
    try:
        body = req.get_json()
        incoming_data = body.get("Table1", [])
        
        if not incoming_data:
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": "No data to process",
                    "processed_groups": 0,
                    "total_records": 0,
                    "pdfs": [],
                    "errors": []
                }),
                status_code=200,
                mimetype="application/json"
            )
        
        logging.info(f"📥 STEP 1: Received {len(incoming_data)} records from Logic App")
        
        # STEP 1: Filter out already processed records (duplicate prevention)
        unprocessed_data = filter_already_processed(incoming_data)
        if len(unprocessed_data) < len(incoming_data):
            skipped = len(incoming_data) - len(unprocessed_data)
            logging.info(f"⏭️ Skipped {skipped} already processed records")
        
        if not unprocessed_data:
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": f"All {len(incoming_data)} records already processed - no duplicates",
                    "processed_groups": 0,
                    "total_records": len(incoming_data),
                    "pdfs": [],
                    "errors": [],
                    "duplicate_prevention": {
                        "total_received": len(incoming_data),
                        "skipped_processed": len(incoming_data),
                        "actually_processed": 0
                    }
                }),
                status_code=200,
                mimetype="application/json"
            )
        
        # STEP 2: Add new unprocessed records to state with FULL TABLE DATA
        logging.info(f"📝 STEP 2: Storing {len(unprocessed_data)} records with complete table data")
        add_records_to_state(unprocessed_data)
        
        # STEP 3: Get unprocessed pending groups for PDF generation
        logging.info("📋 STEP 3: Getting unprocessed groups for PDF generation")
        pending_groups = get_unprocessed_pending_groups()
        logging.info(f"Found {len(pending_groups)} groups with unprocessed records")
        
        if not pending_groups:
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": "No unprocessed groups found",
                    "processed_groups": 0,
                    "total_records": 0,
                    "pdfs": [],
                    "errors": []
                }),
                status_code=200,
                mimetype="application/json"
            )
        
        # STEP 4: Process each group and generate PDFs
        logging.info(f"🔄 STEP 4: Generating PDFs for {len(pending_groups)} groups")
        results = []
        generated_files = []
        processed_groups_dict = {}
        
        for client_month_key, group_objects in pending_groups.items():
            try:
                logging.info(f"🔄 Processing {client_month_key}: {len(group_objects)} records")
                
                # Validate group consistency
                if not validate_group_consistency(group_objects):
                    logging.error(f"❌ Group {client_month_key}: Inconsistent client data")
                    results.append({
                        "client_month_key": client_month_key,
                        "success": False,
                        "error": "Inconsistent client data in group",
                        "record_count": len(group_objects)
                    })
                    continue
                
                # Transform to BestemmingsData
                bestemmings_data = transform_client_group(client_month_key, group_objects)
                
                # Generate PDF
                pdf_bytes = generate_pdf(bestemmings_data)
                pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
                
                # Generate filename
                language = bestemmings_data.client.language.upper()
                klant_clean = group_objects[0]["KLANT"].replace(" ", "").replace("-", "").replace("'", "").upper()
                
                if len(group_objects) > 1:
                    # MERGED PDF: BS-{LANG}{LANG}-{KLANT}.pdf
                    filename = f"BS-{language}{language}-{klant_clean}.pdf"
                    logging.info(f"✅ Generated MERGED PDF for {len(group_objects)} records: {filename}")
                else:
                    # INDIVIDUAL PDF: BS-{LANG}{LANG}-{KLANT}-{ID}.pdf
                    intern_id = group_objects[0]["INTERNFACTUURNUMMER"]
                    filename = f"BS-{language}{language}-{klant_clean}-{intern_id}.pdf"
                    logging.info(f"✅ Generated INDIVIDUAL PDF: {filename}")
                
                # Create file info
                file_info = {
                    "client_month_key": client_month_key,
                    "filename": filename,
                    "pdf_base64": pdf_base64,
                    "size_bytes": len(pdf_bytes),
                    "metadata": {
                        "client": bestemmings_data.client.naam,
                        "language": bestemmings_data.client.language,
                        "record_count": len(group_objects),
                        "line_item_count": len(bestemmings_data.line_items),
                        "total_value": bestemmings_data.total_value,
                        "internfactuurnummer": [r["INTERNFACTUURNUMMER"] for r in group_objects]
                    }
                }
                
                generated_files.append(file_info)
                processed_groups_dict[client_month_key] = [r["INTERNFACTUURNUMMER"] for r in group_objects]
                
                results.append({
                    "client_month_key": client_month_key,
                    "success": True,
                    "filename": filename,
                    "record_count": len(group_objects)
                })
                
            except Exception as e:
                logging.error(f"❌ Failed {client_month_key}: {str(e)}")
                results.append({
                    "client_month_key": client_month_key,
                    "success": False,
                    "error": str(e),
                    "record_count": len(group_objects) if group_objects else 0
                })
        
        # STEP 5: Update state after processing
        if processed_groups_dict:
            logging.info(f"✅ STEP 5: Updating state - marking {sum(len(ids) for ids in processed_groups_dict.values())} records as processed")
            update_after_processing(processed_groups_dict, generated_files)
        
        # STEP 6: Prepare response
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]
        
        pdfs = []
        for file_info in generated_files:
            pdfs.append({
                "client_month_key": file_info["client_month_key"],
                "filename": file_info["filename"],
                "pdf_base64": file_info["pdf_base64"],
                "size_bytes": file_info["size_bytes"],
                "metadata": file_info["metadata"]
            })
        
        errors = []
        for result in failed:
            errors.append({
                "client_month_key": result["client_month_key"],
                "error": result["error"],
                "record_count": result["record_count"]
            })
        
        total_records = sum(r["record_count"] for r in results)
        
        response = {
            "success": len(failed) == 0,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "processed_groups": len(successful),
            "total_records": total_records,
            "pdfs": pdfs,
            "errors": errors,
            "duplicate_prevention": {
                "total_received": len(incoming_data),
                "skipped_processed": len(incoming_data) - len(unprocessed_data),
                "actually_processed": len(unprocessed_data)
            }
        }
        
        logging.info(f"🏁 Processing complete: {len(successful)}/{len(results)} groups successful")
        
        return func.HttpResponse(
            json.dumps(response),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"💥 Critical error: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "error": str(e),
                "processed_groups": 0,
                "total_records": 0,
                "pdfs": [],
                "errors": []
            }),
            status_code=500,
            mimetype="application/json"
        )