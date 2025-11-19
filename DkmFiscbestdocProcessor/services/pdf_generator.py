import logging
import os
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from io import BytesIO


def generate_pdf(data) -> bytes:
    """Generate PDF matching exact design - supports merged records"""
    try:
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=landscape(A4))
        width, height = landscape(A4)
        
        # Register fonts for exact matching
        try:
            pdfmetrics.registerFont(TTFont('Arial', 'Arial.ttf'))
            pdfmetrics.registerFont(TTFont('Arial-Bold', 'Arial Bold.ttf'))
            pdfmetrics.registerFont(TTFont('Arial-Italic', 'Arial Italic.ttf'))
            pdfmetrics.registerFont(TTFont('Arial-BoldItalic', 'Arial Bold Italic.ttf'))
            default_font = 'Arial'
        except:
            default_font = 'Helvetica'
        
        y_position = height - 12*mm
        
        y_position = draw_header(c, data, y_position, width, default_font)
        y_position = draw_title(c, y_position, width, default_font)
        y_position = draw_two_column_section(c, data, y_position, width, default_font)
        y_position = draw_table(c, data, y_position, width, default_font)
        
        c.save()
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        logging.info(f"✅ PDF generated: {len(pdf_bytes)} bytes, {len(data.records)} records, {len(data.line_items)} line items")
        return pdf_bytes
        
    except Exception as e:
        logging.error(f"PDF generation failed: {str(e)}")
        raise


def draw_header(c: canvas.Canvas, data, y: float, width: float, font_family: str) -> float:
    """Draw header with exact positioning"""
    
    # LEFT: DKM Logo - exact positioning
    try:
        current_dir = os.path.dirname(os.path.dirname(__file__))
        logo_path = os.path.join(current_dir, "images", "dkm-logo.png")
        
        if os.path.exists(logo_path):
            c.drawImage(logo_path, 15*mm, y - 10*mm, 
                       width=35*mm, height=12*mm, 
                       preserveAspectRatio=True, mask='auto')
        else:
            c.setFont(f"{font_family}-Bold", 28)
            c.setFillColor(colors.HexColor('#E85D3F'))
            c.drawString(15*mm, y - 8*mm, "DKM")
            c.setFillColor(colors.black)
    except:
        c.setFont(f"{font_family}-Bold", 28)
        c.setFillColor(colors.HexColor('#E85D3F'))
        c.drawString(15*mm, y - 8*mm, "DKM")
        c.setFillColor(colors.black)
    
    # LEFT: Company info - exact spacing
    c.setFont(f"{font_family}-Bold", 10)
    c.setFillColor(colors.black)
    c.drawString(15*mm, y - 18*mm, data.client.naam.upper())
    
    c.setFont(font_family, 8)
    c.drawString(15*mm, y - 23*mm, data.client.straat_en_nummer.upper())
    c.drawString(15*mm, y - 28*mm, f"{data.client.postcode}    {data.client.stad.upper()}")
    c.drawString(15*mm, y - 33*mm, f"{data.client.landcode}  {data.client.plda_operatoridentity}")
    
    # RIGHT: DKM contact - exact positioning
    dkm_x = width - 65*mm
    c.setFont(f"{font_family}-Bold", 8)
    c.drawString(dkm_x, y - 8*mm, "DKM-customs")
    c.setFont(font_family, 7)
    c.drawString(dkm_x, y - 14*mm, "Noorderlaan 72- 2030 Antwerpen")
    c.drawString(dkm_x, y - 20*mm, "BE0796538660")
    
    return y - 38*mm


def draw_title(c: canvas.Canvas, y: float, width: float, font_family: str) -> float:
    """Draw title with exact positioning"""
    c.setFont(f"{font_family}-Bold", 9)
    c.drawCentredString(width/2, y, "Declaration for VAT purposes according to :")
    
    y -= 3.5*mm
    c.setFont(f"{font_family}-Oblique", 7)
    c.drawCentredString(width/2, y, "article 138, paragraph 1, directive 2006/112/EC")
    
    return y - 7*mm


def draw_two_column_section(c: canvas.Canvas, data, y: float, width: float, font_family: str) -> float:
    """
    LEFT: Gray Notice Box
    RIGHT: Declaration + Signature
    Exact positioning and sizing
    """
    
    # LEFT COLUMN: Notice Box - exact dimensions
    left_x = 15*mm
    box_width = 95*mm
    box_height = 22*mm
    box_y = y - box_height + 3*mm
    
    c.setFillColor(colors.HexColor('#D3D3D3'))
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.8)
    c.rect(left_x, box_y, box_width, box_height, fill=True, stroke=True)
    
    c.setFillColor(colors.black)
    notice_y = box_y + box_height - 3*mm
    
    c.setFont(f"{font_family}-BoldOblique", 7)
    c.drawString(left_x + 1.5*mm, notice_y, "NOT TO BE PAID - DOCUMENT JUST FOR VAT MATTERS")
    
    notice_y -= 3.2*mm
    c.setFont(f"{font_family}-Oblique", 6.5)
    c.drawString(left_x + 1.5*mm, notice_y, "PLEASE COMPLETE AND RETURN THIS DECLARATION BY MAIL -->")
    
    notice_y -= 3*mm
    c.drawString(left_x + 1.5*mm, notice_y, "> fiscalrepresenation@dkm-customs.com")
    
    notice_y -= 3*mm
    c.drawString(left_x + 1.5*mm, notice_y, "THANK YOU")
    
    # RIGHT COLUMN: Declaration + Signature - exact positioning
    right_x = left_x + box_width + 8*mm
    right_width = width - right_x - 15*mm
    
    # Declaration text - exact formatting
    text = "Declares that the goods imported into Belgium were properly transported to the country mentioned on the left. Undersigned therefore declares that the acquisition of below mentioned goods will be reported in their VAT return according to the law of the member state which the VAT identification"
    
    c.setFont(font_family, 6.5)
    lines = wrap_text(c, text, right_width, font_family, 6.5)
    
    text_y = y - 2*mm
    for line in lines[:3]:
        c.drawString(right_x, text_y, line)
        text_y -= 2.8*mm
    
    text_y -= 2.5*mm
    
    # Signature fields - exact positioning
    c.setFont(font_family, 7)
    c.drawString(right_x, text_y, "Name of the undersigned :")
    
    text_y -= 4*mm
    c.drawString(right_x, text_y, "function of the undersigned :")
    
    text_y -= 4*mm
    c.drawString(right_x, text_y, "Signature :")
    
    return y - 32*mm


def draw_table(c: canvas.Canvas, data, y: float, width: float, font_family: str) -> float:
    """
    Draw table with EXACT dimensions and styling - FIXED FOR MERGED RECORDS
    """
    
    logging.info(f"Drawing table with {len(data.line_items)} line items from {len(data.records)} records")
    
    if not data.line_items:
        logging.warning("No line items found - table will be empty")
        return y
    
    # Headers - exact column names
    headers = ['MRN', 'ID', 'Supplier', 'Date', 'Reference', 'Detentors', 'Item', 'Commodity', 'Var Value €']
    
    # Create table data with FIXED merged logic
    table_data = [headers]
    
    # Group line items by source record (internfactuurnummer)
    line_items_by_record = {}
    for item in data.line_items:
        record_id = item.source_internfactuurnummer
        if record_id not in line_items_by_record:
            line_items_by_record[record_id] = []
        line_items_by_record[record_id].append(item)
    
    logging.info(f"Line items grouped by record: {list(line_items_by_record.keys())}")
    
    # Build table rows - PROCESS EACH RECORD'S ITEMS
    record_start_rows = {}
    current_row = 1  # Start after header
    
    for record in data.records:
        record_id = record.internfactuurnummer
        record_items = line_items_by_record.get(record_id, [])
        
        logging.info(f"Processing record {record_id} with {len(record_items)} items")
        
        if not record_items:
            continue
        
        record_start_rows[record_id] = current_row
        
        for idx, item in enumerate(record_items):
            if idx == 0:
                # First item for this record - include all record columns
                row = [
                    str(record.mrn),
                    str(record.declarationid),
                    str(record.exportername),
                    str(record.datum),
                    str(record.reference)[:50] + "..." if len(str(record.reference)) > 50 else str(record.reference),
                    str(record.processfactuurnummer),
                    str(item.zendtarieflijnnummer),
                    str(item.goederencode),
                    f"{item.verkoopwaarde:.2f}"
                ]
            else:
                # Subsequent items for same record - empty first 6 columns
                row = [
                    '', '', '', '', '', '',
                    str(item.zendtarieflijnnummer),
                    str(item.goederencode),
                    f"{item.verkoopwaarde:.2f}"
                ]
            
            table_data.append(row)
            current_row += 1
    
    logging.info(f"Table data created with {len(table_data)} rows (including header)")
    
    if len(table_data) <= 1:  # Only header
        logging.warning("No data rows created for table")
        return y
    
    # EXACT column widths from original PDF
    col_widths = [28*mm, 15*mm, 32*mm, 16*mm, 58*mm, 20*mm, 10*mm, 23*mm, 18*mm]
    
    table = Table(table_data, colWidths=col_widths)
    
    # Build EXACT style matching original
    style = [
        # Header - exact styling
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#C0C0C0')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), f'{font_family}-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 7),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
        
        # Data rows - exact styling
        ('FONTNAME', (0, 1), (-1, -1), font_family),
        ('FONTSIZE', (0, 1), (-1, -1), 6.5),
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 1), (-1, -1), 'TOP'),
        
        # Grid - exact styling
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('BOX', (0, 0), (-1, -1), 1, colors.black),
        
        # Padding - exact values
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
    ]
    
    # Apply spanning for each record's items
    for record_id, start_row in record_start_rows.items():
        record_items = line_items_by_record.get(record_id, [])
        if len(record_items) > 1:
            end_row = start_row + len(record_items) - 1
            
            # Span first 6 columns for this record's items
            for col in range(6):
                style.append(('SPAN', (col, start_row), (col, end_row)))
            
            # Center align values in spanned cells
            style.append(('VALIGN', (0, start_row), (5, end_row), 'MIDDLE'))
            style.append(('ALIGN', (0, start_row), (5, end_row), 'CENTER'))
    
    table.setStyle(TableStyle(style))
    
    try:
        table_width, table_height = table.wrap(0, 0)
        table.drawOn(c, 15*mm, y - table_height)
        
        logging.info(f"✅ Table drawn successfully: {table_width}x{table_height}")
        
        return y - table_height - 3*mm
    except Exception as e:
        logging.error(f"Failed to draw table: {str(e)}")
        logging.error(f"Table data: {table_data[:3]}...")  # Show first few rows
        raise


def wrap_text(c: canvas.Canvas, text: str, max_width: float, font_name: str, font_size: int) -> list:
    """Wrap text with exact measurements"""
    words = text.split()
    lines = []
    current_line = ""
    
    for word in words:
        test_line = f"{current_line} {word}".strip()
        line_width = stringWidth(test_line, font_name, font_size)
        
        if line_width <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    
    if current_line:
        lines.append(current_line)
    
    return lines