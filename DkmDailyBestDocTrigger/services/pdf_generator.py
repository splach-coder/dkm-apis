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
        
        # Font registration
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
        
        return pdf_bytes
        
    except Exception as e:
        logging.error(f"PDF generation failed: {str(e)}")
        raise

def draw_header(c: canvas.Canvas, data, y: float, width: float, font_family: str) -> float:
    try:
        # Use relative path from this file to images folder
        # services/pdf_generator.py -> parent -> images/dkm-logo.png
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logo_path = os.path.join(current_dir, "images", "dkm-logo.png")
        
        if os.path.exists(logo_path):
            # Draw logo (Adjusted size/position to match sample)
            # Sample shows logo on top left, slightly larger
            c.drawImage(logo_path, 15*mm, y - 12*mm, width=45*mm, height=15*mm, preserveAspectRatio=True, mask='auto')
        else:
            # Fallback
            c.setFont(f"{font_family}-Bold", 28)
            c.setFillColor(colors.HexColor('#E85D3F'))
            c.drawString(15*mm, y - 8*mm, "DKM")
            c.setFillColor(colors.black)
    except Exception as e:
        logging.warning(f"Logo error: {e}")
    
    # Client Info Box
    c.setFont(f"{font_family}-Bold", 10)
    c.setFillColor(colors.black)
    c.drawString(15*mm, y - 22*mm, data.client.naam.upper())
    c.setFont(font_family, 9)
    c.drawString(15*mm, y - 27*mm, data.client.straat_en_nummer.upper())
    c.drawString(15*mm, y - 32*mm, f"{data.client.postcode}    {data.client.stad.upper()}")
    c.drawString(15*mm, y - 37*mm, f"{data.client.landcode}  {data.client.plda_operatoridentity}")
    
    # DKM header right side
    dkm_x = width - 70*mm
    c.setFont(f"{font_family}-Bold", 10)
    c.drawString(dkm_x, y - 10*mm, "DKM-customs")
    c.setFont(font_family, 8)
    c.drawString(dkm_x, y - 15*mm, "Noorderlaan 72- 2030 Antwerpen")
    c.drawString(dkm_x, y - 20*mm, "BE0796538660")
    
    return y - 45*mm

def draw_title(c: canvas.Canvas, y: float, width: float, font_family: str) -> float:
    c.setFont(f"{font_family}-Bold", 9)
    c.drawCentredString(width/2, y, "Declaration for VAT purposes according to :")
    y -= 3.5*mm
    c.setFont(f"{font_family}-Oblique", 7)
    c.drawCentredString(width/2, y, "article 138, paragraph 1, directive 2006/112/EC")
    return y - 7*mm

def draw_two_column_section(c: canvas.Canvas, data, y: float, width: float, font_family: str) -> float:
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
    
    right_x = left_x + box_width + 8*mm
    right_width = width - right_x - 15*mm
    text = "Declares that the goods imported into Belgium were properly transported to the country mentioned on the left. Undersigned therefore declares that the acquisition of below mentioned goods will be reported in their VAT return according to the law of the member state which the VAT identification"
    c.setFont(font_family, 6.5)
    lines = wrap_text(c, text, right_width, font_family, 6.5)
    text_y = y - 2*mm
    for line in lines[:3]:
        c.drawString(right_x, text_y, line)
        text_y -= 2.8*mm
    text_y -= 4*mm
    c.setFont(font_family, 7)
    
    # Name
    c.drawString(right_x, text_y, "Name of the undersigned :")
    c.setDash(1, 2) # Dotted line style
    c.line(right_x + 35*mm, text_y, right_x + 110*mm, text_y)
    c.setDash([]) # Reset to solid
    text_y -= 6*mm
    
    # Function
    c.drawString(right_x, text_y, "function of the undersigned :")
    c.setDash(1, 2)
    c.line(right_x + 38*mm, text_y, right_x + 110*mm, text_y)
    c.setDash([])
    text_y -= 8*mm # Extra space for signature box
    
    # Signature
    c.drawString(right_x, text_y, "Signature :")
    
    # Draw Rectangle for Signature
    # Position: to the right of "Signature :", slightly down
    sig_box_x = right_x + 18*mm
    sig_box_y = text_y - 15*mm # 15mm height box, going down from text_y
    sig_box_width = 80*mm
    sig_box_height = 20*mm
    
    c.rect(sig_box_x, sig_box_y, sig_box_width, sig_box_height, stroke=1, fill=0)
    
    return y - 45*mm

from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

def draw_table(c: canvas.Canvas, data, y: float, width: float, font_family: str) -> float:
    """Draw table with merged record support + Pagination"""
    
    if not data.line_items:
        return y
    
    headers = ['MRN', 'ID', 'Supplier', 'Date', 'Reference', 'Debetnote', 'Item', 'Commodity', 'Vat Value €']
    # Headers remain simple strings
    table_data = [headers]
    
    # Define wrapping style
    styles = getSampleStyleSheet()
    cell_style = ParagraphStyle(
        'CellStyle',
        parent=styles['Normal'],
        fontName=font_family,
        fontSize=7,
        leading=8.5, # Line spacing
        splitLongWords=True
    )
    
    # 1. Structure Data
    line_items_by_record = {}
    for item in data.line_items:
        rid = item.source_internfactuurnummer
        if rid not in line_items_by_record: line_items_by_record[rid] = []
        line_items_by_record[rid].append(item)
    
    record_start_rows = {}
    current_row = 1
    
    # 2. Build Table Rows
    for record in data.records:
        rid = record.internfactuurnummer
        items = line_items_by_record.get(rid, [])
        if not items: continue
        
        record_start_rows[rid] = current_row
        
        for idx, item in enumerate(items):
            # Format Commodity Column: Code + Description
            desc = item.goederenomschrijving if item.goederenomschrijving else ""
            weight_str = f"Qty: {item.aantal_gewicht}" if item.aantal_gewicht else ""
            
            # Using <br/> for line breaks in Paragraph
            commodity_raw = f"{item.goederencode}\n{desc}\n{weight_str}".strip().replace('\n', '<br/>')
            
            if idx == 0:
                # First item: Full details (All wrapped in Paragraph to ensure fit/wrap)
                row = [
                    Paragraph(str(record.mrn), cell_style),
                    Paragraph(str(record.declarationid), cell_style),
                    Paragraph(str(record.exportername), cell_style),
                    Paragraph(str(record.datum), cell_style),
                    Paragraph(str(record.reference), cell_style),
                    Paragraph(str(record.processfactuurnummer), cell_style),
                    Paragraph(str(item.zendtarieflijnnummer), cell_style),
                    Paragraph(commodity_raw, cell_style),
                    Paragraph(f"{item.verkoopwaarde:.2f}", cell_style)
                ]
            else:
                # Subsequent items
                # Non-merged columns must also be Paragraphs to match style/fitting
                row = ['', '', '', '', '', '', 
                       Paragraph(str(item.zendtarieflijnnummer), cell_style), 
                       Paragraph(commodity_raw, cell_style), 
                       Paragraph(f"{item.verkoopwaarde:.2f}", cell_style)]
            
            table_data.append(row)
            current_row += 1

    # Calculate available width (297mm - 15mm left - 10mm right = 272mm approx)
    # Total targeted width: ~270mm
    col_widths = [35*mm, 15*mm, 45*mm, 20*mm, 50*mm, 25*mm, 10*mm, 50*mm, 20*mm]
    
    # Note: Table handles Paragraph flow automatically based on colWidths
    # repeatRows=1 ensures header repeats on new pages
    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    
    # 3. Define Professional Style
    style = [
        # Header Style
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F35E40')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), f'{font_family}-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('TOPPADDING', (0, 0), (-1, 0), 6),

        # Content Style (Font for cells that are NOT Paragraphs)
        ('FONTNAME', (0, 1), (-1, -1), font_family),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        
        # Alignment & Padding (Top-Left with Padding)
        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 1), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 1), (-1, -1), 3),
        ('RIGHTPADDING', (0, 1), (-1, -1), 3),
        ('TOPPADDING', (0, 1), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 4),
    ]
    
    # 4. Pagination-Friendly "Simulated Spanning"
    # We maintain the VISUAL look of grouped rows (merged cells) by controlling borders,
    # but we keep rows technically independent to allow the PDF engine to split them across pages anywhere.
    
    # Vertical Dividers (All Columns)
    for col in range(len(headers)):
        style.append(('LINEAFTER', (col, 0), (col, -1), 0.5, colors.HexColor('#808080')))
    style.append(('LINEBEFORE', (0, 0), (0, -1), 0.5, colors.HexColor('#808080')))

    # Horizontal Lines
    # 1. Header Separator
    style.append(('LINEBELOW', (0, 0), (-1, 0), 0.5, colors.black))
    
    # 2. Item Grid (Columns 6-9): Draw Separators for EVERY row
    # This ensures every item has a bottom border
    style.append(('LINEBELOW', (6, 1), (-1, -1), 0.5, colors.HexColor('#808080')))
    
    # 3. Record Grouping (Columns 0-5): Draw Separators ONLY between different records
    # We draw a line ABOVE the start of each new record.
    for rid, start in record_start_rows.items():
        if start > 1: # Skip the very first data row (it already has the header line above it)
            style.append(('LINEABOVE', (0, start), (5, start), 0.5, colors.HexColor('#808080')))
            
    # 4. Table Frame
    style.append(('LINEBELOW', (0, -1), (-1, -1), 1, colors.black))
    style.append(('BOX', (0, 0), (-1, -1), 1, colors.black))

    table.setStyle(TableStyle(style))
    
    # --- Pagination Logic ---
    bottom_margin = 15*mm
    
    # --- Pagination Logic ---
    bottom_margin = 15*mm
    loop_count = 0
    
    while True:
        loop_count += 1
        if loop_count > 50:
            logging.error(f"Infinite loop detected in PDF pagination for {data.client.naam} - ID {data.records[0].internfactuurnummer if data.records else '?'}")
            # Force draw remainder and overflow safely
            table.drawOn(c, 15*mm, y - table.wrap(width, 0)[1])
            break

        # Calculate size needed
        table_width, table_height = table.wrap(width, 0)
        avail_height = y - bottom_margin
        
        if table_height <= avail_height:
            # Table fits entirely
            table.drawOn(c, 15*mm, y - table_height)
            return y - table_height - 3*mm
        else:
            # Table too big, try to split
            # split() returns [fitted_part, remainder]
            pieces = table.split(width, avail_height)
            
            if not pieces:
                # Content doesn't fit at all in available space.
                
                # Check if we are already on a fresh page (top of page)?
                # Standard Y for fresh page is roughly (PageHeight - Header).
                # If y is close to that, it means even a full page isn't enough for the first row?
                # We force a split or draw anyway to avoid infinite loop.
                
                # For now, assumption: if we can't split, just force new page once. 
                # If we just did a new page, then we must bail out.
                
                c.showPage()
                # New Page - Reset Y to top (No Header)
                page_width, page_height = landscape(A4)
                y = page_height - 20*mm
                
                # Re-check height on new page
                avail_height_new = y - bottom_margin
                pieces_new = table.split(width, avail_height_new)
                
                if not pieces_new:
                    # Still won't fit on a fresh page (Row too huge)
                    # Force draw and clip/overflow
                    logging.warning("Row too large for single page, forcing draw.")
                    table.drawOn(c, 15*mm, y - table_height)
                    break
                else:
                    pieces = pieces_new
                    # Proceed to handle pieces
            
            # Draw the part that fits
            part0 = pieces[0]
            h0 = part0.wrap(width, avail_height)[1]
            part0.drawOn(c, 15*mm, y - h0)
            
            # Draw the part that fits
            part0 = pieces[0]
            h0 = part0.wrap(width, avail_height)[1]
            part0.drawOn(c, 15*mm, y - h0)
            
            # Prepare for next page (remainder)
            if len(pieces) > 1:
                table = pieces[1]
                c.showPage()
                
                # RESET Y to Top of new page (No Header Repeated)
                # A4 Landscape Height ~210mm. Start 20mm from top.
                page_width, page_height = landscape(A4)
                y = page_height - 20*mm 
                
                logging.info("Pagination: Created new page for continuing table.")
                continue
            else:
                # No remainder? Then we are done.
                break
                
    return y

def wrap_text(c, text, max_width, font_name, font_size):
    words = text.split()
    lines = []; current_line = ""
    for word in words:
        test_line = f"{current_line} {word}".strip()
        if stringWidth(test_line, font_name, font_size) <= max_width:
            current_line = test_line
        else:
            if current_line: lines.append(current_line)
            current_line = word
    if current_line: lines.append(current_line)
    return lines
