from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from textwrap import wrap
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import Table, TableStyle
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from io import BytesIO
import logging
import os
import re

from ..models.debenote_data import DebenoteData
from ..templates.legal_texts import get_legal_text

def generate_pdf(data: DebenoteData) -> bytes:
    """Generate professional PDF from DebenoteData with pagination support"""
    try:
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4
        
        # Start higher for tighter layout
        y_position = height - 25*mm
        page_number = 1
        
        logging.error(f"Generating PDF for DebenoteData: {data}")
        
        # Draw sections with clean professional layout
        y_position = draw_header_clean(c, data, y_position, width)
        y_position = draw_document_info_clean(c, data, y_position, width)
        y_position, page_number = draw_professional_table(c, data.line_items, y_position, width, height, data, page_number)
        y_position = draw_totals_clean(c, data, y_position, width, height)
        draw_footer_clean(c, data, y_position, width, height)
        
        # Add page number on first/last page
        draw_page_number(c, page_number, width, height)
        
        c.save()
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        return pdf_bytes
    except Exception as e:
        logging.error(f"Failed to generate PDF: {str(e)}")
        raise

def draw_header_clean(c: canvas.Canvas, data: DebenoteData, y: float, width: float) -> float:
    """Draw clean header with larger logo and company info"""
    
    start_y = y
    
    # LARGER DKM LOGO (left side)
    try:
        current_dir = os.path.dirname(os.path.dirname(__file__))
        logo_path = os.path.join(current_dir, "images", "dkm-logo.png")
        
        if os.path.exists(logo_path):
            # Bigger logo
            logo_width = 73*mm
            logo_height = 58*mm
            c.drawImage(logo_path, 22*mm, y - logo_height + 5*mm, 
                       width=logo_width, height=logo_height, 
                       preserveAspectRatio=True, mask='auto')
        else:
            # Fallback text logo
            c.setFont("Helvetica-Bold", 60)
            c.setFillColor(colors.HexColor('#E54C37'))
            c.drawString(25*mm, y - 25*mm, "DKM")
            c.setFillColor(colors.black)
    except Exception as e:
        logging.error(f"Error loading logo: {str(e)}")
        c.setFont("Helvetica-Bold", 60)
        c.setFillColor(colors.HexColor('#E54C37'))
        c.drawString(25*mm, y - 25*mm, "DKM")
        c.setFillColor(colors.black)
    
    # COMPANY ADDRESS (right side of logo)
    x_address = 110*mm 
    y_address = y - 20*mm
    
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(colors.black)
    c.drawString(x_address, y_address, data.client.fullName.upper())
    
    c.setFont("Helvetica", 10)
    y_address -= 12
    c.drawString(x_address, y_address, data.client.straat_en_nummer.upper())
    
    y_address -= 11
    c.drawString(x_address, y_address, f"{data.client.landcode} {data.client.postcode} {data.client.stad.upper()}")
    
    y_address -= 11
    c.drawString(x_address, y_address, f"{data.client.landcode}{data.client.plda_operatoridentity}")
    
    y_address -= 15
    if data.email is not None and data.email.strip() != "":
        c.drawString(x_address, y_address, f"mail: {data.email}")
    
    # Tighter spacing
    y = start_y - 58*mm
    return y

def parse_referentie_klant(ref_text: str):
    """
    Parse REFERENTIE_KLANT text and extract:
        - Invoice
        - Commercial reference
        - From
        - As per attached copy
        - Date (Datum)
    Always returns 5 keys, even if some are missing.
    """
    
    # Clean and normalize
    text = ref_text.replace('\r', '').strip()

    # Extract each section using regex (non-greedy, up to next known label)
    invoice_match = re.search(r"Invoice:\s*(.*?)(?=\s*Commercial reference:|$)", text, re.DOTALL | re.IGNORECASE)
    commercial_match = re.search(r"Commercial reference:\s*(.*?)(?=\s*From:|As per attached copy:|Datum:|$)", text, re.DOTALL | re.IGNORECASE)
    from_match = re.search(r"From:\s*(.*?)(?=\s*As per attached copy:|Datum:|$)", text, re.DOTALL | re.IGNORECASE)
    attached_match = re.search(r"As per attached copy:\s*(.*?)(?=\s*Datum:|$)", text, re.DOTALL | re.IGNORECASE)
    date_match = re.search(r"Datum:\s*(.*)", text, re.IGNORECASE)

    # Helper to clean captured text
    def clean(value):
        return value.strip() if value else ""

    components = {
        'invoice': clean(invoice_match.group(1) if invoice_match else ""),
        'commercial_ref': clean(commercial_match.group(1) if commercial_match else ""),
        'From': clean(from_match.group(1) if from_match else ""),
        'As per attached copy': clean(attached_match.group(1) if attached_match else ""),
        'date': clean(date_match.group(1) if date_match else "")
    }

    return components

def draw_document_info_clean(c, data, y: float, width: float) -> float:
    """Draw document information in aligned label layout with field names for each REFERENCE line.
       Skips lines with empty or null values.
    """

    ref_components = parse_referentie_klant(data.referentie_klant)

    label_x = 23 * mm
    text_x = 70 * mm
    right_margin = width - 5 * mm
    line_gap = 12
    font_name = "Helvetica"
    font_size = 9

    c.setFont(font_name, font_size)
    c.setFillColor(colors.black)

    # --- VATNOTE ---
    vatnote_text = f"{data.processfactuurnummer} {data.datum}"
    c.drawString(label_x, y, "VATNOTE:")
    c.drawString(text_x, y, vatnote_text)
    y -= line_gap

    # --- FILENUMBER ---
    file_text = f"{data.commercialreference} ----ID:{data.c88nummer}-----"
    c.drawString(label_x, y, "FILENUMBER:")
    c.drawString(text_x, y, file_text)
    y -= line_gap

    # --- REFERENCE ---
    c.drawString(label_x, y, "REFERENCE:")

    # Field mapping (label → parse_referentie_klant key)
    field_labels = [
        ("Invoice:", "invoice"),
        ("Commercial reference:", "commercial_ref"),
        ("From:", "From"),
        ("As per attached copy:", "As per attached copy"),
        ("Date:", "date"),
    ]

    # Draw each field only if value is not empty
    for label, key in field_labels:
        value = (ref_components.get(key) or "").strip()
        if not value:
            continue  # Skip this line entirely

        full_text = f"{label} {value}".strip()
        words = full_text.split()
        current_line = ""

        for word in words:
            test_line = f"{current_line} {word}".strip()
            line_width = stringWidth(test_line, font_name, font_size)
            if text_x + line_width > right_margin:
                c.drawString(text_x, y, current_line)
                y -= line_gap
                current_line = word
            else:
                current_line = test_line

        if current_line:
            c.drawString(text_x, y, current_line)
            y -= line_gap

    return y - 3

def wrap_description(text: str, max_width: float, font_name: str, font_size: int) -> list:
    """
    Wrap description text to fit within column width
    Returns list of lines that fit within max_width
    """
    words = text.split()
    lines = []
    current_line = ""
    
    for word in words:
        test_line = f"{current_line} {word}".strip() if current_line else word
        line_width = stringWidth(test_line, font_name, font_size)
        
        if line_width <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
                current_line = word
            else:
                # Single word too long, truncate it
                lines.append(word[:30] + "...")
                current_line = ""
    
    if current_line:
        lines.append(current_line)
    
    return lines

def draw_professional_table(c: canvas.Canvas, items: list, y: float, width: float, height: float, data: DebenoteData, page_number: int) -> tuple:
    """Draw professional modern table for line items with pagination and text wrapping"""
    
    if not items:
        return y, page_number
    
    # Table appearance
    header_height = 20
    base_row_height = 18
    min_row_height = 18
    x_shift = 2 * mm
    font_name = "Helvetica"
    font_size = 8
    
    # Column definitions
    col_code = (25*mm) + x_shift
    col_desc = (45*mm) + x_shift
    col_qty = (100*mm) + x_shift
    col_weight = (120*mm) + x_shift
    col_stat = (143*mm) + x_shift
    col_value = (170*mm) + x_shift
    
    # Description column max width (from col_desc to col_qty)
    desc_max_width = col_qty - col_desc - 3*mm
    
    # Minimum Y position before footer (leave space for footer)
    min_y = 110 * mm
    
    # Draw header
    def draw_table_header(canvas, y_pos):
        canvas.saveState()
        canvas.setFillColor(colors.HexColor("#E54C37"))
        canvas.rect((20*mm) + x_shift, y_pos - header_height, (width - 40*mm), header_height, fill=True, stroke=False)
        canvas.setStrokeColor(colors.HexColor('#E54C37'))
        canvas.setLineWidth(0.5)
        canvas.rect((20*mm) + x_shift, y_pos - header_height, (width - 40*mm), header_height, fill=False, stroke=True)
        canvas.restoreState()
        
        canvas.setFillColor(colors.HexColor('#333333'))
        canvas.setFont("Helvetica-Bold", 9)
        
        header_y = y_pos - 13
        canvas.drawString(col_code, header_y, "CODE")
        canvas.drawString(col_desc, header_y, "DESCRIPTION")
        canvas.drawString(col_qty, header_y, "QTY")
        canvas.drawString(col_weight, header_y, "NET WEIGHT")
        canvas.drawString(col_stat, header_y, "STAT UNIT")
        canvas.drawString(col_value, header_y, "VALUE")
        
        return y_pos - header_height
    
    # Draw initial header
    y = draw_table_header(c, y)
    c.setFont(font_name, font_size)
    
    # Draw table rows
    for i, item in enumerate(items):
        # Wrap description text
        desc_lines = wrap_description(
            item.goederenomschrijving.upper(), 
            desc_max_width, 
            font_name, 
            font_size
        )
        
        # Limit to 3 lines max
        desc_lines = desc_lines[:3]
        
        # Calculate row height based on description lines
        row_height = max(min_row_height, len(desc_lines) * 10 + 8)
        
        # Check if we need a new page
        if y - row_height < min_y:
            # Draw page number
            draw_page_number(c, page_number, width, height)
            
            # New page
            c.showPage()
            page_number += 1
            
            # Reset position and redraw header
            y = height - 25*mm
            y = draw_table_header(c, y)
            c.setFont(font_name, font_size)
        
        # Draw row background
        if i % 2 == 0:
            c.saveState()
            c.setFillColor(colors.HexColor('#FAFAFA'))
            c.rect((20*mm) + x_shift, y - row_height, (width - 40*mm), row_height, fill=True, stroke=False)
            c.restoreState()
        
        # Draw row border
        c.setStrokeColor(colors.HexColor('#E0E0E0'))
        c.setLineWidth(0.3)
        c.rect((20*mm) + x_shift, y - row_height, (width - 40*mm), row_height, fill=False, stroke=True)
        
        # Starting Y for text (top of row minus padding)
        row_y = y - 12
        c.setFillColor(colors.black)
        
        # Code
        c.drawString(col_code, row_y, item.goederencode[:12])
        
        # Description (multi-line)
        for line_idx, desc_line in enumerate(desc_lines):
            c.drawString(col_desc, row_y - (line_idx * 10), desc_line)
        
        # Quantity (centered vertically in row)
        unit = item.typepackages
        c.drawString(col_qty, row_y, f"{int(item.aantal_gewicht)} {unit}")
        
        # Net Weight
        c.drawString(col_weight, row_y, f"{item.netmass:,.2f}")
        
        # Stat Unit
        c.drawString(col_stat, row_y, f"{item.supplementaryunits:.2f}")
        
        # Value
        c.setFont("Helvetica-Bold", font_size)
        c.drawString(col_value, row_y, f"{item.verkoopwaarde:,.2f}")
        c.setFont(font_name, font_size)
        
        y -= row_height
    
    y -= 15  # Reduced spacing after table
    return y, page_number

def draw_totals_clean(c: canvas.Canvas, data: DebenoteData, y: float, width: float, height: float) -> float:
    """Draw totals section in clean professional style"""
    
    # Check if we have enough space for totals section
    min_y_for_totals = 110 * mm
    
    if y < min_y_for_totals + 100:  # Need ~100mm for totals + footer
        # Not enough space, this will be handled by footer positioning
        pass
    
    y -= 5
    
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(colors.black)
    c.drawString(23*mm, y, "FOR THE TOTAL AMOUNT OF:")
    
    c.setFont("Helvetica-Bold", 14)
    c.drawString(width - 55*mm, y, f"{data.factuurtotaal:,.2f} {data.munt}")
    
    y -= 13
    
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor('#444444'))
    c.drawString(23*mm, y, f"say {data.amount_in_words}")
    
    y -= 25
    
    # VAT Information
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.black)
    c.drawString(23*mm, y, "exempt from VAT, art 39bis")
    y -= 13
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(23*mm, y, "INVOICE TO BE PAID DIRECTLY THE SUPPLIER")
    y -= 11
    
    c.drawString(23*mm, y, "VAT TO BE PAID BY THE RECEIVER OF THE SERVICES")
    y -= 15
    
    # Legal text
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor('#555555'))
    legal_text = get_legal_text(data.client.language)
    wrapped_lines = wrap_text(legal_text, 110)
    for line in wrapped_lines[:2]:
        c.drawString(23*mm, y, line)
        y -= 9
    
    y -= 8
    c.drawString(23*mm, y, "Importdocument :")
    y -= 20
    
    return y

def draw_footer_clean(c: canvas.Canvas, data: DebenoteData, y: float, width: float, height: float):
    """Draw clean professional footer - dynamically positioned based on content.
       This version uses the 'relatie' information instead of 'client'.
    """
    
    # Minimum safe footer Y position
    min_footer_y = 70 * mm
    if y < min_footer_y:
        y = min_footer_y
    
    # Header text in footer
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(colors.black)
    c.drawString(23 * mm, y, "DKM-CUSTOMS IS FISCAL REPRESENTATIVE FOR:")
    y -= 10

    # Draw relatie info (formerly client info)
    c.setFont("Helvetica", 9)
    c.drawString(23 * mm, y, data.relatie.fullName.upper())
    y -= 9
    c.drawString(23 * mm, y, data.relatie.straat_en_nummer.upper())
    y -= 9
    c.drawString(
        23 * mm,
        y,
        f"{data.relatie.landcode} {data.relatie.postcode} {data.relatie.stad.upper()}"
    )
    y -= 9
    
    c.drawString(
        23 * mm,
        y,
        f"{data.relatie.landcode}{data.relatie.plda_operatoridentity}"
    )
    y -= 9

    # Map country codes to names for readability
    country_names = {
        "FR": "FRANCE", "NL": "NETHERLAND", "DE": "GERMANY",
        "PT": "PORTUGAL", "SK": "SLOVAKIA", "BE": "BELGIUM",
        "ES": "SPAIN", "IT": "ITALY", "PL": "POLAND", "AT": "AUSTRIA"
    }
    country = country_names.get(data.relatie.landcode, data.relatie.landcode)
    c.drawString(23 * mm, y, country)
    y -= 9

    # Draw VAT number (same as before)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(23 * mm, y, f"FISCAL VAT NUMBER {data.btwnummer}")

def draw_page_number(c: canvas.Canvas, page_num: int, width: float, height: float):
    """Draw page number at bottom-right"""
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor('#888888'))
    c.drawRightString(width - 20*mm, 15*mm, f"Page {page_num}")

def wrap_text(text: str, max_chars: int) -> list:
    """Wrap text to max characters per line"""
    words = text.split()
    lines = []
    current_line = ""
    
    for word in words:
        test_line = f"{current_line} {word}".strip()
        if len(test_line) <= max_chars:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word
    
    if current_line:
        lines.append(current_line)
    
    return lines