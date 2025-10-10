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
    """Generate professional PDF from DebenoteData"""
    try:
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4
        
        y_position = height - 30*mm
        
        # Draw sections with clean professional layout
        y_position = draw_header_clean(c, data, y_position, width)
        y_position = draw_document_info_clean(c, data, y_position, width)
        y_position = draw_professional_table(c, data.line_items, y_position, width)
        y_position = draw_totals_clean(c, data, y_position, width)
        draw_footer_clean(c, data, y_position, width)
        
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
            logo_width = 95*mm  # Increased from 65mm
            logo_height = 65*mm  # Increased from 40mm
            c.drawImage(logo_path, 10*mm, y - logo_height, 
                       width=logo_width, height=logo_height, 
                       preserveAspectRatio=True, mask='auto')
        else:
            # Fallback text logo
            c.setFont("Helvetica-Bold", 60)  # Increased from 48
            c.setFillColor(colors.HexColor('#E54C37'))
            c.drawString(25*mm, y - 25*mm, "DKM")
            c.setFillColor(colors.black)
    except Exception as e:
        logging.error(f"Error loading logo: {str(e)}")
        c.setFont("Helvetica-Bold", 60)  # Increased from 48
        c.setFillColor(colors.HexColor('#E54C37'))
        c.drawString(25*mm, y - 25*mm, "DKM")
        c.setFillColor(colors.black)
    
    # COMPANY ADDRESS (right side of logo)
    x_address = 100*mm 
    y_address = y -22*mm
    
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(colors.black)
    c.drawString(x_address, y_address, data.client.naam.upper())
    
    c.setFont("Helvetica", 10)
    y_address -= 14
    c.drawString(x_address, y_address, data.client.straat_en_nummer.upper())
    
    y_address -= 13
    c.drawString(x_address, y_address, f"{data.client.landcode} {data.client.postcode} {data.client.stad.upper()}")
    
    y_address -= 13
    c.drawString(x_address, y_address, f"{data.client.landcode}{data.client.plda_operatoridentity}")
    
    y = start_y - 70*mm  # Adjusted for bigger logo
    return y

def parse_referentie_klant(ref_text: str):
    """
    Parse REFERENTIE_KLANT to extract all components and remove any duplicates or repetitions.
    Handles repeated patterns like 'As per attached copy...' or 'From: ...'.
    """
    components = {
        'invoice': '',
        'commercial_ref': '',
        'from_supplier': '',
        'attached_copy': '',
        'date': ''
    }
    
    if not ref_text:
        return components
    
    # Normalize and clean
    inv, comercial, fromSupplier, attachedCopy, date = ref_text.replace('\n', '').replace('\r', '\n').strip().split('\n')
    
    components['invoice'] = inv
    components['commercial_ref'] = comercial
    components['from_supplier'] = fromSupplier
    components['attached_copy'] = attachedCopy
    components['date'] = date
    
    return components

def draw_document_info_clean(c, data, y: float, width: float) -> float:
    """Draw document information in aligned label layout with proper word wrapping per line."""

    ref_components = parse_referentie_klant(data.referentie_klant)

    label_x = 23 * mm
    text_x = 70 * mm  # start of value column
    right_margin = width - 5 * mm
    line_gap = 14
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

    # Collect each field value as its own line
    ref_text_parts = []
    fields = ['invoice', 'commercial_ref', 'from_supplier', 'attached_copy', 'date']
    for key in fields:
        value = ref_components.get(key)
        if value:
            ref_text_parts.append(value.strip())

    # Draw each line — aligned with other value fields (text_x)
    for line in ref_text_parts:
        words = line.split()
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

    return y - 5

def draw_professional_table(c: canvas.Canvas, items: list, y: float, width: float) -> float:
    """Draw professional modern table for line items (slightly shifted left, better column spacing)."""
    
    if not items:
        return y
    
    # Table appearance
    header_height = 20
    row_height = 18
    x_shift = 2 * mm  # move everything 5mm to the left
    
    # Background + header rectangle
    c.saveState()
    c.setFillColor(colors.HexColor("#E54C37"))
    c.rect((20*mm) + x_shift, y - header_height, (width - 40*mm), header_height, fill=True, stroke=False)
    c.setStrokeColor(colors.HexColor('#E54C37'))
    c.setLineWidth(0.5)
    c.rect((20*mm) + x_shift, y - header_height, (width - 40*mm), header_height, fill=False, stroke=True)
    c.restoreState()
    
    # Header text
    c.setFillColor(colors.HexColor('#333333'))
    c.setFont("Helvetica-Bold", 9)
    
    # Adjusted column positions (more spacing + left shift)
    col_code   = (25*mm)  + x_shift
    col_desc   = (45*mm)  + x_shift
    col_qty    = (105*mm) + x_shift
    col_weight = (125*mm) + x_shift
    col_stat   = (148*mm) + x_shift
    col_value  = (175*mm) + x_shift  # moved 7mm further right to open gap
    
    header_y = y - 13
    c.drawString(col_code, header_y, "CODE")
    c.drawString(col_desc, header_y, "DESCRIPTION")
    c.drawString(col_qty, header_y, "QTY")
    c.drawString(col_weight, header_y, "NET WEIGHT")
    c.drawString(col_stat, header_y, "STAT UNIT")
    c.drawString(col_value, header_y, "VALUE")
    
    y -= header_height
    c.setFont("Helvetica", 8)
    
    # Draw table rows
    for i, item in enumerate(items):
        if i % 2 == 0:
            c.saveState()
            c.setFillColor(colors.HexColor('#FAFAFA'))
            c.rect((20*mm) + x_shift, y - row_height, (width - 40*mm), row_height, fill=True, stroke=False)
            c.restoreState()
        
        c.setStrokeColor(colors.HexColor('#E0E0E0'))
        c.setLineWidth(0.3)
        c.rect((20*mm) + x_shift, y - row_height, (width - 40*mm), row_height, fill=False, stroke=True)
        
        row_y = y - 12
        c.setFillColor(colors.black)
        
        # Code
        c.drawString(col_code, row_y, item.goederencode[:12])
        
        # Description
        desc = item.goederenomschrijving[:45].upper()
        c.drawString(col_desc, row_y, desc)
        
        # Quantity
        unit = "PK" if item.aantal_gewicht >= 10 else "PA"
        c.drawString(col_qty, row_y, f"{int(item.aantal_gewicht)} {unit}")
        
        # Net Weight
        c.drawString(col_weight, row_y, f"{item.netmass:,.2f}")
        
        # Stat Unit
        c.drawString(col_stat, row_y, f"{item.supplementaryunits:.2f}")
        
        # Value
        c.setFont("Helvetica-Bold", 8)
        c.drawString(col_value, row_y, f"{item.verkoopwaarde:,.2f}")
        c.setFont("Helvetica", 8)
        
        y -= row_height
    
    y -= 20
    return y

def draw_totals_clean(c: canvas.Canvas, data: DebenoteData, y: float, width: float) -> float:
    """Draw totals section in clean professional style"""
    
    y -= 10
    
    c.setFont("Helvetica-Bold", 11)
    c.setFillColor(colors.black)
    c.drawString(23*mm, y, "FOR THE TOTAL AMOUNT OF:")
    
    # Left aligned
    c.setFont("Helvetica-Bold", 14)
    c.drawString(width - 55*mm, y, f"{data.factuurtotaal:,.2f} {data.munt}")
    
    y -= 15
    
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.HexColor('#444444'))
    c.drawString(23*mm, y, f"say {data.amount_in_words}")
    
    y -= 35
    
    # VAT Information
    c.setFont("Helvetica", 9)
    c.setFillColor(colors.black)
    c.drawString(23*mm, y, "exempt from VAT, art 39bis")
    y -= 15
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(23*mm, y, "INVOICE TO BE PAID DIRECTLY THE SUPPLIER")
    y -= 12
    
    c.drawString(23*mm, y, "VAT TO BE PAID BY THE RECEIVER OF THE SERVICES")
    y -= 18
    
    # Legal text
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor('#555555'))
    legal_text = get_legal_text(data.client.language)
    wrapped_lines = wrap_text(legal_text, 110)
    for line in wrapped_lines[:2]:
        c.drawString(23*mm, y, line)
        y -= 10
    
    y -= 10
    c.drawString(23*mm, y, "Importdocument :")
    y -= 25
    
    return y

def draw_footer_clean(c: canvas.Canvas, data: DebenoteData, y: float, width: float):
    """Draw clean professional footer with tighter spacing and no overlap."""
    # Ensure we don't overlap with previous sections
    y = 87 * mm
    
    c.setFont("Helvetica-Bold", 9)
    c.setFillColor(colors.black)
    c.drawString(23 * mm, y, "DKM-CUSTOMS IS FISCAL REPRESENTATIVE FOR:")
    y -= 11  # slightly tighter than 12

    c.setFont("Helvetica", 9)
    c.drawString(23 * mm, y, data.client.naam.upper())
    y -= 9
    c.drawString(23 * mm, y, data.client.straat_en_nummer.upper())
    y -= 9
    c.drawString(23 * mm, y, f"{data.client.landcode} {data.client.postcode} {data.client.stad.upper()}")
    y -= 9

    # Map country codes to names
    country_names = {
        "FR": "FRANCE", "NL": "NETHERLANDS", "DE": "GERMANY",
        "PT": "PORTUGAL", "SK": "SLOVAKIA", "BE": "BELGIUM",
        "ES": "SPAIN", "IT": "ITALY", "PL": "POLAND", "AT": "AUSTRIA"
    }
    country = country_names.get(data.client.landcode, data.client.landcode)
    c.drawString(23 * mm, y, country)
    y -= 9

    c.setFont("Helvetica-Bold", 9)
    c.drawString(23 * mm, y, f"FISCAL VAT NUMBER {data.btwnummer}")

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