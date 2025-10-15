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

REFERENTIE_KLANT = "Invoice: 10/10/2025 10/10/2025 \r\nCommercial reference: 615X35190079-615X36696601\r\nFrom:\r\nAs per attached copy: 25BEH1000000Q378R0 \r\nDatum:2025-10-12\r\n",
REFERENTIE_KLANT1 = "Invoice: \r\nCommercial reference: 602X36738179\r\nFrom:GAZI UNIVERSITESI\r\nAs per attached copy: 25BEH1000000Q34AR5 \r\nDatum:2025-10-12\r\n",
