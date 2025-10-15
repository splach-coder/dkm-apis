import re

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


REFERENTIE_KLANT = "Invoice: 10/10/2025 10/10/2025 \r\nCommercial reference: 615X35190079-615X36696601\r\nFrom:\r\nAs per attached copy: 25BEH1000000Q378R0 \r\nDatum:2025-10-12\r\n"
REFERENTIE_KLANT1 = "Invoice: \r\nCommercial reference: 602X36738179\r\nFrom:GAZI UNIVERSITESI\r\nAs per attached copy: 25BEH1000000Q34AR5 \r\nDatum:2025-10-12\r\n"


print(parse_referentie_klant(REFERENTIE_KLANT1))