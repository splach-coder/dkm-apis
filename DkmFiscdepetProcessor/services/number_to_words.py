from num2words import num2words
import logging

def amount_to_words(amount: float, currency: str, language: str) -> str:
    """
    Convert amount to words in specified language
    Matches format from sample PDFs
    
    Args:
        amount: Numeric amount (e.g., 48771.63)
        currency: Currency code (e.g., "EUR")
        language: Language code (e.g., "EN", "NL", "DE", "FR")
        
    Returns:
        Amount in words (e.g., "forty-eight thousand seven hundred seventy-one , sixty-three EUR")
    """
    try:
        # Map client language to num2words language codes
        lang_map = {
            "EN": "en",
            "NL": "nl",
            "DE": "de",
            "FR": "fr"
        }
        
        lang_code = lang_map.get(language.upper(), "en")
        
        # Split into integer and decimal parts
        integer_part = int(amount)
        decimal_part = int(round((amount - integer_part) * 100))
        
        # Convert integer part to words
        words = num2words(integer_part, lang=lang_code)
        
        # Add decimal part if exists
        if decimal_part > 0:
            # For consistency with samples: "forty-eight thousand ... , sixty-three"
            decimal_words = num2words(decimal_part, lang=lang_code)
            words = f"{words} , {decimal_words}"
        
        # Add currency
        words = f"{words} {currency}"
        
        return words
        
    except Exception as e:
        logging.error(f"Error converting amount to words: {str(e)}")
        # Fallback format
        return f"{amount:,.2f} {currency}"