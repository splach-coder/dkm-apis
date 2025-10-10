def get_legal_text(language: str) -> str:
    """
    Get VAT exemption legal text based on language
    Matches exact text from sample PDFs
    
    Args:
        language: Language code (EN, NL, DE, FR)
        
    Returns:
        Legal text in specified language
    """
    texts = {
        "NL": (
            "Intra-communautaire levering: vrijstelling van BTW overeenkomstig art. 39bis, 1° van het WBTW / "
            "Vrij van BTW: art 138 lid 1 van Eur. BTW richtlijn 2006/112/EC"
        ),
        "DE": (
            "Innergem.Lieferung: befreit vom Umsatzsteuer: art.39bis, 1° des Belg. Umsatzsteuergesetz / "
            "Befreit vom Umsatzsteuer: art 138 lid 1 des Eur. VAT Council directive 2006/112/EC"
        ),
        "FR": (
            "Livraison intracommunautaire: exonération de la TVA conformément à l'art. 39bis, 1° de la WBTW / "
            "Hors TVA: art 138 alinea 1 de la Eur. Directive TVA 2006/112/EC"
        ),
        "EN": (
            "Intra-Community supply: exemption from VAT in accordance with art. 39bis, 1° of the WBTW / "
            "Free of VAT: art 138 lid 1 from Eur. VAT Council directive 2006/112/EC"
        )
    }
    
    return texts.get(language.upper(), texts["EN"])