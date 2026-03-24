"""
Standalone test: generates a credit note PDF directly from pdf_generator.
Run from the project root:
    python -m test_credit_note_pdf
"""
import sys
import os

# Make sure the package is importable
sys.path.insert(0, os.path.dirname(__file__))

from DkmFiscdepetProcessor.models.debenote_data import DebenoteData, ClientInfo, RelatieInfo, LineItem
from DkmFiscdepetProcessor.services.pdf_generator import generate_pdf

# --- Mock data with NEGATIVE amount (credit note scenario) ---
data = DebenoteData(
    internfactuurnummer=2026000002,
    processfactuurnummer=2026000002,
    btwnummer="796538660",
    datum="13/03/2026",
    jaar="2026",
    periode="03",
    factuurtotaal=73221.00,          # <-- negative triggers CREDIT NOTE badge
    munt="EUR",
    email="",
    emails_to="billing@gommazone.fr",
    emails_cc="",
    commercialreference="KS1003279",
    referentie_klant=(
        "Invoice: 11 Date: 12/01/2026\n"
        "Commercial reference: KS1003279\n"
        "From: LAVORGOMMA SRL Import related to: 26BEH1000001UT9CR7\n"
        "Date: 2026-01-27"
    ),
    c88nummer=183694,
    client=ClientInfo(
        relatiecode="GOMMAZONE",
        fullName="GOMMAZONE SARL",
        naam="GOMMAZONE",
        straat_en_nummer="RUE DES DUNES",
        postcode="59495",
        stad="LEFFRINCKOUCKE",
        landcode="FR",
        plda_operatoridentity="66927548065",
        language="EN"
    ),
    relatie=RelatieInfo(
        fullName="LAVORGOMMA SRL",
        straat_en_nummer="LOC MOLINO 9-10",
        postcode="61026",
        stad="BELFORTE ALL ISAURO",
        landcode="IT",
        plda_operatoridentity="01342930417",
        language="EN"
    ),
    relatiecode_leverancier="LAVORGOMMA",
    leverancier_naam="LAVORGOMMA SRL",
    line_items=[
        LineItem(
            goederencode="4010120000",
            goederenomschrijving="TRANSPORTBANDEN VAN GEVULKANISEERDE RUBBER, UITSLUITEND MET TEXTIELSTOF",
            aantal_gewicht=8,
            verkoopwaarde=-36087.00,
            netmass=16749.34,
            supplementaryunits=0.0,
            zendtarieflijnnummer=1,
            typepackages="PA"
        ),
        LineItem(
            goederencode="4010120000",
            goederenomschrijving="TRANSPORTBANDEN VAN GEVULKANISEERDE RUBBER, UITSLUITEND MET TEXTIELSTOF",
            aantal_gewicht=6,
            verkoopwaarde=-37134.00,
            netmass=16620.66,
            supplementaryunits=0.0,
            zendtarieflijnnummer=2,
            typepackages="PA"
        ),
    ],
    amount_in_words="minus seventy-three thousand, two hundred and twenty-one EUR",
    vatnote="2026000002-13/03/2026",
    formatted_total="-€73,221.00",
    DECLARATIONGUID="26BEH1000001UT9CR7",
    principal="",
    principal_email="",
    principal_cc=""
)

# --- Generate PDF ---
pdf_bytes = generate_pdf(data)

output_path = os.path.join(os.path.dirname(__file__), "test_output_credit_note.pdf")
with open(output_path, "wb") as f:
    f.write(pdf_bytes)

print(f"PDF written to: {output_path}")
