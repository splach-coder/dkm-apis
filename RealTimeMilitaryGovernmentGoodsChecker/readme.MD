# Address Detection Flow

## Overview
This project implements an Azure Function designed to check if an address is related to a government or military entity. It utilizes an external API (Gemini) to classify the address, logs the results, and performs caching to ensure addresses aren't repeatedly checked. The flow also includes a periodic task to clean up previously checked addresses stored in a CSV file on Azure Blob Storage.

## Architecture

1. **Azure Function**:
    - Accepts incoming requests with address data.
    - Checks if the address has been previously checked by searching a cached CSV file stored in Azure Blob Storage.
    - If not cached, sends the address to the Gemini API for classification (military/government-related or not).
    - Appends the results (address, timestamp) to the cached CSV.
    - If it’s 22:00, clears the CSV file to reset the cache.

2. **Gemini API**:
    - A class (`AddressDetector`) is used to interact with the Gemini API. It checks whether an address is related to the military or government.
    - The API key is securely stored in Azure Key Vault.

3. **Azure Blob Storage**:
    - Stores the CSV file (`CHECKED_IDS.csv`) containing the address IDs that have been checked along with their results and timestamp.
    - This prevents re-checking the same addresses multiple times.

4. **Periodic Task integrated as a function**:
    - A function runs at the end of the function only if time at 22:00 to clean the CSV file by emptying its contents, ensuring the cache is reset daily.

5. **Routes**:
    - Post : check addresses and store the found one
    - Get : get all the found addresses
    - Patch : update checked addresses