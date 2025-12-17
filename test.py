import requests


# rest api url
TOKEN_URL = "https://prod-243.westeurope.logic.azure.com:443/workflows/35c2398954db4915a9c7767fc068166d/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=yrdQOz1v19Tq4fCCUISH7WwHFm3kfkbjl-mYiy6Yk3Y"
container_name = "document-intelligence"
folder_path = "MasterData/MRN_Master_Records.json"


def send_request(payload: dict) -> dict:
    """Send POST request to the API and return the JSON response."""
    try:
        response = requests.post(TOKEN_URL, json=payload)
        return response.json()
    except requests.RequestException as e:
        print(f"Error during API request: {e}")
        return {}
    

if __name__ == "__main__":
    # Example payload
    payload = {
        "container": container_name,
        "filepath": folder_path
    }
    
    # Send request and print response
    api_response = send_request(payload)
    print("API Response:", api_response)    
