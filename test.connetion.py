#!/usr/bin/env python3
"""
Standalone Test Script for DgArrivalProcessor
No pytest required - just run with: python test_connection.py

This script tests:
1. OAuth authentication with ObiBatch
2. Database connection
3. Sending dummy arrival data
"""

import json
import requests
from datetime import datetime


# ============================================
# CONFIGURATION
# ============================================

# OAuth Configuration
TOKEN_URL = "https://obibatch.ad.dkm-customs.com:5002/connect/token"
CLIENT_ID = "client.ssw.dkm"
CLIENT_SECRET = "vZFAWP5E18Lq"
SCOPE = "be:ncts:integration"

# API Configuration
API_BASE_URL = "https://obibatch.ad.dkm-customs.com/api/be-ncts/batch"
API_ENDPOINT = f"{API_BASE_URL}/arrivals/"
TENANT_ID = "DKM_VP"


# ============================================
# HELPER FUNCTIONS
# ============================================

def print_header(text):
    """Print formatted header"""
    print("\n" + "="*60)
    print(f"  {text}")
    print("="*60)


def print_success(text):
    """Print success message"""
    print(f"✅ {text}")


def print_error(text):
    """Print error message"""
    print(f"❌ {text}")


def print_info(text):
    """Print info message"""
    print(f"ℹ️  {text}")


# ============================================
# TEST 1: OAuth Authentication
# ============================================

def test_oauth_authentication():
    """Test OAuth authentication with ObiBatch"""
    print_header("TEST 1: OAuth Authentication")
    
    try:
        print_info("Requesting OAuth token...")
        print_info(f"Token URL: {TOKEN_URL}")
        print_info(f"Client ID: {CLIENT_ID}")
        
        # Prepare OAuth request
        payload = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": SCOPE,
            "grant_type": "client_credentials"
        }
        
        # Request token
        response = requests.post(TOKEN_URL, params=payload, timeout=40)
        
        # Check response
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in")
            
            print_success("OAuth authentication successful!")
            print_info(f"Token received (first 20 chars): {access_token[:20]}...")
            print_info(f"Token expires in: {expires_in} seconds ({expires_in/60:.1f} minutes)")
            
            return access_token
        else:
            print_error(f"OAuth failed with status code: {response.status_code}")
            print_error(f"Response: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        print_error("Request timed out! Check if the OAuth server is accessible.")
        return None
    except requests.exceptions.ConnectionError:
        print_error("Connection error! Check your network and the OAuth server URL.")
        return None
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        return None


# ============================================
# TEST 2: Create Dummy NCTS Data
# ============================================

def create_dummy_ncts_data():
    """Create dummy NCTS arrival notification data"""
    print_header("TEST 2: Creating Dummy NCTS Data")
    
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    dummy_data = {
        "$type": "Arrival.Notification",
        "format": "ncts",
        "language": "EN",
        "declaration": {
            "lrn": "TEST123456789",
            "mrn": "25LVTEST000000001",
            "simplifiedProcedure": False,
            "incidentFlag": False,
            "arrivalNotificationDateTime": timestamp,
            "customsOfficeOfDestination": {
                "referenceNumber": "BE000600"
            },
            "traderAtDestination": {
                "name": "TEST COMPANY",
                "phoneNumber": "",
                "identificationNumber": "",
                "emailAddress": "",
                "references": {
                    "internal": "TEST123456789"
                },
                "communicationLanguageAtDestination": "EN"
            },
            "authorisation": []
        },
        "master": {
            "locationOfGoods": {
                "typeOfLocation": "B",
                "qualifierOfIdentification": "U",
                "authorisationNumber": "",
                "additionalIdentifier": "",
                "unlocode": "BEANR",
                "internalReference": "TEST123456789"
            }
        },
        "integration": {
            "language": "EN",
            "sendingMode": "BATCH",
            "templateCode": "ARRIVAL_NCTS",
            "printGroup": "DEFAULT",
            "createDeclaration": True,
            "autoSendDeclaration": True,
            "simplifiedProcedure": False,
            "consolidateBeforeSending": False,
            "relationGroup": "DKM",
            "commercialReference": "TEST123456789",
            "procedureType": "NCTS",
            "declarationCreatedBy": "DKM_ARRIVAL_TEST",
            "variableFields": [],
            "attachment": [],
            "control": {
                "packages": 0,
                "grossmass": 0.0,
                "netmass": 0.0
            },
            "principal": {
                "references": {
                    "internal": "TEST123456789"
                },
                "contactPerson": {
                    "name": "TEST COMPANY",
                    "references": {
                        "internal": "TEST123456789"
                    }
                },
                "sendMail": False,
                "contactPersonExportConfirmation": {
                    "name": "TEST COMPANY"
                },
                "sendMailExportConfirmation": False
            },
            "externalReferences": {
                "LinkIdErp1": None,
                "LinkIdErp2": None,
                "LinkIdErp3": None,
                "LinkIdErp4": None,
                "LinkIdErp5": None
            }
        }
    }
    
    print_success("Dummy NCTS data created")
    print_info(f"LRN: {dummy_data['declaration']['lrn']}")
    print_info(f"MRN: {dummy_data['declaration']['mrn']}")
    print_info(f"Timestamp: {timestamp}")
    
    # Pretty print the JSON
    print("\n" + "-"*60)
    print("DUMMY DATA (JSON):")
    print("-"*60)
    print(json.dumps(dummy_data, indent=2))
    
    return dummy_data


# ============================================
# TEST 3: Send to Database API
# ============================================

def test_send_to_database(access_token, ncts_data):
    """Test sending arrival data to database API"""
    print_header("TEST 3: Sending Data to Database")
    
    if not access_token:
        print_error("No access token available. Skipping database test.")
        return False
    
    try:
        print_info("Preparing API request...")
        print_info(f"API Endpoint: {API_ENDPOINT}")
        print_info(f"Tenant ID: {TENANT_ID}")
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
            "x-Tenant-Id": TENANT_ID
        }
        
        print_info("Sending PUT request...")
        
        # Send request
        response = requests.put(
            API_ENDPOINT,
            json=ncts_data,
            headers=headers,
            timeout=30
        )
        
        print_info(f"Response Status Code: {response.status_code}")
        
        # Check response
        if response.status_code in [200, 201]:
            print_success("Data sent successfully!")
            
            try:
                response_data = response.json()
                print("\n" + "-"*60)
                print("API RESPONSE:")
                print("-"*60)
                print(json.dumps(response_data, indent=2))
                
                # Extract submission ID if available
                submission_id = response_data.get("id") or response_data.get("lrn")
                if submission_id:
                    print_success(f"Submission ID: {submission_id}")
                    
            except:
                print_info("Response body:")
                print(response.text)
                
            return True
            
        elif response.status_code == 400:
            print_error("Bad Request (400) - Invalid data format")
            print_info("Response:")
            print(response.text)
            return False
            
        elif response.status_code == 401:
            print_error("Unauthorized (401) - Authentication failed")
            print_info("Check if token is still valid")
            print_info("Response:")
            print(response.text)
            return False
            
        elif response.status_code == 500:
            print_error("Internal Server Error (500) - Database error")
            print_info("Response:")
            print(response.text)
            return False
            
        else:
            print_error(f"Unexpected status code: {response.status_code}")
            print_info("Response:")
            print(response.text)
            return False
            
    except requests.exceptions.Timeout:
        print_error("Request timed out! Check if the API server is accessible.")
        return False
    except requests.exceptions.ConnectionError:
        print_error("Connection error! Check your network and the API URL.")
        return False
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        return False


# ============================================
# MAIN TEST RUNNER
# ============================================

def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("  DgArrivalProcessor - Standalone Connection Test")
    print("  No pytest required - Direct testing")
    print("="*60)
    
    # Track results
    results = {
        "oauth": False,
        "dummy_data": False,
        "database": False
    }
    
    # Test 1: OAuth Authentication
    access_token = test_oauth_authentication()
    results["oauth"] = (access_token is not None)
    
    if not access_token:
        print_error("\nCannot proceed without authentication token.")
        print_info("Please check:")
        print_info("  1. Network connectivity")
        print_info("  2. OAuth server URL is correct")
        print_info("  3. Client credentials are correct")
        print_summary(results)
        return
    
    # Test 2: Create Dummy Data
    dummy_data = create_dummy_ncts_data()
    results["dummy_data"] = (dummy_data is not None)
    
    # Test 3: Send to Database
    results["database"] = test_send_to_database(access_token, dummy_data)
    
    # Print summary
    print_summary(results)


def print_summary(results):
    """Print test summary"""
    print_header("TEST SUMMARY")
    
    print(f"\n1. OAuth Authentication:    {'✅ PASS' if results['oauth'] else '❌ FAIL'}")
    print(f"2. Dummy Data Creation:     {'✅ PASS' if results['dummy_data'] else '❌ FAIL'}")
    print(f"3. Database Submission:     {'✅ PASS' if results['database'] else '❌ FAIL'}")
    
    total = sum(results.values())
    print(f"\nTotal: {total}/3 tests passed")
    
    if total == 3:
        print("\n" + "🎉 "*20)
        print("ALL TESTS PASSED! Connection to database is working!")
        print("🎉 "*20)
    else:
        print("\n⚠️  Some tests failed. Please review the errors above.")


# ============================================
# RUN TESTS
# ============================================

if __name__ == "__main__":
    main()