#!/usr/bin/env python3
import requests
import re
import json
from datetime import datetime


def download_facility_data(facility_type="ASC", search_term=None):
    """
    Download facility data by extracting LicenseID and LinkID from the JavaScript data
    """
    session = requests.Session()

    # Set proper headers
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
        }
    )

    # Step 1: Load the facility search results page
    print(f"Loading {facility_type} search results page...")
    search_url = (
        f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_type}?type=1"
    )

    if search_term:
        # Add search parameters if provided
        search_url += f"&facilityName={search_term}"
        print(f"Searching for: {search_term}")

    response = session.get(search_url)

    if response.status_code != 200:
        print(f"Failed to load search page: {response.status_code}")
        return False

    print("✅ Loaded search results page")

    # Step 2: Extract verification token
    token_match = re.search(
        r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', response.text
    )
    if not token_match:
        print("Could not find verification token")
        return False

    token = token_match.group(1)
    print(f"Got verification token: {token[:20]}...")

    # Step 3: Extract LicenseID and LinkID from JavaScript data
    print("Extracting facility IDs from JavaScript data...")

    license_ids = []
    link_ids = []

    # Look for the allData JavaScript variable
    alldata_match = re.search(r"const allData = \[(.*?)\];", response.text, re.DOTALL)
    if alldata_match:
        print("Found allData JavaScript variable")
        try:
            # Extract the JavaScript array content
            js_content = alldata_match.group(1)

            # Find all LicenseID values
            license_matches = re.findall(r'"?LicenseID"?\s*:\s*"(\d+)"', js_content)
            license_ids.extend(license_matches)

            # Find all LinkId values (note: sometimes it's LinkId, sometimes LinkID)
            link_matches = re.findall(r'"?LinkId"?\s*:\s*"(\d+)"', js_content)
            link_ids.extend(link_matches)

            # Also try LinkID (uppercase)
            link_matches2 = re.findall(r'"?LinkID"?\s*:\s*"(\d+)"', js_content)
            link_ids.extend(link_matches2)

        except Exception as e:
            print(f"Error parsing JavaScript data: {e}")

    # Alternative: Look for hidden form fields directly
    if not license_ids:
        license_field_match = re.search(
            r'<input[^>]*name="LicenseID"[^>]*value="([^"]+)"', response.text
        )
        if license_field_match:
            license_ids = license_field_match.group(1).split(",")

    if not link_ids:
        link_field_match = re.search(
            r'<input[^>]*name="LinkID"[^>]*value="([^"]+)"', response.text
        )
        if link_field_match:
            link_ids = link_field_match.group(1).split(",")

    # Clean up the IDs
    license_ids = [lid.strip() for lid in license_ids if lid.strip()]
    link_ids = [lid.strip() for lid in link_ids if lid.strip()]

    print(f"Found {len(license_ids)} license IDs: {license_ids}")
    print(f"Found {len(link_ids)} link IDs: {link_ids}")

    if not license_ids or not link_ids:
        print("❌ Could not extract facility IDs from page")
        # Save page for debugging
        with open(f"debug_{facility_type}_page.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Saved page to debug_{facility_type}_page.html for inspection")
        return False

    # Step 4: Make the export request
    export_url = f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_type}?handler=Export"

    # Update headers for POST request
    session.headers.update(
        {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://quality.healthfinder.fl.gov",
            "Referer": search_url,
        }
    )

    # Prepare form data exactly like the browser
    post_data = {
        "__RequestVerificationToken": token,
        "LicenseID": ",".join(license_ids),
        "LinkID": ",".join(link_ids),
    }

    print(
        f"Submitting export request with {len(license_ids)} licenses and {len(link_ids)} links..."
    )

    response = session.post(export_url, data=post_data)

    print(f"Response status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
    print(f"Content-Length: {len(response.content):,} bytes")

    # Check if we got the Excel file
    content_type = response.headers.get("content-type", "").lower()
    content_disposition = response.headers.get("content-disposition", "")

    if (
        "spreadsheet" in content_type
        or "excel" in content_type
        or "attachment" in content_disposition
    ):

        # Save the Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        search_suffix = f"_{search_term}" if search_term else ""
        filename = f"{facility_type}_FacilityData{search_suffix}_{timestamp}.xlsx"

        with open(filename, "wb") as f:
            f.write(response.content)

        print(f"✅ Successfully downloaded: {filename}")

        # Try to validate the Excel file
        try:
            import pandas as pd

            df = pd.read_excel(filename)
            print(f"📊 Excel contains {len(df)} rows and {len(df.columns)} columns")
            if len(df.columns) > 0:
                print(f"📋 Sample columns: {list(df.columns)[:3]}...")
                if len(df) > 0:
                    print(
                        f"📄 Sample data: {df.iloc[0]['Name'] if 'Name' in df.columns else 'N/A'}"
                    )
        except Exception as e:
            print(f"⚠️ File saved but couldn't validate: {e}")

        return True

    else:
        # Save error response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = f"error_response_{facility_type}_{timestamp}.html"

        with open(error_file, "w", encoding="utf-8") as f:
            f.write(response.text)

        print(f"❌ Did not receive Excel file")
        print(f"Saved error response to: {error_file}")

        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Facility Data Downloader")
    print("=" * 60)

    # Download different facility types
    facilities = [
        ("ASC", None),  # All ASC facilities
        ("Hospice", None),  # All Hospice facilities
        ("Hospital", "CAPE"),  # Hospitals with "CAPE" in name
    ]

    for facility_type, search_term in facilities:
        print(f"\n{'='*40}")
        print(f"Downloading {facility_type} data...")
        print(f"{'='*40}")

        success = download_facility_data(facility_type, search_term)

        if success:
            print(f"✅ {facility_type} data downloaded successfully!")
        else:
            print(f"❌ {facility_type} download failed")

        print()  # Add spacing between downloads

    print("=" * 60)
    print("Download process completed!")
    print("=" * 60)
