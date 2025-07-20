import requests
import json
import re
from bs4 import BeautifulSoup


def get_facility_ids(facility_code):
    """
    Scrape facility data and extract LicenseID and LinkId arrays

    Args:
        facility_code (str): The facility type code (e.g., 'Abortion', 'Hospital')

    Returns:
        dict: Dictionary containing license_ids and link_ids arrays
    """
    url = (
        f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_code}?&type=1"
    )

    try:
        # Make the request
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Parse the HTML
        soup = BeautifulSoup(response.content, "html.parser")

        # Method 1: Extract using regex (most reliable for JavaScript variables)
        script_content = response.text

        # Look for the data variable assignment
        # This regex looks for: const data = [...] or var data = [...]
        data_match = re.search(
            r"(?:const|var|let)\s+data\s*=\s*(\[.*?\]);", script_content, re.DOTALL
        )

        if data_match:
            data_str = data_match.group(1)
            # Parse the JavaScript array as JSON
            data = json.loads(data_str)

            # Extract IDs using Method 2 (filtered list comprehension)
            return extract_ids_filtered(data)
        else:
            print(f"Could not find data variable in the response for {facility_code}")
            return {"license_ids": [], "link_ids": []}

    except requests.RequestException as e:
        print(f"Request failed for {facility_code}: {e}")
        return {"license_ids": [], "link_ids": []}
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON data for {facility_code}: {e}")
        return {"license_ids": [], "link_ids": []}
    except Exception as e:
        print(f"Unexpected error for {facility_code}: {e}")
        return {"license_ids": [], "link_ids": []}


def extract_ids_filtered(data):
    """
    Extract LicenseID and LinkId from facility data using list comprehension

    Args:
        data (list): The data array from the JavaScript variable

    Returns:
        dict: Dictionary with license_ids and link_ids arrays
    """
    if not data or len(data) == 0:
        return {"license_ids": [], "link_ids": []}

    facilities = data[0] if isinstance(data[0], list) else data

    license_ids = [
        facility["LicenseID"]
        for facility in facilities
        if facility.get("LicenseID") and facility["LicenseID"].strip()
    ]
    link_ids = [
        facility["LinkId"]
        for facility in facilities
        if facility.get("LinkId") and facility["LinkId"].strip()
    ]

    return {"license_ids": license_ids, "link_ids": link_ids}


def export_facility_data(facility_code, license_ids, link_ids, session=None):
    """
    Export facility data using the extracted IDs

    Args:
        facility_code (str): The facility type code
        license_ids (list): List of license IDs
        link_ids (list): List of link IDs
        session (requests.Session): Optional session to maintain cookies

    Returns:
        requests.Response: The response from the export endpoint
    """
    if session is None:
        session = requests.Session()

    # First, get the page to extract the RequestVerificationToken
    url = (
        f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_code}?&type=1"
    )

    try:
        # Get the initial page
        response = session.get(url)
        response.raise_for_status()

        # Extract the RequestVerificationToken
        soup = BeautifulSoup(response.content, "html.parser")
        token_input = soup.find("input", {"name": "__RequestVerificationToken"})

        if not token_input:
            # Try to find it in a script tag or hidden form
            token_match = re.search(
                r'__RequestVerificationToken["\']?\s*:\s*["\']([^"\']+)["\']',
                response.text,
            )
            if token_match:
                verification_token = token_match.group(1)
            else:
                print("Could not find RequestVerificationToken")
                return None
        else:
            verification_token = token_input.get("value")

        # Prepare the export URL
        export_url = f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_code}?handler=Export"

        # Join the IDs with commas and URL encode them
        license_ids_str = ",".join(str(id) for id in license_ids if id)
        link_ids_str = ",".join(str(id) for id in link_ids if id)

        # Prepare form data
        form_data = {
            "LicenseID": license_ids_str,
            "LinkID": link_ids_str,
            "__RequestVerificationToken": verification_token,
        }

        # Set headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": url,
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        }

        # Make the POST request
        export_response = session.post(export_url, data=form_data, headers=headers)
        export_response.raise_for_status()

        return export_response

    except Exception as e:
        print(f"Export failed for {facility_code}: {e}")
        return None


def get_and_export_facility_data(facility_code, export_filename=None):
    """
    Complete workflow: scrape facility data and export it

    Args:
        facility_code (str): The facility type code
        export_filename (str): Optional filename to save the export

    Returns:
        tuple: (ids_data, export_response)
    """
    # Create a session to maintain cookies
    session = requests.Session()

    print(f"Scraping {facility_code} facility data...")

    # Get the facility IDs
    ids_data = get_facility_ids_with_session(facility_code, session)

    if not ids_data["license_ids"] and not ids_data["link_ids"]:
        print(f"No data found for {facility_code}")
        return ids_data, None

    print(
        f"Found {len(ids_data['license_ids'])} License IDs and {len(ids_data['link_ids'])} Link IDs"
    )

    # Export the data
    print(f"Exporting {facility_code} data...")
    export_response = export_facility_data(
        facility_code, ids_data["license_ids"], ids_data["link_ids"], session
    )

    if export_response and export_response.status_code == 200:
        print(f"Export successful for {facility_code}")

        # Save to file if filename provided
        if export_filename:
            with open(export_filename, "wb") as f:
                f.write(export_response.content)
            print(f"Data saved to {export_filename}")
    else:
        print(f"Export failed for {facility_code}")

    return ids_data, export_response


def get_facility_ids_with_session(facility_code, session):
    """
    Modified version of get_facility_ids that uses a session
    """
    url = (
        f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_code}?&type=1"
    )

    try:
        # Make the request using the session
        response = session.get(url)
        response.raise_for_status()

        # Extract using regex
        script_content = response.text
        data_match = re.search(
            r"(?:const|var|let)\s+data\s*=\s*(\[.*?\]);", script_content, re.DOTALL
        )

        if data_match:
            data_str = data_match.group(1)
            data = json.loads(data_str)
            return extract_ids_filtered(data)
        else:
            print(f"Could not find data variable in the response for {facility_code}")
            return {"license_ids": [], "link_ids": []}

    except Exception as e:
        print(f"Error scraping {facility_code}: {e}")
        return {"license_ids": [], "link_ids": []}


# Example usage
if __name__ == "__main__":
    # Method 1: Get IDs and export in one step
    print("=== Complete workflow: Scrape and Export ===")
    ids_data, export_response = get_and_export_facility_data(
        "Abortion", "abortion_facilities.csv"
    )

    if export_response:
        print(f"Export content type: {export_response.headers.get('content-type')}")
        print(f"Export content length: {len(export_response.content)} bytes")

    # Method 2: Manual step-by-step process
    print("\n=== Manual step-by-step process ===")
    session = requests.Session()

    # Step 1: Get the facility IDs
    facility_code = "Hospital"
    ids_data = get_facility_ids_with_session(facility_code, session)
    print(f"License IDs: {len(ids_data['license_ids'])}")
    print(f"Link IDs: {len(ids_data['link_ids'])}")

    # Step 2: Export the data
    if ids_data["license_ids"] or ids_data["link_ids"]:
        export_response = export_facility_data(
            facility_code, ids_data["license_ids"], ids_data["link_ids"], session
        )

        if export_response:
            # Save the exported data
            with open(f"{facility_code.lower()}_export.csv", "wb") as f:
                f.write(export_response.content)
            print(f"Exported {facility_code} data successfully")

    # Method 3: Bulk export multiple facility types
    print("\n=== Bulk export multiple facility types ===")
    facility_codes = ["Abortion", "Hospital", "ALF", "Nursing-Home"]

    for code in facility_codes:
        try:
            ids_data, export_response = get_and_export_facility_data(
                code, f"{code.lower()}.xlsx"
            )
            print(f"✓ Completed {code}")
        except Exception as e:
            print(f"✗ Failed {code}: {e}")
        print("-" * 50)
