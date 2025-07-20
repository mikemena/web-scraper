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


def get_facility_ids_selenium(facility_code):
    """
    Alternative method using Selenium (more reliable but slower)
    Uncomment and use if the regex method doesn't work
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    url = (
        f"https://quality.healthfinder.fl.gov/Facility-Provider/{facility_code}?&type=1"
    )

    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)

        # Wait for the page to load and execute JavaScript
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        # Execute JavaScript to get the data variable
        data = driver.execute_script(
            "return typeof data !== 'undefined' ? data : null;"
        )

        driver.quit()

        if data:
            return extract_ids_filtered(data)
        else:
            print(f"Data variable not found for {facility_code}")
            return {"license_ids": [], "link_ids": []}

    except Exception as e:
        print(f"Selenium error for {facility_code}: {e}")
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


def scrape_multiple_facilities(facility_codes):
    """
    Scrape multiple facility types and combine results

    Args:
        facility_codes (list): List of facility codes to scrape

    Returns:
        dict: Combined results for all facility types
    """
    all_results = {}

    for code in facility_codes:
        print(f"Scraping {code}...")
        result = get_facility_ids(code)
        all_results[code] = result
        print(
            f"Found {len(result['license_ids'])} License IDs and {len(result['link_ids'])} Link IDs"
        )

    return all_results


# Example usage
if __name__ == "__main__":
    # Test with a single facility type
    print("Testing with Abortion clinics:")
    result = get_facility_ids("Abortion")
    print(f"License IDs: {result['license_ids']}")
    print(f"Link IDs: {result['link_ids']}")
    print(f"Total License IDs: {len(result['license_ids'])}")
    print(f"Total Link IDs: {len(result['link_ids'])}")

    # Test with multiple facility types
    print("\nTesting with multiple facility types:")
    facility_codes = ["Abortion", "Hospital", "Nursing-Home"]
    results = scrape_multiple_facilities(facility_codes)

    for code, data in results.items():
        print(
            f"{code}: {len(data['license_ids'])} License IDs, {len(data['link_ids'])} Link IDs"
        )

# Requirements for this script:
# pip install requests beautifulsoup4
#
# For Selenium method (optional):
# pip install selenium
# You'll also need ChromeDriver installed
