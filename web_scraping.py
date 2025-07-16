import requests
from bs4 import BeautifulSoup
import time
import json
from urllib.parse import urljoin
import pandas as pd

class FloridaHealthFinderCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://quality.healthfinder.fl.gov"
        self.search_url = f"{self.base_url}/Facility-Search/FacilityLocateSearch"

        # Set headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def get_form_data(self):
        """Get the initial form page and extract necessary form data"""
        try:
            response = self.session.get(self.search_url)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the form
            form = soup.find('form', {'name': 'AdvancedSearch'})
            if not form:
                raise Exception("Could not find the search form")

            # Extract the verification token
            token_input = form.find('input', {'name': '__RequestVerificationToken'})
            verification_token = token_input['value'] if token_input else None

            return verification_token, soup

        except Exception as e:
            print(f"Error getting form data: {e}")
            return None, None

    def search_facility(self, facility_name="Cape Canaveral Hospital"):
        """Search for a facility by name"""
        verification_token, soup = self.get_form_data()

        if not verification_token:
            print("Could not retrieve verification token")
            return None

        # Prepare form data based on the HTML structure
        form_data = {
            '__RequestVerificationToken': verification_token,
            'FacilityTypeSelection': 'All',  # Search all facility types
            'OpenClosed_LicenseStatus': '',  # Active/Open (default)
            'facilityName': facility_name,
            'address': '',
            'city': '',
            'zip': '',
            'countySelection': '',  # All counties
            'filenumber': '',
            'AHCAFieldOffice': '',
            'licensenumber': '',
            'AffiliatedWith': '',
            'IsForProfit': '',
            'CurrentEmergencyActions': '',
            'BakerAct': '',
            'BakerAct_validate': '',
            'UrgentCare': '',
            'UrgentCare_validate': '',
            'crh': '',
            'CRH_validate': '',
            'LicenseStatus': ''
        }

        try:
            print(f"Searching for: {facility_name}")

            # Submit the search form
            response = self.session.post(
                f"{self.search_url}?handler=AdvancedSearch",
                data=form_data,
                allow_redirects=True
            )
            response.raise_for_status()

            return self.parse_results(response.content, facility_name)

        except Exception as e:
            print(f"Error during search: {e}")
            return None

    def parse_results(self, html_content, search_term):
        """Parse the search results page"""
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []

        try:
            # Look for results table or result containers
            # The exact structure may vary, so we'll look for common patterns

            # Check if we're on a results page or redirected to facility details
            page_title = soup.find('title')
            if page_title:
                print(f"Page title: {page_title.get_text().strip()}")

            # Look for facility information tables
            tables = soup.find_all('table')

            # Look for facility profile sections
            facility_sections = soup.find_all('section', id=lambda x: x and 'facility' in x.lower() if x else False)

            # Look for facility information in various containers
            facility_info = {}

            # Try to find facility name
            facility_name_elements = soup.find_all(['h1', 'h2', 'h3', 'h4'],
                                                  string=lambda text: search_term.lower() in text.lower() if text else False)

            if facility_name_elements:
                facility_info['name'] = facility_name_elements[0].get_text().strip()

            # Look for address information
            address_elements = soup.find_all(string=lambda text:
                text and any(keyword in text.lower() for keyword in ['address', 'street', 'location']) if text else False)

            # Look for license information
            license_elements = soup.find_all(string=lambda text:
                text and any(keyword in text.lower() for keyword in ['license', 'ahca', 'file number']) if text else False)

            # Parse tables for structured data
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = cells[0].get_text().strip()
                        value = cells[1].get_text().strip()
                        if key and value:
                            facility_info[key] = value

            # Look for specific data fields commonly found in facility profiles
            data_fields = [
                'AHCA File Number', 'License Number', 'License Status',
                'Address', 'City', 'County', 'Zip Code', 'Phone',
                'Administrator', 'Facility Type', 'Ownership Type'
            ]

            for field in data_fields:
                # Look for labels and corresponding values
                label_element = soup.find(string=lambda text:
                    text and field.lower() in text.lower() if text else False)

                if label_element:
                    # Try to find the corresponding value
                    parent = label_element.parent
                    if parent:
                        # Look for sibling elements or nearby text
                        siblings = parent.find_next_siblings()
                        for sibling in siblings[:2]:  # Check next 2 siblings
                            text = sibling.get_text().strip()
                            if text and text != field:
                                facility_info[field] = text
                                break

            # If we found any facility information, add it to results
            if facility_info:
                results.append(facility_info)

            # Also capture the raw HTML for manual inspection if needed
            facility_info['raw_html_sample'] = str(soup)[:1000] + "..." if len(str(soup)) > 1000 else str(soup)

            return results

        except Exception as e:
            print(f"Error parsing results: {e}")
            return [{'error': str(e), 'raw_html_sample': str(soup)[:500]}]

    def save_results(self, results, filename="cape_canaveral_hospital_data.json"):
        """Save results to a JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Results saved to {filename}")
        except Exception as e:
            print(f"Error saving results: {e}")

    def save_results_csv(self, results, filename="cape_canaveral_hospital_data.csv"):
        """Save results to a CSV file"""
        try:
            if results:
                # Flatten the data for CSV
                flattened_results = []
                for result in results:
                    flat_result = {}
                    for key, value in result.items():
                        if key != 'raw_html_sample':  # Skip raw HTML in CSV
                            flat_result[key] = value
                    flattened_results.append(flat_result)

                df = pd.DataFrame(flattened_results)
                df.to_csv(filename, index=False, encoding='utf-8')
                print(f"Results saved to {filename}")
        except Exception as e:
            print(f"Error saving CSV: {e}")

def main():
    """Main function to run the crawler"""
    crawler = FloridaHealthFinderCrawler()

    print("Florida Health Finder Crawler")
    print("=" * 40)

    # Search for Cape Canaveral Hospital
    results = crawler.search_facility("Cape Canaveral Hospital")

    if results:
        print(f"\nFound {len(results)} result(s):")
        print("-" * 40)

        for i, result in enumerate(results, 1):
            print(f"\nResult {i}:")
            for key, value in result.items():
                if key != 'raw_html_sample':  # Don't print raw HTML
                    print(f"  {key}: {value}")

        # Save results
        crawler.save_results(results)

        # Also save as CSV (requires pandas)
        try:
            crawler.save_results_csv(results)
        except ImportError:
            print("pandas not available - CSV export skipped")

    else:
        print("No results found or error occurred")

if __name__ == "__main__":
    main()
