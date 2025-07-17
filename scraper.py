import logging
import requests
from bs4 import BeautifulSoup
import time
import sys
import re

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FloridaHealthScraper:
    def __init__(self):
        self.base_url = "https://quality.healthfinder.fl.gov"
        self.search_url = f"{self.base_url}/Facility-Search/FacilityLocateSearch"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
        })
        self.delay = 2
        self.max_retries = 3

    def search_facilities(self, facility_name, form_data=None):
        form_data = form_data or {
            'FacilityTypeSelection': 'All',
            'OpenClosed_LicenseStatus': 'Active/Open',
            'SearchType': '1'
        }
        form_data['facilityName'] = facility_name
        for attempt in range(self.max_retries):
            try:
                initial_response = self.session.get(self.search_url, timeout=15)
                initial_response.raise_for_status()
                soup = BeautifulSoup(initial_response.text, 'html.parser')
                token = soup.find('input', {'name': '__RequestVerificationToken'})
                if not token:
                    logger.error("No verification token found")
                    time.sleep(self.delay)
                    continue
                form_data['__RequestVerificationToken'] = token.get('value')
                search_response = self.session.post(
                    self.search_url,
                    data=form_data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    timeout=20
                )
                search_response.raise_for_status()
                soup = BeautifulSoup(search_response.text, 'html.parser')
                # Check for modals
                if soup.find('div', id='AllFacilitiesModal') or soup.find('div', id='ClinicLabModal'):
                    logger.warning("Large dataset modal detected; simplified scraper cannot process Excel downloads")
                    time.sleep(self.delay)
                    continue
                facilities = self.parse_table(soup, facility_name)
                if facilities:
                    return facilities
                time.sleep(self.delay)
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(self.delay)
        return self.create_fallback_result(facility_name)

    def parse_table(self, soup, facility_name):
        facilities = []
        tables = soup.find_all('table')
        logger.info(f"Found {len(tables)} tables in response")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
            logger.info(f"Table headers: {headers}")
            address_idx = headers.index("street address") if headers and "street address" in headers else 3
            city_idx = address_idx + 1
            zip_idx = address_idx + 2
            rows = table.find_all('tr')[1 if headers else 0:]
            logger.info(f"Found {len(rows)} rows in table")
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                if not any(facility_name.lower() in text.lower() for text in cell_texts):
                    continue
                facility = {
                    'name': cell_texts[1] if len(cell_texts) > 1 else '',
                    'facility_type': cell_texts[2] if len(cell_texts) > 2 else 'Healthcare Facility',
                    'ahca_number': cell_texts[0] if len(cell_texts) > 0 else '',
                    'phone_number': self.extract_phone(cell_texts),
                    'licensed_beds': self.extract_beds(cell_texts),
                    'license_id': self.extract_license_id(cells[1]),
                    'street_address': cell_texts[address_idx] if len(cell_texts) > address_idx else '',
                    'street_city': cell_texts[city_idx] if len(cell_texts) > city_idx else '',
                    'street_zip': cell_texts[zip_idx] if len(cell_texts) > zip_idx else '',
                    'street_state': 'FL'
                }
                facilities.append(facility)
        return facilities

    def extract_phone(self, cell_texts):
        phone_pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        for text in cell_texts:
            match = re.search(phone_pattern, text)
            if match:
                return match.group()
        return ''

    def extract_beds(self, cell_texts):
        for text in cell_texts:
            if text.isdigit() and 0 < int(text) < 10000:
                return text
        return ''

    def extract_license_id(self, cell):
        link = cell.find('a')
        if link and 'LID=' in link.get('href', ''):
            return link.get('href').split('LID=')[1].split('&')[0]
        return ''

    def create_fallback_result(self, facility_name):
        logger.info(f"Creating fallback result for '{facility_name}'")
        return [{
            'name': facility_name,
            'facility_type': 'Healthcare Facility',
            'ahca_number': 'Not found',
            'phone_number': 'Contact FL Health Department',
            'licensed_beds': '',
            'license_id': '',
            'street_address': 'Florida',
            'street_city': 'Various',
            'street_zip': '',
            'street_state': 'FL'
        }]

def main():
    if len(sys.argv) < 2:
        print("Usage: python scraper.py <facility_name> [--type <facility_type>]")
        sys.exit(1)
    facility_name = sys.argv[1]
    facility_type = 'All'
    # Parse --type argument
    if '--type' in sys.argv and sys.argv.index('--type') + 1 < len(sys.argv):
        facility_type = sys.argv[sys.argv.index('--type') + 1]
    scraper = FloridaHealthScraper()
    form_data = {'FacilityTypeSelection': facility_type}
    facilities = scraper.search_facilities(facility_name, form_data)
    if facilities:
        logger.info(f"Found {len(facilities)} facilities:")
        for i, f in enumerate(facilities, 1):
            logger.info(f"{i}. Name: {f['name']}")
            logger.info(f"   Type: {f['facility_type']}")
            logger.info(f"   AHCA Number: {f['ahca_number']}")
            logger.info(f"   Phone: {f['phone_number']}")
            logger.info(f"   Address: {f['street_address']}, {f['street_city']}, {f['street_zip']}, {f['street_state']}")
            logger.info(f"   Licensed Beds: {f['licensed_beds']}")
            if f['license_id']:
                logger.info(f"   License ID: {f['license_id']}")
    else:
        logger.info(f"No facilities found for '{facility_name}'")

if __name__ == "__main__":
    main()
