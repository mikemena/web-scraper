# web-scraper

1 - Activate the virtual enviornment by sourcing the activate script in its bin directory

    source myenv/bin/activate

2 - To deactivate the virtual enviornment, just type 'deactivate':

    deactivate

# Florida Healthcare Facility Scraper

A high-performance, pure Python scraper for extracting healthcare facility data from Florida's Department of Health website.

## Features

- **Pure Python Implementation**: No JavaScript dependencies or browser automation
- **Fast Performance**: Direct data extraction without headless browsers
- **Complete Data Extraction**: All facility details including licensing, addresses, and ownership
- **Multiple Export Formats**: CSV and JSON output options
- **Command Line Interface**: Easy integration into automated workflows

## Installation

1. Install Python 3.7 or higher
2. Install required dependencies:
   ```bash
   pip install requests beautifulsoup4
   ```

## Usage

### Basic Usage
```bash
python simple_scraper.py "CAPE CANAVERAL HOSPITAL"
```
This will automatically export results to a timestamped CSV file.

### Export to Specific Files
```bash
# Export to CSV
python simple_scraper.py "CAPE CANAVERAL HOSPITAL" --csv results.csv

# Export to JSON
python simple_scraper.py "CAPE CANAVERAL HOSPITAL" --json results.json

# Export to both formats
python simple_scraper.py "CAPE CANAVERAL HOSPITAL" --csv data.csv --json data.json
```

### Programmatic Usage
```python
from simple_scraper import SimpleScraper

scraper = SimpleScraper()
facilities = scraper.scrape_facility("CAPE CANAVERAL HOSPITAL")

# Export to CSV
scraper.export_to_csv(facilities, "output.csv")

# Export to JSON
scraper.export_to_json(facilities, "output.json")
```

## Output Format

The scraper extracts comprehensive facility information including:

- **Basic Information**: Name, facility type, AHCA number, phone, licensed beds
- **License Details**: License ID, number, status, effective/expiration dates
- **Address Information**: Complete street and mailing addresses
- **Ownership Data**: Owner information, profit status, administrator/CEO
- **Additional Details**: Web addresses when available

## CSV Output Columns

The CSV output includes all standard Florida health facility data fields:
- Name, Facility Type, AHCA Number, Phone Number, Licensed Beds
- License ID, License Number, License Status, License Dates
- Street Address, City, State, ZIP, County (both street and mailing)
- Owner, Owner Since, Profit Status, Web Address, Admin/CEO

## Requirements

- Python 3.7+
- requests
- beautifulsoup4

## Data Source

This scraper extracts data from Florida's official Health Finder website:
https://quality.healthfinder.fl.gov/Facility-Search/FacilityLocateSearch

## License

This project is for educational and research purposes. Please respect the terms of use of the Florida Department of Health website.
