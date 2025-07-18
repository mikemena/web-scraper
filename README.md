# web-scraper

1 - Activate the virtual enviornment by sourcing the activate script in its bin directory

    source myenv/bin/activate

2 - To deactivate the virtual enviornment, just type 'deactivate':

    deactivate


## Installation

1. Install Python 3.7 or higher
2. Install required dependencies:
   ```bash
   pip install requests beautifulsoup4
   ```

## Usage

### Basic Usage
```bash
python production_downloader.py
```
This will automatically export results to a excel file.

## Data Source

This scraper extracts data from Florida's official Health Finder website:
https://quality.healthfinder.fl.gov/Facility-Search/FacilityLocateSearch
