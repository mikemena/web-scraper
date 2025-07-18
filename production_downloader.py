#!/usr/bin/env python3
"""
Direct URL approach - skip navigation, go straight to search form
https://quality.healthfinder.fl.gov/Facility-Search/FacilityLocateSearch
"""

import time
import logging
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f'direct_url_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(),
    ],
)


class DirectURLDownloader:
    def __init__(self, download_dir="./downloads"):
        self.download_dir = os.path.abspath(download_dir)
        self.search_url = (
            "https://quality.healthfinder.fl.gov/Facility-Search/FacilityLocateSearch"
        )
        self.driver = None
        self.wait = None

        os.makedirs(download_dir, exist_ok=True)
        logging.info(f"Download directory: {self.download_dir}")

    def setup_driver(self):
        """Set up Chrome driver with download preferences"""
        try:
            chrome_options = Options()

            # Configure download preferences
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
            }
            chrome_options.add_experimental_option("prefs", prefs)

            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 30)

            logging.info("✅ Chrome driver initialized")
            return True

        except Exception as e:
            logging.error(f"❌ Failed to initialize driver: {str(e)}")
            return False

    def navigate_directly_to_search(self):
        """Navigate directly to the search form URL"""
        try:
            logging.info(f"🎯 Navigating directly to: {self.search_url}")
            self.driver.get(self.search_url)

            # Wait for page to load
            time.sleep(2)

            # Check if we're on the right page
            page_title = self.driver.title
            current_url = self.driver.current_url
            logging.info(f"Page title: {page_title}")
            logging.info(f"Current URL: {current_url}")

            # Look for search form elements
            form_elements = self.driver.find_elements(By.TAG_NAME, "form")
            select_elements = self.driver.find_elements(By.TAG_NAME, "select")

            logging.info(
                f"Found {len(form_elements)} forms and {len(select_elements)} select elements"
            )

            if len(select_elements) > 0:
                logging.info("✅ Successfully loaded search form page")
                return True
            else:
                logging.warning("⚠️ Page loaded but no form elements found")
                return True  # Continue anyway

        except Exception as e:
            logging.error(f"❌ Failed to navigate to search page: {str(e)}")
            return False

    def analyze_search_form(self):
        """Analyze the search form to understand available options"""
        try:
            logging.info("🔍 Analyzing search form structure...")

            # Find all select elements
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            logging.info(f"Found {len(selects)} dropdown elements:")

            for i, select in enumerate(selects):
                try:
                    name = select.get_attribute("name")
                    id_attr = select.get_attribute("id")

                    logging.info(f"\n  Dropdown {i}: name='{name}', id='{id_attr}'")

                    # Get options
                    select_obj = Select(select)
                    options = select_obj.options

                    logging.info(f"    {len(options)} options:")
                    for j, option in enumerate(options):
                        value = option.get_attribute("value")
                        text = option.text.strip()
                        logging.info(f"      {j}: '{text}' (value: '{value}')")

                        # Look for ASC-related options
                        if any(
                            keyword in text.upper()
                            for keyword in ["AMBULATORY", "SURGERY", "ASC"]
                        ):
                            logging.info(f"        🎯 ASC-RELATED OPTION FOUND!")

                except Exception as e:
                    logging.debug(f"Error analyzing select {i}: {str(e)}")

            return True

        except Exception as e:
            logging.error(f"❌ Error analyzing form: {str(e)}")
            return False

    def configure_asc_search(self):
        """Configure search for Ambulatory Surgery Centers"""
        try:
            logging.info("⚙️ Configuring search for ASC...")

            # Find facility type dropdown
            facility_selectors = [
                "FacilityTypeSelection",
                "facilityType",
                "FacilityType",
            ]

            facility_element = None
            for selector in facility_selectors:
                try:
                    facility_element = self.driver.find_element(By.NAME, selector)
                    logging.info(f"✅ Found facility dropdown by name: {selector}")
                    break
                except:
                    try:
                        facility_element = self.driver.find_element(By.ID, selector)
                        logging.info(f"✅ Found facility dropdown by id: {selector}")
                        break
                    except:
                        continue

            if not facility_element:
                # Try broader search
                all_selects = self.driver.find_elements(By.TAG_NAME, "select")
                for select in all_selects:
                    select_obj = Select(select)
                    options_text = [opt.text for opt in select_obj.options]
                    if any(
                        "Hospital" in text or "Surgery" in text for text in options_text
                    ):
                        facility_element = select
                        logging.info("✅ Found facility dropdown by content analysis")
                        break

            if facility_element:
                select_obj = Select(facility_element)

                # Show available options
                logging.info("Available facility type options:")
                for option in select_obj.options:
                    value = option.get_attribute("value")
                    text = option.text
                    logging.info(f"  '{text}' (value: '{value}')")

                # Try to select ASC option
                asc_options = [
                    "Ambulatory Surgery Centers",
                    "Ambulatory Surgery Center",
                    "ASC",
                    "Surgery Centers",
                    "Surgery Center",
                ]

                selected = False
                for asc_option in asc_options:
                    try:
                        select_obj.select_by_visible_text(asc_option)
                        logging.info(f"✅ Selected facility type: {asc_option}")
                        selected = True
                        break
                    except:
                        try:
                            select_obj.select_by_value(asc_option)
                            logging.info(
                                f"✅ Selected facility type by value: {asc_option}"
                            )
                            selected = True
                            break
                        except:
                            continue

                if not selected:
                    logging.warning(
                        "Could not find exact ASC option, checking all options..."
                    )
                    for option in select_obj.options:
                        option_text = option.text.upper()
                        if "AMBULATORY" in option_text or "SURGERY" in option_text:
                            try:
                                select_obj.select_by_visible_text(option.text)
                                logging.info(
                                    f"✅ Selected closest match: {option.text}"
                                )
                                selected = True
                                break
                            except:
                                continue

                if not selected:
                    logging.warning(
                        "No ASC option found, selecting first non-empty option"
                    )
                    select_obj.select_by_index(1)

            else:
                logging.error("❌ Could not find facility type dropdown")
                return False

            # Configure license status
            license_selectors = [
                "OpenClosed_LicenseStatus",
                "licenseStatus",
                "LicenseStatus",
            ]

            license_element = None
            for selector in license_selectors:
                try:
                    license_element = self.driver.find_element(By.NAME, selector)
                    break
                except:
                    try:
                        license_element = self.driver.find_element(By.ID, selector)
                        break
                    except:
                        continue

            if license_element:
                try:
                    select_obj = Select(license_element)

                    logging.info("Available license status options:")
                    for option in select_obj.options:
                        value = option.get_attribute("value")
                        text = option.text
                        logging.info(f"  '{text}' (value: '{value}')")

                    # Try to select Active/Open
                    try:
                        select_obj.select_by_visible_text("Active/Open")
                        logging.info("✅ Selected license status: Active/Open")
                    except:
                        try:
                            select_obj.select_by_value("Active/Open")
                            logging.info(
                                "✅ Selected license status by value: Active/Open"
                            )
                        except:
                            logging.warning("Could not select Active/Open status")

                except Exception as e:
                    logging.warning(f"Error configuring license status: {str(e)}")
            else:
                logging.warning("License status dropdown not found")

            return True

        except Exception as e:
            logging.error(f"❌ Error configuring search: {str(e)}")
            return False

    def execute_search(self):
        """Execute the search"""
        try:
            logging.info("🔍 Executing search...")

            # Find search button
            search_strategies = [
                (By.XPATH, "//button[contains(text(), 'Search')]"),
                (By.XPATH, "//input[@type='submit']"),
                (By.XPATH, "//button[@type='submit']"),
                (By.XPATH, "//input[contains(@value, 'Search')]"),
                (By.ID, "search"),
                (By.NAME, "search"),
            ]

            search_button = None
            for method, selector in search_strategies:
                try:
                    search_button = self.driver.find_element(method, selector)
                    if search_button.is_displayed() and search_button.is_enabled():
                        logging.info(f"✅ Found search button: {selector}")
                        break
                except:
                    continue

            if not search_button:
                logging.error("❌ Could not find search button")
                return False

            # Click search button
            try:
                search_button.click()
                logging.info("✅ Search button clicked")
            except Exception as e:
                logging.info("Regular click failed, trying JavaScript...")
                self.driver.execute_script("arguments[0].click();", search_button)
                logging.info("✅ Search executed with JavaScript")

            # Wait for results
            logging.info("⏳ Waiting for search results...")
            time.sleep(2)

            return True

        except Exception as e:
            logging.error(f"❌ Error executing search: {str(e)}")
            return False

    def find_and_trigger_download(self):
        """Find and trigger download/export functionality"""
        try:
            logging.info("📥 Looking for download/export options...")

            # Wait a bit more for any dynamic content to load
            time.sleep(2)

            # Look for export/download buttons
            download_strategies = [
                (By.XPATH, "//button[contains(text(), 'Export')]"),
                (By.XPATH, "//a[contains(text(), 'Export')]"),
                (By.XPATH, "//button[contains(text(), 'Download')]"),
                (By.XPATH, "//a[contains(text(), 'Download')]"),
                (By.XPATH, "//*[contains(text(), 'Export Facility Data')]"),
                (By.XPATH, "//*[contains(text(), 'XLSX')]"),
                (By.XPATH, "//*[contains(text(), 'CSV')]"),
                (By.XPATH, "//*[contains(text(), 'Excel')]"),
                (By.XPATH, "//button[contains(@class, 'export')]"),
                (By.XPATH, "//a[contains(@class, 'export')]"),
            ]

            download_elements = []
            for method, selector in download_strategies:
                try:
                    elements = self.driver.find_elements(method, selector)
                    for element in elements:
                        if element.is_displayed():
                            text = element.text.strip()
                            tag = element.tag_name
                            download_elements.append((element, text, tag, selector))
                except:
                    continue

            logging.info(f"Found {len(download_elements)} potential download elements:")
            for element, text, tag, selector in download_elements:
                logging.info(f"  <{tag}> '{text}' - {selector}")

            # Try clicking download elements
            for element, text, tag, selector in download_elements:
                if any(
                    keyword in text.upper()
                    for keyword in ["EXPORT", "DOWNLOAD", "XLSX", "CSV", "EXCEL"]
                ):
                    try:
                        logging.info(f"🎯 Trying to click: '{text}'")
                        element.click()
                        time.sleep(3)

                        # Check if a dropdown appeared
                        time.sleep(2)

                        # Look for XLSX/Excel option in dropdown
                        xlsx_options = self.driver.find_elements(
                            By.XPATH,
                            "//*[contains(text(), 'XLSX') or contains(text(), 'Excel') or contains(text(), 'xlsx')]",
                        )
                        for xlsx_option in xlsx_options:
                            if xlsx_option.is_displayed():
                                try:
                                    xlsx_option.click()
                                    logging.info("✅ Clicked XLSX download option")
                                    return True
                                except:
                                    pass

                        # If no dropdown, the download might have started
                        logging.info("✅ Download may have been triggered")
                        return True

                    except Exception as e:
                        logging.debug(f"Failed to click {text}: {str(e)}")
                        continue

            logging.warning("⚠️ No working download buttons found")
            return False

        except Exception as e:
            logging.error(f"❌ Error finding download options: {str(e)}")
            return False

    def wait_for_download(self, timeout=60):
        """Wait for download to complete"""
        try:
            logging.info("⏳ Waiting for download to complete...")

            start_time = time.time()
            initial_files = set(os.listdir(self.download_dir))

            while time.time() - start_time < timeout:
                current_files = set(os.listdir(self.download_dir))
                new_files = current_files - initial_files

                if new_files:
                    complete_files = [
                        f
                        for f in new_files
                        if not f.endswith((".crdownload", ".tmp", ".part"))
                    ]

                    if complete_files:
                        logging.info(f"✅ Download completed: {complete_files}")
                        return complete_files

                time.sleep(2)

            logging.warning(f"⏰ Download timeout after {timeout} seconds")
            return []

        except Exception as e:
            logging.error(f"❌ Error waiting for download: {str(e)}")
            return []

    def main_process(self):
        """Main process using direct URL approach"""
        try:
            # Setup driver
            if not self.setup_driver():
                return False

            # Navigate directly to search form
            if not self.navigate_directly_to_search():
                return False

            # Analyze form structure
            self.analyze_search_form()

            # Configure search for ASC
            if not self.configure_asc_search():
                return False

            # Execute search
            if not self.execute_search():
                return False

            # Find and trigger download
            if self.find_and_trigger_download():
                # Wait for download
                downloaded_files = self.wait_for_download()

                if downloaded_files:
                    logging.info("✅ Files downloaded successfully!")
                    for file in downloaded_files:
                        file_path = os.path.join(self.download_dir, file)
                        if os.path.exists(file_path):
                            file_size = os.path.getsize(file_path)
                            logging.info(f"  📄 {file} ({file_size:,} bytes)")

                            # Check if it's Excel and count rows
                            if file.endswith(".xlsx"):
                                try:
                                    import pandas as pd

                                    df = pd.read_excel(file_path)
                                    logging.info(
                                        f"      📊 Rows: {len(df)}, Columns: {len(df.columns)}"
                                    )
                                    if len(df) == 539:
                                        logging.info(
                                            "      🎯 PERFECT! Got exactly 539 entries!"
                                        )
                                    else:
                                        logging.info(
                                            f"      📈 Got {len(df)} entries (expected 539)"
                                        )
                                except:
                                    pass
                else:
                    logging.warning("⚠️ Download triggered but no files received")

            # Keep browser open for manual inspection
            logging.info(
                "🔍 Keeping browser open for 30 seconds for manual inspection..."
            )
            time.sleep(30)

            return True

        except Exception as e:
            logging.error(f"❌ Main process failed: {str(e)}")
            return False

        finally:
            if self.driver:
                logging.info("🔚 Closing browser...")
                self.driver.quit()


def main():
    """Main execution"""

    logging.info("=" * 70)
    logging.info("🎯 DIRECT URL APPROACH - SKIP NAVIGATION!")
    logging.info("Target: 539 Ambulatory Surgery Centers (Active/Open)")
    logging.info(
        "URL: https://quality.healthfinder.fl.gov/Facility-Search/FacilityLocateSearch"
    )
    logging.info("=" * 70)

    downloader = DirectURLDownloader()

    try:
        success = downloader.main_process()

        if success:
            logging.info("✅ Direct URL process completed successfully!")
        else:
            logging.error("❌ Direct URL process failed")

    except Exception as e:
        logging.error(f"❌ Main execution error: {str(e)}")

    logging.info("=" * 70)


if __name__ == "__main__":
    main()
