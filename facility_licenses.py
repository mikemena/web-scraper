import requests
import os
import json
import re
from bs4 import BeautifulSoup
import logging
import pandas as pd  # Added for _merge_excel_files

# Set up logging
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)


class FacilityLicenseManager:
    def __init__(self, api_url, output_dir="ahca_data"):
        self.api_url = api_url
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.merged_df = None

    def _get_facility_ids(self, facility_code):
        url = f"{self.api_url}/{facility_code}?&type=1"
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()
            script_content = response.text
            data_match = re.search(
                r"(?:(?:const|var)\s+data\s*=\s*\[.*?\]|\[.*?])\s*;",
                script_content,
                re.DOTALL,
            )
            if data_match:
                data = json.loads(data_match.group(1))
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
                logging.info(
                    f"Found {len(license_ids)} License IDs and {len(link_ids)} Link IDs for {facility_code}"
                )
                return {"license_ids": license_ids, "link_ids": link_ids}
            else:
                logging.warning(
                    f"Could not find data variable in the response for {facility_code}"
                )
                return {"license_ids": [], "link_ids": []}
        except requests.RequestException as e:
            logging.error(f"Request failed for {facility_code}: {e}")
            return {"license_ids": [], "link_ids": []}
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON data for {facility_code}: {e}")
            return {"license_ids": [], "link_ids": []}
        except Exception as e:
            logging.error(f"Unexpected error for {facility_code}: {e}")
            return {"license_ids": [], "link_ids": []}

    def _get_facility_ids_with_session(self, facility_code, session):
        # Note: This method appears unused. Consider removing if not needed.
        url = f"{self.api_url}/{facility_code}?&type=1"
        try:
            response = session.get(url, verify=False)
            response.raise_for_status()
            script_content = response.text
            data_match = re.search(
                r"(?:(?:const|var)\s+data\s*=\s*\[.*?\]|\[.*?])\s*;",
                script_content,
                re.DOTALL,
            )
            if data_match:
                data = json.loads(data_match.group(1))
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
                logging.info(
                    f"Found {len(license_ids)} License IDs and {len(link_ids)} Link IDs for {facility_code}"
                )
                return {"license_ids": license_ids, "link_ids": link_ids}
            else:
                logging.warning(
                    f"Could not find data variable in the response for {facility_code}"
                )
                return {"license_ids": [], "link_ids": []}
        except requests.RequestException as e:
            logging.error(f"Request failed for {facility_code}: {e}")
            return {"license_ids": [], "link_ids": []}
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON data for {facility_code}: {e}")
            return {"license_ids": [], "link_ids": []}
        except Exception as e:
            logging.error(f"Unexpected error for {facility_code}: {e}")
            return {"license_ids": [], "link_ids": []}

    def _export_facility_data(self, facility_code, license_ids, link_ids, session=None):
        if session is None:
            session = requests.Session()
        url = f"{self.api_url}/{facility_code}?&type=1"
        try:
            response = session.get(url, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            token_input = soup.find("input", {"name": "__RequestVerificationToken"})
            if not token_input:
                token_match = re.search(
                    r"__RequestVerificationToken\"\s*value\s*=\s*\"([^\"]+)\"",
                    response.text,
                )
                if token_match:
                    verification_token = token_match.group(1)
                else:
                    logging.warning("Could not find RequestVerificationToken")
                    return None
            else:
                verification_token = token_input.get("value")
            export_url = f"{self.api_url}/{facility_code}?handler=Export"
            license_ids_str = ",".join(str(id) for id in license_ids if id)
            link_ids_str = ",".join(str(id) for id in link_ids if id)
            form_data = {
                "LicenseID": license_ids_str,
                "LinkID": link_ids_str,
                "__RequestVerificationToken": verification_token,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": url,
            }
            export_response = session.post(
                export_url, data=form_data, headers=headers, verify=False
            )
            export_response.raise_for_status()
            export_filename = f"{facility_code.lower()}_facilities.xlsx"
            full_path = os.path.join(self.output_dir, export_filename)
            with open(full_path, "wb") as f:
                f.write(export_response.content)
            logging.info(f"Data saved to: {full_path}")
            return export_response
        except Exception as e:
            logging.error(f"Export failed for {facility_code}: {e}")
            return None

    def _get_and_export_facility_data(self, facility_code, export_filename=None):
        ids_data = self._get_facility_ids(facility_code)
        export_response = self._export_facility_data(
            facility_code, ids_data["license_ids"], ids_data["link_ids"]
        )
        if export_response and export_response.status_code == 200:
            logging.info(f"Export successful for {facility_code}")
            if export_filename:
                full_path = os.path.join(self.output_dir, export_filename)
                with open(full_path, "wb") as f:
                    f.write(export_response.content)
                logging.info(f"Data saved to: {full_path}")
            return ids_data, export_response
        else:
            logging.error(f"Export failed for {facility_code}")
            return ids_data, None

    def _merge_excel_files(self):
        excel_files = [
            f
            for f in os.listdir(self.output_dir)
            if f.lower().endswith((".xlsx", ".xls"))
        ]
        if not excel_files:
            logging.warning("No Excel files found in the directory")
            return None
        dfs = []
        for file in excel_files:
            file_path = os.path.join(self.output_dir, file)
            try:
                df = pd.read_excel(file_path)
                dfs.append(df)
            except Exception as e:
                logging.error(f"Failed to read {file}: {e}")
        if dfs:
            self.merged_df = pd.concat(dfs, ignore_index=True)
            logging.info(
                f"Merged {len(dfs)} Excel files into a DataFrame with {len(self.merged_df)} rows"
            )
            return self.merged_df
        logging.warning("No DataFrames to merge")
        return None

    def get_merged_data(self):
        """Public method to access the merged DataFrame for the pipeline."""
        if self.merged_df is None:
            self._merge_excel_files()
        return self.merged_df

    def cleanup_excel_files(self):
        """Clean up individual Excel files, excluding all_facilities.xlsx."""
        for filename in os.listdir(self.output_dir):
            if (
                filename.lower().endswith((".xlsx", ".xls"))
                and filename != "all_facilities.xlsx"
            ):
                file_path = os.path.join(self.output_dir, filename)
                try:
                    os.remove(file_path)
                    logging.info(f"Deleted: {filename}")
                except Exception as e:
                    logging.error(f"Failed to delete {filename}: {e}")


if __name__ == "__main__":
    api_url = "https://quality.healthfinder.fl.gov/Facility-Provider"
    facility_manager = FacilityLicenseManager(api_url, output_dir="ahca_data")
    try:
        facility_manager._get_and_export_facility_data(
            "TFL", "transitional_living_facility.xlsx"
        )
        merged_df = facility_manager.get_merged_data()
        if merged_df is not None:
            output_path = os.path.join(
                facility_manager.output_dir, "all_facilities.xlsx"
            )
            merged_df.to_excel(output_path, index=False)
            logging.info(f"Merged data saved to: {output_path}")
            facility_manager.cleanup_excel_files()  # Clean up individual files after saving
    except Exception as e:
        logging.error(f"Main execution failed: {e}")
