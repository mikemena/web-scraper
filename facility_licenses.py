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
        self.facility_files = []  # Track facility files separately

    def _get_facility_ids(self, facility_code):
        url = f"{self.api_url}/{facility_code}?&type=1"
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()
            script_content = response.text

            # Try multiple regex patterns to find the data
            patterns = [
                r"(?:const|var)\s+data\s*=\s*(\[.*?\])\s*;",
                r"data\s*=\s*(\[.*?\])",
                r"var\s+data\s*=\s*(\[.*?\])",
                r"const\s+data\s*=\s*(\[.*?\])",
            ]

            data_match = None
            for pattern in patterns:
                data_match = re.search(pattern, script_content, re.DOTALL)
                if data_match:
                    break

            if data_match:
                try:
                    json_data = data_match.group(1)
                    data = json.loads(json_data)
                    facilities = (
                        data[0]
                        if isinstance(data, list)
                        and len(data) > 0
                        and isinstance(data[0], list)
                        else data
                    )

                    license_ids = []
                    link_ids = []

                    if isinstance(facilities, list):
                        for facility in facilities:
                            if isinstance(facility, dict):
                                if (
                                    facility.get("LicenseID")
                                    and str(facility["LicenseID"]).strip()
                                ):
                                    license_ids.append(str(facility["LicenseID"]))
                                if (
                                    facility.get("LinkId")
                                    and str(facility["LinkId"]).strip()
                                ):
                                    link_ids.append(str(facility["LinkId"]))

                    logging.info(
                        f"Found {len(license_ids)} License IDs and {len(link_ids)} Link IDs for {facility_code}"
                    )
                    return {"license_ids": license_ids, "link_ids": link_ids}

                except json.JSONDecodeError as json_error:
                    logging.error(
                        f"Failed to parse JSON data for {facility_code}: {json_error}"
                    )
                    logging.debug(
                        f"JSON data that failed: {data_match.group(1)[:200]}..."
                    )

            else:
                logging.warning(
                    f"Could not find data variable in the response for {facility_code}"
                )
                # Log a sample of the response for debugging
                logging.debug(f"Response sample: {script_content[:500]}...")

            return {"license_ids": [], "link_ids": []}

        except requests.RequestException as e:
            logging.error(f"Request failed for {facility_code}: {e}")
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

        # Skip export if no IDs found
        if not license_ids and not link_ids:
            logging.warning(
                f"No license or link IDs found for {facility_code}, skipping export"
            )
            return None

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

            logging.info(
                f"Exporting {len(license_ids)} licenses and {len(link_ids)} links for {facility_code}"
            )

            export_response = session.post(
                export_url, data=form_data, headers=headers, verify=False
            )
            export_response.raise_for_status()

            # Check content type and size
            content_type = export_response.headers.get("content-type", "")
            content_length = len(export_response.content)
            logging.info(
                f"Export response: Content-Type: {content_type}, Size: {content_length} bytes"
            )

            # Check if response is actually Excel
            if content_length < 100:
                logging.error(
                    f"Export response too small ({content_length} bytes), likely an error"
                )
                logging.error(f"Response content: {export_response.text[:200]}")
                return None

            if (
                "excel" not in content_type.lower()
                and "spreadsheet" not in content_type.lower()
            ):
                logging.warning(f"Unexpected content type: {content_type}")
                # Check if it's an error page
                if "text/html" in content_type:
                    logging.error("Received HTML instead of Excel file")
                    logging.error(f"HTML content sample: {export_response.text[:300]}")
                    return None

            export_filename = f"{facility_code.lower()}_facilities.xlsx"
            full_path = os.path.join(self.output_dir, export_filename)
            with open(full_path, "wb") as f:
                f.write(export_response.content)
            logging.info(f"Data saved to: {full_path}")

            # Track this as a facility file for merging
            self.facility_files.append(full_path)

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

                # Track this as a facility file for merging
                if full_path not in self.facility_files:
                    self.facility_files.append(full_path)

            return ids_data, export_response
        else:
            logging.error(f"Export failed for {facility_code}")
            return ids_data, None

    def _merge_excel_files(self):
        """Merge only facility data files (not provider files)"""
        if not self.facility_files:
            logging.warning("No facility files to merge")
            return None

        dfs = []
        for file_path in self.facility_files:
            try:
                if os.path.exists(file_path):
                    # Check if file is actually an Excel file by trying to read it
                    try:
                        # First, check if it's a valid Excel file by checking file content
                        with open(file_path, "rb") as f:
                            header = f.read(8)
                            if not (
                                header.startswith(b"PK")
                                or header.startswith(b"\xd0\xcf")
                            ):
                                logging.error(
                                    f"File {file_path} is not a valid Excel file (invalid header)"
                                )
                                continue

                        # Try reading with openpyxl engine first, then xlrd
                        try:
                            df = pd.read_excel(file_path, engine="openpyxl")
                        except:
                            try:
                                df = pd.read_excel(file_path, engine="xlrd")
                            except:
                                # Last resort - try without specifying engine
                                df = pd.read_excel(file_path)

                        if df.empty:
                            logging.warning(f"Excel file {file_path} is empty")
                            continue

                        # Add a column to track which facility type this data came from
                        facility_type = (
                            os.path.basename(file_path)
                            .replace("_facilities.xlsx", "")
                            .upper()
                        )
                        df["facility_type_source"] = facility_type
                        dfs.append(df)
                        logging.info(
                            f"Added {len(df)} rows from {os.path.basename(file_path)}"
                        )

                    except Exception as read_error:
                        logging.error(
                            f"Failed to read Excel file {file_path}: {read_error}"
                        )
                        # Check if the file might be HTML or text instead
                        try:
                            with open(file_path, "r", encoding="utf-8") as f:
                                content = f.read(200)
                                if content.strip().startswith("<"):
                                    logging.error(
                                        f"File {file_path} appears to be HTML, not Excel"
                                    )
                                elif "error" in content.lower():
                                    logging.error(
                                        f"File {file_path} contains error message: {content[:100]}"
                                    )
                                else:
                                    logging.error(
                                        f"File {file_path} content sample: {content[:100]}"
                                    )
                        except:
                            pass
                        continue

                else:
                    logging.warning(f"File not found: {file_path}")
            except Exception as e:
                logging.error(f"Failed to process {file_path}: {e}")

        if dfs:
            self.merged_df = pd.concat(dfs, ignore_index=True)
            logging.info(
                f"Merged {len(dfs)} facility files into a DataFrame with {len(self.merged_df)} rows"
            )
            return self.merged_df
        logging.warning("No facility DataFrames to merge")
        return None

    def get_merged_data(self):
        """Public method to access the merged DataFrame for the pipeline."""
        if self.merged_df is None:
            self._merge_excel_files()
        return self.merged_df

    def cleanup_excel_files(self):
        """Clean up individual facility Excel files, excluding all_facilities.xlsx and filtered_providers.xlsx."""
        for file_path in self.facility_files:
            try:
                filename = os.path.basename(file_path)
                if filename not in ["all_facilities.xlsx", "filtered_providers.xlsx"]:
                    os.remove(file_path)
                    logging.info(f"Deleted: {filename}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")


if __name__ == "__main__":
    api_url = "https://quality.healthfinder.fl.gov/Facility-Provider"
    facility_manager = FacilityLicenseManager(api_url, output_dir="ahca_data")
    try:
        facility_manager._get_and_export_facility_data(
            "ASC", "ambulatory_surgery_centers.xlsx"
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
