import os
from facility_licenses import FacilityLicenseManager
from providers import ProviderManager
import logging

# Set up logging
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)


class DataPipeline:
    def __init__(self, api_url, output_dir="ahca_data", provider_file_path=None):
        self.facility_manager = FacilityLicenseManager(api_url, output_dir)
        self.provider_manager = ProviderManager(provider_file_path)
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def run_pipeline(self, facility_code, export_filename):
        try:
            # Process facility data - only export, don't merge yet
            ids_data, export_response = (
                self.facility_manager._get_and_export_facility_data(
                    facility_code, export_filename
                )
            )

            if export_response and export_response.status_code == 200:
                logging.info(f"Successfully exported {facility_code} data")
                return True
            else:
                logging.error(f"Failed to export data for {facility_code}")
                return False

        except Exception as e:
            logging.error(f"Pipeline failed for {facility_code}: {e}")
            return False

    def run_multiple_facilities(self, facility_codes):
        """Run pipeline for multiple facility types and merge at the end"""
        successful_exports = 0

        # Process each facility type
        for code in facility_codes:
            try:
                filename = f"{code.lower()}_facilities.xlsx"
                logging.info(f"Processing facility type: {code}")
                if self.run_pipeline(code, filename):
                    successful_exports += 1
            except Exception as e:
                logging.error(f"Failed to process {code}: {e}")
                continue

        # Only proceed with merge and provider processing if we have successful exports
        if successful_exports > 0:
            # Merge all facility files
            try:
                logging.info("Starting merge of all facility files...")
                merged_df = self.facility_manager.get_merged_data()
                if merged_df is not None and not merged_df.empty:
                    output_path = os.path.join(self.output_dir, "all_facilities.xlsx")
                    merged_df.to_excel(output_path, index=False)
                    logging.info(
                        f"Merged data saved to: {output_path} ({len(merged_df)} total rows)"
                    )

                    # Clean up individual facility files
                    self.facility_manager.cleanup_excel_files()
                else:
                    logging.warning("No merged data available")
            except Exception as merge_error:
                logging.error(f"Merge failed: {merge_error}")

            # Process provider data (once at the end)
            try:
                provider_df = self.provider_manager.filter_excel_data()
                if provider_df is not None and not provider_df.empty:
                    provider_output_path = os.path.join(
                        self.output_dir, "filtered_providers.xlsx"
                    )
                    provider_df.to_excel(provider_output_path, index=False)
                    logging.info(
                        f"Filtered provider data saved to: {provider_output_path}"
                    )
                else:
                    logging.info("No provider data available to filter")
            except Exception as provider_error:
                logging.warning(f"Provider processing failed: {provider_error}")
        else:
            logging.error(
                "No successful facility exports, skipping merge and provider processing"
            )


if __name__ == "__main__":
    import json

    api_url = "https://quality.healthfinder.fl.gov/Facility-Provider"
    pipeline = DataPipeline(api_url, output_dir="ahca_data")

    try:
        # Load facility mappings from JSON file
        with open("facility_type_mapping.json", "r") as f:
            facility_mappings = json.load(f)

        logging.info("=== Starting Complete Workflow: Scrape and Export ===")

        # Extract facility codes for batch processing
        facility_codes = [mapping["code"] for mapping in facility_mappings]
        logging.info(f"Will process facility codes: {facility_codes}")

        # Process all facilities at once
        pipeline.run_multiple_facilities(facility_codes)

        logging.info("=== Complete workflow finished ===")

    except FileNotFoundError:
        logging.error("facility_type_mapping.json not found")
    except json.JSONDecodeError:
        logging.error("Invalid JSON in facility_type_mapping.json")
    except Exception as e:
        logging.error(f"Main pipeline execution failed: {e}")
