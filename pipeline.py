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
            # Process facility data
            ids_data, export_response = (
                self.facility_manager._get_and_export_facility_data(
                    facility_code, export_filename
                )
            )

            if export_response and export_response.status_code == 200:
                logging.info(f"Successfully exported {facility_code} data")

                # Merge data
                merged_df = self.facility_manager.get_merged_data()
                if merged_df is not None and not merged_df.empty:
                    output_path = os.path.join(self.output_dir, "all_facilities.xlsx")
                    merged_df.to_excel(output_path, index=False)
                    logging.info(f"Merged data saved to: {output_path}")
                    self.facility_manager.cleanup_excel_files()  # Clean up individual files
                else:
                    logging.warning("No merged data available")
            else:
                logging.error(f"Failed to export data for {facility_code}")

            # Process provider data (optional - won't fail if no provider data)
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
                logging.warning(
                    f"Provider processing failed (continuing anyway): {provider_error}"
                )
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise

    def run_multiple_facilities(
        self, facility_codes, base_filename="facilities_data.xlsx"
    ):
        """Run pipeline for multiple facility types"""
        for code in facility_codes:
            try:
                filename = f"{code.lower()}_{base_filename}"
                logging.info(f"Processing facility type: {code}")
                self.run_pipeline(code, filename)
            except Exception as e:
                logging.error(f"Failed to process {code}: {e}")
                continue


if __name__ == "__main__":
    import json

    api_url = "https://quality.healthfinder.fl.gov/Facility-Provider"
    pipeline = DataPipeline(api_url, output_dir="ahca_data")

    try:
        # Load facility mappings from JSON file
        with open("facility_type_mapping.json", "r") as f:
            facility_mappings = json.load(f)

        logging.info("=== Starting Complete Workflow: Scrape and Export ===")

        # Iterate over each facility type in the JSON array
        for mapping in facility_mappings:
            facility_code = mapping["code"]
            facility_description = mapping["description"]
            export_filename = f"{facility_code.lower()}_facilities.xlsx"

            logging.info(f"Processing {facility_code} - {facility_description}")

            try:
                pipeline.run_pipeline(facility_code, export_filename)
                logging.info(f"Successfully completed {facility_code}")
            except Exception as facility_error:
                logging.error(f"Failed to process {facility_code}: {facility_error}")
                continue  # Continue with next facility type

        logging.info("=== Complete workflow finished ===")

    except FileNotFoundError:
        logging.error("facility_type_mapping.json not found")
    except json.JSONDecodeError:
        logging.error("Invalid JSON in facility_type_mapping.json")
    except Exception as e:
        logging.error(f"Main pipeline execution failed: {e}")
