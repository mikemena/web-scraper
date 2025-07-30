import os
from facility_licenses import FacilityLicenseManager
from providers import ProviderManager
from data_matcher import DataMatcher  # Import the new DataMatcher class
import logging

# Set up logging
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file, mode="w"), logging.StreamHandler()],
    force=True,
)


class DataPipeline:
    def __init__(self, api_url, output_dir="ahca_data", provider_file_path=None):
        self.facility_manager = FacilityLicenseManager(api_url, output_dir)
        self.provider_manager = ProviderManager(provider_file_path)
        self.data_matcher = DataMatcher(output_dir)  # Initialize the DataMatcher
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
                merged_facilities_df = self.facility_manager.get_merged_data()
                if merged_facilities_df is not None and not merged_facilities_df.empty:
                    facility_output_path = os.path.join(
                        self.output_dir, "all_facilities.xlsx"
                    )
                    merged_facilities_df.to_excel(facility_output_path, index=False)
                    logging.info(
                        f"Merged facility data saved to: {facility_output_path} ({len(merged_facilities_df)} total rows)"
                    )

                    # Clean up individual facility files
                    self.facility_manager.cleanup_excel_files()
                else:
                    logging.warning("No merged facility data available")
                    merged_facilities_df = None
            except Exception as merge_error:
                logging.error(f"Facility merge failed: {merge_error}")
                merged_facilities_df = None

            # Process provider data
            try:
                filtered_providers_df = self.provider_manager.filter_excel_data()
                if (
                    filtered_providers_df is not None
                    and not filtered_providers_df.empty
                ):
                    provider_output_path = os.path.join(
                        self.output_dir, "filtered_providers.xlsx"
                    )
                    filtered_providers_df.to_excel(provider_output_path, index=False)
                    logging.info(
                        f"Filtered provider data saved to: {provider_output_path} ({len(filtered_providers_df)} rows)"
                    )
                else:
                    logging.info("No provider data available to filter")
                    filtered_providers_df = None
            except Exception as provider_error:
                logging.warning(f"Provider processing failed: {provider_error}")
                filtered_providers_df = None

            # Perform data matching using the DataMatcher class
            if merged_facilities_df is not None and filtered_providers_df is not None:
                try:
                    logging.info("=== Starting Data Matching Process ===")

                    # Get matching summary for debugging
                    summary = self.data_matcher.get_matching_summary(
                        merged_facilities_df, filtered_providers_df
                    )
                    logging.info(f"Matching summary: {summary}")

                    # Perform the actual matching
                    matching_results = self.data_matcher.match_provider_facility_data(
                        all_facilities_df=merged_facilities_df,
                        filtered_providers_df=filtered_providers_df,
                        save_output=True,
                        output_filename="matched_provider_facility_data.xlsx",
                    )

                    if matching_results and isinstance(matching_results, dict):
                        update_count = len(
                            matching_results.get("update_licenses", pd.DataFrame())
                        )
                        new_count = len(
                            matching_results.get("new_licenses", pd.DataFrame())
                        )
                        logging.info(
                            f"Successfully matched - Update licenses: {update_count}, New licenses: {new_count}"
                        )
                        logging.info("=== Data Matching Process Completed ===")
                    else:
                        logging.warning("No matching results returned")

                except Exception as matching_error:
                    logging.error(f"Data matching failed: {matching_error}")
                    import traceback

                    logging.error(f"Matching traceback: {traceback.format_exc()}")
            else:
                logging.warning(
                    "Cannot perform matching - missing facility or provider data"
                )
                if merged_facilities_df is None:
                    logging.warning("- No facility data available")
                if filtered_providers_df is None:
                    logging.warning("- No provider data available")

        else:
            logging.error(
                "No successful facility exports, skipping merge and provider processing"
            )

    def run_matching_only(self, facility_file_path=None, provider_file_path=None):
        try:
            # Load facility data
            if facility_file_path and os.path.exists(facility_file_path):
                facilities_df = pd.read_excel(facility_file_path)
                logging.info(
                    f"Loaded facility data from {facility_file_path}: {len(facilities_df)} rows"
                )
            else:
                facility_default_path = os.path.join(
                    self.output_dir, "all_facilities.xlsx"
                )
                if os.path.exists(facility_default_path):
                    facilities_df = pd.read_excel(facility_default_path)
                    logging.info(
                        f"Loaded facility data from {facility_default_path}: {len(facilities_df)} rows"
                    )
                else:
                    logging.error("No facility data file found")
                    return None

            # Load provider data
            if provider_file_path and os.path.exists(provider_file_path):
                providers_df = pd.read_excel(provider_file_path)
                logging.info(
                    f"Loaded provider data from {provider_file_path}: {len(providers_df)} rows"
                )
            else:
                provider_default_path = os.path.join(
                    self.output_dir, "filtered_providers.xlsx"
                )
                if os.path.exists(provider_default_path):
                    providers_df = pd.read_excel(provider_default_path)
                    logging.info(
                        f"Loaded provider data from {provider_default_path}: {len(providers_df)} rows"
                    )
                else:
                    logging.error("No provider data file found")
                    return None

            # Perform matching
            logging.info("=== Starting Matching-Only Process ===")
            matching_results = self.data_matcher.match_provider_facility_data(
                all_facilities_df=facilities_df,
                filtered_providers_df=providers_df,
                save_output=True,
                output_filename="matched_provider_facility_data.xlsx",
            )

            logging.info("=== Matching-Only Process Completed ===")
            return matching_results

        except Exception as e:
            logging.error(f"Matching-only process failed: {e}")
            import traceback

            logging.error(f"Traceback: {traceback.format_exc()}")
            return None


if __name__ == "__main__":
    import json
    import pandas as pd

    api_url = "https://quality.healthfinder.fl.gov/Facility-Provider"
    pipeline = DataPipeline(api_url, output_dir="ahca_data")

    try:
        # Load facility mappings from JSON file
        with open("facility_type_mapping.json", "r") as f:
            facility_mappings = json.load(f)

        logging.info("=== Starting Complete Workflow ===")

        # Extract facility codes for batch processing
        facility_codes = [mapping["code"] for mapping in facility_mappings]
        logging.info(f"Will process facility codes: {facility_codes}")

        # Choose workflow mode
        # Option 1: Full workflow (download data + matching)
        # pipeline.run_multiple_facilities(facility_codes)

        # Option 2: Matching only (uncomment to use existing data files)
        pipeline.run_matching_only()

        logging.info("=== Complete workflow finished ===")

    except FileNotFoundError:
        logging.error("facility_type_mapping.json not found")
    except json.JSONDecodeError:
        logging.error("Invalid JSON in facility_type_mapping.json")
    except Exception as e:
        logging.error(f"Main pipeline execution failed: {e}")
        import traceback

        logging.error(f"Traceback: {traceback.format_exc()}")
