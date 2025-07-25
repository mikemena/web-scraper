import json
import os
import pandas as pd
import logging

# Set up logging
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, "provider_manager.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),  # Optional: also log to console
    ],
)


class ProviderManager:
    def __init__(self, provider_file_path=None):
        self._distinct_specialties = None
        self._latest_file_path = provider_file_path
        self.default_directories = [
            "./Report 129",  # Your local directory
            "./provider_data",  # Alternative local directory
            r"R:\Delivery Systems\Work Operations\Projects\2. Project Repository - Open Projects\PC Report 129 - License",  # Original network path
            ".",  # Current directory
        ]

    def _get_providers(self):
        try:
            with open("facility_type_mapping.json", "r") as fi:
                facility_mappings = json.load(fi)
            self._distinct_specialties = set()
            for mapping in facility_mappings:
                specialties = mapping.get("pc_specialties", [])
                self._distinct_specialties.update(specialties)
            logging.info("Successfully loaded distinct specialties")
            return self._distinct_specialties
        except FileNotFoundError:
            logging.error("facility_type_mapping.json not found")
            self._distinct_specialties = set()
        except json.JSONDecodeError:
            logging.error("Invalid JSON in facility_type_mapping.json")
            self._distinct_specialties = set()

    def _get_report(self):
        # If a specific file path was provided, use it
        if self._latest_file_path and os.path.exists(self._latest_file_path):
            logging.info(f"Using provided file path: {self._latest_file_path}")
            return self._latest_file_path

        # Try multiple directory locations
        for directory_path in self.default_directories:
            try:
                if not os.path.exists(directory_path):
                    logging.warning(f"Directory not found: {directory_path}")
                    continue

                excel_files = [
                    f
                    for f in os.listdir(directory_path)
                    if f.lower().endswith((".xlsx", ".xls"))
                ]

                if not excel_files:
                    logging.warning(f"No Excel files found in: {directory_path}")
                    continue

                self._latest_file_path = max(
                    excel_files,
                    key=lambda f: os.path.getctime(os.path.join(directory_path, f)),
                )
                self._latest_file_path = os.path.join(
                    directory_path, self._latest_file_path
                )
                logging.info(
                    f"Latest report file path set to: {self._latest_file_path}"
                )
                return self._latest_file_path

            except Exception as e:
                logging.error(f"Error checking directory {directory_path}: {e}")
                continue

        logging.error("No valid provider data file found in any directory")
        return None

    def filter_excel_data(self):
        if self._distinct_specialties is None:
            self._get_providers()

        if self._latest_file_path is None:
            self._get_report()

        if not self._latest_file_path or not os.path.exists(self._latest_file_path):
            logging.warning(f"Report file not found: {self._latest_file_path}")
            # Return empty DataFrame instead of raising exception
            return pd.DataFrame()

        try:
            df = pd.read_excel(self._latest_file_path, sheet_name="PROV")

            # Only filter if we have specialties to filter by
            if self._distinct_specialties:
                filtered_df = df[df["SPECIALTY_DE"].isin(self._distinct_specialties)]
            else:
                filtered_df = df

            desired_columns = [
                "FB_NUMBER",
                "PROVIDER_ID",
                "NAME",
                "PROVIDER_CATEGORY_CD",
                "SPECIALTY_DE",
                "LICENSE_NB",
                "LICENSE_TYPE_DES",
            ]

            # Only keep columns that exist in the DataFrame
            available_columns = [
                col for col in desired_columns if col in filtered_df.columns
            ]
            if available_columns:
                filtered_df = filtered_df[available_columns]

            logging.info(f"Filtered rows count: {len(filtered_df)}")
            return filtered_df

        except Exception as e:
            logging.error(f"Error filtering Excel data: {e}")
            return pd.DataFrame()  # Return empty DataFrame instead of raising


if __name__ == "__main__":
    provider_manager = ProviderManager()
    try:
        filtered_data = provider_manager.filter_excel_data()
        if not filtered_data.empty:
            output_path = "filtered_providers.xlsx"
            filtered_data.to_excel(output_path, index=False)
            logging.info(f"Filtered data saved to: {output_path}")
        else:
            logging.warning("No provider data to save")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
