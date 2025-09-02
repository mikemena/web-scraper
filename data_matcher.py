import pandas as pd
import logging
import os

# Set up logging
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, "pipeline.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler()],
    force=True,
)


class DataMatcher:
    def __init__(self, output_dir="ahca_data"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        # Define exact column names
        self.facility_columns = {
            "name": "Name",
            "license": "License Number",
            "expiration": "License Expiration Date",
        }

        self.provider_columns = {
            "name": "NAME",
            "license": "LICENSE_NB",
            "expiration": "EXPIRATION_DATE",
        }

    def _clean_data_for_matching(self, df, name_col, license_col):
        df_clean = df.copy()

        # Clean name column - normalize for matching
        df_clean["name_clean"] = (
            df_clean[name_col]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(r"\s+", " ", regex=True)  # Normalize whitespace
        )

        # Clean license column - normalize for matching
        df_clean["license_clean"] = (
            df_clean[license_col].astype(str).str.strip().str.upper()
        )

        return df_clean

    def _validate_input_data(self, all_facilities_df, filtered_providers_df):
        """Validate input DataFrames and check for required columns."""
        if all_facilities_df is None or all_facilities_df.empty:
            logging.warning("No facility data available for matching")
            return False

        if filtered_providers_df is None or filtered_providers_df.empty:
            logging.warning("No provider data available for matching")
            return False

        logging.info(f"Starting data matching process...")
        logging.info(f"Facility data: {len(all_facilities_df)} rows")
        logging.info(f"Provider data: {len(filtered_providers_df)} rows")

        # Check if required columns exist
        missing_facility_cols = [
            col_name
            for col_name in self.facility_columns.values()
            if col_name not in all_facilities_df.columns
        ]
        missing_provider_cols = [
            col_name
            for col_name in self.provider_columns.values()
            if col_name not in filtered_providers_df.columns
        ]

        if missing_facility_cols:
            logging.error(f"Missing facility columns: {missing_facility_cols}")
            return False
        if missing_provider_cols:
            logging.error(f"Missing provider columns: {missing_provider_cols}")
            return False

        logging.info("All required columns found - proceeding with matching")
        return True

    def _prepare_data_for_matching(self, all_facilities_df, filtered_providers_df):
        """Clean and prepare data for matching operations."""
        facilities_clean = self._clean_data_for_matching(
            all_facilities_df,
            self.facility_columns["name"],
            self.facility_columns["license"],
        )
        providers_clean = self._clean_data_for_matching(
            filtered_providers_df,
            self.provider_columns["name"],
            self.provider_columns["license"],
        )

        # Convert expiration dates to datetime
        facilities_clean["facility_exp_date"] = pd.to_datetime(
            facilities_clean[self.facility_columns["expiration"]], errors="coerce"
        )
        providers_clean["provider_exp_date"] = pd.to_datetime(
            providers_clean[self.provider_columns["expiration"]], errors="coerce"
        )

        # Remove records with missing matching keys
        facilities_clean = facilities_clean.dropna(
            subset=["name_clean", "license_clean"]
        )
        providers_clean = providers_clean.dropna(subset=["name_clean", "license_clean"])

        logging.info(
            f"After cleaning - Facilities: {len(facilities_clean)}, Providers: {len(providers_clean)}"
        )

        return facilities_clean, providers_clean

    def _find_update_licenses(self, facilities_clean, providers_clean):
        """Find licenses that need updates (name + license match with newer facility expiration)."""
        update_licenses_df = pd.merge(
            facilities_clean,
            providers_clean,
            left_on=["name_clean", "license_clean"],
            right_on=["name_clean", "license_clean"],
            how="inner",
            suffixes=("_facility", "_provider"),
        )

        logging.info(
            f"Initial merge on NAME and LICENSE_NB resulted in {len(update_licenses_df)} records"
        )

        if update_licenses_df.empty:
            logging.warning("No matching records found for update licenses scenario")
            return pd.DataFrame()

        # Filter for records with valid expiration dates
        valid_dates_mask = (
            update_licenses_df["facility_exp_date"].notna()
            & update_licenses_df["provider_exp_date"].notna()
        )

        update_licenses_df = update_licenses_df[valid_dates_mask]
        logging.info(
            f"Update licenses - records with valid dates: {len(update_licenses_df)}"
        )

        # Apply the main filter: facility expiration > provider expiration
        update_licenses_df = update_licenses_df[
            update_licenses_df["facility_exp_date"]
            > update_licenses_df["provider_exp_date"]
        ]

        logging.info(
            f"Update licenses - final records (facility exp > provider exp): {len(update_licenses_df)}"
        )
        return update_licenses_df

    def _find_new_licenses(self, facilities_clean, providers_clean):
        """Find new licenses (name exists but license number doesn't exist in providers)."""
        # Create sets for efficient lookup
        provider_licenses = set(providers_clean["license_clean"].unique())
        facility_names_in_providers = set(providers_clean["name_clean"].unique())

        # Filter facilities: name exists in providers BUT license doesn't exist in providers
        new_licenses_mask = facilities_clean["name_clean"].isin(
            facility_names_in_providers
        ) & ~facilities_clean["license_clean"].isin(provider_licenses)

        new_licenses_df = facilities_clean[new_licenses_mask].copy()

        logging.info(
            f"New licenses - records where name exists but license is new: {len(new_licenses_df)}"
        )
        return new_licenses_df

    def _find_expired_licenses(self, facilities_clean, providers_clean):
        """Find expired licenses in providers that do not exist in facilities (no name + license match) and exp <= today."""
        merged = pd.merge(
            providers_clean,
            facilities_clean,
            on=["name_clean", "license_clean"],
            how="left",
            indicator=True,
            suffixes=("_provider", "_facility"),
        )

        # Providers that have no match in facilities
        no_match_df = merged[merged["_merge"] == "left_only"].copy()

        # Drop facility columns (which are NaN) and _merge
        facility_cols = [
            col for col in no_match_df.columns if col.endswith("_facility")
        ]
        no_match_df = no_match_df.drop(columns=facility_cols + ["_merge"])

        if no_match_df.empty:
            logging.warning(
                "No non-matching provider records found for expired licenses"
            )
            return pd.DataFrame()

        # Get today's date (normalized to date only)
        today = pd.to_datetime("today").normalize()

        # Filter for expired: provider_exp_date <= today and not NaT
        expired_mask = (no_match_df["provider_exp_date"] <= today) & no_match_df[
            "provider_exp_date"
        ].notna()
        expired_licenses_df = no_match_df[expired_mask]

        logging.info(
            f"Expired licenses - records where provider exp <= today and no facility match: {len(expired_licenses_df)}"
        )
        return expired_licenses_df

    def _finalize_results(
        self, update_licenses_df, new_licenses_df, expired_licenses_df
    ):
        """Clean up DataFrames and add metadata."""
        columns_to_drop = [
            "name_clean",
            "license_clean",
            "facility_exp_date",
            "provider_exp_date",
        ]

        if not update_licenses_df.empty:
            update_licenses_df = update_licenses_df.drop(
                columns=[
                    col for col in columns_to_drop if col in update_licenses_df.columns
                ]
            )
            update_licenses_df["match_timestamp"] = pd.Timestamp.now()
            update_licenses_df["match_criteria"] = (
                "name_and_license_match_with_exp_date_filter"
            )

        if not new_licenses_df.empty:
            new_licenses_df = new_licenses_df.drop(
                columns=[
                    col for col in columns_to_drop if col in new_licenses_df.columns
                ]
            )
            new_licenses_df["match_timestamp"] = pd.Timestamp.now()
            new_licenses_df["match_criteria"] = "name_match_but_new_license"

        if not expired_licenses_df.empty:
            expired_licenses_df = expired_licenses_df.drop(
                columns=[
                    col for col in columns_to_drop if col in expired_licenses_df.columns
                ]
            )
            expired_licenses_df["match_timestamp"] = pd.Timestamp.now()
            expired_licenses_df["match_criteria"] = "expired_license_not_in_facilities"

        return {
            "update_licenses": update_licenses_df,
            "new_licenses": new_licenses_df,
            "expired_licenses": expired_licenses_df,
        }

    def _format_expiration_dates(self, df):
        """Format expiration date columns to 'MM/DD/YYYY' string format."""
        expiration_cols = [
            self.facility_columns["expiration"],
            self.provider_columns["expiration"],
        ]

        for col in df.columns:
            if col in expiration_cols and pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%m/%d/%Y")

        return df

    def _save_results_to_excel(self, results, output_filename):
        """Save results to Excel file with separate worksheets."""
        output_path = os.path.join(self.output_dir, output_filename)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            update_licenses_df = results["update_licenses"]
            new_licenses_df = results["new_licenses"]
            expired_licenses_df = results["expired_licenses"]

            if not update_licenses_df.empty:
                update_licenses_df = self._format_expiration_dates(update_licenses_df)
                update_licenses_df.to_excel(
                    writer, sheet_name="update_licenses", index=False
                )
                logging.info(
                    f"Update licenses saved: {len(update_licenses_df)} records"
                )
            else:
                pd.DataFrame().to_excel(
                    writer, sheet_name="update_licenses", index=False
                )
                logging.info("Update licenses sheet created (empty)")

            if not new_licenses_df.empty:
                new_licenses_df = self._format_expiration_dates(new_licenses_df)
                new_licenses_df.to_excel(writer, sheet_name="new_licenses", index=False)
                logging.info(f"New licenses saved: {len(new_licenses_df)} records")
            else:
                pd.DataFrame().to_excel(writer, sheet_name="new_licenses", index=False)
                logging.info("New licenses sheet created (empty)")

            if not expired_licenses_df.empty:
                expired_licenses_df = self._format_expiration_dates(expired_licenses_df)
                expired_licenses_df.to_excel(
                    writer, sheet_name="expired_licenses", index=False
                )
                logging.info(
                    f"Expired licenses saved: {len(expired_licenses_df)} records"
                )
            else:
                pd.DataFrame().to_excel(
                    writer, sheet_name="expired_licenses", index=False
                )
                logging.info("Expired licenses sheet created (empty)")

        logging.info(f"Matched data saved to: {output_path}")

    def match_provider_facility_data(
        self,
        all_facilities_df,
        filtered_providers_df,
        save_output=True,
        output_filename="matched_provider_facility_data.xlsx",
    ):
        try:
            # Validate input data
            if not self._validate_input_data(all_facilities_df, filtered_providers_df):
                return {
                    "update_licenses": pd.DataFrame(),
                    "new_licenses": pd.DataFrame(),
                    "expired_licenses": pd.DataFrame(),
                }

            # Prepare data for matching
            facilities_clean, providers_clean = self._prepare_data_for_matching(
                all_facilities_df, filtered_providers_df
            )

            # Find update licenses
            update_licenses_df = self._find_update_licenses(
                facilities_clean, providers_clean
            )

            # Find new licenses
            new_licenses_df = self._find_new_licenses(facilities_clean, providers_clean)

            # Find expired licenses
            expired_licenses_df = self._find_expired_licenses(
                facilities_clean, providers_clean
            )

            # Finalize results
            results = self._finalize_results(
                update_licenses_df, new_licenses_df, expired_licenses_df
            )

            # Save output if requested
            if save_output:
                self._save_results_to_excel(results, output_filename)

            logging.info(
                f"Successfully processed - Update licenses: {len(results['update_licenses'])}, New licenses: {len(results['new_licenses'])}, Expired licenses: {len(results['expired_licenses'])}"
            )
            return results

        except Exception as e:
            logging.error(f"Error in matching provider and facility data: {e}")
            import traceback

            logging.error(f"Traceback: {traceback.format_exc()}")
            return {
                "update_licenses": pd.DataFrame(),
                "new_licenses": pd.DataFrame(),
                "expired_licenses": pd.DataFrame(),
            }

    def get_matching_summary(self, all_facilities_df, filtered_providers_df):
        try:
            summary = {
                "facility_records": (
                    len(all_facilities_df) if all_facilities_df is not None else 0
                ),
                "provider_records": (
                    len(filtered_providers_df)
                    if filtered_providers_df is not None
                    else 0
                ),
                "facility_columns": (
                    list(all_facilities_df.columns)
                    if all_facilities_df is not None
                    else []
                ),
                "provider_columns": (
                    list(filtered_providers_df.columns)
                    if filtered_providers_df is not None
                    else []
                ),
                "expected_facility_columns": list(self.facility_columns.values()),
                "expected_provider_columns": list(self.provider_columns.values()),
            }

            if all_facilities_df is not None and filtered_providers_df is not None:
                # Check if all required columns exist
                facility_cols_exist = all(
                    col in all_facilities_df.columns
                    for col in self.facility_columns.values()
                )
                provider_cols_exist = all(
                    col in filtered_providers_df.columns
                    for col in self.provider_columns.values()
                )

                summary["facility_columns_ready"] = facility_cols_exist
                summary["provider_columns_ready"] = provider_cols_exist
                summary["ready_for_matching"] = (
                    facility_cols_exist and provider_cols_exist
                )

                if not facility_cols_exist:
                    missing_facility = [
                        col
                        for col in self.facility_columns.values()
                        if col not in all_facilities_df.columns
                    ]
                    summary["missing_facility_columns"] = missing_facility

                if not provider_cols_exist:
                    missing_provider = [
                        col
                        for col in self.provider_columns.values()
                        if col not in filtered_providers_df.columns
                    ]
                    summary["missing_provider_columns"] = missing_provider

            return summary

        except Exception as e:
            logging.error(f"Error generating matching summary: {e}")
            return {}


if __name__ == "__main__":
    # Example usage for testing
    matcher = DataMatcher()
    logging.info("DataMatcher class initialized successfully")
    logging.info(
        f"Expected facility columns: {list(matcher.facility_columns.values())}"
    )
    logging.info(
        f"Expected provider columns: {list(matcher.provider_columns.values())}"
    )
