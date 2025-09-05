import pandas as pd
import logging
import os
from datetime import datetime

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
            "licensed_beds": "Licensed Beds",
        }
        self.provider_columns = {
            "name": "NAME",
            "license": "LICENSE_NB",
            "expiration": "EXPIRATION_DATE",
            "business_entity_name": "BUSINESS_ENTITY_NAME",
            "facility_bed_count": "FACILITY_BED_COUNT",
        }

    def _clean_data_for_matching(self, df, name_col, license_col):
        df_clean = df.copy()
        df_clean["name_clean"] = (
            df_clean[name_col]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(r"\s+", " ", regex=True)
        )
        df_clean["license_clean"] = (
            df_clean[license_col].astype(str).str.strip().str.upper()
        )
        return df_clean

    def _validate_input_data(self, all_facilities_df, filtered_providers_df):
        if all_facilities_df is None or all_facilities_df.empty:
            logging.warning("No facility data available for matching")
            return False
        if filtered_providers_df is None or filtered_providers_df.empty:
            logging.warning("No provider data available for matching")
            return False
        logging.info(f"Starting data matching process...")
        logging.info(f"Facility data: {len(all_facilities_df)} rows")
        logging.info(f"Provider data: {len(filtered_providers_df)} rows")
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
        providers_clean["business_entity_name_clean"] = (
            providers_clean[self.provider_columns["business_entity_name"]]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(r"[^A-Z0-9 ]", "", regex=True)
        )
        facilities_clean["facility_exp_date"] = pd.to_datetime(
            facilities_clean[self.facility_columns["expiration"]], errors="coerce"
        )
        providers_clean["provider_exp_date"] = pd.to_datetime(
            providers_clean[self.provider_columns["expiration"]], errors="coerce"
        )
        facilities_clean = facilities_clean.dropna(
            subset=["name_clean", "license_clean"]
        )
        providers_clean = providers_clean.dropna(subset=["name_clean", "license_clean"])
        facilities_clean["licensed_beds"] = pd.to_numeric(
            facilities_clean[self.facility_columns["licensed_beds"]], errors="coerce"
        )
        providers_clean["facility_bed_count"] = pd.to_numeric(
            providers_clean[self.provider_columns["facility_bed_count"]],
            errors="coerce",
        )
        logging.info(
            f"After cleaning - Facilities: {len(facilities_clean)}, Providers: {len(providers_clean)}"
        )
        return facilities_clean, providers_clean

    def _find_update_licenses(self, facilities_clean, providers_clean):
        primary_match = pd.merge(
            facilities_clean,
            providers_clean,
            left_on=["name_clean", "license_clean"],
            right_on=["name_clean", "license_clean"],
            how="inner",
            suffixes=("_facility", "_provider"),
        )
        primary_matched_keys = (
            primary_match["license_clean"] + primary_match["name_clean"]
        )
        unmatched_providers = providers_clean[
            ~(providers_clean["license_clean"] + providers_clean["name_clean"]).isin(
                primary_matched_keys
            )
        ]
        secondary_match = pd.merge(
            facilities_clean,
            unmatched_providers,
            left_on=["name_clean", "license_clean"],
            right_on=["business_entity_name_clean", "license_clean"],
            how="inner",
            suffixes=("_facility", "_provider"),
        )
        update_licenses_df = pd.concat(
            [primary_match, secondary_match]
        ).drop_duplicates()
        if update_licenses_df.empty:
            logging.warning("No matching records found for update licenses scenario")
            return pd.DataFrame()
        valid_dates_mask = (
            update_licenses_df["facility_exp_date"]
            > update_licenses_df["provider_exp_date"]
        )
        update_licenses_df = update_licenses_df[valid_dates_mask]
        logging.info(
            f"Update licenses - final records (facility exp > provider exp): {len(update_licenses_df)}"
        )
        return update_licenses_df

    def _find_new_licenses(self, facilities_clean, providers_clean):
        provider_licenses = set(providers_clean["license_clean"].unique())
        facility_names_in_providers = set(providers_clean["name_clean"].unique())
        new_licenses_mask = facilities_clean["name_clean"].isin(
            facility_names_in_providers
        ) & ~facilities_clean["license_clean"].isin(provider_licenses)
        new_licenses_df = facilities_clean[new_licenses_mask].copy()
        logging.info(
            f"New licenses - records where name exists but license is new: {len(new_licenses_df)}"
        )
        return new_licenses_df

    def _find_expired_licenses(self, facilities_clean, providers_clean):
        all_matches = self._find_update_licenses(facilities_clean, providers_clean)
        matched_keys = all_matches["license_clean"] + all_matches["name_clean_provider"]
        no_match_df = providers_clean[
            ~(providers_clean["license_clean"] + providers_clean["name_clean"]).isin(
                matched_keys
            )
        ]
        today = pd.to_datetime("today").normalize()
        expired_mask = (no_match_df["provider_exp_date"] < today) & no_match_df[
            "provider_exp_date"
        ].notna()
        expired_licenses_df = no_match_df[expired_mask]
        logging.info(
            f"Expired licenses - records where provider license exp < today and no AHCA match: {len(expired_licenses_df)}"
        )
        return expired_licenses_df

    def _find_bed_updates(self, update_licenses_df):
        # Define desired columns
        add_beds_columns = [
            "PROVIDER_ID",
            "FB_NUMBER",
            "PROVIDER_CATEGORY_CD",
            "licensed_beds",
            "effectiveDate",
        ]
        update_beds_columns = [
            "PROVIDER_ID",
            "FB_NUMBER",
            "PROVIDER_CATEGORY_CD",
            "licensed_beds",
            "FACILITY_BED_ID",
            "effectiveDate",
        ]

        # Check if required columns exist
        required_provider_cols = ["PROVIDER_ID", "FB_NUMBER", "PROVIDER_CATEGORY_CD"]
        missing_cols = [
            col
            for col in required_provider_cols
            if col not in update_licenses_df.columns
        ]
        if missing_cols:
            logging.error(
                f"Missing required provider columns for bed updates: {missing_cols}"
            )
            return {
                "update_hospital_beds": pd.DataFrame(columns=update_beds_columns),
                "add_hospital_beds": pd.DataFrame(columns=add_beds_columns),
            }

        # Update beds: where counts differ
        update_mask = (
            update_licenses_df["facility_bed_count"]
            != update_licenses_df["licensed_beds"]
        )
        update_hospital_beds = update_licenses_df[update_mask].copy()
        update_hospital_beds["licensed_beds"] = update_hospital_beds[
            "licensed_beds"
        ]  # Use facility's licensed_beds
        update_hospital_beds["effectiveDate"] = datetime.now().strftime("%m/%d/%Y")

        # Filter to desired columns, include FACILITY_BED_ID if present
        available_update_cols = [
            col
            for col in update_beds_columns
            if col in update_hospital_beds.columns or col == "effectiveDate"
        ]
        update_hospital_beds = update_hospital_beds[available_update_cols]

        # Add beds: where provider is blank and facility is not
        add_mask = (
            update_licenses_df["facility_bed_count"].isna()
            & ~update_licenses_df["licensed_beds"].isna()
        )
        add_hospital_beds = update_licenses_df[add_mask].copy()
        add_hospital_beds["licensed_beds"] = add_hospital_beds["licensed_beds"]
        add_hospital_beds["effectiveDate"] = datetime.now().strftime("%m/%d/%Y")

        # Filter to desired columns
        available_add_cols = [
            col
            for col in add_beds_columns
            if col in add_hospital_beds.columns or col == "effectiveDate"
        ]
        add_hospital_beds = add_hospital_beds[available_add_cols]

        logging.info(f"Update hospital beds - records: {len(update_hospital_beds)}")
        logging.info(f"Add hospital beds - records: {len(add_hospital_beds)}")

        return {
            "update_hospital_beds": update_hospital_beds,
            "add_hospital_beds": add_hospital_beds,
        }

    def _finalize_results(
        self, update_licenses_df, new_licenses_df, expired_licenses_df
    ):
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

        bed_updates = self._find_bed_updates(update_licenses_df)

        results = {
            "update_licenses": update_licenses_df,
            "new_licenses": new_licenses_df,
            "expired_licenses": expired_licenses_df,
            "update_hospital_beds": bed_updates["update_hospital_beds"],
            "add_hospital_beds": bed_updates["add_hospital_beds"],
        }

        return results

    def _format_expiration_dates(self, df):
        expiration_cols = [
            self.facility_columns["expiration"],
            self.provider_columns["expiration"],
        ]
        for col in df.columns:
            if col in expiration_cols and pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%m/%d/%Y")
        return df

    def _save_results_to_excel(self, results, output_filename):
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
            if not results["update_hospital_beds"].empty:
                results["update_hospital_beds"] = self._format_expiration_dates(
                    results["update_hospital_beds"]
                )
                results["update_hospital_beds"].to_excel(
                    writer, sheet_name="update_hospital_beds", index=False
                )
                logging.info(
                    f"Update hospital beds saved: {len(results['update_hospital_beds'])} records"
                )
            else:
                pd.DataFrame().to_excel(
                    writer, sheet_name="update_hospital_beds", index=False
                )
                logging.info("Update hospital beds sheet created (empty)")
            if not results["add_hospital_beds"].empty:
                results["add_hospital_beds"] = self._format_expiration_dates(
                    results["add_hospital_beds"]
                )
                results["add_hospital_beds"].to_excel(
                    writer, sheet_name="add_hospital_beds", index=False
                )
                logging.info(
                    f"Add hospital beds saved: {len(results['add_hospital_beds'])} records"
                )
            else:
                pd.DataFrame().to_excel(
                    writer, sheet_name="add_hospital_beds", index=False
                )
                logging.info("Add hospital beds sheet created (empty)")
        logging.info(f"Matched data saved to: {output_path}")

    def match_provider_facility_data(
        self,
        all_facilities_df,
        filtered_providers_df,
        save_output=True,
        output_filename="matched_provider_facility_data.xlsx",
    ):
        try:
            if not self._validate_input_data(all_facilities_df, filtered_providers_df):
                return {
                    "update_licenses": pd.DataFrame(),
                    "new_licenses": pd.DataFrame(),
                    "expired_licenses": pd.DataFrame(),
                    "update_hospital_beds": pd.DataFrame(),
                    "add_hospital_beds": pd.DataFrame(),
                }
            facilities_clean, providers_clean = self._prepare_data_for_matching(
                all_facilities_df, filtered_providers_df
            )
            update_licenses_df = self._find_update_licenses(
                facilities_clean, providers_clean
            )
            new_licenses_df = self._find_new_licenses(facilities_clean, providers_clean)
            expired_licenses_df = self._find_expired_licenses(
                facilities_clean, providers_clean
            )
            results = self._finalize_results(
                update_licenses_df, new_licenses_df, expired_licenses_df
            )
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
                "update_hospital_beds": pd.DataFrame(),
                "add_hospital_beds": pd.DataFrame(),
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
    matcher = DataMatcher()
    logging.info("DataMatcher class initialized successfully")
    logging.info(
        f"Expected facility columns: {list(matcher.facility_columns.values())}"
    )
    logging.info(
        f"Expected provider columns: {list(matcher.provider_columns.values())}"
    )
