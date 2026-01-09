"""
CSV Validation Module
Handles all validation logic based on schema configuration
"""

import pandas as pd
import re
import json
from datetime import datetime
import os


class CSVValidator:
    """
    CSV Validator class that validates user data based on schema
    """

    def __init__(self, schema_path="config/validation_schema.json",
                 roles_mapping_path="templates/rolesmapping.json",
                 boundary_path="templates/boundary_template.csv"):
        """
        Initialize validator with schema and reference data

        Args:
            schema_path: Path to validation schema JSON
            roles_mapping_path: Path to roles mapping JSON
            boundary_path: Path to boundary CSV
        """
        # Load schema
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

        # Load roles mapping
        with open(roles_mapping_path, 'r') as f:
            self.roles_map = json.load(f)
        self.allowed_roles = set(self.roles_map.keys())

        # Load boundary data
        self.boundary_df = pd.read_csv(boundary_path)
        self.boundary_dict = {
            str(row["id"]).strip(): str(row["name"]).strip()
            for _, row in self.boundary_df.iterrows()
        }

        self.tenant_id = self.schema.get("tenant_id", "bi")
        self.expected_cols = self.schema.get("expected_columns", [])
        self.mandatory_cols = self.schema.get("mandatory_columns", [])
        self.validation_rules = self.schema.get("validation_rules", {})

    def validate_headers(self, df):
        """
        Validate CSV headers against expected columns

        Args:
            df: pandas DataFrame

        Returns:
            tuple: (status, message)
        """
        csv_headers = list(df.columns)
        missing = set(self.expected_cols) - set(csv_headers)
        extra = set(csv_headers) - set(self.expected_cols)

        if self.expected_cols == csv_headers:
            return "CORRECT", "Headers match. No issues."
        else:
            msg = ""
            if missing:
                msg += f"Missing columns: {list(missing)}. "
            if extra:
                msg += f"Extra columns: {list(extra)}."
            return "ERROR", msg.strip()

    def validate_field(self, field_name, value, rule):
        """
        Validate a single field based on its rule

        Args:
            field_name: Name of the field
            value: Value to validate
            rule: Validation rule from schema

        Returns:
            str or None: Error message if validation fails, None otherwise
        """
        value_str = str(value).strip()

        # Handle empty values
        if value_str == "" or value_str.lower() == "nan":
            if rule.get("required", False):
                return f"{field_name} is required"
            return None

        validation_type = rule.get("type")

        if validation_type == "regex":
            pattern = rule.get("pattern")
            if not re.fullmatch(pattern, value_str):
                return rule.get("error_message", f"Invalid {field_name}")

        elif validation_type == "enum":
            allowed_values = rule.get("allowed_values", [])
            if value_str.upper() not in [v.upper() for v in allowed_values]:
                return rule.get("error_message", f"Invalid {field_name}")

        elif validation_type == "date":
            date_format = rule.get("format", "%d/%m/%Y")
            try:
                datetime.strptime(value_str, date_format)
            except:
                return rule.get("error_message", f"Invalid date format for {field_name}")

        return None

    def validate_date_of_joining(self, date_str):
        """
        Validate and correct date of joining

        Args:
            date_str: Date string to validate

        Returns:
            tuple: (corrected_date, error_message)
        """
        today = datetime.now().strftime("%d/%m/%Y")
        date_str = str(date_str).strip()

        if date_str == "" or date_str.lower() == "nan":
            return today, None

        try:
            datetime.strptime(date_str, "%d/%m/%Y")
            return date_str, None
        except:
            return today, "date_of_joining must be in DD/MM/YYYY format"

    def validate_roles(self, role_string):
        """
        Validate roles against roles mapping

        Args:
            role_string: Comma-separated roles

        Returns:
            str or None: Error message if validation fails
        """
        if str(role_string).strip() == "":
            return "roles cannot be empty"

        user_roles = [r.strip() for r in str(role_string).split(",")]
        errors = [f"Invalid role: {r}" for r in user_roles if r not in self.allowed_roles]

        return ", ".join(errors) if errors else None

    def validate_boundary(self, boundary_code, administrative_area):
        """
        Validate boundary code and administrative area

        Args:
            boundary_code: Boundary code
            administrative_area: Administrative area name

        Returns:
            str or None: Error message if validation fails
        """
        boundary_code = str(boundary_code).strip()
        administrative_area = str(administrative_area).strip()

        errors = []

        if boundary_code not in self.boundary_dict:
            errors.append(f"Invalid boundary_code: {boundary_code}")

        if administrative_area not in self.boundary_dict.values():
            errors.append(f"Invalid administrative_area: {administrative_area}")

        if errors:
            return ", ".join(errors)

        expected_name = self.boundary_dict.get(boundary_code)

        if expected_name and administrative_area != expected_name:
            errors.append(
                f"administrative_area '{administrative_area}' does not match "
                f"boundary '{expected_name}' for code '{boundary_code}'"
            )

        return ", ".join(errors) if errors else None

    def validate_csv(self, csv_path):
        """
        Validate entire CSV file

        Args:
            csv_path: Path to CSV file

        Returns:
            tuple: (validated_df, summary_dict)
        """
        # Read CSV
        df = pd.read_csv(csv_path)

        # Validate headers
        header_status, header_message = self.validate_headers(df)

        # Check for duplicate mobile numbers
        mobile_counts = df["mobile_number"].astype(str).value_counts()
        duplicate_mobiles = {m for m, c in mobile_counts.items() if c > 1}

        # Process rows
        output_rows = []
        csv_headers = list(df.columns)
        final_headers = csv_headers + ["validation_status", "validation_errors"]
        output_rows.append(final_headers)

        correct_count = 0
        error_count = 0

        for idx, row in df.iterrows():
            error_list = []

            # Validate each field based on schema
            for field_name, rule in self.validation_rules.items():
                if field_name not in df.columns:
                    continue

                validation_type = rule.get("type")

                # Standard validations
                if validation_type in ["regex", "enum"]:
                    err = self.validate_field(field_name, row[field_name], rule)
                    if err:
                        error_list.append(err)

                # Date validations
                elif validation_type == "date" and field_name == "date_of_joining":
                    corrected_date, err = self.validate_date_of_joining(row[field_name])
                    if err:
                        error_list.append(err)
                    row[field_name] = corrected_date

                elif validation_type == "date" and field_name == "date_of_birth":
                    err = self.validate_field(field_name, row[field_name], rule)
                    if err:
                        error_list.append(err)

                # Custom validations
                elif validation_type == "custom":
                    if field_name == "roles":
                        err = self.validate_roles(row[field_name])
                        if err:
                            error_list.append(err)

                    elif field_name == "mobile_number":
                        mobile = str(row["mobile_number"]).strip()
                        if mobile in duplicate_mobiles:
                            error_list.append("Duplicate mobile_number inside CSV")

                    elif field_name in ["boundary_code", "administrative_area"]:
                        # Validate boundary only once
                        if field_name == "boundary_code" and "administrative_area" in df.columns:
                            err = self.validate_boundary(
                                row["boundary_code"],
                                row["administrative_area"]
                            )
                            if err:
                                error_list.append(err)

            # Determine status
            status = "CORRECT" if not error_list else "ERROR"

            if status == "CORRECT":
                correct_count += 1
            else:
                error_count += 1

            # Append row with validation results
            row_data = [row[col] for col in csv_headers]
            row_data.extend([status, str(error_list)])
            output_rows.append(row_data)

        # Create output DataFrame
        validated_df = pd.DataFrame(output_rows[1:], columns=output_rows[0])

        # Summary
        summary = {
            "header_status": header_status,
            "header_message": header_message,
            "total_users": len(df),
            "correct_users": correct_count,
            "error_users": error_count
        }

        return validated_df, summary
