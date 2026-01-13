"""
Generic MDMS CSV Validation Module
Plug-and-play validator for any MDMS JSON Schema
"""

import pandas as pd
import re
import json
from datetime import datetime
import os


class CSVValidator:
    """
   CSV Validator that works with any MDMS JSON Schema
    Supports custom validation hooks for domain-specific logic
    """

    def __init__(self, schema_path=None, reference_data=None, custom_validators=None):
        """
        Initialize generic validator with MDMS schema

        Args:
            schema_path: Path to MDMS JSON Schema
            reference_data: Dict of reference data for lookups
                           e.g., {"roles": {...}, "boundaries": {...}}
            custom_validators: Dict of custom validation functions
                              e.g., {"field_name": validation_function}
        """
        # Determine schema path: use provided path or default project schema
        if not schema_path:
            default_schema = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'user_validation_mdms_schema.json')
            schema_path = default_schema

        # Load MDMS schema
        try:
            with open(schema_path, 'r') as f:
                self.schema = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found at: {schema_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Schema file is not valid JSON: {schema_path}")

        # Extract metadata from MDMS schema
        self.required_fields = self.schema.get("required", [])
        self.unique_fields = self.schema.get("x-unique", [])
        self.properties = self.schema.get("properties", {})
        self.expected_cols = list(self.properties.keys())

        # Store reference data for custom validations
        self.reference_data = reference_data or {}

        # Store custom validators (field_name -> validation_function)
        self.custom_validators = custom_validators or {}

    def validate_headers(self, df):
        """
        Validate CSV headers against MDMS schema properties

        Args:
            df: pandas DataFrame

        Returns:
            tuple: (status, message)
        """
        csv_headers = list(df.columns)
        missing = set(self.expected_cols) - set(csv_headers)
        extra = set(csv_headers) - set(self.expected_cols)

        if not missing and not extra:
            return "CORRECT", "Headers match schema. No issues."
        else:
            msg = ""
            if missing:
                msg += f"Missing columns: {list(missing)}. "
            if extra:
                msg += f"Extra columns: {list(extra)}."
            return "ERROR", msg.strip()

    def validate_field_against_schema(self, field_name, value):
        """
        Validate a single field against MDMS JSON schema definition

        Args:
            field_name: Name of the field
            value: Value to validate

        Returns:
            list: List of error messages
        """
        errors = []

        if field_name not in self.properties:
            return errors

        field_schema = self.properties[field_name]
        value_str = str(value).strip()

        # Handle null/empty values
        is_empty = value_str == "" or value_str.lower() == "nan" or pd.isna(value)

        if is_empty:
            if field_name in self.required_fields:
                errors.append(f"{field_name} is required")
            return errors

        # Check type
        field_type = field_schema.get("type")
        if isinstance(field_type, list):
            # Handle nullable types like ["string", "null"]
            if "null" in field_type and is_empty:
                return errors
            field_type = [t for t in field_type if t != "null"][0]

        # Validate pattern
        if "pattern" in field_schema:
            pattern = field_schema["pattern"]
            if not re.fullmatch(pattern, value_str):
                errors.append(field_schema.get("description", f"Invalid {field_name}"))

        # Validate enum
        if "enum" in field_schema:
            allowed = [str(v) if v is not None else None for v in field_schema["enum"]]
            if value_str.upper() not in [v.upper() if v else None for v in allowed if v]:
                errors.append(f"{field_name} must be one of: {[v for v in allowed if v]}")

        # Validate min/max length
        if "minLength" in field_schema and len(value_str) < field_schema["minLength"]:
            errors.append(f"{field_name} must be at least {field_schema['minLength']} characters")

        if "maxLength" in field_schema and len(value_str) > field_schema["maxLength"]:
            errors.append(f"{field_name} exceeds maximum length of {field_schema['maxLength']}")

        # Validate numeric range
        if field_type == "number":
            try:
                num_value = float(value_str)
                if "minimum" in field_schema and num_value < field_schema["minimum"]:
                    errors.append(f"{field_name} must be >= {field_schema['minimum']}")
                if "maximum" in field_schema and num_value > field_schema["maximum"]:
                    errors.append(f"{field_name} must be <= {field_schema['maximum']}")
            except ValueError:
                errors.append(f"{field_name} must be a number")

        return errors


    def check_uniqueness(self, df, field_name):
        """
        Check for duplicate values in unique fields

        Args:
            df: DataFrame to check
            field_name: Field to check for duplicates

        Returns:
            set: Set of duplicate values
        """
        if field_name not in df.columns:
            return set()

        # Filter out empty/null values
        values = df[field_name].astype(str).str.strip()
        values = values[~values.isin(["", "nan", "None"])]

        counts = values.value_counts()
        return {val for val, count in counts.items() if count > 1}

    def validate_csv(self, csv_path):
        """
        Validate entire CSV file using MDMS schema 

        Args:
            csv_path: Path to CSV file

        Returns:
            tuple: (validated_df, summary_dict)
        """
        # Read CSV
        df = pd.read_csv(csv_path)

        # Validate headers
        header_status, header_message = self.validate_headers(df)

        # Check uniqueness constraints
        duplicate_values = {}
        for field in self.unique_fields:
            if field in df.columns:
                duplicate_values[field] = self.check_uniqueness(df, field)

        # Prepare output
        csv_headers = list(df.columns)
        output_rows = []
        final_headers = csv_headers + ["validation_status", "validation_errors"]
        output_rows.append(final_headers)

        correct_count = 0
        error_count = 0

        # Validate each row
        for idx, row in df.iterrows():
            error_list = []

            # Validate each field
            for field_name in csv_headers:
                if field_name not in self.properties:
                    continue

                # 1. Standard MDMS schema validation
                field_errors = self.validate_field_against_schema(field_name, row[field_name])
                error_list.extend(field_errors)

                # 2. Custom validator hook (if provided)
                if field_name in self.custom_validators:
                    custom_errors = self.custom_validators[field_name](
                        row[field_name], row, self.reference_data
                    )
                    if custom_errors:
                        if isinstance(custom_errors, list):
                            error_list.extend(custom_errors)
                        else:
                            error_list.append(custom_errors)

                # 3. Check uniqueness
                if field_name in self.unique_fields:
                    value = str(row[field_name]).strip()
                    if value and value not in ["", "nan", "None"] and value in duplicate_values[field_name]:
                        error_list.append(f"Duplicate {field_name}: {value}")

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
            "total_rows": len(df),
            "correct_rows": correct_count,
            "error_rows": error_count,
            # Backwards-compatible aliases expected by caller notebooks
            "total_users": len(df),
            "correct_users": correct_count,
            "error_users": error_count
        }

        return validated_df, summary
