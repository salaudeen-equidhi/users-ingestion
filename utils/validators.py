"""
Generic CSV validator using MDMS JSON Schema
"""

import pandas as pd
import re
import json
from datetime import datetime
import os


class CSVValidator:
    """Validates CSV files against an MDMS JSON schema. Supports custom validation hooks."""

    def __init__(self, schema_path=None, reference_data=None, custom_validators=None):
        if not schema_path:
            default_schema = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'user_validation_mdms_schema.json')
            schema_path = default_schema

        try:
            with open(schema_path, 'r') as f:
                self.schema = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found at: {schema_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Schema file is not valid JSON: {schema_path}")

        self.required_fields = self.schema.get("required", [])
        self.unique_fields = self.schema.get("x-unique", [])
        self.properties = self.schema.get("properties", {})
        self.expected_cols = list(self.properties.keys())

        self.reference_data = reference_data or {}
        self.custom_validators = custom_validators or {}

    def validate_headers(self, df):
        """Check if CSV headers match what the schema expects."""
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
        """Validate a single field value against its schema definition."""
        errors = []

        if field_name not in self.properties:
            return errors

        field_schema = self.properties[field_name]
        value_str = str(value).strip()

        is_empty = value_str == "" or value_str.lower() == "nan" or pd.isna(value)

        if is_empty:
            if field_name in self.required_fields:
                errors.append(f"{field_name} is required")
            return errors

        field_type = field_schema.get("type")
        if isinstance(field_type, list):
            # Handle nullable types like ["string", "null"]
            if "null" in field_type and is_empty:
                return errors
            field_type = [t for t in field_type if t != "null"][0]

        if "pattern" in field_schema:
            pattern = field_schema["pattern"]
            if not re.fullmatch(pattern, value_str):
                errors.append(field_schema.get("description", f"Invalid {field_name}"))

        if "enum" in field_schema:
            allowed = [str(v) if v is not None else None for v in field_schema["enum"]]
            if value_str.upper() not in [v.upper() if v else None for v in allowed if v]:
                errors.append(f"{field_name} must be one of: {[v for v in allowed if v]}")

        if "minLength" in field_schema and len(value_str) < field_schema["minLength"]:
            errors.append(f"{field_name} must be at least {field_schema['minLength']} characters")

        if "maxLength" in field_schema and len(value_str) > field_schema["maxLength"]:
            errors.append(f"{field_name} exceeds maximum length of {field_schema['maxLength']}")

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
        """Find duplicate values in a column (ignores blanks)."""
        if field_name not in df.columns:
            return set()

        values = df[field_name].astype(str).str.strip()
        values = values[~values.isin(["", "nan", "None"])]

        counts = values.value_counts()
        return {val for val, count in counts.items() if count > 1}

    def validate_csv(self, csv_path):
        """Run all validations on a CSV and return the DataFrame with status columns."""
        df = pd.read_csv(csv_path)

        header_status, header_message = self.validate_headers(df)

        # Find duplicates for unique fields
        duplicate_values = {}
        for field in self.unique_fields:
            if field in df.columns:
                duplicate_values[field] = self.check_uniqueness(df, field)

        csv_headers = list(df.columns)
        output_rows = []
        final_headers = csv_headers + ["validation_status", "validation_errors"]
        output_rows.append(final_headers)

        correct_count = 0
        error_count = 0

        for idx, row in df.iterrows():
            error_list = []

            for field_name in csv_headers:
                if field_name not in self.properties:
                    continue

                # Schema validation
                field_errors = self.validate_field_against_schema(field_name, row[field_name])
                error_list.extend(field_errors)

                # Custom validator (if any)
                if field_name in self.custom_validators:
                    custom_errors = self.custom_validators[field_name](
                        row[field_name], row, self.reference_data
                    )
                    if custom_errors:
                        if isinstance(custom_errors, list):
                            error_list.extend(custom_errors)
                        else:
                            error_list.append(custom_errors)

                # Uniqueness check
                if field_name in self.unique_fields:
                    value = str(row[field_name]).strip()
                    if value and value not in ["", "nan", "None"] and value in duplicate_values[field_name]:
                        error_list.append(f"Duplicate {field_name}: {value}")

            status = "CORRECT" if not error_list else "ERROR"

            if status == "CORRECT":
                correct_count += 1
            else:
                error_count += 1

            row_data = [row[col] for col in csv_headers]
            row_data.extend([status, str(error_list)])
            output_rows.append(row_data)

        validated_df = pd.DataFrame(output_rows[1:], columns=output_rows[0])

        summary = {
            "header_status": header_status,
            "header_message": header_message,
            "total_rows": len(df),
            "correct_rows": correct_count,
            "error_rows": error_count,
            # Aliases used by the notebook
            "total_users": len(df),
            "correct_users": correct_count,
            "error_users": error_count
        }

        return validated_df, summary
