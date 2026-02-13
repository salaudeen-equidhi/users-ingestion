"""
Custom validators for user ingestion fields (roles, dates, boundaries)
"""

import pandas as pd
from datetime import datetime
import re


def validate_roles(value, row, reference_data):
    """Check each role against the roles mapping file."""
    errors = []
    roles_map = reference_data.get("roles", {})

    if not roles_map:
        return errors

    if str(value).strip() == "" or pd.isna(value):
        return ["roles cannot be empty"]

    user_roles = [r.strip() for r in str(value).split(",")]
    invalid_roles = [r for r in user_roles if r not in roles_map]

    if invalid_roles:
        errors.append(f"Invalid roles: {', '.join(invalid_roles)}")

    return errors


def validate_date_of_joining(value, row, reference_data):
    """Validate date format (DD/MM/YYYY or DD-MM-YYYY) and check it's a real date."""
    date_str = str(value).strip()

    if date_str == "" or date_str.lower() == "nan" or pd.isna(value):
        return ["date_of_joining is required"]

    # Accepts both / and - separators
    pattern = r"^(0[1-9]|[12][0-9]|3[01])[/-](0[1-9]|1[0-2])[/-][0-9]{4}$"
    if not re.fullmatch(pattern, date_str):
        return ["date_of_joining must be in DD/MM/YYYY or DD-MM-YYYY format"]

    try:
        datetime.strptime(date_str, "%d/%m/%Y")
        return []
    except ValueError:
        try:
            datetime.strptime(date_str, "%d-%m-%Y")
            return []
        except ValueError:
            return ["Invalid date_of_joining"]


def validate_boundary(value, row, reference_data):
    """Make sure boundary_code and administrative_area match in boundary_template."""
    errors = []
    boundaries = reference_data.get("boundaries", {})

    if not boundaries:
        return errors

    boundary_code = str(value).strip()
    administrative_area = str(row.get("administrative_area", "")).strip()

    if boundary_code not in boundaries:
        errors.append(f"Invalid boundary_code: {boundary_code}")

    if administrative_area not in boundaries.values():
        errors.append(f"Invalid administrative_area: {administrative_area}")

    if not errors:
        expected_name = boundaries.get(boundary_code)
        if expected_name and administrative_area != expected_name:
            errors.append(
                f"administrative_area '{administrative_area}' does not match "
                f"boundary '{expected_name}' for code '{boundary_code}'"
            )

    return errors
