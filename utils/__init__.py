"""
Utilities module
Provides easy access to validators and API clients
"""

from .validators import CSVValidator
from .api_client import APIClient
from .user_custom_validators import validate_roles, validate_date_of_joining, validate_boundary
import pandas as pd
import json


def create_user_validator():
    """
    Factory function to create a pre-configured validator for user ingestion

    Returns:
        CSVValidator: Configured validator for user data
    """
    # Load reference data
    with open("templates/rolesmapping.json", 'r') as f:
        roles_map = json.load(f)

    boundary_df = pd.read_csv("templates/boundary_template.csv")
    boundaries = {
        str(row["id"]).strip(): str(row["name"]).strip()
        for _, row in boundary_df.iterrows()
    }

    # Setup reference data
    reference_data = {
        "roles": roles_map,
        "boundaries": boundaries
    }

    # Setup custom validators
    custom_validators = {
        "roles": validate_roles,
        "date_of_joining": validate_date_of_joining,
        "boundary_code": validate_boundary
    }

    # Return configured validator
    return CSVValidator(
        schema_path="config/user_validation_mdms_schema.json",
        reference_data=reference_data,
        custom_validators=custom_validators
    )


__all__ = ['CSVValidator', 'APIClient', 'create_user_validator']
