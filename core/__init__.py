"""
Utilities for validation and API upload
"""

from .validators import CSVValidator
from .api_client import APIClient
from .user_custom_validators import validate_roles, validate_date_of_joining, validate_date_of_birth, validate_boundary
import os
import pandas as pd
import json

_DIR = os.path.dirname(__file__)


def create_user_validator():
    """Set up a CSVValidator with roles, boundaries, and custom field checks."""
    with open(os.path.join(_DIR, "templates", "rolesmapping.json"), 'r') as f:
        roles_map = json.load(f)

    boundary_df = pd.read_csv(os.path.join(_DIR, "templates", "boundary_template.csv"))
    boundaries = {
        str(row["id"]).strip(): str(row["name"]).strip()
        for _, row in boundary_df.iterrows()
    }

    reference_data = {
        "roles": roles_map,
        "boundaries": boundaries
    }

    custom_validators = {
        "roles": validate_roles,
        "date_of_joining": validate_date_of_joining,
        "date_of_birth": validate_date_of_birth,
        "boundary_code": validate_boundary
    }

    return CSVValidator(
        schema_path=os.path.join(_DIR, "config", "user_validation_mdms_schema.json"),
        reference_data=reference_data,
        custom_validators=custom_validators
    )


__all__ = ['CSVValidator', 'APIClient', 'create_user_validator']
