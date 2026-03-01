"""
API client for DHIS2 user ingestion
"""

import requests
import json
from datetime import datetime


class APIClient:
    """Handles uploading user CSV files to the DHIS2 ingestion endpoint."""

    def __init__(self, api_url, tenant_id="bi", auth_token=None):
        self.api_url = api_url
        self.tenant_id = tenant_id
        self.auth_token = auth_token or "ee36fdd7-64e7-4583-9c16-998479ff53c0"

        self.user_info = {
            "id": 97,
            "userName": "ab-prd",
            "salutation": None,
            "name": "System User",
            "gender": None,
            "mobileNumber": "9999999999",
            "emailId": None,
            "altContactNumber": None,
            "pan": None,
            "aadhaarNumber": None,
            "permanentAddress": None,
            "permanentCity": None,
            "permanentPinCode": None,
            "correspondenceAddress": None,
            "correspondenceCity": None,
            "correspondencePinCode": None,
            "alternatemobilenumber": None,
            "active": True,
            "locale": None,
            "type": "EMPLOYEE",
            "accountLocked": False,
            "accountLockedDate": 0,
            "fatherOrHusbandName": None,
            "relationship": None,
            "signature": None,
            "bloodGroup": None,
            "photo": None,
            "identificationMark": None,
            "createdBy": 23287,
            "lastModifiedBy": 23287,
            "tenantId": self.tenant_id,
            "roles": self._get_default_roles(),
            "uuid": f"{self.tenant_id}-prd",
            "createdDate": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "lastModifiedDate": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "dob": None,
            "pwdExpiryDate": None
        }

    def _get_default_roles(self):
        return [
            {"code": "SUPERVISOR", "name": "Supervisor", "tenantId": self.tenant_id},
            {"code": "DISTRICT_SUPERVISOR", "name": "District Supervisor", "tenantId": self.tenant_id},
            {"code": "SYSTEM_ADMINISTRATOR", "name": "System Administrator", "tenantId": self.tenant_id},
            {"code": "SUPERUSER", "name": "Super User", "tenantId": self.tenant_id},
            {"code": "NATIONAL_SUPERVISOR", "name": "National Supervisor", "tenantId": self.tenant_id},
            {"code": "DISTRIBUTOR", "name": "Distributor", "tenantId": self.tenant_id},
            {"code": "WAREHOUSE_MANAGER", "name": "Warehouse Manager", "tenantId": self.tenant_id},
            {"code": "REGISTRAR", "name": "Registrar", "tenantId": self.tenant_id},
            {"code": "PROVINCIAL_SUPERVISOR", "name": "Provincial Supervisor", "tenantId": self.tenant_id}
        ]

    def _build_payload(self):
        return {
            "DHIS2IngestionRequest": json.dumps({
                "tenantId": self.tenant_id,
                "dataType": "Users",
                "requestInfo": {
                    "authToken": self.auth_token,
                    "userInfo": self.user_info
                }
            })
        }

    def _check_if_user_exists(self, response_text, status_code):
        """Check if the API error is about a user that already exists."""
        exists_patterns = [
            "already exists",
            "already exist",
            "duplicate",
            "user exists",
            "username already",
            "conflict",
            "err_hrms_user_exist",
            "user_exist_mob",
        ]

        response_lower = response_text.lower()

        if status_code == 409:
            return True

        return any(pattern in response_lower for pattern in exists_patterns)

    def _parse_api_response(self, response_text):
        """
        Parse the response body for real success/failure.
        The API sometimes returns HTTP 200 but has errors inside the JSON.
        """
        try:
            data = json.loads(response_text)
            job_status = data.get("jobStatus", "")
            errors = data.get("errors", [])
            response_status = data.get("ResponseInfo", {}).get("status", "")

            is_success = (
                job_status in ["Completed", "Success"] or
                (response_status == "Success" and not errors and job_status != "Partial Completed")
            )

            return {
                "success": is_success,
                "errors": errors,
                "job_status": job_status,
                "response_status": response_status
            }
        except json.JSONDecodeError:
            # Not JSON, just go by HTTP status
            return {
                "success": True,
                "errors": [],
                "job_status": "",
                "response_status": ""
            }

    def _upload_to_endpoint(self, file_path, endpoint_url):
        """Send the file to the API and return parsed result."""
        payload = self._build_payload()
        headers = {'Accept': 'application/json'}

        try:
            with open(file_path, 'rb') as f:
                files = [('file', ('file', f, 'application/octet-stream'))]
                response = requests.post(
                    endpoint_url,
                    headers=headers,
                    data=payload,
                    files=files,
                    timeout=60
                )

            parsed = self._parse_api_response(response.text)

            if response.status_code == 200 and parsed["success"]:
                status = "SUCCESS"
            else:
                # Could be HTTP 200 with errors in body, or a non-200 code
                status = "ERROR"

            if parsed["errors"]:
                error_msg = "; ".join(parsed["errors"]) if isinstance(parsed["errors"], list) else str(parsed["errors"])
            else:
                error_msg = response.text

            return {
                "status": status,
                "status_code": response.status_code,
                "message": error_msg if status == "ERROR" else response.text,
                "job_status": parsed["job_status"]
            }

        except requests.exceptions.Timeout:
            return {
                "status": "ERROR",
                "status_code": 408,
                "message": "Request timeout",
                "job_status": ""
            }
        except requests.exceptions.RequestException as e:
            return {
                "status": "ERROR",
                "status_code": 500,
                "message": str(e),
                "job_status": ""
            }
        except Exception as e:
            return {
                "status": "ERROR",
                "status_code": 500,
                "message": f"Unexpected error: {str(e)}",
                "job_status": ""
            }

    def upload_file(self, file_path):
        """Upload a single CSV file. Returns SUCCESS or FAILED."""
        result = self._upload_to_endpoint(file_path, self.api_url)
        if result["status"] == "SUCCESS":
            result["status"] = "SUCCESS"
        else:
            result["status"] = "FAILED"
            if self._check_if_user_exists(result["message"], result["status_code"]):
                result["message"] = f"User already exists. {result['message']}"

        return result

