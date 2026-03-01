"""
CSV processing helpers and upload orchestration.
Extracted from notebook cell-2.
"""

import os
import glob
import shutil
import time
import pandas as pd

from core import create_user_validator
from core.api_client import APIClient


def clear_uploads_folder(exclude_file=None):
    """Remove old files from uploads/, keep the one we're about to process."""
    for file in glob.glob("uploads/*"):
        if os.path.isfile(file):
            try:
                if exclude_file and os.path.samefile(file, exclude_file):
                    continue
            except (FileNotFoundError, OSError):
                pass
            os.remove(file)


def resolve_file_path(path):
    path = path.strip()
    if os.path.isabs(path):
        return path
    return os.path.abspath(path)


def normalize_date(date_str):
    """Convert DD-MM-YYYY to DD/MM/YYYY so the API accepts it."""
    if pd.isna(date_str) or str(date_str).strip() == '' or str(date_str).lower() == 'nan':
        return date_str
    return str(date_str).strip().replace('-', '/')


def normalize_row_dates(row):
    for col in ['date_of_joining', 'date_of_birth']:
        if col in row.index:
            row[col] = normalize_date(row[col])
    return row


def cleanup_temp_files():
    """Remove any leftover temp upload files."""
    for f in glob.glob("temp_upload_*.csv"):
        try:
            os.remove(f)
        except OSError:
            pass


def process_csv(csv_path, api_url, tenant_id, auth_token, log):
    """
    Run validation and upload flow on a CSV file.

    Args:
        csv_path: Resolved absolute path to the CSV.
        api_url: API endpoint URL.
        tenant_id: Tenant identifier.
        auth_token: Auth token string.
        log: Callable that accepts a string message for output.

    Returns:
        A summary_data dict describing the outcome.
    """
    uploaded_filename = os.path.basename(csv_path)
    upload_path = f"uploads/{uploaded_filename}"

    # If file is already in uploads/, use it directly; otherwise copy it there
    source_in_uploads = csv_path.replace('\\', '/').find('/uploads/') != -1 or csv_path.startswith('uploads/')

    if source_in_uploads:
        clear_uploads_folder(exclude_file=csv_path)
        upload_path = csv_path
    else:
        clear_uploads_folder()
        shutil.copy(csv_path, upload_path)

    # --- Phase 1: Validation ---
    log("=" * 70)
    log("[PHASE 1] CSV VALIDATION")
    log("=" * 70)
    log(f"\n[INFO] File: {uploaded_filename}")
    log(f"[INFO] Source path: {csv_path}")
    log(f"[INFO] Working copy: {upload_path}")

    validator = create_user_validator()
    log(f"\n[VALIDATING] Running validation checks...")

    validated_df, summary = validator.validate_csv(upload_path)

    log(f"\n{'=' * 70}")
    log("[VALIDATION SUMMARY]")
    log(f"{'=' * 70}")
    log(f"[HEADER STATUS] {summary['header_status']}")
    if summary['header_status'] == 'ERROR':
        log(f"[HEADER ERROR] {summary['header_message']}")
    log(f"[TOTAL USERS] {summary['total_users']}")
    log(f"[ VALID USERS] {summary['correct_users']}")
    log(f"[ INVALID USERS] {summary['error_users']}")
    log(f"{'=' * 70}\n")

    if summary['error_users'] > 0 or summary['header_status'] == 'ERROR':
        # Validation failed
        base_name = uploaded_filename.rsplit('.', 1)[0]
        error_report_path = f"uploads/{base_name}_errors.csv"

        if summary['error_users'] > 0:
            error_df = validated_df[validated_df['validation_status'] == 'ERROR']
        else:
            error_df = validated_df
        error_df.to_csv(error_report_path, index=False)

        error_label = summary['header_message'] if summary['header_status'] == 'ERROR' else f"{summary['error_users']} errors"

        log(f"[FAILED]  Validation failed: {error_label}\n")
        log(f" Updating error report: {error_report_path}")
        log(f"    Error report saved successfully!")

        return {
            'status': 'FAILED',
            'summary': summary,
            'error_report': error_report_path,
            'status_label': f"<h3 style='color: red;'> Validation Failed: {error_label}. Run the Summary cell below!</h3>"
        }

    # --- Phase 2: API Upload ---
    log(f"[SUCCESS]  All validations passed!\n")
    log(f"{'=' * 70}")
    log(f"[PHASE 2] API UPLOAD ")
    log(f"{'=' * 70}\n")

    validated_df.to_csv(upload_path, index=False)

    log(f"[UPLOADING] User Data")
    log(f"   Total Users: {summary['total_users']}")
    log(f"   API URL: {api_url}")
    log(f"   Tenant ID: {tenant_id}")
    log(f"{'=' * 70}\n")

    client = APIClient(api_url, tenant_id, auth_token)

    base_name = uploaded_filename.rsplit('.', 1)[0]
    final_report_path = f"uploads/{base_name}_result.csv"

    df = pd.read_csv(upload_path)
    df['api_status'] = ''
    df['api_status_code'] = ''
    df['api_message'] = ''

    success_count = 0
    failed_count = 0
    skipped_count = 0

    for idx, row in df.iterrows():
        if row.get('validation_status') == 'CORRECT':
            log(f"[PROCESSING] Row {idx + 1}/{len(df)}: {row.get('username', 'N/A')}")

            row_normalized = normalize_row_dates(row.copy())

            temp_file = f"temp_upload_{idx}.csv"
            single_row_df = pd.DataFrame([row_normalized])
            single_row_df.to_csv(temp_file, index=False)

            result = client.upload_file(temp_file)

            df.at[idx, 'api_status'] = result['status']
            df.at[idx, 'api_status_code'] = str(result['status_code'])
            df.at[idx, 'api_message'] = result['message']

            if result['status'] == 'SUCCESS':
                success_count += 1
                log(f" -> SUCCESS (Status: {result['status_code']})")
            else:
                failed_count += 1
                log(f" -> FAILED (Status: {result['status_code']})")

            if os.path.exists(temp_file):
                os.remove(temp_file)

            time.sleep(5)
        else:
            df.at[idx, 'api_status'] = 'SKIPPED'
            df.at[idx, 'api_status_code'] = 'N/A'
            df.at[idx, 'api_message'] = 'Validation failed'
            skipped_count += 1

    df.to_csv(final_report_path, index=False)

    log(f"\n{'=' * 70}")
    log("[API UPLOAD SUMMARY]")
    log(f"{'=' * 70}")
    log(f"[TOTAL UPLOADED] {len(df)}")
    log(f"[SUCCESS] {success_count}")
    log(f"[FAILED] {failed_count}")
    log(f"[SKIPPED] {skipped_count}")
    log(f"{'=' * 70}\n")
    log(f" Updating result file: {final_report_path}")
    log(f"    Result file updated successfully!")
    log(f"    Updated {len(df)} rows\n")
    log(" DATA UPLOAD COMPLETED!")

    return {
        'status': 'SUCCESS',
        'summary': summary,
        'success_count': success_count,
        'failed_count': failed_count,
        'final_report': final_report_path,
        'status_label': f"<h3 style='color: green;'> Complete! {success_count} success, {failed_count} failed. Run Summary cell below!</h3>"
    }
