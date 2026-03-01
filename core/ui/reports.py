"""
HTML summary report rendering for the User Ingestor notebook.
"""

import os
from datetime import datetime
from IPython.display import display, HTML


def render_summary(summary_data):
    """Display an HTML report based on the processing results."""
    if not summary_data:
        print(" No results yet. Run the cell above first to process a CSV.")
        return

    timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if summary_data['status'] == 'FAILED':
        summary = summary_data['summary']
        error_report = summary_data['error_report']

        display(HTML(f"""
        <div style='background-color: #FADBD8; padding: 20px; border-radius: 10px; border-left: 5px solid #E74C3C; margin: 20px 0;'>
            <h2 style='color: #E74C3C; margin-top: 0;'> VALIDATION FAILED</h2>
            <table style='width: 100%; border-collapse: collapse; background-color: white; margin-top: 15px;'>
                <tr style='background-color: #E74C3C; color: white;'>
                    <th style='padding: 10px; text-align: left;'>Metric</th>
                    <th style='padding: 10px; text-align: center;'>Count</th>
                </tr>
                <tr>
                    <td style='padding: 8px; border: 1px solid #ddd;'>Total Users</td>
                    <td style='padding: 8px; border: 1px solid #ddd; text-align: center; font-weight: bold;'>{summary['total_users']}</td>
                </tr>
                <tr>
                    <td style='padding: 8px; border: 1px solid #ddd;'> Valid Users</td>
                    <td style='padding: 8px; border: 1px solid #ddd; text-align: center; color: green; font-weight: bold;'>{summary['correct_users']}</td>
                </tr>
                <tr>
                    <td style='padding: 8px; border: 1px solid #ddd;'> Invalid Users</td>
                    <td style='padding: 8px; border: 1px solid #ddd; text-align: center; color: red; font-weight: bold;'>{summary['error_users']}</td>
                </tr>
            </table>
            <p style='margin-top: 15px; font-weight: bold;'> Fix errors and re-upload</p>
        </div>
        <h3 style='color: #E74C3C;'> Download Error Report:</h3>
        <div style="padding: 10px; background-color: white; border-radius: 5px; border: 1px solid #E74C3C; display: inline-block; margin-top: 10px;">
            <a href="{error_report}" download="{os.path.basename(error_report)}"
               style="display: inline-block; padding: 10px 20px; background-color: #E74C3C; color: white;
                      text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 14px;">
                 Download Error Report ({os.path.basename(error_report)})
            </a>
        </div>
        """))

    elif summary_data['status'] == 'SUCCESS':
        summary = summary_data['summary']
        success_count = summary_data['success_count']
        failed_count = summary_data['failed_count']
        final_report = summary_data['final_report']

        display(HTML(f"""
        <div style="font-family: Arial, sans-serif; padding: 20px; border: 2px solid #007bff; border-radius: 10px; background-color: #f8f9fa; margin: 20px 0;">
            <h2 style="color: #007bff; margin-top: 0;"> Data Upload Summary Report</h2>
            <p style="color: #666; margin-bottom: 10px;">Generated: {timestamp_str}</p>

            <div style="display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap;">
                <div style="text-align: center; padding: 15px; background-color: #d4edda; border-radius: 5px; flex: 1; margin: 5px; min-width: 150px;">
                    <div style="font-size: 32px; font-weight: bold; color: #155724;">{success_count}</div>
                    <div style="color: #155724;">Success</div>
                </div>
                <div style="text-align: center; padding: 15px; background-color: #f8d7da; border-radius: 5px; flex: 1; margin: 5px; min-width: 150px;">
                    <div style="font-size: 32px; font-weight: bold; color: #721c24;">{failed_count}</div>
                    <div style="color: #721c24;">Failed</div>
                </div>
            </div>
        </div>
        """))

        status_badge = ' ALL SUCCESS' if failed_count == 0 else ' HAS ERRORS'
        badge_color = '#28a745' if failed_count == 0 else '#dc3545'

        display(HTML(f"""
        <div style="margin: 20px 0; padding: 15px; background-color: #e7f3ff; border-left: 4px solid #007bff; border-radius: 5px;">
            <h3 style="margin-top: 0; color: #004085;"> Result CSV</h3>
            <p style="color: #004085; margin-bottom: 15px;">
                The CSV now has <b>api_status</b>, <b>api_status_code</b>, and <b>api_message</b> columns:
            </p>
            <div style="padding: 10px; background-color: white; border-radius: 5px; border: 1px solid #ddd;">
                <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
                    <div style="flex: 1; min-width: 300px;">
                        <strong style="color: #007bff;"> {os.path.basename(final_report)}</strong>
                        <span style="background-color: {badge_color}; color: white; padding: 3px 8px; border-radius: 3px; font-size: 11px; margin-left: 10px;">{status_badge}</span>
                        <br>
                    </div>
                    <a href="{final_report}" download="{os.path.basename(final_report)}"
                       style="display: inline-block; padding: 8px 16px; background-color: #007bff; color: white;
                              text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 14px; margin-top: 5px;">
                         Download Result File
                    </a>
                </div>
            </div>
            <p style="color: #004085; font-size: 12px; margin-top: 15px; border-top: 1px solid #bee5eb; padding-top: 10px;">
                <b> Status Values in CSV:</b>
            </p>
            <ul style="font-size: 12px; color: #004085; margin: 5px 0;">
                <li><span style="color: green; font-weight: bold;">SUCCESS:</span> User created successfully (or updated if already exists)</li>
                <li><span style="color: red; font-weight: bold;">FAILED:</span> API request failed - check api_message column for details</li>
                <li><span style="color: gray; font-weight: bold;">SKIPPED:</span> Validation failed (not uploaded)</li>
            </ul>
        </div>
        """))
