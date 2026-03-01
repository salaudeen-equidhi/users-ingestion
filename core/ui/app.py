"""
Interactive widget-based UI for the User Ingestor notebook.
"""

import os
import glob
import ipywidgets as widgets
from IPython.display import display, HTML, clear_output

from core.utils.processor import process_csv, resolve_file_path, cleanup_temp_files


class UserIngestorApp:
    """Encapsulates all widgets, layout, and button handlers for the ingestor UI."""

    def __init__(self):
        self.summary_data = None
        self._build_widgets()

    def _build_widgets(self):
        # --- Config inputs ---
        self.api_url_input = widgets.Text(
            value='http://hcm-moz-impl.egov:8080/hcm-moz-impl/v1/dhis2/users/ingest?source=EXCEL',
            placeholder='Enter API URL',
            description='API URL:',
            style={'description_width': '120px'},
            layout=widgets.Layout(width='800px')
        )
        self.tenant_id_input = widgets.Text(
            value='bi',
            placeholder='Enter Tenant ID',
            description='Tenant ID:',
            style={'description_width': '120px'},
            layout=widgets.Layout(width='300px')
        )
        # --- File browser ---
        self.file_dropdown = widgets.Dropdown(
            options=[('-- Click Refresh to scan --', '')],
            value='',
            description='Select File:',
            style={'description_width': '120px'},
            layout=widgets.Layout(width='450px')
        )

        self.refresh_button = widgets.Button(
            description='Refresh',
            button_style='info',
            icon='refresh',
            layout=widgets.Layout(width='100px')
        )
        self.refresh_button.on_click(self._refresh_file_list)

        self.file_path_input = widgets.Text(
            value='',
            placeholder='Or type path manually: templates/users.csv',
            description='Manual Path:',
            style={'description_width': '120px'},
            layout=widgets.Layout(width='450px')
        )

        self.process_button = widgets.Button(
            description=' Process & Upload',
            button_style='primary',
            icon='upload',
            layout=widgets.Layout(width='250px', height='50px')
        )
        self.process_button.on_click(self._on_process_click)

        self.process_output = widgets.Output()
        self.status_label = widgets.HTML(
            value="<h3 style='color: #3498DB;'> Ready to process CSV</h3>"
        )

        # Initial file scan
        self._refresh_file_list()

    # ---- Handlers ----

    def _refresh_file_list(self, b=None):
        csv_files = sorted(set(glob.glob('uploads/*.csv')))
        if csv_files:
            self.file_dropdown.options = [('-- Select a CSV file --', '')] + [(f, f) for f in csv_files]
        else:
            self.file_dropdown.options = [('No CSV files found', '')]
        self.file_dropdown.value = ''

    def _on_process_click(self, b):
        with self.process_output:
            clear_output()

        input_path = self.file_dropdown.value if self.file_dropdown.value else self.file_path_input.value.strip()

        if not input_path:
            self.status_label.value = "<h3 style='color: red;'> Please select a CSV file or enter a path!</h3>"
            return

        csv_path = resolve_file_path(input_path)

        if not os.path.exists(csv_path):
            self.status_label.value = f"<h3 style='color: red;'> File not found: {input_path}</h3>"
            with self.process_output:
                print(f"[ERROR] File not found")
                print(f"   Input path: {input_path}")
                print(f"   Resolved to: {csv_path}")
                print(f"\n[TIP] Click 'Refresh' to scan for available CSV files")
            return

        self.process_button.disabled = True
        self.status_label.value = "<h3 style='color: orange;'> Processing...</h3>"

        def log(msg):
            with self.process_output:
                print(msg)

        try:
            self.summary_data = process_csv(
                csv_path,
                self.api_url_input.value,
                self.tenant_id_input.value,
                None,
                log,
                output_widget=self.process_output
            )
            self.status_label.value = self.summary_data['status_label']

        except Exception as e:
            with self.process_output:
                print(f"\n Error: {str(e)}")
                import traceback
                traceback.print_exc()
            self.status_label.value = f"<h3 style='color: red;'> Process failed: {str(e)}</h3>"

        finally:
            self.process_button.disabled = False
            cleanup_temp_files()

    # ---- Display ----

    def display(self):
        display(HTML("<hr><h2 style='color: #2E86C1;'> Configuration</h2>"))
        display(widgets.VBox([
            self.api_url_input,
            self.tenant_id_input,
        ]))
        display(HTML("<hr><h2 style='color: #2E86C1;'> CSV File</h2>"))
        display(HTML("<p style='color: #666;'>Select from dropdown or type path manually:</p>"))
        display(widgets.HBox([self.file_dropdown, self.refresh_button]))
        display(self.file_path_input)
        display(self.process_button)
        display(self.status_label)
        display(HTML("<hr><h2 style='color: #2E86C1;'> Processing Logs</h2>"))
        display(self.process_output)
        print("\n Application ready! Select a CSV file and click 'Process & Upload'")
