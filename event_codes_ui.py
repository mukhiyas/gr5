"""
Event Codes Management UI
User interface for configuring all event codes, risk scores, and definitions
"""

from nicegui import ui
from typing import List, Dict, Any
import logging
from database_driven_codes import database_driven_codes

logger = logging.getLogger(__name__)

class EventCodesUI:
    """User interface for managing event codes configuration"""
    
    def __init__(self):
        """Initialize the event codes UI"""
        self.current_filter = 'all'
        self.search_term = ''
        self.table_container = None
        self.stats_container = None
    
    def create_event_codes_interface(self):
        """Create the complete event codes management interface"""
        with ui.column().classes('w-full gap-6 p-6'):
            # Header
            with ui.row().classes('w-full items-center mb-6'):
                ui.icon('code').classes('text-3xl text-primary')
                ui.label('Event Codes Configuration').classes('text-2xl font-bold')
            
            # Description
            with ui.card().classes('w-full p-4 bg-blue-50'):
                ui.label('Event Codes Management').classes('text-lg font-semibold mb-2')
                ui.label('Configure risk scores, names, and descriptions for all event codes. Changes are automatically saved and applied to risk calculations.').classes('text-gray-700')
            
            # Statistics
            self.stats_container = ui.row().classes('w-full gap-4 mb-4')
            self._update_stats()
            
            # Controls
            self._create_controls()
            
            # Table container
            self.table_container = ui.column().classes('w-full')
            
            # Initial table load
            self._render_codes_table()
    
    def _update_stats(self):
        """Update statistics display"""
        try:
            self.stats_container.clear()
            
            all_codes = database_driven_codes.get_all_codes_summary()
            
            with self.stats_container:
                # Total codes
                with ui.card().classes('flex-1 p-4 text-center'):
                    ui.label('Total Codes').classes('text-sm text-gray-600')
                    ui.label(str(len(all_codes))).classes('text-2xl font-bold text-blue-600')
                
                # User customized
                user_customized = len([c for c in all_codes if c['user_customized']])
                with ui.card().classes('flex-1 p-4 text-center'):
                    ui.label('User Customized').classes('text-sm text-gray-600')
                    ui.label(str(user_customized)).classes('text-2xl font-bold text-green-600')
                
                # Severity breakdown
                severities = {'critical': 0, 'valuable': 0, 'investigative': 0, 'probative': 0, 'unknown': 0}
                for code in all_codes:
                    severity = code['severity']
                    severities[severity] = severities.get(severity, 0) + 1
                
                with ui.card().classes('flex-1 p-4 text-center'):
                    ui.label('Critical Risk').classes('text-sm text-gray-600')
                    ui.label(str(severities['critical'])).classes('text-2xl font-bold text-red-600')
                
                with ui.card().classes('flex-1 p-4 text-center'):
                    ui.label('Unknown/Zero Score').classes('text-sm text-gray-600')
                    unknown_count = len([c for c in all_codes if c['risk_score'] == 0])
                    ui.label(str(unknown_count)).classes('text-2xl font-bold text-orange-600')
        
        except Exception as e:
            logger.error(f"Error updating stats: {e}")
    
    def _create_controls(self):
        """Create search and filter controls"""
        with ui.card().classes('w-full p-4 mb-4'):
            with ui.row().classes('w-full items-center gap-4'):
                # Search
                search_input = ui.input(
                    placeholder='Search codes, names, or descriptions...',
                    value=self.search_term
                ).classes('flex-grow')
                search_input.on('input', self._handle_search)
                
                # Filter by severity
                severity_filter = ui.select(
                    options=['all', 'critical', 'valuable', 'investigative', 'probative', 'unknown'],
                    value=self.current_filter,
                    label='Filter by Severity'
                ).classes('w-48')
                severity_filter.on('change', self._handle_filter)
                
                # Add new code button
                ui.button(
                    'Add New Code',
                    on_click=self._show_add_code_dialog,
                    icon='add'
                ).props('color=primary')
                
                # Refresh from database
                ui.button(
                    'Refresh from Database',
                    on_click=self._refresh_from_database,
                    icon='refresh'
                ).props('flat color=primary')
                
                # Export/Import buttons
                ui.button(
                    'Export Config',
                    on_click=self._export_configuration,
                    icon='download'
                ).props('flat')
                
                ui.button(
                    'Import Config',
                    on_click=self._show_import_dialog,
                    icon='upload'
                ).props('flat')
    
    def _render_codes_table(self):
        """Render the event codes table"""
        try:
            self.table_container.clear()
            
            # Get filtered data
            all_codes = database_driven_codes.get_all_codes_summary()
            filtered_codes = self._filter_codes(all_codes)
            
            if not filtered_codes:
                with self.table_container:
                    ui.label('No codes match your search criteria').classes('text-center text-gray-500 p-8')
                return
            
            # Prepare table data
            table_rows = []
            for code_info in filtered_codes:
                table_rows.append({
                    'code': code_info['code'],
                    'name': code_info['name'],
                    'risk_score': code_info['risk_score'],
                    'severity': code_info['severity'],
                    'usage_count': code_info['usage_count'],
                    'frequency_rank': code_info['frequency_rank'],
                    'user_customized': '✓' if code_info['user_customized'] else '',
                    'description': code_info['description'],
                    '_full_data': code_info
                })
            
            # Column definitions
            columns = [
                {'name': 'code', 'label': 'Code', 'field': 'code', 'sortable': True, 'align': 'left'},
                {'name': 'name', 'label': 'Name', 'field': 'name', 'sortable': True, 'align': 'left'},
                {'name': 'risk_score', 'label': 'Risk Score', 'field': 'risk_score', 'sortable': True, 'align': 'right'},
                {'name': 'severity', 'label': 'Severity', 'field': 'severity', 'sortable': True, 'align': 'center'},
                {'name': 'usage_count', 'label': 'Usage Count', 'field': 'usage_count', 'sortable': True, 'align': 'right'},
                {'name': 'frequency_rank', 'label': 'Rank', 'field': 'frequency_rank', 'sortable': True, 'align': 'right'},
                {'name': 'user_customized', 'label': 'Custom', 'field': 'user_customized', 'align': 'center'},
                {'name': 'description', 'label': 'Description', 'field': 'description', 'align': 'left'},
            ]
            
            # Create table
            with self.table_container:
                table = ui.table(
                    columns=columns,
                    rows=table_rows,
                    row_key='code'
                ).classes('w-full')
                
                # Enhanced styling
                table.style('''
                    .q-table {
                        border-radius: 8px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                    }
                    .q-table__container {
                        max-height: 600px;
                        overflow-y: auto;
                    }
                    .q-table thead th {
                        background-color: #f8f9fa;
                        font-weight: 600;
                        position: sticky;
                        top: 0;
                        z-index: 10;
                    }
                    .q-table tbody tr:hover {
                        background-color: #f3f4f6;
                    }
                    .severity-critical { background-color: #fee2e2; color: #dc2626; }
                    .severity-valuable { background-color: #fed7aa; color: #ea580c; }
                    .severity-investigative { background-color: #fef3c7; color: #d97706; }
                    .severity-probative { background-color: #dcfce7; color: #16a34a; }
                    .severity-unknown { background-color: #f3f4f6; color: #6b7280; }
                ''')
                
                # Row click handler
                table.on('rowClick', self._handle_row_click)
                
                # Add action buttons for each row
                table.add_slot('body-cell-code', '''
                    <q-td key="code" :props="props">
                        <span class="font-bold">{{ props.value }}</span>
                        <q-btn flat size="sm" icon="edit" @click="$parent.$emit('edit-code', props.row)" />
                    </q-td>
                ''')
                
            logger.info(f"✅ Rendered table with {len(table_rows)} codes")
            
        except Exception as e:
            logger.error(f"❌ Error rendering codes table: {e}", exc_info=True)
            with self.table_container:
                ui.label(f"Error rendering table: {str(e)}").classes('text-red-500 p-4')
    
    def _filter_codes(self, codes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter codes based on search term and severity filter"""
        filtered = codes
        
        # Apply search filter
        if self.search_term:
            search_lower = self.search_term.lower()
            filtered = [
                code for code in filtered
                if (search_lower in code['code'].lower() or
                    search_lower in code['name'].lower() or
                    search_lower in code['description'].lower())
            ]
        
        # Apply severity filter
        if self.current_filter != 'all':
            if self.current_filter == 'unknown':
                filtered = [code for code in filtered if code['risk_score'] == 0]
            else:
                filtered = [code for code in filtered if code['severity'] == self.current_filter]
        
        return filtered
    
    def _handle_search(self, e):
        """Handle search input"""
        self.search_term = e.value if hasattr(e, 'value') else str(e)
        self._render_codes_table()
        self._update_stats()
    
    def _handle_filter(self, e):
        """Handle severity filter change"""
        self.current_filter = e.value if hasattr(e, 'value') else str(e)
        self._render_codes_table()
    
    def _handle_row_click(self, e):
        """Handle table row click to edit code"""
        if hasattr(e, 'row') and e.row:
            full_data = e.row.get('_full_data')
            if full_data:
                self._show_edit_code_dialog(full_data)
    
    def _show_edit_code_dialog(self, code_data: Dict[str, Any]):
        """Show dialog to edit code configuration"""
        try:
            with ui.dialog() as dialog, ui.card().classes('w-full max-w-2xl p-6'):
                ui.label(f'Edit Event Code: {code_data["code"]}').classes('text-h5 font-bold mb-4')
                
                # Get full code info
                full_info = database_driven_codes.get_code_info(code_data['code'])
                
                # Form fields
                name_input = ui.input('Name', value=full_info['name']).classes('w-full mb-4')
                
                risk_score_input = ui.number(
                    'Risk Score (0-100)',
                    value=full_info['risk_score'],
                    min=0,
                    max=100
                ).classes('w-full mb-4')
                
                severity_select = ui.select(
                    options=['critical', 'valuable', 'investigative', 'probative'],
                    value=full_info['severity'],
                    label='Severity Level'
                ).classes('w-full mb-4')
                
                description_input = ui.textarea(
                    'Description',
                    value=full_info['description']
                ).classes('w-full mb-4')
                
                # Current info display
                with ui.card().classes('w-full p-4 bg-gray-50 mb-4'):
                    ui.label('Current Information').classes('font-bold mb-2')
                    ui.label(f"Usage Count: {full_info['usage_count']:,}")
                    ui.label(f"Frequency Rank: {full_info['frequency_rank']}")
                    ui.label(f"Source: {full_info['source']}")
                    if full_info.get('reasoning'):
                        ui.label(f"Auto-assignment Reasoning: {full_info['reasoning']}")
                
                # Buttons
                with ui.row().classes('w-full justify-end gap-4 mt-4'):
                    ui.button('Cancel', on_click=dialog.close).props('flat')
                    
                    def save_changes():
                        try:
                            database_driven_codes.update_code_config(
                                code_data['code'],
                                name=name_input.value,
                                risk_score=int(risk_score_input.value),
                                severity=severity_select.value,
                                description=description_input.value
                            )
                            database_driven_codes.save_user_customizations()
                            
                            ui.notify(f'Updated {code_data["code"]} successfully', type='positive')
                            dialog.close()
                            
                            # Refresh table
                            self._render_codes_table()
                            self._update_stats()
                            
                        except Exception as e:
                            logger.error(f"Error saving code changes: {e}")
                            ui.notify(f'Error saving changes: {str(e)}', type='negative')
                    
                    ui.button('Save Changes', on_click=save_changes).props('color=primary')
            
            dialog.open()
            
        except Exception as e:
            logger.error(f"Error showing edit dialog: {e}")
            ui.notify(f'Error opening edit dialog: {str(e)}', type='negative')
    
    def _show_add_code_dialog(self):
        """Show dialog to add new event code"""
        try:
            with ui.dialog() as dialog, ui.card().classes('w-full max-w-xl p-6'):
                ui.label('Add New Event Code').classes('text-h5 font-bold mb-4')
                
                # Form fields
                code_input = ui.input('Event Code (3 letters)', placeholder='ABC').classes('w-full mb-4')
                name_input = ui.input('Name', placeholder='Event Name').classes('w-full mb-4')
                risk_score_input = ui.number('Risk Score (0-100)', value=50, min=0, max=100).classes('w-full mb-4')
                severity_select = ui.select(
                    options=['critical', 'valuable', 'investigative', 'probative'],
                    value='probative',
                    label='Severity Level'
                ).classes('w-full mb-4')
                description_input = ui.textarea('Description').classes('w-full mb-4')
                
                # Buttons
                with ui.row().classes('w-full justify-end gap-4 mt-4'):
                    ui.button('Cancel', on_click=dialog.close).props('flat')
                    
                    def add_code():
                        try:
                            code = code_input.value.upper().strip()
                            
                            if len(code) != 3 or not code.isalpha():
                                ui.notify('Code must be exactly 3 letters', type='negative')
                                return
                            
                            database_driven_codes.update_code_config(
                                code,
                                name=name_input.value,
                                risk_score=int(risk_score_input.value),
                                severity=severity_select.value,
                                description=description_input.value
                            )
                            database_driven_codes.save_user_customizations()
                            
                            ui.notify(f'Added {code} successfully', type='positive')
                            dialog.close()
                            
                            # Refresh table
                            self._render_codes_table()
                            self._update_stats()
                            
                        except Exception as e:
                            logger.error(f"Error adding new code: {e}")
                            ui.notify(f'Error adding code: {str(e)}', type='negative')
                    
                    ui.button('Add Code', on_click=add_code).props('color=primary')
            
            dialog.open()
            
        except Exception as e:
            logger.error(f"Error showing add dialog: {e}")
    
    def _export_configuration(self):
        """Export configuration for backup"""
        try:
            config = database_driven_codes.export_configuration()
            
            import json
            config_json = json.dumps(config, indent=2, ensure_ascii=False)
            
            # Create download
            ui.download(config_json.encode(), 'event_codes_config.json')
            ui.notify('Configuration exported successfully', type='positive')
            
        except Exception as e:
            logger.error(f"Error exporting configuration: {e}")
            ui.notify(f'Export error: {str(e)}', type='negative')
    
    def _refresh_from_database(self):
        """Refresh codes from database"""
        try:
            ui.notify('Refreshing codes from database...', type='info')
            database_driven_codes.refresh_from_database()
            
            # Refresh UI
            self._render_codes_table()
            self._update_stats()
            
            ui.notify('Successfully refreshed codes from database', type='positive')
            
        except Exception as e:
            logger.error(f"Error refreshing from database: {e}")
            ui.notify(f'Refresh error: {str(e)}', type='negative')
    
    def _show_import_dialog(self):
        """Show dialog to import configuration"""
        ui.notify('Import functionality would be implemented here', type='info')

# Global instance
event_codes_ui = EventCodesUI()