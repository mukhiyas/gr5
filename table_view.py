"""
Modern Table View Component for Entity Data
Comprehensive, responsive data table with all entity fields
"""

from nicegui import ui
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class EntityTableView:
    """Modern responsive table component for entity data"""
    
    def __init__(self, app_instance=None):
        """Initialize the table view component"""
        self.current_data = []
        self.filtered_data = []
        self.page_size = 50
        self.current_page = 1
        self.sort_column = None
        self.sort_direction = 'asc'
        self.table_container = None
        self.pagination_container = None
        self.app_instance = app_instance
        
        # Define column configurations
        self.column_configs = self._get_column_configs()
    
    def _get_code_description(self, code, code_type='event'):
        """Get code description using database-driven system from main app"""
        if not self.app_instance or not code:
            return code or "Unknown"
        
        try:
            if code_type == 'event':
                return self.app_instance.get_event_description(code)
            elif code_type == 'pep':
                return self.app_instance.get_pep_description(code)
            elif code_type == 'relationship':
                return self.app_instance.get_relationship_description(code)
            elif code_type == 'attribute':
                return self.app_instance.get_entity_attribute_description(code)
            else:
                return code or "Unknown"
        except Exception as e:
            logger.warning(f"Failed to get {code_type} description for {code}: {e}")
            return code or "Unknown"
    
    def create_table(self, container, data: List[Dict[str, Any]]):
        """
        Create the complete table with data
        
        Args:
            container: UI container to place the table
            data: List of entity dictionaries
        """
        try:
            logger.info(f"Creating table view for {len(data)} entities")
            
            # Validate input data
            if not data or not isinstance(data, list):
                logger.error(f"Invalid data provided to table view: {type(data)} with length {len(data) if hasattr(data, '__len__') else 'unknown'}")
                with container:
                    ui.label("No valid data provided for table view").classes('text-red-500 p-4')
                return
            
            # Log sample entity structure for debugging
            if data:
                # Filter out None entities first
                valid_data = [entity for entity in data if entity is not None]
                if valid_data:
                    sample_entity = valid_data[0]
                    logger.debug(f"Sample entity keys: {list(sample_entity.keys()) if isinstance(sample_entity, dict) else 'Not a dict'}")
                    if len(valid_data) < len(data):
                        logger.warning(f"Filtered out {len(data) - len(valid_data)} None entities from data")
                    data = valid_data
                else:
                    logger.error("All entities in data are None")
                    return
            
            self.current_data = data
            self.filtered_data = data.copy()
            
            with container:
                # Debug info
                ui.label(f"🔍 Table View Debug: {len(data)} entities loaded").classes('text-blue-600 p-2 bg-blue-50 rounded mb-2')
                
                # Table header with controls
                self._create_table_header()
                
                # Main table container
                self.table_container = ui.element('div').classes('w-full')
                
                # Debug: Add a visible element to ensure container works
                with self.table_container:
                    ui.label("🔄 Loading table...").classes('text-gray-600 p-4')
                
                # Pagination container
                self.pagination_container = ui.element('div').classes('w-full mt-4')
                
                # Initial table render
                self._render_table()
                self._render_pagination()
                
                logger.info("Table view created successfully")
                
        except Exception as e:
            logger.error(f"Error creating table view: {e}", exc_info=True)
            with container:
                ui.label(f"Error creating table: {str(e)}").classes('text-red-500 p-4')
                ui.label("Please try switching to card view or refreshing the search.").classes('text-gray-600 p-2')
    
    def _create_table_header(self):
        """Create table header with search and controls"""
        with ui.card().classes('w-full mb-4 p-4'):
            with ui.row().classes('w-full items-center justify-between'):
                # Search and filter controls
                with ui.row().classes('items-center gap-4'):
                    ui.label('Entity Data Table').classes('text-h6 font-bold')
                    
                    # Search input
                    search_input = ui.input('Search entities...').classes('w-64')
                    search_input.on('input', lambda e: self._handle_search(e.value))
                    
                    # Page size selector
                    page_size_select = ui.select(
                        options=[25, 50, 100, 200],
                        value=self.page_size,
                        label='Rows per page'
                    ).classes('w-32')
                    page_size_select.on('change', lambda e: self._handle_page_size_change(e.value))
                
                # Table info
                with ui.row().classes('items-center gap-2'):
                    ui.icon('table_view', size='sm')
                    self.info_label = ui.label(f'{len(self.filtered_data)} entities')
    
    def _render_table(self):
        """Render the main data table"""
        try:
            logger.debug(f"_render_table called. Container exists: {self.table_container is not None}")
            logger.debug(f"Filtered data length: {len(self.filtered_data)}")
            logger.debug(f"Current page: {self.current_page}, Page size: {self.page_size}")
            
            self.table_container.clear()
            
            # Calculate pagination
            start_idx = (self.current_page - 1) * self.page_size
            end_idx = start_idx + self.page_size
            page_data = self.filtered_data[start_idx:end_idx]
            
            logger.debug(f"Page data: {start_idx}-{end_idx}, {len(page_data)} entities")
            
            if not page_data:
                logger.warning("No page data to display")
                with self.table_container:
                    ui.label('No data to display').classes('text-center text-gray-500 p-8')
                return
            
            # Prepare table data
            table_rows = []
            logger.debug(f"Processing {len(page_data)} entities for table display")
            for i, entity in enumerate(page_data):
                try:
                    row = self._entity_to_table_row(entity)
                    table_rows.append(row)
                    if i < 3:  # Log first 3 rows for debugging
                        logger.debug(f"Row {i+1}: {row.get('entity_name', 'Unknown')} - {row.get('entity_id', 'No ID')}")
                except Exception as e:
                    logger.error(f"Error processing entity {i+1}: {e}")
            
            logger.info(f"Prepared {len(table_rows)} table rows from {len(page_data)} entities")
            
            # Create NiceGUI table
            with self.table_container:
                # Try creating a minimal table first to test
                try:
                    logger.info("Attempting to create minimal test table")
                    test_columns = [
                        {'name': 'entity_name', 'label': 'Name', 'field': 'entity_name'},
                        {'name': 'entity_type', 'label': 'Type', 'field': 'entity_type'},
                        {'name': 'risk_score', 'label': 'Risk Score', 'field': 'risk_score'}
                    ]
                    
                    # Create minimal rows
                    minimal_rows = []
                    for row in table_rows[:10]:  # Just first 10 for testing
                        minimal_rows.append({
                            'entity_name': row.get('entity_name', 'Unknown'),
                            'entity_type': row.get('entity_type', 'Unknown'),
                            'risk_score': row.get('risk_score', 0)
                        })
                    
                    logger.info(f"Creating table with {len(minimal_rows)} rows")
                    
                    # Create simple table first
                    table = ui.table(
                        columns=test_columns,
                        rows=minimal_rows
                    ).classes('w-full border')
                    
                    logger.info(f"✅ Simple table created successfully")
                    
                    # If that works, try the full table
                    if len(table_rows) > 10:
                        ui.separator().classes('my-4')
                        ui.label(f"Full table with {len(table_rows)} rows:").classes('font-bold')
                        
                        full_table = ui.table(
                            columns=self.column_configs,
                            rows=table_rows
                        ).classes('w-full border mt-2')
                        
                        logger.info(f"✅ Full table created successfully")
                
                except Exception as table_error:
                    logger.error(f"Failed to create table, falling back to card view: {table_error}")
                    
                    # Fallback: Create a simple card-based display
                    self._create_fallback_table(table_rows)
                    return
                
                # Add table styling (only if table was created successfully)
                try:
                    if 'table' in locals():
                        table.style('''
                        .q-table__container {
                            border-radius: 8px;
                            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                            max-height: 600px;
                            overflow-y: auto;
                        }
                        .q-table thead th {
                            background-color: #f5f5f5;
                            font-weight: 600;
                            color: #374151;
                        }
                        .q-table tbody tr:hover {
                            background-color: #f9fafb;
                        }
                        ''')
                        
                        # Add row click handler for entity selection
                        table.on('rowClick', self._handle_row_click)
                    
                    logger.debug(f"Rendered table with {len(table_rows)} rows")
                    
                except Exception as style_error:
                    logger.warning(f"Could not apply table styling: {style_error}")
        
        except Exception as e:
            logger.error(f"Error rendering table: {e}", exc_info=True)
            with self.table_container:
                ui.label(f"Error rendering table: {str(e)}").classes('text-red-500 p-4')
    
    def _render_pagination(self):
        """Render pagination controls"""
        try:
            self.pagination_container.clear()
            
            total_pages = max(1, (len(self.filtered_data) + self.page_size - 1) // self.page_size)
            
            with self.pagination_container:
                with ui.row().classes('w-full items-center justify-between'):
                    # Results info
                    start_idx = (self.current_page - 1) * self.page_size + 1
                    end_idx = min(self.current_page * self.page_size, len(self.filtered_data))
                    
                    ui.label(
                        f'Showing {start_idx}-{end_idx} of {len(self.filtered_data)} entities'
                    ).classes('text-sm text-gray-600')
                    
                    # Pagination controls
                    if total_pages > 1:
                        with ui.row().classes('items-center gap-2'):
                            # Previous button
                            prev_btn = ui.button(
                                icon='chevron_left',
                                on_click=lambda: self._go_to_page(self.current_page - 1)
                            ).props('flat').classes('px-2')
                            prev_btn.set_enabled(self.current_page > 1)
                            
                            # Page numbers
                            page_start = max(1, self.current_page - 2)
                            page_end = min(total_pages, self.current_page + 2)
                            
                            for page_num in range(page_start, page_end + 1):
                                page_btn = ui.button(
                                    str(page_num),
                                    on_click=lambda p=page_num: self._go_to_page(p)
                                )
                                
                                if page_num == self.current_page:
                                    page_btn.props('color=primary')
                                else:
                                    page_btn.props('flat')
                                
                                page_btn.classes('px-3')
                            
                            # Next button
                            next_btn = ui.button(
                                icon='chevron_right',
                                on_click=lambda: self._go_to_page(self.current_page + 1)
                            ).props('flat').classes('px-2')
                            next_btn.set_enabled(self.current_page < total_pages)
        
        except Exception as e:
            logger.error(f"Error rendering pagination: {e}", exc_info=True)
    
    def _entity_to_table_row(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Convert entity to table row format"""
        try:
            # Format risk severity with color class
            risk_severity = entity.get('risk_severity', 'Probative').lower()
            risk_class = f'risk-{risk_severity}'
            
            # Format PEP status
            pep_status = '✓ PEP' if entity.get('is_pep', False) else ''
            
            # Format event codes (first 5)
            event_codes = entity.get('event_codes', [])
            if isinstance(event_codes, list):
                events_display = ', '.join(event_codes[:5])
                if len(event_codes) > 5:
                    events_display += f' (+{len(event_codes) - 5} more)'
            else:
                events_display = str(event_codes) if event_codes else ''
            
            # Format aliases (first 3)
            aliases = entity.get('aliases', [])
            if isinstance(aliases, list) and aliases:
                aliases_display = ', '.join([str(a) for a in aliases[:3]])
                if len(aliases) > 3:
                    aliases_display += f' (+{len(aliases) - 3} more)'
            else:
                aliases_display = ''
            
            # Format relationships
            relationships = entity.get('relationships', [])
            if isinstance(relationships, list) and relationships:
                rel_names = [rel.get('related_entity_name', '') for rel in relationships[:3] if isinstance(rel, dict)]
                rel_display = ', '.join([name for name in rel_names if name])
                if len(relationships) > 3:
                    rel_display += f' (+{len(relationships) - 3} more)'
            else:
                rel_display = ''
            
            # Format date of birth
            dob = entity.get('date_of_birth', {})
            if isinstance(dob, dict) and dob.get('date_of_birth_year'):
                year = dob.get('date_of_birth_year', '')
                month = dob.get('date_of_birth_month', '')
                day = dob.get('date_of_birth_day', '')
                dob_display = f"{year}-{month}-{day}" if all([year, month, day]) else year
            else:
                dob_display = ''
            
            row_data = {
                'entity_id': entity.get('entity_id', ''),
                'entity_name': entity.get('entity_name', ''),
                'entity_type': entity.get('entity_type', ''),
                'risk_score': entity.get('risk_score', 0),
                'risk_severity': entity.get('risk_severity', ''),
                'pep_status': pep_status,
                'event_count': entity.get('event_count', 0),
                'event_codes': events_display,
                'country': entity.get('primary_country', entity.get('country', '')),
                'aliases': aliases_display,
                'relationships': rel_display,
                'relationship_count': entity.get('relationship_count', 0),
                'date_of_birth': dob_display,
                'latest_event_date': entity.get('latest_event_date', ''),
                'entity_date': self._format_date(entity.get('entity_date')),
                '_row_class': risk_class  # Add class for styling
            }
            
            return row_data
        
        except Exception as e:
            logger.error(f"Error converting entity to table row: {e}")
            return {
                'entity_id': entity.get('entity_id', ''),
                'entity_name': entity.get('entity_name', ''),
                'error': str(e)
            }
    
    def _get_column_configs(self) -> List[Dict[str, Any]]:
        """Get table column configurations"""
        return [
            {
                'name': 'entity_id',
                'label': 'Entity ID',
                'field': 'entity_id',
                'sortable': True,
                'align': 'left',
                'style': 'width: 120px; font-family: monospace;'
            },
            {
                'name': 'entity_name',
                'label': 'Entity Name',
                'field': 'entity_name',
                'sortable': True,
                'align': 'left',
                'style': 'width: 200px; font-weight: 600;'
            },
            {
                'name': 'entity_type',
                'label': 'Type',
                'field': 'entity_type',
                'sortable': True,
                'align': 'center',
                'style': 'width: 100px;'
            },
            {
                'name': 'risk_score',
                'label': 'Risk Score',
                'field': 'risk_score',
                'sortable': True,
                'align': 'center',
                'style': 'width: 100px; font-weight: 600;'
            },
            {
                'name': 'risk_severity',
                'label': 'Risk Severity',
                'field': 'risk_severity',
                'sortable': True,
                'align': 'center',
                'style': 'width: 120px;'
            },
            {
                'name': 'pep_status',
                'label': 'PEP',
                'field': 'pep_status',
                'sortable': True,
                'align': 'center',
                'style': 'width: 80px; color: #059669;'
            },
            {
                'name': 'event_count',
                'label': 'Events',
                'field': 'event_count',
                'sortable': True,
                'align': 'center',
                'style': 'width: 80px;'
            },
            {
                'name': 'event_codes',
                'label': 'Event Codes',
                'field': 'event_codes',
                'sortable': False,
                'align': 'left',
                'style': 'width: 150px; font-size: 12px;'
            },
            {
                'name': 'country',
                'label': 'Country',
                'field': 'country',
                'sortable': True,
                'align': 'left',
                'style': 'width: 120px;'
            },
            {
                'name': 'aliases',
                'label': 'Aliases',
                'field': 'aliases',
                'sortable': False,
                'align': 'left',
                'style': 'width: 150px; font-size: 12px;'
            },
            {
                'name': 'relationships',
                'label': 'Related Entities',
                'field': 'relationships',
                'sortable': False,
                'align': 'left',
                'style': 'width: 150px; font-size: 12px;'
            },
            {
                'name': 'relationship_count',
                'label': 'Rel. Count',
                'field': 'relationship_count',
                'sortable': True,
                'align': 'center',
                'style': 'width: 90px;'
            },
            {
                'name': 'date_of_birth',
                'label': 'Date of Birth',
                'field': 'date_of_birth',
                'sortable': True,
                'align': 'center',
                'style': 'width: 110px;'
            },
            {
                'name': 'latest_event_date',
                'label': 'Latest Event',
                'field': 'latest_event_date',
                'sortable': True,
                'align': 'center',
                'style': 'width: 110px;'
            },
            {
                'name': 'entity_date',
                'label': 'Entity Date',
                'field': 'entity_date',
                'sortable': True,
                'align': 'center',
                'style': 'width: 110px;'
            }
        ]
    
    def _handle_search(self, search_term: str):
        """Handle search input"""
        if not search_term.strip():
            self.filtered_data = self.current_data.copy()
        else:
            search_lower = search_term.lower()
            self.filtered_data = []
            
            for entity in self.current_data:
                # Search in multiple fields
                search_fields = [
                    entity.get('entity_name', ''),
                    entity.get('entity_id', ''),
                    entity.get('primary_country', ''),
                    entity.get('country', ''),
                    str(entity.get('aliases', [])),
                    str(entity.get('event_codes', [])),
                ]
                
                if any(search_lower in str(field).lower() for field in search_fields):
                    self.filtered_data.append(entity)
        
        # Reset to first page
        self.current_page = 1
        self._update_display()
    
    def _handle_page_size_change(self, new_size: int):
        """Handle page size change"""
        self.page_size = new_size
        self.current_page = 1
        self._update_display()
    
    def _go_to_page(self, page_num: int):
        """Navigate to specific page"""
        total_pages = max(1, (len(self.filtered_data) + self.page_size - 1) // self.page_size)
        
        if 1 <= page_num <= total_pages:
            self.current_page = page_num
            self._update_display()
    
    def _update_display(self):
        """Update table and pagination display"""
        try:
            # Update info label
            if hasattr(self, 'info_label'):
                self.info_label.set_text(f'{len(self.filtered_data)} entities')
            
            # Re-render table and pagination
            self._render_table()
            self._render_pagination()
            
        except Exception as e:
            logger.error(f"Error updating display: {e}", exc_info=True)
    
    def _handle_row_click(self, event):
        """Handle table row click"""
        try:
            if event and hasattr(event, 'row'):
                entity_id = event.row.get('entity_id')
                if entity_id:
                    logger.info(f"Table row clicked: Entity ID {entity_id}")
                    # Find full entity data
                    for entity in self.current_data:
                        if entity.get('entity_id') == entity_id:
                            self._show_entity_details(entity)
                            break
        except Exception as e:
            logger.error(f"Error handling row click: {e}")
    
    def _show_entity_details(self, entity: Dict[str, Any]):
        """Show entity details in a modal"""
        try:
            with ui.dialog() as dialog, ui.card().classes('w-full max-w-4xl p-6'):
                # Header
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label(f'Entity Details: {entity.get("entity_name", "Unknown")}').classes('text-h5 font-bold')
                    ui.button(icon='close', on_click=dialog.close).props('flat round')
                
                # Content in tabs
                with ui.tabs().classes('w-full') as tabs:
                    overview_tab = ui.tab('Overview', icon='info')
                    events_tab = ui.tab(f'Events ({entity.get("event_count", 0)})', icon='event')
                    relationships_tab = ui.tab(f'Relationships ({entity.get("relationship_count", 0)})', icon='people')
                    details_tab = ui.tab('All Details', icon='data_object')
                
                with ui.tab_panels(tabs, value=overview_tab).classes('w-full'):
                    # Overview panel
                    with ui.tab_panel(overview_tab):
                        self._create_overview_panel(entity)
                    
                    # Events panel
                    with ui.tab_panel(events_tab):
                        self._create_events_panel(entity)
                    
                    # Relationships panel
                    with ui.tab_panel(relationships_tab):
                        self._create_relationships_panel(entity)
                    
                    # Details panel
                    with ui.tab_panel(details_tab):
                        self._create_details_panel(entity)
            
            dialog.open()
            
        except Exception as e:
            logger.error(f"Error showing entity details: {e}")
            ui.notify(f"Error showing details: {str(e)}", type='negative')
    
    def _create_overview_panel(self, entity: Dict[str, Any]):
        """Create entity overview panel"""
        with ui.column().classes('w-full gap-4'):
            # Basic info
            with ui.card().classes('w-full p-4'):
                ui.label('Basic Information').classes('text-h6 font-bold mb-2')
                with ui.grid(columns=2).classes('w-full gap-4'):
                    ui.label(f"ID: {entity.get('entity_id', '')}")
                    ui.label(f"Type: {entity.get('entity_type', '')}")
                    ui.label(f"Risk Score: {entity.get('risk_score', 0)}")
                    ui.label(f"Risk Severity: {entity.get('risk_severity', '')}")
                    ui.label(f"PEP Status: {'Yes' if entity.get('is_pep') else 'No'}")
                    ui.label(f"Country: {entity.get('primary_country', entity.get('country', ''))}")
    
    def _create_events_panel(self, entity: Dict[str, Any]):
        """Create events panel"""
        events = entity.get('events', [])
        if not events:
            ui.label('No events found').classes('text-center text-gray-500 p-4')
            return
        
        with ui.column().classes('w-full gap-2'):
            for i, event in enumerate(events[:20]):  # Show first 20 events
                if isinstance(event, dict):
                    with ui.card().classes('w-full p-3'):
                        with ui.row().classes('w-full items-start justify-between'):
                            with ui.column():
                                ui.label(f"{event.get('event_category_code', '')} - {event.get('event_sub_category_code', '')}").classes('font-bold')
                                ui.label(event.get('event_description_short', '')).classes('text-sm')
                            ui.label(event.get('event_date', '')).classes('text-sm text-gray-600')
    
    def _create_relationships_panel(self, entity: Dict[str, Any]):
        """Create relationships panel"""
        relationships = entity.get('relationships', [])
        if not relationships:
            ui.label('No relationships found').classes('text-center text-gray-500 p-4')
            return
        
        with ui.column().classes('w-full gap-2'):
            for rel in relationships:
                if isinstance(rel, dict):
                    with ui.card().classes('w-full p-3'):
                        with ui.row().classes('w-full items-center justify-between'):
                            with ui.column():
                                ui.label(rel.get('related_entity_name', '')).classes('font-bold')
                                ui.label(f"Type: {rel.get('type', '')}").classes('text-sm')
                            ui.label(f"Direction: {rel.get('direction', '')}").classes('text-sm text-gray-600')
    
    def _create_details_panel(self, entity: Dict[str, Any]):
        """Create full details panel"""
        with ui.scroll_area().classes('w-full h-96'):
            # Convert entity to formatted JSON
            import json
            json_str = json.dumps(entity, indent=2, default=str, ensure_ascii=False)
            ui.code(json_str).classes('w-full text-xs')
    
    def _format_date(self, date_obj) -> str:
        """Format date object to string"""
        if not date_obj:
            return ''
        
        if hasattr(date_obj, 'isoformat'):
            return date_obj.isoformat()
        elif hasattr(date_obj, 'strftime'):
            return date_obj.strftime('%Y-%m-%d')
        else:
            return str(date_obj)
    
    def _create_fallback_table(self, table_rows):
        """Create a simple fallback table when NiceGUI table fails"""
        try:
            logger.info("Creating fallback HTML table")
            
            # Create a simple scrollable container with table data
            with ui.scroll_area().classes('w-full h-96 border rounded'):
                with ui.column().classes('w-full p-4'):
                    ui.label(f'Entity Data ({len(table_rows)} records)').classes('text-h6 font-bold mb-4')
                    
                    # Create cards for each entity as fallback
                    for i, row in enumerate(table_rows[:50]):  # Limit to 50 for performance
                        with ui.card().classes('w-full mb-2 p-3'):
                            with ui.row().classes('w-full items-center justify-between'):
                                with ui.column().classes('flex-grow'):
                                    # Main entity info
                                    ui.label(f"{row.get('entity_name', 'Unknown')} ({row.get('entity_id', '')})").classes('font-bold')
                                    
                                    # Secondary info
                                    info_parts = []
                                    if row.get('entity_type'):
                                        info_parts.append(f"Type: {row.get('entity_type')}")
                                    if row.get('risk_score'):
                                        info_parts.append(f"Risk: {row.get('risk_score')}")
                                    if row.get('country'):
                                        info_parts.append(f"Country: {row.get('country')}")
                                    
                                    if info_parts:
                                        ui.label(' | '.join(info_parts)).classes('text-sm text-gray-600')
                                
                                # Risk severity badge
                                if row.get('risk_severity'):
                                    severity = row.get('risk_severity', '').lower()
                                    color = {
                                        'critical': 'red',
                                        'valuable': 'orange', 
                                        'investigative': 'yellow',
                                        'probative': 'green'
                                    }.get(severity, 'gray')
                                    ui.badge(row.get('risk_severity'), color=color)
                    
                    if len(table_rows) > 50:
                        ui.label(f'Showing first 50 of {len(table_rows)} records').classes('text-center text-gray-500 mt-4')
            
            logger.info("Fallback table created successfully")
            
        except Exception as e:
            logger.error(f"Error creating fallback table: {e}")
            ui.label(f"Error displaying data: {str(e)}").classes('text-red-500 p-4')