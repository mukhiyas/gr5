"""
Dedicated Table View Tab - Separate from Search Results
Full-featured table with all entity data fields
"""

from nicegui import ui
from typing import List, Dict, Any, Optional
import logging
import json

logger = logging.getLogger(__name__)

class DedicatedTableTab:
    """Dedicated table view tab with full entity data display"""
    
    def __init__(self):
        """Initialize the dedicated table tab"""
        self.current_data = []
        self.filtered_data = []
        self.page_size = 50
        self.current_page = 1
        self.search_term = ""
        self.sort_column = None
        self.sort_direction = 'asc'
        self.app_instance = None  # Will be set by main.py for export functionality
        
    def create_table_tab(self, data: List[Dict[str, Any]]):
        """
        Create the dedicated table tab
        
        Args:
            data: List of entity dictionaries
        """
        try:
            logger.info(f"🔍 DedicatedTableTab: Creating table tab for {len(data)} entities")
            
            self.current_data = data
            self.filtered_data = data.copy()
            self._reset_pagination()
            
            with ui.column().classes('w-full gap-4 p-4'):
                # Tab header
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label('Entity Data Table').classes('text-h4 font-bold')
                    ui.label(f'{len(data)} entities loaded').classes('text-sm text-gray-600')
                
                # Controls section
                self._create_controls()
                
                # Table container
                self.table_container = ui.column().classes('w-full')
                
                # Pagination container
                self.pagination_container = ui.row().classes('w-full justify-between items-center mt-4')
                
                # Initial render with error handling
                try:
                    self._render_table()
                    self._render_pagination()
                    logger.info("✅ Dedicated table tab created successfully")
                except Exception as e:
                    logger.error(f"❌ Error rendering initial table: {e}")
                    with self.table_container:
                        ui.label(f"Table rendering error: {str(e)}").classes('text-red-500 p-4')
                
        except Exception as e:
            logger.error(f"❌ Error creating dedicated table tab: {e}", exc_info=True)
            ui.label(f"Error creating table tab: {str(e)}").classes('text-red-500 p-4')
    
    def load_data(self, data: List[Dict[str, Any]]):
        """Load new data into the table (can be called externally)"""
        try:
            self.current_data = data
            self.filtered_data = data.copy()
            self._reset_pagination()
            
            # Re-render table and pagination
            self._render_table()
            self._render_pagination()
            
            logger.info(f"✅ Loaded {len(data)} entities into table")
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            if hasattr(self, 'table_container'):
                self.table_container.clear()
                with self.table_container:
                    ui.label(f"Error loading data: {str(e)}").classes('text-red-500 p-4')
    
    def _create_controls(self):
        """Create search and filter controls"""
        with ui.card().classes('w-full p-4 mb-4'):
            with ui.row().classes('w-full items-center gap-4'):
                # Search input
                search_input = ui.input(
                    placeholder='Search entities by name, ID, country...',
                    value=self.search_term
                ).classes('flex-grow')
                search_input.on('input', self._handle_search)
                
                # Page size selector
                page_size_select = ui.select(
                    options=[25, 50, 100, 200],
                    value=self.page_size,
                    label='Rows per page'
                ).classes('w-40')
                page_size_select.on('change', self._handle_page_size_change)
                
                # Export button with better error handling
                ui.button('Export to Excel', on_click=self._export_data).props('color=primary')
                ui.button('Export to CSV', on_click=self._export_csv).props('color=secondary flat')
                
                # Clear search button
                ui.button('Clear', on_click=self._clear_search).props('flat')
    
    def _render_table(self):
        """Render the main data table"""
        try:
            self.table_container.clear()
            
            # Calculate pagination
            start_idx = (self.current_page - 1) * self.page_size
            end_idx = start_idx + self.page_size
            page_data = self.filtered_data[start_idx:end_idx]
            
            logger.info(f"📊 Rendering table: page {self.current_page}, showing {len(page_data)} entities")
            
            if not page_data:
                with self.table_container:
                    ui.label('No entities match your search criteria').classes('text-center text-gray-500 p-8')
                return
            
            # Prepare table data
            table_rows = []
            for entity in page_data:
                row = self._entity_to_table_row(entity)
                table_rows.append(row)
            
            # Column definitions - comprehensive
            columns = [
                {'name': 'entity_id', 'label': 'Entity ID', 'field': 'entity_id', 'sortable': True, 'align': 'left'},
                {'name': 'entity_name', 'label': 'Entity Name', 'field': 'entity_name', 'sortable': True, 'align': 'left'},
                {'name': 'entity_type', 'label': 'Type', 'field': 'entity_type', 'sortable': True, 'align': 'center'},
                {'name': 'risk_score', 'label': 'Risk Score', 'field': 'risk_score', 'sortable': True, 'align': 'right'},
                {'name': 'risk_severity', 'label': 'Risk Level', 'field': 'risk_severity', 'sortable': True, 'align': 'center'},
                {'name': 'pep_status', 'label': 'PEP', 'field': 'pep_status', 'sortable': True, 'align': 'center'},
                {'name': 'event_count', 'label': 'Events', 'field': 'event_count', 'sortable': True, 'align': 'right'},
                {'name': 'event_codes', 'label': 'Event Codes', 'field': 'event_codes', 'sortable': False, 'align': 'left'},
                {'name': 'relationship_count', 'label': 'Relations', 'field': 'relationship_count', 'sortable': True, 'align': 'right'},
                {'name': 'country', 'label': 'Country', 'field': 'country', 'sortable': True, 'align': 'left'},
                {'name': 'aliases_summary', 'label': 'Aliases', 'field': 'aliases_summary', 'sortable': False, 'align': 'left'},
                {'name': 'latest_event_date', 'label': 'Latest Event', 'field': 'latest_event_date', 'sortable': True, 'align': 'center'},
            ]
            
            # Create the table
            with self.table_container:
                table = ui.table(
                    columns=columns,
                    rows=table_rows,
                    row_key='entity_id'
                ).classes('w-full')
                
                # Apply basic styling
                table.props('flat bordered').classes('shadow-lg')
                
                # Row click handler for entity details
                table.on('rowClick', self._handle_row_click)
                
                logger.info(f"✅ Table rendered with {len(table_rows)} rows")
                
        except Exception as e:
            logger.error(f"❌ Error rendering table: {e}", exc_info=True)
            with self.table_container:
                ui.label(f"Error rendering table: {str(e)}").classes('text-red-500 p-4')
    
    def _entity_to_table_row(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Convert entity to table row format with comprehensive data"""
        try:
            # Basic entity info
            entity_id = str(entity.get('entity_id', ''))
            entity_name = str(entity.get('entity_name', 'Unknown'))
            entity_type = str(entity.get('entity_type', entity.get('recordDefinitionType', 'Unknown')))
            
            # Risk information
            risk_score = entity.get('risk_score', 0)
            if isinstance(risk_score, (int, float)):
                risk_score = round(float(risk_score), 1)
            else:
                risk_score = 0.0
            
            risk_severity = str(entity.get('risk_severity', 'Unknown'))
            
            # PEP status with styling
            is_pep = entity.get('is_pep', False)
            pep_status = 'PEP' if is_pep else ''
            
            # Event information
            events = entity.get('events', [])
            event_count = len(events) if isinstance(events, list) else entity.get('event_count', 0)
            
            # Event codes (first 5)
            event_codes = entity.get('event_codes', [])
            if isinstance(event_codes, list) and event_codes:
                event_codes_display = ', '.join(str(code) for code in event_codes[:5])
                if len(event_codes) > 5:
                    event_codes_display += f' (+{len(event_codes) - 5} more)'
            else:
                event_codes_display = ''
            
            # Relationship count
            relationships = entity.get('relationships', [])
            relationship_count = len(relationships) if isinstance(relationships, list) else entity.get('relationship_count', 0)
            
            # Country information
            country = str(entity.get('primary_country', entity.get('country', '')))
            
            # Aliases summary
            aliases = entity.get('aliases', [])
            if isinstance(aliases, list) and aliases:
                aliases_summary = ', '.join(str(alias) for alias in aliases[:3])
                if len(aliases) > 3:
                    aliases_summary += f' (+{len(aliases) - 3} more)'
            else:
                aliases_summary = ''
            
            # Latest event date
            latest_event_date = ''
            if events and isinstance(events, list):
                sorted_events = sorted(events, key=lambda x: x.get('event_date', ''), reverse=True)
                if sorted_events:
                    latest_event_date = sorted_events[0].get('event_date', '')
            
            return {
                'entity_id': entity_id,
                'entity_name': entity_name,
                'entity_type': entity_type,
                'risk_score': risk_score,
                'risk_severity': risk_severity,
                'pep_status': pep_status,
                'event_count': event_count,
                'event_codes': event_codes_display,
                'relationship_count': relationship_count,
                'country': country,
                'aliases_summary': aliases_summary,
                'latest_event_date': latest_event_date,
                '_entity_data': entity  # Store full entity data for details view
            }
            
        except Exception as e:
            logger.error(f"Error converting entity to table row: {e}")
            return {
                'entity_id': str(entity.get('entity_id', 'Error')),
                'entity_name': 'Data Processing Error',
                'entity_type': 'Error',
                'risk_score': 0,
                'risk_severity': 'Unknown',
                'pep_status': '',
                'event_count': 0,
                'event_codes': '',
                'relationship_count': 0,
                'country': '',
                'aliases_summary': '',
                'latest_event_date': '',
                '_entity_data': entity
            }
    
    def _render_pagination(self):
        """Render pagination controls"""
        try:
            self.pagination_container.clear()
            
            total_entities = len(self.filtered_data)
            total_pages = max(1, (total_entities + self.page_size - 1) // self.page_size)
            start_idx = (self.current_page - 1) * self.page_size + 1
            end_idx = min(self.current_page * self.page_size, total_entities)
            
            with self.pagination_container:
                # Results info
                ui.label(f'Showing {start_idx}-{end_idx} of {total_entities} entities').classes('text-sm text-gray-600')
                
                # Pagination controls
                if total_pages > 1:
                    with ui.row().classes('items-center gap-2'):
                        # First page
                        ui.button('First', on_click=lambda: self._go_to_page(1)).props('flat').set_enabled(self.current_page > 1)
                        
                        # Previous page
                        ui.button('Previous', on_click=lambda: self._go_to_page(self.current_page - 1)).props('flat').set_enabled(self.current_page > 1)
                        
                        # Page numbers (show 5 pages around current)
                        start_page = max(1, self.current_page - 2)
                        end_page = min(total_pages, self.current_page + 2)
                        
                        for page_num in range(start_page, end_page + 1):
                            btn = ui.button(str(page_num), on_click=lambda p=page_num: self._go_to_page(p))
                            if page_num == self.current_page:
                                btn.props('color=primary')
                            else:
                                btn.props('flat')
                        
                        # Next page
                        ui.button('Next', on_click=lambda: self._go_to_page(self.current_page + 1)).props('flat').set_enabled(self.current_page < total_pages)
                        
                        # Last page
                        ui.button('Last', on_click=lambda: self._go_to_page(total_pages)).props('flat').set_enabled(self.current_page < total_pages)
                        
        except Exception as e:
            logger.error(f"Error rendering pagination: {e}")
    
    def _handle_search(self, e):
        """Handle search input"""
        try:
            search_term = e.value.lower().strip() if hasattr(e, 'value') else str(e).lower().strip()
            self.search_term = search_term
            
            if not search_term:
                self.filtered_data = self.current_data.copy()
            else:
                self.filtered_data = []
                for entity in self.current_data:
                    # Search in multiple fields
                    search_fields = [
                        str(entity.get('entity_name', '')),
                        str(entity.get('entity_id', '')),
                        str(entity.get('primary_country', '')),
                        str(entity.get('country', '')),
                        str(entity.get('entity_type', '')),
                        str(entity.get('aliases', [])),
                        str(entity.get('event_codes', [])),
                    ]
                    
                    if any(search_term in field.lower() for field in search_fields):
                        self.filtered_data.append(entity)
            
            self._reset_pagination()
            self._update_display()
            
        except Exception as e:
            logger.error(f"Error handling search: {e}")
    
    def _handle_page_size_change(self, e):
        """Handle page size change"""
        try:
            new_size = e.value if hasattr(e, 'value') else e
            self.page_size = int(new_size)
            self._reset_pagination()
            self._update_display()
        except Exception as e:
            logger.error(f"Error changing page size: {e}")
    
    def _go_to_page(self, page_num: int):
        """Navigate to specific page"""
        try:
            total_pages = max(1, (len(self.filtered_data) + self.page_size - 1) // self.page_size)
            if 1 <= page_num <= total_pages:
                self.current_page = page_num
                self._update_display()
        except Exception as e:
            logger.error(f"Error navigating to page {page_num}: {e}")
    
    def _reset_pagination(self):
        """Reset pagination to first page"""
        self.current_page = 1
    
    def _update_display(self):
        """Update table and pagination display"""
        try:
            self._render_table()
            self._render_pagination()
        except Exception as e:
            logger.error(f"Error updating display: {e}")
    
    def _clear_search(self):
        """Clear search and reset filters"""
        try:
            self.search_term = ""
            self.filtered_data = self.current_data.copy()
            self._reset_pagination()
            self._update_display()
        except Exception as e:
            logger.error(f"Error clearing search: {e}")
    
    def _handle_row_click(self, e):
        """Handle table row click to show entity details"""
        try:
            if hasattr(e, 'row') and e.row:
                entity_data = e.row.get('_entity_data')
                if entity_data:
                    self._show_entity_details(entity_data)
        except Exception as e:
            logger.error(f"Error handling row click: {e}")
    
    def _show_entity_details(self, entity: Dict[str, Any]):
        """Show comprehensive entity details in a modal"""
        try:
            with ui.dialog() as dialog, ui.card().classes('w-full max-w-6xl p-6'):
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label(f'Entity Details: {entity.get("entity_name", "Unknown")}').classes('text-h4 font-bold')
                    ui.button(icon='close', on_click=dialog.close).props('flat round')
                
                # Tabbed content for comprehensive details
                with ui.tabs().classes('w-full') as tabs:
                    overview_tab = ui.tab('Overview', icon='info')
                    events_tab = ui.tab(f'Events ({len(entity.get("events", []))})', icon='event')
                    relationships_tab = ui.tab(f'Relationships ({len(entity.get("relationships", []))})', icon='people')
                    aliases_tab = ui.tab(f'Aliases ({len(entity.get("aliases", []))})', icon='badge')
                    raw_data_tab = ui.tab('Raw Data', icon='data_object')
                
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
                    
                    # Aliases panel
                    with ui.tab_panel(aliases_tab):
                        self._create_aliases_panel(entity)
                    
                    # Raw data panel
                    with ui.tab_panel(raw_data_tab):
                        self._create_raw_data_panel(entity)
            
            dialog.open()
            
        except Exception as e:
            logger.error(f"Error showing entity details: {e}")
            ui.notify(f"Error showing details: {str(e)}", type='negative')
    
    def _create_overview_panel(self, entity: Dict[str, Any]):
        """Create entity overview panel"""
        with ui.column().classes('w-full gap-4'):
            # Basic information
            with ui.card().classes('w-full p-4'):
                ui.label('Basic Information').classes('text-h6 font-bold mb-3')
                with ui.grid(columns=2).classes('w-full gap-4'):
                    ui.label(f"Entity ID: {entity.get('entity_id', '')}")
                    ui.label(f"Entity Type: {entity.get('entity_type', entity.get('recordDefinitionType', ''))}")
                    ui.label(f"Risk Score: {entity.get('risk_score', 0)}")
                    ui.label(f"Risk Severity: {entity.get('risk_severity', '')}")
                    ui.label(f"PEP Status: {'Yes' if entity.get('is_pep') else 'No'}")
                    ui.label(f"Country: {entity.get('primary_country', entity.get('country', ''))}")
            
            # Counts summary
            with ui.card().classes('w-full p-4'):
                ui.label('Data Summary').classes('text-h6 font-bold mb-3')
                with ui.grid(columns=3).classes('w-full gap-4'):
                    ui.label(f"Events: {len(entity.get('events', []))}")
                    ui.label(f"Relationships: {len(entity.get('relationships', []))}")
                    ui.label(f"Aliases: {len(entity.get('aliases', []))}")
    
    def _create_events_panel(self, entity: Dict[str, Any]):
        """Create events panel"""
        events = entity.get('events', [])
        if not events:
            ui.label('No events found for this entity').classes('text-center text-gray-500 p-8')
            return
        
        with ui.column().classes('w-full gap-2'):
            for i, event in enumerate(events):
                if isinstance(event, dict):
                    with ui.card().classes('w-full p-3'):
                        with ui.row().classes('w-full items-start justify-between'):
                            with ui.column().classes('flex-grow'):
                                ui.label(f"{event.get('event_category_code', '')} - {event.get('event_sub_category_code', '')}").classes('font-bold')
                                ui.label(event.get('event_description_short', event.get('event_description', ''))).classes('text-sm')
                            ui.label(event.get('event_date', '')).classes('text-sm text-gray-600 whitespace-nowrap')
    
    def _create_relationships_panel(self, entity: Dict[str, Any]):
        """Create relationships panel"""
        relationships = entity.get('relationships', [])
        if not relationships:
            ui.label('No relationships found for this entity').classes('text-center text-gray-500 p-8')
            return
        
        with ui.column().classes('w-full gap-2'):
            for rel in relationships:
                if isinstance(rel, dict):
                    with ui.card().classes('w-full p-3'):
                        with ui.row().classes('w-full items-center justify-between'):
                            with ui.column().classes('flex-grow'):
                                ui.label(rel.get('related_entity_name', '')).classes('font-bold')
                                ui.label(f"Type: {rel.get('type', '')}").classes('text-sm')
                            ui.label(f"Direction: {rel.get('direction', '')}").classes('text-sm text-gray-600')
    
    def _create_aliases_panel(self, entity: Dict[str, Any]):
        """Create aliases panel"""
        aliases = entity.get('aliases', [])
        if not aliases:
            ui.label('No aliases found for this entity').classes('text-center text-gray-500 p-8')
            return
        
        with ui.column().classes('w-full gap-2'):
            for i, alias in enumerate(aliases):
                with ui.card().classes('w-full p-3'):
                    ui.label(f"{i+1}. {alias}").classes('font-medium')
    
    def _create_raw_data_panel(self, entity: Dict[str, Any]):
        """Create raw data panel"""
        with ui.scroll_area().classes('w-full h-96'):
            json_str = json.dumps(entity, indent=2, default=str, ensure_ascii=False)
            ui.code(json_str).classes('w-full text-xs')
    
    def _export_data(self):
        """Export filtered data to Excel"""
        try:
            if not self.filtered_data:
                ui.notify('No data to export. Please load data first.', type='warning')
                return
            
            logger.info(f"Excel export requested for {len(self.filtered_data)} entities")
            
            # Use passed app_instance if available, otherwise create new exporter
            if self.app_instance and hasattr(self.app_instance, 'exporter'):
                filename = self.app_instance.exporter.export_to_excel(self.filtered_data)
            else:
                # Create new exporter instance
                from entity_exports import EntityExporter
                exporter = EntityExporter()
                filename = exporter.export_to_excel(self.filtered_data)
            
            ui.notify(f'✅ Exported {len(self.filtered_data)} entities to Excel', type='positive')
            logger.info(f"Excel export completed: {filename}")
                
        except Exception as e:
            logger.error(f"Excel export error: {e}", exc_info=True)
            ui.notify(f"❌ Excel export failed: {str(e)}", type='negative')
    
    def _export_csv(self):
        """Export filtered data to CSV"""
        try:
            if not self.filtered_data:
                ui.notify('No data to export. Please load data first.', type='warning')
                return
            
            logger.info(f"CSV export requested for {len(self.filtered_data)} entities")
            
            # Use passed app_instance if available, otherwise create new exporter
            if self.app_instance and hasattr(self.app_instance, 'exporter'):
                filename = self.app_instance.exporter.export_to_csv(self.filtered_data)
            else:
                # Create new exporter instance
                from entity_exports import EntityExporter
                exporter = EntityExporter()
                filename = exporter.export_to_csv(self.filtered_data)
            
            ui.notify(f'✅ Exported {len(self.filtered_data)} entities to CSV', type='positive')
            logger.info(f"CSV export completed: {filename}")
                
        except Exception as e:
            logger.error(f"CSV export error: {e}", exc_info=True)
            ui.notify(f"❌ CSV export failed: {str(e)}", type='negative')