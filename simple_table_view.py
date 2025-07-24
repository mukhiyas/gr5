"""
Simple, Reliable Table View Component for Entity Data
Completely rebuilt with minimal complexity for maximum reliability
"""

from nicegui import ui
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class SimpleTableView:
    """Ultra-simple table component that just works"""
    
    def __init__(self):
        """Initialize the simple table view"""
        self.current_data = []
        self.page_size = 50
        self.current_page = 1
        
    def create_table(self, container, data: List[Dict[str, Any]]):
        """
        Create a simple, reliable table display
        
        Args:
            container: UI container to place the table
            data: List of entity dictionaries
        """
        try:
            logger.info(f"🔍 SimpleTableView: Creating table for {len(data)} entities")
            
            self.current_data = data
            
            with container:
                # Simple header
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label(f'Entity Data ({len(data)} entities)').classes('text-h6 font-bold')
                    
                    # Simple page size selector
                    ui.select(
                        options=[25, 50, 100],
                        value=self.page_size,
                        label='Rows per page'
                    ).classes('w-32').bind_value_to(self, 'page_size')
                
                # Calculate pagination
                start_idx = (self.current_page - 1) * self.page_size
                end_idx = start_idx + self.page_size
                page_data = data[start_idx:end_idx]
                
                logger.info(f"📊 Displaying {len(page_data)} entities (page {self.current_page})")
                
                # Create simple table data
                if page_data:
                    # Prepare rows for NiceGUI table
                    rows = []
                    for i, entity in enumerate(page_data):
                        row = self._prepare_table_row(entity, i)
                        rows.append(row)
                    
                    # Simple column definitions
                    columns = [
                        {'name': 'entity_id', 'label': 'Entity ID', 'field': 'entity_id', 'sortable': True},
                        {'name': 'entity_name', 'label': 'Entity Name', 'field': 'entity_name', 'sortable': True},
                        {'name': 'entity_type', 'label': 'Type', 'field': 'entity_type'},
                        {'name': 'risk_score', 'label': 'Risk Score', 'field': 'risk_score', 'sortable': True},
                        {'name': 'risk_severity', 'label': 'Risk Level', 'field': 'risk_severity'},
                        {'name': 'pep_status', 'label': 'PEP', 'field': 'pep_status'},
                        {'name': 'event_count', 'label': 'Events', 'field': 'event_count', 'sortable': True},
                        {'name': 'relationship_count', 'label': 'Relations', 'field': 'relationship_count'},
                        {'name': 'country', 'label': 'Country', 'field': 'country'},
                    ]
                    
                    logger.info(f"📋 Creating NiceGUI table with {len(columns)} columns and {len(rows)} rows")
                    
                    # Create the actual NiceGUI table - VERY SIMPLE
                    try:
                        table = ui.table(
                            columns=columns,
                            rows=rows,
                            row_key='entity_id'
                        ).classes('w-full')
                        
                        # Simple styling
                        table.style('''
                            .q-table {
                                font-size: 14px;
                            }
                            .q-table th {
                                background-color: #f5f5f5;
                                font-weight: 600;
                            }
                            .q-table tbody tr:hover {
                                background-color: #f9f9f9;
                            }
                        ''')
                        
                        # Row click handler
                        def handle_row_click(e):
                            if hasattr(e, 'row') and e.row:
                                entity_id = e.row.get('entity_id')
                                if entity_id:
                                    self._show_entity_details(entity_id)
                        
                        table.on('rowClick', handle_row_click)
                        
                        logger.info("✅ Table created successfully!")
                        
                    except Exception as table_error:
                        logger.error(f"❌ NiceGUI table creation failed: {table_error}")
                        # Fallback to simple display
                        self._create_simple_fallback(page_data)
                        
                else:
                    ui.label('No data to display').classes('text-center text-gray-500 p-8')
                
                # Simple pagination info
                if len(data) > self.page_size:
                    total_pages = (len(data) + self.page_size - 1) // self.page_size
                    with ui.row().classes('w-full justify-center mt-4'):
                        ui.label(f'Showing {start_idx + 1}-{min(end_idx, len(data))} of {len(data)} entities (Page {self.current_page} of {total_pages})').classes('text-sm text-gray-600')
                        
                        # Simple page navigation
                        if self.current_page > 1:
                            ui.button('Previous', on_click=lambda: self._change_page(-1)).props('flat')
                        if self.current_page < total_pages:
                            ui.button('Next', on_click=lambda: self._change_page(1)).props('flat')
                
        except Exception as e:
            logger.error(f"❌ SimpleTableView creation failed: {e}", exc_info=True)
            with container:
                ui.label(f"Table creation error: {str(e)}").classes('text-red-500 p-4')
                ui.label("Please try refreshing or contact support.").classes('text-gray-600 p-2')
    
    def _prepare_table_row(self, entity: Dict[str, Any], index: int) -> Dict[str, Any]:
        """Prepare a single table row with safe data extraction"""
        try:
            # Extract key fields safely
            entity_id = str(entity.get('entity_id', f'entity_{index}'))
            entity_name = str(entity.get('entity_name', 'Unknown'))
            entity_type = str(entity.get('entity_type', entity.get('recordDefinitionType', 'Unknown')))
            
            # Risk data
            risk_score = entity.get('risk_score', 0)
            if isinstance(risk_score, (int, float)):
                risk_score = round(float(risk_score), 1)
            else:
                risk_score = 0.0
            
            risk_severity = str(entity.get('risk_severity', 'Unknown'))
            
            # PEP status
            is_pep = entity.get('is_pep', False)
            pep_status = 'PEP' if is_pep else 'Non-PEP'
            
            # Event count
            events = entity.get('events', [])
            event_count = len(events) if isinstance(events, list) else entity.get('event_count', 0)
            
            # Relationship count
            relationships = entity.get('relationships', [])
            rel_count = len(relationships) if isinstance(relationships, list) else entity.get('relationship_count', 0)
            
            # Country
            country = str(entity.get('primary_country', entity.get('country', 'Unknown')))
            
            return {
                'entity_id': entity_id,
                'entity_name': entity_name,
                'entity_type': entity_type,
                'risk_score': risk_score,
                'risk_severity': risk_severity,
                'pep_status': pep_status,
                'event_count': event_count,
                'relationship_count': rel_count,
                'country': country
            }
            
        except Exception as e:
            logger.error(f"Error preparing row for entity {index}: {e}")
            return {
                'entity_id': f'entity_{index}',
                'entity_name': 'Data Error',
                'entity_type': 'Unknown',
                'risk_score': 0,
                'risk_severity': 'Unknown',
                'pep_status': 'Unknown',
                'event_count': 0,
                'relationship_count': 0,
                'country': 'Unknown'
            }
    
    def _create_simple_fallback(self, page_data: List[Dict[str, Any]]):
        """Create simple fallback display when table fails"""
        logger.info("📋 Creating fallback display")
        
        ui.label("Entity Data (Table View Unavailable)").classes('text-h6 font-bold mb-4')
        
        for i, entity in enumerate(page_data):
            row = self._prepare_table_row(entity, i)
            
            with ui.card().classes('w-full mb-2 p-3'):
                with ui.row().classes('w-full items-center justify-between'):
                    with ui.column().classes('flex-grow'):
                        ui.label(f"{row['entity_name']} ({row['entity_id']})").classes('font-bold')
                        ui.label(f"Type: {row['entity_type']} | Risk: {row['risk_score']} ({row['risk_severity']})").classes('text-sm text-gray-600')
                    
                    with ui.column().classes('text-right'):
                        ui.label(f"Events: {row['event_count']}").classes('text-sm')
                        ui.label(f"Relations: {row['relationship_count']}").classes('text-sm')
    
    def _show_entity_details(self, entity_id: str):
        """Show entity details in a dialog"""
        try:
            # Find the entity
            entity = None
            for e in self.current_data:
                if str(e.get('entity_id')) == str(entity_id):
                    entity = e
                    break
            
            if not entity:
                logger.warning(f"Entity {entity_id} not found for details")
                return
            
            with ui.dialog() as dialog, ui.card().classes('w-full max-w-2xl p-6'):
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label(f'Entity Details: {entity.get("entity_name", "Unknown")}').classes('text-h5 font-bold')
                    ui.button(icon='close', on_click=dialog.close).props('flat round')
                
                # Basic info
                with ui.card().classes('w-full p-4 mb-4'):
                    ui.label('Basic Information').classes('text-h6 font-bold mb-2')
                    ui.label(f"ID: {entity.get('entity_id', '')}")
                    ui.label(f"Type: {entity.get('entity_type', entity.get('recordDefinitionType', ''))}")
                    ui.label(f"Risk Score: {entity.get('risk_score', 0)}")
                    ui.label(f"Risk Severity: {entity.get('risk_severity', '')}")
                    ui.label(f"PEP Status: {'Yes' if entity.get('is_pep') else 'No'}")
                    ui.label(f"Country: {entity.get('primary_country', entity.get('country', ''))}")
                
                # Event info
                events = entity.get('events', [])
                if events:
                    with ui.card().classes('w-full p-4'):
                        ui.label(f'Events ({len(events)})').classes('text-h6 font-bold mb-2')
                        for event in events[:5]:  # Show first 5
                            if isinstance(event, dict):
                                ui.label(f"• {event.get('event_category_code', '')} - {event.get('event_date', '')}")
            
            dialog.open()
            
        except Exception as e:
            logger.error(f"Error showing entity details: {e}")
    
    def _change_page(self, direction: int):
        """Change page (simple implementation)"""
        try:
            new_page = self.current_page + direction
            total_pages = (len(self.current_data) + self.page_size - 1) // self.page_size
            
            if 1 <= new_page <= total_pages:
                self.current_page = new_page
                logger.info(f"📄 Page changed to {self.current_page}")
                # Note: In a full implementation, you'd recreate the table here
                # For now, this is a placeholder for page navigation
                
        except Exception as e:
            logger.error(f"Error changing page: {e}")