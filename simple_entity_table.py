"""
Simple Entity Table - Complete Redesign
Clean, working implementation with guaranteed data display and exports
"""

from nicegui import ui
from typing import List, Dict, Any
import logging
import json

logger = logging.getLogger(__name__)

class SimpleEntityTable:
    """Ultra-simple, reliable entity table implementation"""
    
    def __init__(self):
        self.entities = []
        self.page_size = 25
        self.current_page = 0
        self.search_filter = ""
        
    def create_interface(self, entities: List[Dict[str, Any]]):
        """Create the complete entity table interface"""
        self.entities = entities or []
        logger.info(f"Creating simple entity table with {len(self.entities)} entities")
        
        with ui.column().classes('w-full gap-4 p-4'):
            # Header with entity count
            ui.label(f'Entity Table ({len(self.entities)} entities)').classes('text-h4 font-bold')
            
            if not self.entities:
                ui.label('No entities to display. Please run a search first.').classes('text-center text-gray-500 p-8')
                return
            
            # Controls row
            with ui.row().classes('w-full gap-4 items-center mb-4'):
                # Search
                search_input = ui.input('Filter entities...', placeholder='Search by name, ID, or country')
                search_input.on('input', lambda e: self._apply_filter(e.value))
                
                # Export buttons
                ui.button('Export Excel', on_click=self._export_excel, icon='file_download').props('color=primary')
                ui.button('Export CSV', on_click=self._export_csv, icon='file_download').props('color=secondary')
                
                # Refresh button
                ui.button('Refresh', on_click=lambda: self._refresh_data(), icon='refresh').props('flat')
            
            # Create simple data table
            self._create_data_table()
    
    def _create_data_table(self):
        """Create a simple, working data table"""
        try:
            # Filter entities
            filtered_entities = self._get_filtered_entities()
            
            if not filtered_entities:
                ui.label('No entities match your filter.').classes('text-center text-gray-500 p-4')
                return
            
            # Calculate pagination
            total_pages = (len(filtered_entities) + self.page_size - 1) // self.page_size
            start_idx = self.current_page * self.page_size
            end_idx = start_idx + self.page_size
            page_entities = filtered_entities[start_idx:end_idx]
            
            # Status
            ui.label(f'Showing {len(page_entities)} of {len(filtered_entities)} entities (Page {self.current_page + 1} of {total_pages})').classes('text-sm text-gray-600 mb-2')
            
            # Create table data
            columns = [
                {'name': 'name', 'label': 'Entity Name', 'field': 'entity_name', 'align': 'left', 'sortable': True},
                {'name': 'id', 'label': 'Entity ID', 'field': 'entity_id', 'align': 'left'},
                {'name': 'type', 'label': 'Type', 'field': 'entity_type', 'align': 'center'},
                {'name': 'risk', 'label': 'Risk Score', 'field': 'risk_score', 'align': 'right', 'sortable': True},
                {'name': 'events', 'label': 'Events', 'field': 'event_count', 'align': 'right'},
                {'name': 'country', 'label': 'Country', 'field': 'primary_country', 'align': 'left'},
            ]
            
            # Prepare rows
            rows = []
            for entity in page_entities:
                row = {
                    'entity_name': entity.get('entity_name', 'N/A'),
                    'entity_id': entity.get('entity_id', 'N/A'),
                    'entity_type': self._get_entity_type(entity),
                    'risk_score': f"{entity.get('risk_score', 0):.0f}",
                    'event_count': len(entity.get('events', [])),
                    'primary_country': entity.get('primary_country', entity.get('country', 'N/A')),
                }
                rows.append(row)
            
            # Create table
            table = ui.table(columns=columns, rows=rows).classes('w-full')
            table.props('flat bordered')
            
            # Pagination controls
            if total_pages > 1:
                with ui.row().classes('w-full justify-center gap-2 mt-4'):
                    ui.button('Previous', 
                             on_click=lambda: self._change_page(-1),
                             icon='chevron_left').props('flat').set_enabled(self.current_page > 0)
                    
                    ui.label(f'Page {self.current_page + 1} of {total_pages}').classes('px-4 py-2')
                    
                    ui.button('Next',
                             on_click=lambda: self._change_page(1), 
                             icon='chevron_right').props('flat').set_enabled(self.current_page < total_pages - 1)
            
            logger.info(f"✅ Table created with {len(rows)} rows")
            
        except Exception as e:
            logger.error(f"❌ Error creating table: {e}")
            ui.label(f'Error creating table: {str(e)}').classes('text-red-500 p-4')
    
    def _get_entity_type(self, entity: Dict[str, Any]) -> str:
        """Get clean entity type"""
        entity_type = entity.get('recordDefinitionType', entity.get('entity_type', ''))
        
        if entity_type in ['I', 'INDIVIDUAL', 'Individual']:
            return 'Individual'
        elif entity_type in ['O', 'ORGANIZATION', 'Organization']:
            return 'Organization'
        else:
            return entity_type or 'Unknown'
    
    def _get_filtered_entities(self) -> List[Dict[str, Any]]:
        """Get entities that match the current filter"""
        if not self.search_filter:
            return self.entities
        
        filter_lower = self.search_filter.lower()
        filtered = []
        
        for entity in self.entities:
            # Search in multiple fields
            searchable_text = f"{entity.get('entity_name', '')} {entity.get('entity_id', '')} {entity.get('primary_country', '')} {entity.get('country', '')}".lower()
            
            if filter_lower in searchable_text:
                filtered.append(entity)
        
        return filtered
    
    def _apply_filter(self, filter_text: str):
        """Apply search filter and refresh table"""
        self.search_filter = filter_text or ""
        self.current_page = 0  # Reset to first page
        self._refresh_table()
    
    def _change_page(self, direction: int):
        """Change page and refresh table"""
        self.current_page += direction
        self._refresh_table()
    
    def _refresh_table(self):
        """Refresh the table display"""
        # Find the parent container and recreate the table
        ui.run_javascript('''
            // Simple refresh by reloading the page section
            window.location.reload();
        ''')
    
    def _refresh_data(self):
        """Refresh data from main app"""
        try:
            # Try to get fresh data from main app
            import main
            if hasattr(main, 'app_instance') and hasattr(main.app_instance, 'current_results'):
                fresh_data = main.app_instance.current_results
                if fresh_data:
                    self.entities = fresh_data
                    self.current_page = 0
                    self.search_filter = ""
                    ui.notify(f'Refreshed data: {len(fresh_data)} entities', type='positive')
                    # Reload page to show fresh data
                    ui.run_javascript('window.location.reload();')
                else:
                    ui.notify('No fresh data available', type='warning')
            else:
                ui.notify('Cannot access main app data', type='negative')
        except Exception as e:
            logger.error(f"Error refreshing data: {e}")
            ui.notify(f'Refresh error: {str(e)}', type='negative')
    
    def _export_excel(self):
        """Export current entities to Excel"""
        try:
            if not self.entities:
                ui.notify('No data to export', type='warning')
                return
            
            # Use entity exports
            from entity_exports import EntityExporter
            exporter = EntityExporter()
            
            # Export filtered entities
            filtered_entities = self._get_filtered_entities()
            filename = exporter.export_to_excel(filtered_entities)
            
            ui.notify(f'✅ Exported {len(filtered_entities)} entities to Excel', type='positive')
            logger.info(f"Excel export completed: {filename}")
            
        except Exception as e:
            logger.error(f"Excel export error: {e}")
            ui.notify(f'❌ Excel export failed: {str(e)}', type='negative')
    
    def _export_csv(self):
        """Export current entities to CSV"""
        try:
            if not self.entities:
                ui.notify('No data to export', type='warning')
                return
            
            # Use entity exports
            from entity_exports import EntityExporter
            exporter = EntityExporter()
            
            # Export filtered entities
            filtered_entities = self._get_filtered_entities()
            filename = exporter.export_to_csv(filtered_entities)
            
            ui.notify(f'✅ Exported {len(filtered_entities)} entities to CSV', type='positive')
            logger.info(f"CSV export completed: {filename}")
            
        except Exception as e:
            logger.error(f"CSV export error: {e}")
            ui.notify(f'❌ CSV export failed: {str(e)}', type='negative')

# Global instance
simple_entity_table = SimpleEntityTable()