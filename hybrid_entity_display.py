"""
Hybrid Entity Display - Scrollable List with Expandable Details
Combines compact list view with rich detail panels for optimal UX
"""

from nicegui import ui
from typing import Dict, Any, List
import logging
import json
import time

logger = logging.getLogger(__name__)

class HybridEntityDisplay:
    """Hybrid display combining list view with expandable details"""
    
    def __init__(self, app_instance=None):
        self.app_instance = app_instance
        self.entities = []
        self.filtered_entities = []
        self.search_term = ""
        self.expanded_entities = set()  # Track which entities are expanded
        self.data_timestamp = 0  # Track when data was loaded to prevent stale displays
    
    def create_hybrid_display(self, entities: List[Dict[str, Any]]):
        """Create the hybrid display interface"""
        try:
            # Clear any cached state from previous loads
            self.expanded_entities.clear()  # Clear expanded entities to prevent stale data
            self.data_timestamp = time.time()  # Update timestamp for fresh data tracking
            self.entities = entities
            self.filtered_entities = entities.copy()
            
            with ui.column().classes('w-full h-full'):
                # Header with controls (fixed height)
                self._create_header()
                
                # Search and filter bar (fixed height)
                self._create_search_bar()
                
                # Scrollable entity list container (flexible height)
                with ui.scroll_area().classes('w-full').style('height: calc(100vh - 300px); max-height: 600px;'):
                    self.entity_container = ui.column().classes('w-full gap-2 p-2')
                    self._render_entity_list()
                
                logger.info(f"✅ Hybrid entity display created with {len(entities)} entities")
                
        except Exception as e:
            logger.error(f"❌ Error creating hybrid display: {e}")
            ui.label(f'Error creating display: {str(e)}').classes('text-red-500 p-4')
    
    def _create_header(self):
        """Create header with title and export controls"""
        with ui.card().classes('w-full p-4 mb-2'):
            with ui.row().classes('w-full items-center justify-between'):
                ui.label('Entity Data Browser').classes('text-h5 font-bold')
                
                with ui.row().classes('gap-2'):
                    # View toggle
                    self.view_mode = 'list'  # 'list' or 'table'
                    self.view_toggle_btn = ui.button('Table View', 
                             on_click=self._toggle_view_mode,
                             icon='table_view').props('flat')
                    
                    # Expand/Collapse controls
                    ui.button('Collapse All', 
                             on_click=self._collapse_all,
                             icon='unfold_less').props('flat')
                    
                    ui.button('Expand All', 
                             on_click=self._expand_all,
                             icon='unfold_more').props('flat')
    
    def _create_search_bar(self):
        """Create search and filter controls"""
        with ui.card().classes('w-full p-3 mb-2'):
            with ui.row().classes('w-full items-center gap-4'):
                # Search input
                search_input = ui.input('Search entities...', 
                                      placeholder='Search by name, ID, country, etc.').classes('flex-grow')
                search_input.on('input', lambda e: self._filter_entities(e.value))
                
                # Clear button
                ui.button('Clear', 
                         on_click=lambda: (search_input.set_value(''), self._filter_entities(''))).props('flat')
                
                # Results count
                self.results_label = ui.label(f'{len(self.entities)} entities').classes('text-sm text-gray-600')
    
    def _render_entity_list(self):
        """Render the scrollable entity list"""
        try:
            self.entity_container.clear()
            
            if not self.filtered_entities:
                with self.entity_container:
                    ui.label('No entities match your search.').classes('text-center text-gray-500 p-8')
                return
            
            with self.entity_container:
                # Check view mode
                if hasattr(self, 'view_mode') and self.view_mode == 'table':
                    # Table view - clean data table with expandable rows
                    self._create_table_view()
                else:
                    # List layout - clean minimalist rows
                    for entity in self.filtered_entities:
                        self._create_clean_row(entity)
            
            # Update results count
            self.results_label.text = f'{len(self.filtered_entities)} entities'
            
        except Exception as e:
            logger.error(f"Error rendering entity list: {e}")
            with self.entity_container:
                ui.label(f'Error rendering list: {str(e)}').classes('text-red-500 p-4')
    
    def _create_entity_row(self, entity: Dict[str, Any]):
        """Create a single entity row with expandable details"""
        try:
            # Use risk_id as primary identifier, entity_id as fallback for compatibility
            risk_id = entity.get('risk_id', entity.get('entity_id', 'Unknown'))
            entity_name = entity.get('entity_name', 'Unknown Entity')
            risk_score = entity.get('risk_score', 0)
            risk_severity = entity.get('risk_severity', 'Probative')
            entity_count = entity.get('entity_count', 1)
            
            # Determine styling based on risk
            severity_colors = {
                'Critical': 'border-l-red-500 bg-red-50',
                'Valuable': 'border-l-orange-500 bg-orange-50',
                'Investigative': 'border-l-yellow-500 bg-yellow-50',
                'Probative': 'border-l-blue-500 bg-blue-50'
            }
            row_class = severity_colors.get(risk_severity.title(), 'border-l-gray-500 bg-gray-50')
            
            is_expanded = risk_id in self.expanded_entities
            
            with ui.card().classes(f'w-full border-l-4 {row_class} mb-3 hover:shadow-lg transition-all duration-300 cursor-pointer rounded-lg'):
                # Main row - always visible
                with ui.row().classes('w-full items-center justify-between p-4 cursor-pointer') as main_row:
                    # Left side - entity info
                    with ui.row().classes('items-center gap-4 flex-grow'):
                        # Expand/collapse icon
                        expand_icon = ui.icon('expand_more' if not is_expanded else 'expand_less').classes('text-gray-500')
                        
                        # Entity info with version information
                        with ui.column().classes('gap-1'):
                            ui.label(entity_name).classes('font-bold text-h6')
                            ui.label(f'Risk ID: {risk_id}').classes('text-caption text-gray-600')
                            if entity_count > 1:
                                ui.label(f'📊 {entity_count} Entity Versions').classes('text-caption text-blue-600 font-semibold')
                            
                            # Entity type badge
                            entity_type = self._get_entity_type(entity)
                            if entity_type and entity_type != 'Unknown':
                                ui.badge(entity_type).classes('px-2 py-1 rounded-full text-xs mt-1')
                        
                        # Quick stats in more card-like layout
                        with ui.column().classes('gap-2'):
                            # First row: Risk and severity
                            with ui.row().classes('gap-2 items-center'):
                                ui.icon('warning', size='sm').classes('text-orange-600')
                                ui.label(f'Risk: {risk_score}').classes('font-medium')
                                ui.badge(risk_severity).classes(f'risk-{risk_severity.lower()} px-2 py-1 rounded text-xs')
                            
                            # Second row: Events, PEP, Country
                            with ui.row().classes('gap-3 items-center text-sm text-gray-600'):
                                # Events
                                event_count = len(entity.get('events', []))
                                if event_count > 0:
                                    ui.label(f'📅 {event_count} events')
                                
                                # PEP
                                if entity.get('pep_status') == 'PEP' or entity.get('pep_levels'):
                                    ui.label('🏛️ PEP').classes('text-orange-600 font-medium')
                                
                                # Country
                                country = entity.get('primary_country', entity.get('country', ''))
                                if country:
                                    ui.label(f'📍 {country}')
                                
                                # Relationships
                                rel_count = len(entity.get('relationships', []))
                                if rel_count > 0:
                                    ui.label(f'🔗 {rel_count} connections')
                    
                    # Right side - actions
                    with ui.column().classes('gap-2 items-end'):
                        # Expand/View Details button
                        ui.button('Details' if not is_expanded else 'Hide', 
                                 icon='visibility' if not is_expanded else 'visibility_off').props('size=sm color=primary')
                
                # Click handler for main row
                main_row.on('click', lambda e=entity: self._toggle_entity_details(e))
                
                # Expandable details section
                if is_expanded:
                    self._create_entity_details(entity)
        
        except Exception as e:
            logger.error(f"Error creating entity row: {e}")
            with ui.card().classes('w-full p-3 border-red-300 bg-red-50'):
                ui.label(f'Error displaying: {entity.get("entity_name", "Unknown")}').classes('text-red-600')
    
    def _create_entity_card(self, entity: Dict[str, Any]):
        """Create a card-style entity display (similar to original enhanced cards)"""
        try:
            # Use risk_id as primary identifier
            risk_id = entity.get('risk_id', entity.get('entity_id', 'Unknown'))
            entity_name = entity.get('entity_name', 'Unknown Entity')
            risk_score = entity.get('risk_score', 0)
            risk_severity = entity.get('risk_severity', 'Probative')
            entity_count = entity.get('entity_count', 1)
            
            # Determine styling based on risk
            severity_colors = {
                'Critical': 'border-red-500 bg-red-50',
                'Valuable': 'border-orange-500 bg-orange-50',
                'Investigative': 'border-yellow-500 bg-yellow-50',
                'Probative': 'border-blue-500 bg-blue-50'
            }
            card_class = severity_colors.get(risk_severity.title(), 'border-gray-500 bg-gray-50')
            
            is_expanded = risk_id in self.expanded_entities
            
            with ui.card().classes(f'cursor-pointer hover:shadow-xl transition-all duration-300 border-l-4 {card_class} rounded-lg overflow-hidden'):
                with ui.column().classes('gap-3 p-5'):
                    # Header row with better spacing and visual hierarchy
                    with ui.row().classes('items-start justify-between w-full mb-2'):
                        with ui.column().classes('gap-1 flex-grow'):
                            # Entity name with better typography
                            ui.label(entity_name).classes('text-h6 font-bold text-gray-800 leading-tight')
                            ui.label(f'Risk ID: {risk_id}').classes('text-sm text-gray-500 font-mono')
                            if entity_count > 1:
                                ui.label(f'📊 {entity_count} Entity Versions').classes('text-xs text-blue-600 font-semibold')
                        
                        # Entity type badge with improved styling
                        entity_type = self._get_entity_type(entity)
                        if entity_type and entity_type != 'Unknown':
                            ui.badge(entity_type).classes('px-3 py-1 rounded-full text-xs font-semibold bg-indigo-100 text-indigo-800')
                    
                    # Risk score with visual progress bar
                    with ui.row().classes('items-center gap-3 w-full mb-3'):
                        ui.icon('shield_alt', size='sm').classes('text-orange-600')
                        ui.label('Risk Score:').classes('text-sm font-medium text-gray-700')
                        
                        # Risk score with mini progress bar
                        with ui.column().classes('flex-grow'):
                            with ui.row().classes('items-center gap-2'):
                                ui.label(f'{risk_score}/100').classes('font-bold text-lg')
                                ui.badge(risk_severity).classes(f'px-2 py-1 rounded text-xs font-medium')
                            # Mini progress bar
                            ui.linear_progress(value=risk_score/100).classes('h-1 rounded-full')
                    
                    # Key metrics row with better visual icons
                    with ui.row().classes('items-center justify-between w-full text-sm'):
                        # Left side metrics
                        with ui.row().classes('items-center gap-4'):
                            # Event count with icon
                            events = entity.get('events', [])
                            event_count = len(events) if isinstance(events, list) else entity.get('event_count', 0)
                            if event_count > 0:
                                with ui.row().classes('items-center gap-1'):
                                    ui.icon('event_note', size='sm').classes('text-blue-600')
                                    ui.label(f'{event_count}').classes('font-semibold')
                                    ui.label('events').classes('text-gray-600')
                            
                            # Relationships with icon
                            relationships = entity.get('relationships', [])
                            rel_count = len(relationships) if isinstance(relationships, list) else entity.get('relationship_count', 0)
                            if rel_count > 0:
                                with ui.row().classes('items-center gap-1'):
                                    ui.icon('account_tree', size='sm').classes('text-green-600')
                                    ui.label(f'{rel_count}').classes('font-semibold')
                                    ui.label('relations').classes('text-gray-600')
                        
                        # Right side badges
                        with ui.row().classes('items-center gap-2'):
                            # PEP status with better styling
                            if entity.get('pep_status') == 'PEP' or entity.get('pep_levels'):
                                ui.badge('PEP').classes('px-2 py-1 rounded text-xs font-semibold bg-orange-100 text-orange-800')
                    
                    # Location and additional info with better styling
                    country = entity.get('primary_country', entity.get('country', ''))
                    if country:
                        with ui.row().classes('items-center gap-2 mt-2'):
                            ui.icon('public', size='sm').classes('text-gray-500')
                            ui.label(country).classes('text-sm font-medium text-gray-700 bg-gray-100 px-2 py-1 rounded')
                    
                    # Action buttons row with enhanced styling
                    with ui.row().classes('justify-between items-center mt-4 pt-3 border-t border-gray-100'):
                        # Status indicator
                        with ui.row().classes('items-center gap-1'):
                            ui.icon('circle', size='xs').classes('text-green-500')
                            ui.label('Active').classes('text-xs text-gray-600')
                        
                        # Action buttons
                        with ui.row().classes('gap-2'):
                            ui.button('Details', 
                                     on_click=lambda e=entity: self._toggle_entity_details(e),
                                     icon='visibility' if not is_expanded else 'visibility_off').props('size=sm color=primary outline')
                
                # Expandable details (show below card if expanded)
                if is_expanded:
                    self._create_entity_details(entity)
            
            logger.debug(f"Created entity card for {entity_name}")
            
        except Exception as e:
            logger.error(f"Error creating entity card: {e}")
            # Fallback simple card
            with ui.card().classes('border-red-300 bg-red-50 p-4'):
                ui.label(f'Error displaying: {entity.get("entity_name", "Unknown")}').classes('text-red-600')
    
    def _create_entity_details(self, entity: Dict[str, Any]):
        """Create expanded details section for an entity"""
        try:
            with ui.column().classes('w-full p-4 bg-white border-t'):
                # Tabbed details
                with ui.tabs().classes('w-full') as tabs:
                    overview_tab = ui.tab('Overview', icon='info')
                    events_tab = ui.tab(f'Events ({len(entity.get("events", []))})', icon='event')
                    relationships_tab = ui.tab(f'Relations ({len(entity.get("relationships", []))})', icon='group')
                    addresses_tab = ui.tab('Addresses', icon='location_on')
                
                with ui.tab_panels(tabs, value=overview_tab).classes('w-full max-h-96 overflow-auto'):
                    # Overview panel
                    with ui.tab_panel(overview_tab):
                        self._create_overview_content(entity)
                    
                    # Events panel
                    with ui.tab_panel(events_tab):
                        self._create_events_content(entity)
                    
                    # Relationships panel
                    with ui.tab_panel(relationships_tab):
                        self._create_relationships_content(entity)
                    
                    # Addresses panel
                    with ui.tab_panel(addresses_tab):
                        self._create_addresses_content(entity)
        
        except Exception as e:
            logger.error(f"Error creating entity details: {e}")
            ui.label(f'Error loading details: {str(e)}').classes('text-red-500 p-2')
    
    def _create_overview_content(self, entity: Dict[str, Any]):
        """Create overview content for entity details"""
        with ui.row().classes('w-full gap-6'):
            # Basic info
            with ui.column().classes('flex-1'):
                ui.label('Basic Information').classes('font-bold mb-2')
                info_items = [
                    ('Entity Type', self._get_entity_type(entity)),
                    ('Risk Score', f"{entity.get('risk_score', 0)}/100"),
                    ('Risk Severity', entity.get('risk_severity', 'Unknown')),
                    ('Created Date', str(entity.get('created_date', 'N/A'))),
                ]
                
                for label, value in info_items:
                    with ui.row().classes('justify-between mb-1'):
                        ui.label(f'{label}:').classes('text-sm text-gray-600')
                        ui.label(str(value)).classes('text-sm font-medium')
            
            # Risk breakdown
            with ui.column().classes('flex-1'):
                ui.label('Risk Factors').classes('font-bold mb-2')
                risk_factors = entity.get('risk_factors', {})
                if risk_factors and isinstance(risk_factors, dict):
                    for factor, score in risk_factors.items():
                        with ui.row().classes('justify-between mb-1'):
                            ui.label(f'{factor.replace("_", " ").title()}:').classes('text-sm text-gray-600')
                            ui.label(str(score)).classes('text-sm font-medium')
                else:
                    ui.label('No detailed risk factors available').classes('text-sm text-gray-500')
    
    def _create_events_content(self, entity: Dict[str, Any]):
        """Create events content for entity details"""
        events = entity.get('events', [])
        if not events:
            ui.label('No events recorded').classes('text-center text-gray-500 p-4')
            return
        
        # Sort events by created_date to show most recently added first
        sorted_events = sorted(events, 
                             key=lambda x: x.get('created_date', x.get('event_date', '')), 
                             reverse=True)
        
        with ui.scroll_area().classes('w-full h-64'):
            for event in sorted_events[:10]:  # Show first 10 most recent events
                # Check if this is a recently added event
                is_recent = False
                if event.get('created_date'):
                    from datetime import datetime
                    try:
                        created = datetime.strptime(event['created_date'][:10], '%Y-%m-%d')
                        is_recent = (datetime.now() - created).days <= 7
                    except:
                        pass
                
                card_color = 'bg-yellow-50' if is_recent else 'bg-white'
                
                with ui.card().classes(f'w-full p-2 mb-2 {card_color}'):
                    with ui.row().classes('w-full justify-between items-start'):
                        event_info = f"{event.get('event_category_code', '')} - {event.get('event_date', '')}"
                        if is_recent:
                            with ui.row().classes('items-center gap-1'):
                                ui.label(event_info).classes('font-medium')
                                ui.badge('NEW', color='yellow').classes('text-xs')
                        else:
                            ui.label(event_info).classes('font-medium')
                        
                        # Show creation timestamp if available
                        if event.get('created_date'):
                            ui.label(f"Added: {event['created_date'][:10]}").classes('text-xs text-orange-600')
                    
                    # Show source if available
                    if event.get('source_name'):
                        with ui.row().classes('items-center gap-1'):
                            ui.icon('source', size='xs').classes('text-green-600')
                            ui.label(event['source_name'][:50] + '...' if len(event.get('source_name', '')) > 50 else event.get('source_name', '')).classes('text-xs text-green-700')
                    
                    if event.get('event_description_short'):
                        ui.label(event['event_description_short']).classes('text-sm text-gray-600')
    
    def _create_relationships_content(self, entity: Dict[str, Any]):
        """Create relationships content for entity details"""
        relationships = entity.get('relationships', [])
        if not relationships:
            ui.label('No relationships recorded').classes('text-center text-gray-500 p-4')
            return
        
        with ui.scroll_area().classes('w-full h-64'):
            for rel in relationships[:10]:  # Show first 10 relationships
                with ui.card().classes('w-full p-2 mb-2'):
                    ui.label(rel.get('related_entity_name', 'Unknown')).classes('font-medium')
                    ui.label(f"Type: {rel.get('type', 'Unknown')}").classes('text-sm text-gray-600')
    
    def _create_addresses_content(self, entity: Dict[str, Any]):
        """Create addresses content for entity details"""
        addresses = entity.get('addresses', [])
        if not addresses:
            ui.label('No addresses recorded').classes('text-center text-gray-500 p-4')
            return
        
        with ui.scroll_area().classes('w-full h-64'):
            for addr in addresses:
                with ui.card().classes('w-full p-2 mb-2'):
                    # Build address string
                    addr_parts = []
                    for field in ['address_line1', 'address_city', 'address_province', 'address_country']:
                        if addr.get(field):
                            addr_parts.append(addr[field])
                    
                    if addr_parts:
                        ui.label(', '.join(addr_parts)).classes('font-medium')
                    
                    if addr.get('address_type'):
                        ui.label(f"Type: {addr['address_type']}").classes('text-sm text-gray-600')
    
    def _get_entity_type(self, entity: Dict[str, Any]) -> str:
        """Get clean entity type"""
        entity_type = entity.get('recordDefinitionType', entity.get('entity_type', ''))
        
        if entity_type in ['I', 'INDIVIDUAL', 'Individual']:
            return 'Individual'
        elif entity_type in ['O', 'ORGANIZATION', 'Organization']:
            return 'Organization'
        elif entity_type:
            return entity_type.title()
        return 'Unknown'
    
    def _toggle_entity_details(self, entity: Dict[str, Any]):
        """Toggle entity details expansion"""
        # Use risk_id as primary identifier
        risk_id = entity.get('risk_id', entity.get('entity_id', 'Unknown'))
        entity_id = risk_id
        
        # Find the current entity data to ensure we're using fresh data
        current_entity = None
        for e in self.filtered_entities:
            # Check both risk_id and entity_id for compatibility
            e_risk_id = e.get('risk_id', e.get('entity_id', 'Unknown'))
            if e_risk_id == entity_id:
                current_entity = e
                break
        
        # If entity not found in current data, don't expand
        if not current_entity:
            logger.warning(f"Risk ID {entity_id} not found in current data")
            return
        
        if entity_id in self.expanded_entities:
            self.expanded_entities.remove(entity_id)
        else:
            self.expanded_entities.add(entity_id)
        
        # Re-render the list to show/hide details
        self._render_entity_list()
    
    def _filter_entities(self, search_term: str):
        """Filter entities based on search term"""
        self.search_term = search_term.lower() if search_term else ''
        
        if not self.search_term:
            self.filtered_entities = self.entities.copy()
        else:
            self.filtered_entities = []
            for entity in self.entities:
                # Search in multiple fields including risk_id
                risk_id = entity.get('risk_id', entity.get('entity_id', ''))
                searchable_text = f"{entity.get('entity_name', '')} {risk_id} {entity.get('primary_country', '')} {entity.get('country', '')}".lower()
                
                if self.search_term in searchable_text:
                    self.filtered_entities.append(entity)
        
        # Re-render with filtered results
        self._render_entity_list()
    
    
    def _collapse_all(self):
        """Collapse all expanded entities"""
        self.expanded_entities.clear()
        self._render_entity_list()
    
    def _expand_all(self):
        """Expand all entities (limited to first 5 for performance)"""
        # Limit expansion to prevent performance issues
        for entity in self.filtered_entities[:5]:
            risk_id = entity.get('risk_id', entity.get('entity_id', 'Unknown'))
            self.expanded_entities.add(risk_id)
        self._render_entity_list()
    
    def _create_clean_row(self, entity: Dict[str, Any]):
        """Create a sophisticated entity row with comprehensive data display"""
        try:
            # Use risk_id as primary identifier
            risk_id = entity.get('risk_id', entity.get('entity_id', 'Unknown'))
            entity_name = entity.get('entity_name', 'Unknown Entity')
            risk_score = entity.get('risk_score', 0)
            risk_severity = entity.get('risk_severity', 'Probative')
            entity_count = entity.get('entity_count', 1)
            all_entity_ids = entity.get('all_entity_ids', [])
            all_entity_dates = entity.get('all_entity_dates', [])
            
            # If no all_entity_ids, try to get from entity_id field or use risk_id as fallback
            if not all_entity_ids:
                entity_id_fallback = entity.get('entity_id', '')
                if entity_id_fallback and entity_id_fallback != risk_id:
                    all_entity_ids = [entity_id_fallback]
                else:
                    all_entity_ids = [risk_id]  # Last resort fallback
            
            # Enhanced styling based on risk
            severity_styles = {
                'Critical': 'border-l-red-500 bg-red-50 hover:bg-red-100',
                'Valuable': 'border-l-orange-500 bg-orange-50 hover:bg-orange-100',
                'Investigative': 'border-l-yellow-500 bg-yellow-50 hover:bg-yellow-100',
                'Probative': 'border-l-blue-500 bg-blue-50 hover:bg-blue-100'
            }
            row_style = severity_styles.get(risk_severity.title(), 'border-l-gray-500 bg-gray-50 hover:bg-gray-100')
            
            is_expanded = risk_id in self.expanded_entities
            
            # Sophisticated card-like row with comprehensive data
            with ui.card().classes(f'w-full border-l-4 {row_style} mb-3 transition-all duration-300 cursor-pointer shadow-sm hover:shadow-md'):
                with ui.column().classes('w-full p-4'):
                    # Header section with primary info
                    with ui.row().classes('w-full items-start justify-between mb-3'):
                        with ui.column().classes('flex-grow'):
                            # Entity name and risk ID with version info
                            ui.label(entity_name).classes('text-lg font-bold text-gray-800 mb-1')
                            ui.label(f'Risk ID: {risk_id}').classes('text-sm font-mono text-gray-600 bg-gray-100 px-2 py-1 rounded')
                            
                            # Entity IDs display
                            if len(all_entity_ids) > 1:
                                ui.label(f'Entity IDs: {all_entity_ids[0][:12]}... (+{len(all_entity_ids)-1} more)').classes('text-sm font-mono text-gray-600 bg-green-100 px-2 py-1 rounded mt-1')
                            elif len(all_entity_ids) == 1:
                                ui.label(f'Entity ID: {all_entity_ids[0]}').classes('text-sm font-mono text-gray-600 bg-green-100 px-2 py-1 rounded mt-1')
                            
                            if entity_count > 1:
                                ui.label(f'📊 {entity_count} Entity Versions').classes('text-sm text-blue-600 font-semibold bg-blue-50 px-2 py-1 rounded mt-1')
                            
                            # Entity type
                            entity_type = self._get_entity_type(entity)
                            if entity_type and entity_type != 'Unknown':
                                ui.badge(entity_type).classes('mt-2 px-3 py-1 bg-indigo-100 text-indigo-800 font-semibold')
                        
                        # Risk score section
                        with ui.column().classes('items-end text-right'):
                            ui.label('Risk Assessment').classes('text-xs text-gray-500 mb-1')
                            ui.label(f'{risk_score}/100').classes('text-2xl font-bold text-gray-800')
                            ui.badge(risk_severity).classes('px-2 py-1 text-xs font-medium')
                    
                    # Comprehensive metrics section - centered and naturally flowing
                    with ui.row().classes('w-full items-center justify-center gap-16 py-3 px-6'):
                        # Events section
                        events = entity.get('events', [])
                        event_count = len(events) if isinstance(events, list) else entity.get('event_count', 0)
                        with ui.column().classes('items-center'):
                            ui.icon('event_note').classes('text-blue-600 mb-1')
                            ui.label(str(event_count)).classes('text-lg font-bold text-gray-800')
                            ui.label('Events').classes('text-xs text-gray-600')
                        
                        # Relationships section
                        relationships = entity.get('relationships', [])
                        rel_count = len(relationships) if isinstance(relationships, list) else entity.get('relationship_count', 0)
                        with ui.column().classes('items-center'):
                            ui.icon('account_tree').classes('text-green-600 mb-1')
                            ui.label(str(rel_count)).classes('text-lg font-bold text-gray-800')
                            ui.label('Relations').classes('text-xs text-gray-600')
                        
                        # PEP status
                        with ui.column().classes('items-center'):
                            pep_status = entity.get('pep_status') == 'PEP' or entity.get('pep_levels')
                            ui.icon('account_balance').classes('text-orange-600 mb-1' if pep_status else 'text-gray-400 mb-1')
                            ui.label('YES' if pep_status else 'NO').classes('text-lg font-bold text-gray-800')
                            ui.label('PEP Status').classes('text-xs text-gray-600')
                        
                        # Country section with full name display
                        country = entity.get('primary_country', entity.get('country', ''))
                        with ui.column().classes('items-center'):
                            ui.icon('public').classes('text-purple-600 mb-1')
                            ui.label(country if country else 'N/A').classes('text-lg font-bold text-gray-800')
                            ui.label('Country').classes('text-xs text-gray-600')
                    
                    # Additional info section
                    with ui.row().classes('w-full items-center justify-between mt-3 pt-3 border-t border-gray-200'):
                        # Latest event date
                        latest_event = ''
                        if events and isinstance(events, list):
                            sorted_events = sorted(events, key=lambda x: x.get('event_date', ''), reverse=True)
                            if sorted_events:
                                latest_event = sorted_events[0].get('event_date', '')
                        
                        with ui.row().classes('items-center gap-2 text-sm text-gray-600'):
                            ui.icon('schedule', size='sm')
                            ui.label(f'Last Event: {latest_event[:10] if latest_event else "None"}')
                        
                        # Created date
                        created_date = str(entity.get('created_date', ''))[:10]
                        with ui.row().classes('items-center gap-2 text-sm text-gray-600'):
                            ui.icon('calendar_today', size='sm')
                            ui.label(f'Created: {created_date if created_date != "N" else "Unknown"}')
                        
                        # Expand/collapse button with proper entity capture
                        ui.button(
                            'View Details' if not is_expanded else 'Hide Details',
                            icon='visibility' if not is_expanded else 'visibility_off',
                            on_click=lambda e=entity: self._toggle_entity_details(e)
                        ).props('size=sm outline')
                    
                    # Expandable details section
                    if is_expanded:
                        with ui.column().classes('w-full mt-4 pt-4 border-t border-gray-300'):
                            self._create_comprehensive_details(entity)
                    
        except Exception as e:
            logger.error(f"Error creating sophisticated row: {e}")
            with ui.card().classes('w-full mb-3 border-red-300 bg-red-50 p-4'):
                ui.label(f'Error displaying: {entity.get("entity_name", "Unknown")}').classes('text-red-600')
    
    def _create_table_view(self):
        """Create a sophisticated table view with comprehensive data"""
        try:
            # Enhanced table columns with more data
            columns = [
                {'name': 'indicator', 'label': '●', 'field': 'indicator', 'align': 'center'},
                {'name': 'name', 'label': 'Entity Name', 'field': 'name', 'align': 'left', 'sortable': True},
                {'name': 'risk_id', 'label': 'Risk ID', 'field': 'risk_id', 'align': 'left'},
                {'name': 'entity_ids', 'label': 'Entity IDs', 'field': 'entity_ids', 'align': 'left'},
                {'name': 'type', 'label': 'Type', 'field': 'type', 'align': 'center'},
                {'name': 'risk_score', 'label': 'Risk Score', 'field': 'risk_score', 'align': 'right', 'sortable': True},
                {'name': 'risk_level', 'label': 'Risk Level', 'field': 'risk_level', 'align': 'center'},
                {'name': 'events', 'label': 'Events', 'field': 'events', 'align': 'right', 'sortable': True},
                {'name': 'relations', 'label': 'Relations', 'field': 'relations', 'align': 'right'},
                {'name': 'pep', 'label': 'PEP', 'field': 'pep', 'align': 'center'},
                {'name': 'country', 'label': 'Country', 'field': 'country', 'align': 'left'},
                {'name': 'last_event', 'label': 'Latest Event', 'field': 'last_event', 'align': 'center'},
                {'name': 'actions', 'label': 'Actions', 'field': 'actions', 'align': 'center'},
            ]
            
            rows = []
            for entity in self.filtered_entities[:100]:  # Limit for performance
                risk_score = entity.get('risk_score', 0)
                risk_severity = entity.get('risk_severity', 'Probative')
                
                # Risk indicator with color
                severity_indicators = {
                    'Critical': '🔴',
                    'Valuable': '🟠', 
                    'Investigative': '🟡',
                    'Probative': '🔵'
                }
                indicator = severity_indicators.get(risk_severity.title(), '⚫')
                
                # Get latest event
                events = entity.get('events', [])
                latest_event = 'None'
                if events and isinstance(events, list):
                    sorted_events = sorted(events, key=lambda x: x.get('event_date', ''), reverse=True)
                    if sorted_events:
                        latest_event = sorted_events[0].get('event_date', '')[:10]
                
                # PEP status
                pep_status = entity.get('pep_status') == 'PEP' or entity.get('pep_levels')
                
                # Format entity IDs display - try multiple sources
                all_entity_ids = entity.get('all_entity_ids', [])
                
                # If no all_entity_ids, try alternative sources
                if not all_entity_ids or all_entity_ids == []:
                    # Try getting from entity_id field first
                    entity_id_fallback = entity.get('entity_id', '')
                    if entity_id_fallback and entity_id_fallback != entity.get('risk_id', ''):
                        all_entity_ids = [entity_id_fallback]
                    else:
                        # Try to get from system_id or other identifier fields
                        system_id = entity.get('system_id', '')
                        if system_id:
                            all_entity_ids = [system_id]
                        else:
                            # Last resort: use risk_id but mark it clearly
                            risk_id = entity.get('risk_id', '')
                            if risk_id:
                                all_entity_ids = [f"(Risk ID) {risk_id}"]
                
                # Format display
                if len(all_entity_ids) > 1:
                    entity_ids_display = f"{all_entity_ids[0][:12]}... (+{len(all_entity_ids)-1})"
                elif len(all_entity_ids) == 1 and all_entity_ids[0]:
                    entity_id = str(all_entity_ids[0])
                    entity_ids_display = entity_id[:18] + ('...' if len(entity_id) > 18 else '')
                else:
                    entity_ids_display = 'No Entity ID'
                
                rows.append({
                    'indicator': indicator,
                    'name': entity.get('entity_name', 'Unknown'),
                    'risk_id': entity.get('risk_id', 'Unknown')[:15] + ('...' if len(str(entity.get('risk_id', ''))) > 15 else ''),
                    'entity_ids': entity_ids_display,
                    'type': self._get_entity_type(entity),
                    'risk_score': f'{risk_score}/100',
                    'risk_level': risk_severity,
                    'events': len(entity.get('events', [])),
                    'relations': len(entity.get('relationships', [])),
                    'pep': '✅ YES' if pep_status else '❌ NO',
                    'country': entity.get('primary_country', entity.get('country', 'N/A')),
                    'last_event': latest_event,
                    'actions': '👁️ Details',
                    '_entity': entity  # Store full entity for row clicks
                })
            
            # Create enhanced table with horizontal scrolling for long text
            with ui.scroll_area().classes('w-full').style('overflow-x: auto;'):
                table = ui.table(columns=columns, rows=rows, row_key='name').classes('shadow-lg rounded-lg')
                table.props('flat bordered dense')
                table.style('font-size: 14px; min-width: 1200px; white-space: nowrap;')
            
                # Row click handler with enhanced data extraction
                def handle_row_click(e):
                    try:
                        logger.debug(f"Table row click - Args: {getattr(e, 'args', None)}")
                        
                        # Extract entity from the row click event
                        entity = None
                        if hasattr(e, 'args') and e.args:
                            # Handle different args formats
                            if isinstance(e.args, dict) and '_entity' in e.args:
                                entity = e.args['_entity']
                            elif isinstance(e.args, list) and len(e.args) >= 2:
                                # Second item in list is often the row data
                                row_data = e.args[1]
                                if isinstance(row_data, dict) and '_entity' in row_data:
                                    entity = row_data['_entity']
                            elif hasattr(e.args, '_entity'):
                                entity = getattr(e.args, '_entity')
                        
                        if entity:
                            logger.info(f"Opening modal for: {entity.get('entity_name', 'Unknown')}")
                            self._show_table_entity_modal(entity)
                        else:
                            logger.warning(f"No entity found in click event. Args type: {type(getattr(e, 'args', None))}, Args length: {len(getattr(e, 'args', []))}") 
                            ui.notify('Unable to load entity details. Row data not accessible.', type='warning')
                            
                    except Exception as ex:
                        logger.error(f"Error handling table row click: {ex}", exc_info=True)
                        ui.notify('Error opening entity details', type='negative')
                
                table.on('rowClick', handle_row_click)
            
        except Exception as e:
            logger.error(f"Error creating table view: {e}")
            ui.label(f'Error creating table: {str(e)}').classes('text-red-500 p-4')
    
    def _create_comprehensive_details(self, entity: Dict[str, Any]):
        """Create comprehensive expandable details section with ALL entity data"""
        try:
            # Create tabs for organized data display
            with ui.tabs().classes('w-full') as tabs:
                overview_tab = ui.tab('Overview', icon='dashboard')
                events_tab = ui.tab(f'Events ({len(entity.get("events", []))})', icon='event')
                relationships_tab = ui.tab(f'Relations ({len(entity.get("relationships", []))})', icon='group')
                addresses_tab = ui.tab('Addresses', icon='location_on')
                raw_data_tab = ui.tab('Raw Data', icon='code')
            
            with ui.tab_panels(tabs, value=overview_tab).classes('w-full'):
                # Overview Tab
                with ui.tab_panel(overview_tab):
                    with ui.row().classes('w-full gap-6'):
                        # Left column - Basic info
                        with ui.column().classes('flex-1'):
                            ui.label('📋 Entity Information').classes('text-h6 font-bold mb-3')
                            with ui.card().classes('w-full p-4 bg-gray-50'):
                                info_items = [
                                    ('Risk ID', entity.get('risk_id', entity.get('entity_id', 'N/A'))),
                                    ('Entity Versions', str(entity.get('entity_count', 1))),
                                    ('System ID', entity.get('system_id', 'N/A')),
                                    ('BVD ID', entity.get('bvd_id', 'N/A')),
                                    ('Source Item ID', entity.get('source_item_id', 'N/A')),
                                    ('Entity Type', self._get_entity_type(entity)),
                                    ('Created Date', str(entity.get('created_date', 'N/A'))),
                                ]
                                for label, value in info_items:
                                    with ui.row().classes('justify-between mb-2'):
                                        ui.label(f'{label}:').classes('font-medium text-gray-700')
                                        ui.label(str(value)).classes('text-gray-900')
                            
                            # Entity Version History (if multiple versions exist)
                            entity_count = entity.get('entity_count', 1)
                            all_entity_ids = entity.get('all_entity_ids', [])
                            all_entity_dates = entity.get('all_entity_dates', [])
                            if entity_count > 1 and len(all_entity_ids) > 1:
                                ui.label(f'📈 Entity Version History ({entity_count} versions)').classes('text-h6 font-bold mb-3 mt-4')
                                with ui.card().classes('w-full p-3 bg-blue-50'):
                                    ui.label('Chronological order (newest first):').classes('text-sm font-medium mb-2 text-blue-800')
                                    # Combine entity IDs with dates and sort by date (newest first)
                                    if len(all_entity_ids) == len(all_entity_dates):
                                        version_data = list(zip(all_entity_ids, all_entity_dates))
                                        # Sort by date descending (newest first)
                                        version_data.sort(key=lambda x: x[1] if x[1] else '', reverse=True)
                                        for i, (eid, date) in enumerate(version_data[:10]):  # Show up to 10 versions
                                            date_str = str(date) if date else 'Unknown Date'
                                            version_label = f'v{i+1}' if i == 0 else f'v{i+1}'
                                            if i == 0:
                                                ui.label(f'• {version_label} (Latest): {eid} - {date_str}').classes('text-sm mb-1 font-semibold text-blue-700')
                                            else:
                                                ui.label(f'• {version_label}: {eid} - {date_str}').classes('text-sm mb-1 text-blue-600')
                                    else:
                                        # Fallback if dates don't match
                                        for i, eid in enumerate(all_entity_ids[:10]):
                                            ui.label(f'• Version {i+1}: {eid}').classes('text-sm mb-1 text-blue-600')
                            
                            # Aliases
                            aliases = entity.get('aliases', [])
                            if aliases:
                                ui.label(f'🏷️ Aliases ({len(aliases)})').classes('text-h6 font-bold mb-3 mt-4')
                                with ui.card().classes('w-full p-3 bg-yellow-50'):
                                    for alias in aliases[:10]:
                                        ui.label(f'• {alias}').classes('text-sm mb-1')
                        
                        # Right column - Risk and PEP
                        with ui.column().classes('flex-1'):
                            # Risk Information
                            ui.label('⚠️ Risk Assessment').classes('text-h6 font-bold mb-3')
                            with ui.card().classes('w-full p-4 bg-red-50'):
                                risk_factors = entity.get('risk_factors', {})
                                ui.label(f'Total Risk Score: {entity.get("risk_score", 0)}/100').classes('text-lg font-bold mb-2')
                                ui.label(f'Risk Severity: {entity.get("risk_severity", "Unknown")}').classes('font-medium mb-3')
                                
                                if risk_factors and isinstance(risk_factors, dict):
                                    ui.label('Risk Breakdown:').classes('font-medium mb-2')
                                    for factor, score in risk_factors.items():
                                        with ui.row().classes('justify-between mb-1'):
                                            ui.label(f'{factor.replace("_", " ").title()}:').classes('text-sm')
                                            ui.label(str(score)).classes('text-sm font-medium')
                            
                            # PEP Information
                            if entity.get('pep_status') == 'PEP' or entity.get('pep_levels'):
                                ui.label('🏛️ PEP Information').classes('text-h6 font-bold mb-3 mt-4')
                                with ui.card().classes('w-full p-3 bg-orange-50 border-l-4 border-orange-500'):
                                    pep_levels = entity.get('pep_levels', [])
                                    if pep_levels:
                                        ui.label('PEP Levels:').classes('font-medium mb-2')
                                        for level in pep_levels[:10]:
                                            ui.label(f'• {level}').classes('text-sm mb-1')
                                    pep_descriptions = entity.get('pep_descriptions', [])
                                    if pep_descriptions:
                                        ui.label('PEP Roles:').classes('font-medium mt-3 mb-2')
                                        for desc in pep_descriptions[:10]:
                                            ui.label(f'• {desc}').classes('text-sm mb-1')
                
                # Events Tab
                with ui.tab_panel(events_tab):
                    events = entity.get('events', [])
                    if events:
                        # Sort events by created_date (newest first) to show recently added events at top
                        sorted_events = sorted(events, 
                                             key=lambda x: x.get('created_date', x.get('event_date', '')), 
                                             reverse=True)
                        
                        # Add a summary of event timeline
                        if len(sorted_events) > 1:
                            first_event_date = min(e.get('created_date', e.get('event_date', '9999')) for e in sorted_events if e.get('created_date') or e.get('event_date'))
                            last_event_date = max(e.get('created_date', e.get('event_date', '0000')) for e in sorted_events if e.get('created_date') or e.get('event_date'))
                            
                            with ui.card().classes('w-full p-3 mb-4 bg-gray-100'):
                                ui.label(f'Event Timeline: {len(sorted_events)} events tracked').classes('font-bold text-sm')
                                ui.label(f'First added: {first_event_date[:10] if first_event_date != "9999" else "Unknown"}').classes('text-xs text-gray-600')
                                ui.label(f'Most recent: {last_event_date[:10] if last_event_date != "0000" else "Unknown"}').classes('text-xs text-gray-600')
                        
                        with ui.scroll_area().classes('w-full h-96'):
                            for i, event in enumerate(sorted_events):
                                # Determine if this is a recent event (added in last 7 days)
                                is_recent = False
                                if event.get('created_date'):
                                    from datetime import datetime, timedelta
                                    try:
                                        created = datetime.strptime(event['created_date'][:10], '%Y-%m-%d')
                                        is_recent = (datetime.now() - created).days <= 7
                                    except:
                                        pass
                                
                                # Card styling based on recency
                                card_classes = 'w-full p-4 mb-3 border-l-4'
                                if is_recent:
                                    card_classes += ' bg-yellow-50 border-yellow-500'  # Highlight recent events
                                else:
                                    card_classes += ' bg-blue-50 border-blue-500'
                                
                                with ui.card().classes(card_classes):
                                    with ui.row().classes('justify-between items-start mb-2'):
                                        event_label = f'Event #{i+1}'
                                        if is_recent:
                                            with ui.row().classes('items-center gap-2'):
                                                ui.label(event_label).classes('font-bold text-blue-800')
                                                ui.badge('NEW', color='yellow').classes('text-xs')
                                        else:
                                            ui.label(event_label).classes('font-bold text-blue-800')
                                        ui.label(f"Occurred: {event.get('event_date', 'N/A')}").classes('text-sm text-gray-600')
                                    
                                    ui.label(f"Category: {event.get('event_category_code', 'N/A')}").classes('font-medium mb-1')
                                    ui.label(f"Sub-category: {event.get('event_sub_category_code', 'N/A')}").classes('text-sm mb-2')
                                    
                                    # Entity version information for this event
                                    if event.get('entity_id') and event.get('entity_version_date'):
                                        ui.label(f"📌 Entity Version: {event['entity_id']} (Date: {str(event['entity_version_date'])[:10]})").classes('text-xs text-purple-700 bg-purple-50 px-2 py-1 rounded mb-2')
                                    
                                    # Enhanced timestamp information
                                    timestamp_info = []
                                    if event.get('created_date'):
                                        timestamp_info.append(f"Created: {event['created_date'][:10]}")
                                    if event.get('modified_date') and event.get('modified_date') != event.get('created_date'):
                                        timestamp_info.append(f"Modified: {event['modified_date'][:10]}")
                                    if event.get('publish_date'):
                                        timestamp_info.append(f"Published: {event['publish_date'][:10]}")
                                    
                                    if timestamp_info:
                                        with ui.row().classes('items-center gap-2 mb-2'):
                                            ui.icon('schedule', size='sm').classes('text-orange-600')
                                            ui.label(' | '.join(timestamp_info)).classes('text-xs text-orange-700 font-medium')
                                    
                                    # Enhanced source information  
                                    source_info = []
                                    if event.get('source_name'):
                                        source_info.append(f"Source: {event['source_name']}")
                                    if event.get('source_publication'):
                                        source_info.append(f"Publication: {event['source_publication']}")
                                    if event.get('source_publisher'):
                                        source_info.append(f"Publisher: {event['source_publisher']}")
                                    
                                    if source_info:
                                        with ui.row().classes('items-center gap-2 mb-2'):
                                            ui.icon('source', size='sm').classes('text-green-600')
                                            ui.label(' | '.join(source_info)).classes('text-xs text-green-700 font-medium')
                                    
                                    desc = event.get('event_description', event.get('event_description_short', ''))
                                    if desc:
                                        ui.label('Description:').classes('font-medium')
                                        ui.label(desc).classes('text-sm text-gray-700 mt-1')
                    else:
                        ui.label('No events recorded').classes('text-center text-gray-500 p-8')
                
                # Relationships Tab
                with ui.tab_panel(relationships_tab):
                    relationships = entity.get('relationships', [])
                    reverse_relationships = entity.get('reverse_relationships', [])
                    
                    if relationships or reverse_relationships:
                        with ui.scroll_area().classes('w-full h-96'):
                            if relationships:
                                ui.label(f'➡️ Outgoing Relationships ({len(relationships)})').classes('text-h6 font-bold mb-3')
                                for rel in relationships:
                                    with ui.card().classes('w-full p-3 mb-2 bg-green-50'):
                                        ui.label(f"{rel.get('related_entity_name', 'Unknown')}").classes('font-medium')
                                        ui.label(f"Type: {rel.get('type', 'Unknown')}").classes('text-sm')
                                        ui.label(f"ID: {rel.get('related_entity_id', 'N/A')}").classes('text-xs text-gray-600')
                            
                            if reverse_relationships:
                                ui.label(f'⬅️ Incoming Relationships ({len(reverse_relationships)})').classes('text-h6 font-bold mb-3 mt-6')
                                for rel in reverse_relationships:
                                    with ui.card().classes('w-full p-3 mb-2 bg-purple-50'):
                                        ui.label(f"{rel.get('related_entity_name', 'Unknown')}").classes('font-medium')
                                        ui.label(f"Type: {rel.get('type', 'Unknown')}").classes('text-sm')
                                        ui.label(f"ID: {rel.get('related_entity_id', 'N/A')}").classes('text-xs text-gray-600')
                    else:
                        ui.label('No relationships recorded').classes('text-center text-gray-500 p-8')
                
                # Addresses Tab
                with ui.tab_panel(addresses_tab):
                    addresses = entity.get('addresses', [])
                    identifications = entity.get('identifications', [])
                    
                    with ui.scroll_area().classes('w-full h-96'):
                        if addresses:
                            ui.label(f'📍 Addresses ({len(addresses)})').classes('text-h6 font-bold mb-3')
                            for addr in addresses:
                                with ui.card().classes('w-full p-4 mb-3 bg-purple-50'):
                                    if addr.get('address_type'):
                                        ui.badge(addr['address_type']).classes('mb-2')
                                    
                                    # Full address
                                    addr_fields = ['address_line1', 'address_line2', 'address_city', 
                                                  'address_province', 'address_postal_code', 'address_country']
                                    addr_parts = [addr.get(field, '') for field in addr_fields if addr.get(field)]
                                    if addr_parts:
                                        ui.label(', '.join(addr_parts)).classes('font-medium')
                                    
                                    if addr.get('address_raw_format'):
                                        ui.label('Raw format:').classes('text-sm font-medium mt-2')
                                        ui.label(addr['address_raw_format']).classes('text-sm text-gray-600')
                        
                        if identifications:
                            ui.label(f'🆔 Identifications ({len(identifications)})').classes('text-h6 font-bold mb-3 mt-6')
                            for ident in identifications:
                                with ui.card().classes('w-full p-3 mb-2 bg-indigo-50'):
                                    ui.label(f"Type: {ident.get('type', 'Unknown')}").classes('font-medium')
                                    ui.label(f"Value: {ident.get('value', 'N/A')}").classes('text-sm')
                                    if ident.get('location'):
                                        ui.label(f"Location: {ident['location']}").classes('text-xs text-gray-600')
                                    if ident.get('country'):
                                        ui.label(f"Country: {ident['country']}").classes('text-xs text-gray-600')
                        
                        if not addresses and not identifications:
                            ui.label('No address or identification data').classes('text-center text-gray-500 p-8')
                
                # Raw Data Tab
                with ui.tab_panel(raw_data_tab):
                    ui.label('🔍 Complete Entity Data (JSON)').classes('text-h6 font-bold mb-3')
                    with ui.row().classes('w-full justify-end mb-2'):
                        ui.button('Copy JSON', 
                                 on_click=lambda: ui.run_javascript(f'navigator.clipboard.writeText({json.dumps(json.dumps(entity, indent=2, default=str, ensure_ascii=False))})'),
                                 icon='content_copy').props('size=sm')
                    
                    with ui.scroll_area().classes('w-full h-96 bg-gray-50 rounded border'):
                        ui.code(json.dumps(entity, indent=2, default=str, ensure_ascii=False)).classes('w-full text-xs')
            
        except Exception as e:
            logger.error(f"Error creating comprehensive details: {e}", exc_info=True)
            ui.label(f'Error loading details: {str(e)}').classes('text-red-500')
    
    def _show_table_entity_modal(self, entity: Dict[str, Any]):
        """Show entity details in a modal when clicked from table view"""
        try:
            entity_name = entity.get('entity_name', 'Unknown Entity')
            
            with ui.dialog().classes('w-full max-w-6xl') as dialog:
                with ui.card().classes('w-full p-0'):
                    # Modal header
                    with ui.row().classes('w-full items-center justify-between p-4 bg-gradient-to-r from-blue-600 to-purple-600 text-white'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('account_circle', size='lg')
                            with ui.column().classes('gap-0'):
                                ui.label(entity_name).classes('text-h5 font-bold')
                                ui.label(f'Risk ID: {entity.get("risk_id", entity.get("entity_id", "N/A"))}').classes('text-sm opacity-90')
                                if entity.get('entity_count', 1) > 1:
                                    ui.label(f'📊 {entity.get("entity_count", 1)} Entity Versions').classes('text-xs text-blue-300')
                        
                        ui.button(icon='close', on_click=dialog.close).props('flat round color=white')
                    
                    # Modal content
                    with ui.column().classes('w-full p-6'):
                        self._create_comprehensive_details(entity)
            
            dialog.open()
            
        except Exception as e:
            logger.error(f"Error showing table modal: {e}")
            ui.notify(f'Error opening details: {str(e)}', type='negative')
    
    def _toggle_view_mode(self):
        """Toggle between list and table view modes"""
        if hasattr(self, 'view_mode'):
            self.view_mode = 'table' if self.view_mode == 'list' else 'list'
        else:
            self.view_mode = 'table'
        
        # Update button text and icon
        if hasattr(self, 'view_toggle_btn'):
            if self.view_mode == 'table':
                self.view_toggle_btn.props('icon=view_list')
                self.view_toggle_btn.text = 'List View'
            else:
                self.view_toggle_btn.props('icon=table_view')
                self.view_toggle_btn.text = 'Table View'
        
        # Re-render with new view mode
        self._render_entity_list()
        ui.notify(f'Switched to {self.view_mode} view', type='info')

# Global instance
hybrid_entity_display = HybridEntityDisplay()