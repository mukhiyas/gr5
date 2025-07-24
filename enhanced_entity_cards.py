"""
Enhanced Entity Cards with Comprehensive Detail Modals
Beautiful display of all entity data in organized modal views
"""

from nicegui import ui
from typing import Dict, Any, List
import logging
import json

logger = logging.getLogger(__name__)

class EnhancedEntityCards:
    """Enhanced card system with comprehensive detail modals"""
    
    def __init__(self, app_instance=None):
        self.app_instance = app_instance
    
    def create_enhanced_card(self, entity: Dict[str, Any]):
        """Create an enhanced card with detailed modal functionality"""
        try:
            # Get basic entity info
            entity_name = entity.get('entity_name', 'Unknown Entity')
            entity_id = entity.get('entity_id', 'Unknown ID')
            risk_score = entity.get('risk_score', 0)
            risk_severity = entity.get('risk_severity', 'Probative')
            
            # Determine card styling based on risk
            severity_colors = {
                'Critical': 'border-red-500 bg-red-50',
                'Valuable': 'border-orange-500 bg-orange-50', 
                'Investigative': 'border-yellow-500 bg-yellow-50',
                'Probative': 'border-blue-500 bg-blue-50'
            }
            card_class = severity_colors.get(risk_severity.title(), 'border-gray-500 bg-gray-50')
            
            # Create the card
            with ui.card().classes(f'cursor-pointer hover:shadow-lg transition-all border-l-4 {card_class} relative'):
                with ui.column().classes('gap-2 p-4'):
                    # Header row
                    with ui.row().classes('items-start justify-between w-full'):
                        with ui.column().classes('gap-1 flex-grow'):
                            ui.label(entity_name).classes('text-h6 font-bold')
                            ui.label(f'ID: {entity_id}').classes('text-caption text-gray-600')
                        
                        # Entity type badge
                        entity_type = self._get_clean_entity_type(entity)
                        if entity_type:
                            ui.badge(entity_type).classes('px-2 py-1 rounded-full text-xs')
                    
                    # Risk and key info row
                    with ui.row().classes('items-center gap-4 w-full'):
                        # Risk score
                        with ui.row().classes('items-center gap-1'):
                            ui.icon('warning', size='sm')
                            ui.label(f'Risk: {risk_score}').classes('font-medium')
                            ui.badge(risk_severity).classes(f'risk-{risk_severity.lower()} px-2 py-1 rounded text-xs')
                        
                        # Event count
                        events = entity.get('events', [])
                        event_count = len(events) if isinstance(events, list) else entity.get('event_count', 0)
                        if event_count > 0:
                            ui.chip(f'{event_count} events', icon='event').classes('text-xs')
                        
                        # PEP status
                        if entity.get('pep_status') == 'PEP' or entity.get('pep_levels'):
                            ui.chip('PEP', icon='account_balance', color='orange').classes('text-xs')
                    
                    # Quick info row
                    with ui.row().classes('items-center gap-2 w-full text-sm text-gray-600'):
                        # Country
                        country = entity.get('primary_country', entity.get('country', ''))
                        if country:
                            ui.label(f'📍 {country}')
                        
                        # Relationship count
                        relationships = entity.get('relationships', [])
                        rel_count = len(relationships) if isinstance(relationships, list) else entity.get('relationship_count', 0)
                        if rel_count > 0:
                            ui.label(f'🔗 {rel_count} connections')
                    
                    # Action buttons row
                    with ui.row().classes('justify-end gap-2 mt-2'):
                        ui.button('View Details', 
                                 on_click=lambda: self._show_detailed_modal(entity),
                                 icon='visibility').props('size=sm color=primary')
                        
                        ui.button('Export', 
                                 on_click=lambda: self._export_single_entity(entity),
                                 icon='file_download').props('size=sm flat')
            
            logger.debug(f"Created enhanced card for {entity_name}")
            
        except Exception as e:
            logger.error(f"Error creating enhanced card: {e}")
            # Fallback simple card
            with ui.card().classes('border-red-300 bg-red-50 p-4'):
                ui.label(f'Error displaying: {entity.get("entity_name", "Unknown")}').classes('text-red-600')
    
    def _get_clean_entity_type(self, entity: Dict[str, Any]) -> str:
        """Get clean entity type for display"""
        entity_type = entity.get('recordDefinitionType', entity.get('entity_type', ''))
        
        if entity_type in ['I', 'INDIVIDUAL', 'Individual']:
            return 'Individual'
        elif entity_type in ['O', 'ORGANIZATION', 'Organization']:
            return 'Organization'
        elif entity_type and entity_type.lower() != 'unknown':
            return entity_type.title()
        return None
    
    def _show_detailed_modal(self, entity: Dict[str, Any]):
        """Show comprehensive detail modal for entity"""
        try:
            entity_name = entity.get('entity_name', 'Unknown Entity')
            
            with ui.dialog().classes('w-full max-w-6xl') as dialog:
                with ui.card().classes('w-full p-0'):
                    # Modal header
                    with ui.row().classes('w-full items-center justify-between p-4 border-b bg-blue-50'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('account_circle', size='lg').classes('text-blue-600')
                            with ui.column().classes('gap-0'):
                                ui.label(entity_name).classes('text-h5 font-bold')
                                ui.label(f'ID: {entity.get("entity_id", "N/A")}').classes('text-sm text-gray-600')
                        
                        ui.button(icon='close', on_click=dialog.close).props('flat round')
                    
                    # Modal content with tabs
                    with ui.tabs().classes('w-full') as tabs:
                        overview_tab = ui.tab('Overview', icon='info')
                        events_tab = ui.tab('Events', icon='event')
                        relationships_tab = ui.tab('Relationships', icon='group')
                        addresses_tab = ui.tab('Addresses', icon='location_on')
                        documents_tab = ui.tab('Documents', icon='description')
                        raw_data_tab = ui.tab('Raw Data', icon='code')
                    
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
                        
                        # Addresses panel
                        with ui.tab_panel(addresses_tab):
                            self._create_addresses_panel(entity)
                        
                        # Documents panel
                        with ui.tab_panel(documents_tab):
                            self._create_documents_panel(entity)
                        
                        # Raw data panel
                        with ui.tab_panel(raw_data_tab):
                            self._create_raw_data_panel(entity)
            
            dialog.open()
            logger.info(f"Opened detailed modal for {entity_name}")
            
        except Exception as e:
            logger.error(f"Error showing detailed modal: {e}")
            ui.notify(f'Error opening details: {str(e)}', type='negative')
    
    def _create_overview_panel(self, entity: Dict[str, Any]):
        """Create overview panel with key information"""
        with ui.column().classes('w-full gap-4 p-4'):
            # Basic info cards
            with ui.row().classes('w-full gap-4'):
                # Entity info card
                with ui.card().classes('flex-1 p-4'):
                    ui.label('Basic Information').classes('text-h6 font-bold mb-3')
                    
                    info_items = [
                        ('Name', entity.get('entity_name', 'N/A')),
                        ('Entity ID', entity.get('entity_id', 'N/A')),
                        ('Risk ID', entity.get('risk_id', 'N/A')),
                        ('Type', self._get_clean_entity_type(entity) or 'N/A'),
                        ('System ID', entity.get('systemId', 'N/A')),
                        ('Created Date', str(entity.get('created_date', 'N/A'))),
                    ]
                    
                    for label, value in info_items:
                        with ui.row().classes('justify-between items-center py-1'):
                            ui.label(f'{label}:').classes('font-medium text-gray-700')
                            ui.label(str(value)).classes('text-gray-900')
                
                # Risk assessment card
                with ui.card().classes('flex-1 p-4'):
                    ui.label('Risk Assessment').classes('text-h6 font-bold mb-3')
                    
                    risk_score = entity.get('risk_score', 0)
                    risk_severity = entity.get('risk_severity', 'Probative')
                    
                    # Risk score with progress bar
                    ui.label(f'Risk Score: {risk_score}/100').classes('font-medium mb-2')
                    ui.linear_progress(value=risk_score/100).classes('mb-3')
                    
                    # Risk severity badge
                    with ui.row().classes('items-center gap-2'):
                        ui.label('Severity:').classes('font-medium')
                        ui.badge(risk_severity).classes(f'risk-{risk_severity.lower()}')
                    
                    # Risk factors if available
                    risk_factors = entity.get('risk_factors', {})
                    if risk_factors:
                        ui.label('Risk Factors:').classes('font-medium mt-3 mb-1')
                        for factor, score in risk_factors.items():
                            ui.label(f'• {factor}: {score}').classes('text-sm text-gray-600')
            
            # PEP Information
            if entity.get('pep_status') == 'PEP' or entity.get('pep_levels'):
                with ui.card().classes('w-full p-4 bg-orange-50 border-orange-200'):
                    ui.label('🏛️ Politically Exposed Person (PEP)').classes('text-h6 font-bold mb-3')
                    
                    pep_levels = entity.get('pep_levels', [])
                    pep_descriptions = entity.get('pep_descriptions', [])
                    
                    if pep_levels:
                        ui.label('PEP Levels:').classes('font-medium mb-1')
                        for level in pep_levels[:5]:  # Show first 5
                            ui.chip(str(level), color='orange').classes('mr-1 mb-1')
                    
                    if pep_descriptions:
                        ui.label('Descriptions:').classes('font-medium mt-2 mb-1')
                        for desc in pep_descriptions[:3]:  # Show first 3
                            ui.label(f'• {desc}').classes('text-sm')
            
            # Geographic Information
            countries = []
            if entity.get('primary_country'):
                countries.append(entity['primary_country'])
            if entity.get('country') and entity['country'] not in countries:
                countries.append(entity['country'])
            
            if countries:
                with ui.card().classes('w-full p-4'):
                    ui.label('🌍 Geographic Information').classes('text-h6 font-bold mb-3')
                    ui.label('Countries: ' + ', '.join(countries)).classes('text-gray-900')
    
    def _create_events_panel(self, entity: Dict[str, Any]):
        """Create events panel"""
        with ui.column().classes('w-full gap-4 p-4'):
            events = entity.get('events', [])
            
            if not events:
                ui.label('No events recorded for this entity.').classes('text-center text-gray-500 p-8')
                return
            
            ui.label(f'Events ({len(events)})').classes('text-h6 font-bold mb-4')
            
            for i, event in enumerate(events[:20]):  # Show first 20 events
                with ui.card().classes('w-full p-4 mb-2'):
                    # Event header
                    with ui.row().classes('items-start justify-between mb-2'):
                        with ui.column().classes('gap-1'):
                            # Event code and description
                            event_code = event.get('event_category_code', 'N/A')
                            
                            # Get database description
                            try:
                                from database_driven_codes import get_event_description, get_event_risk_score
                                event_desc = get_event_description(event_code)
                                event_risk = get_event_risk_score(event_code)
                            except:
                                event_desc = event_code
                                event_risk = 0
                            
                            ui.label(f'{event_code}: {event_desc}').classes('font-bold')
                            
                            if event.get('event_sub_category_code'):
                                ui.label(f'Sub-category: {event["event_sub_category_code"]}').classes('text-sm text-gray-600')
                        
                        # Event risk badge
                        if event_risk > 0:
                            ui.badge(f'Risk: {event_risk}').classes('bg-red-100 text-red-800')
                    
                    # Event details
                    event_date = event.get('event_date', 'N/A')
                    event_description = event.get('event_description', event.get('event_description_short', ''))
                    
                    ui.label(f'Date: {event_date}').classes('text-sm font-medium mb-1')
                    
                    if event_description:
                        ui.label('Description:').classes('text-sm font-medium')
                        ui.label(event_description).classes('text-sm text-gray-700 italic')
    
    def _create_relationships_panel(self, entity: Dict[str, Any]):
        """Create relationships panel"""
        with ui.column().classes('w-full gap-4 p-4'):
            relationships = entity.get('relationships', [])
            reverse_relationships = entity.get('reverse_relationships', [])
            
            if not relationships and not reverse_relationships:
                ui.label('No relationships recorded for this entity.').classes('text-center text-gray-500 p-8')
                return
            
            # Outgoing relationships
            if relationships:
                ui.label(f'Outgoing Relationships ({len(relationships)})').classes('text-h6 font-bold mb-3')
                
                for rel in relationships[:15]:  # Show first 15
                    with ui.card().classes('w-full p-3 mb-2 bg-blue-50'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('arrow_forward', color='blue')
                            with ui.column().classes('gap-0 flex-grow'):
                                ui.label(rel.get('related_entity_name', 'Unknown Entity')).classes('font-medium')
                                ui.label(f'Type: {rel.get("type", "Unknown")}').classes('text-sm text-gray-600')
                            
                            if rel.get('related_entity_id'):
                                ui.label(f'ID: {rel["related_entity_id"]}').classes('text-xs text-gray-500')
            
            # Incoming relationships
            if reverse_relationships:
                ui.label(f'Incoming Relationships ({len(reverse_relationships)})').classes('text-h6 font-bold mb-3 mt-6')
                
                for rel in reverse_relationships[:15]:  # Show first 15
                    with ui.card().classes('w-full p-3 mb-2 bg-green-50'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon('arrow_back', color='green')
                            with ui.column().classes('gap-0 flex-grow'):
                                ui.label(rel.get('related_entity_name', 'Unknown Entity')).classes('font-medium')
                                ui.label(f'Type: {rel.get("type", "Unknown")}').classes('text-sm text-gray-600')
                            
                            if rel.get('related_entity_id'):
                                ui.label(f'ID: {rel["related_entity_id"]}').classes('text-xs text-gray-500')
    
    def _create_addresses_panel(self, entity: Dict[str, Any]):
        """Create addresses panel"""
        with ui.column().classes('w-full gap-4 p-4'):
            addresses = entity.get('addresses', [])
            
            if not addresses:
                ui.label('No addresses recorded for this entity.').classes('text-center text-gray-500 p-8')
                return
            
            ui.label(f'Addresses ({len(addresses)})').classes('text-h6 font-bold mb-4')
            
            for addr in addresses:
                with ui.card().classes('w-full p-4 mb-3'):
                    # Address type
                    addr_type = addr.get('address_type', 'Unknown')
                    if addr_type:
                        ui.badge(addr_type).classes('mb-2')
                    
                    # Formatted address
                    address_parts = []
                    if addr.get('address_line1'):
                        address_parts.append(addr['address_line1'])
                    if addr.get('address_line2'):
                        address_parts.append(addr['address_line2'])
                    if addr.get('address_city'):
                        address_parts.append(addr['address_city'])
                    if addr.get('address_province'):
                        address_parts.append(addr['address_province'])
                    if addr.get('address_postal_code'):
                        address_parts.append(addr['address_postal_code'])
                    if addr.get('address_country'):
                        address_parts.append(addr['address_country'])
                    
                    if address_parts:
                        ui.label(', '.join(address_parts)).classes('text-gray-900')
                    
                    # Raw format if available
                    if addr.get('address_raw_format'):
                        ui.label('Raw:').classes('text-sm font-medium mt-2')
                        ui.label(addr['address_raw_format']).classes('text-sm text-gray-600 italic')
    
    def _create_documents_panel(self, entity: Dict[str, Any]):
        """Create documents/identifications panel"""
        with ui.column().classes('w-full gap-4 p-4'):
            # Identifications
            identifications = entity.get('identifications', [])
            aliases = entity.get('aliases', [])
            
            # Identifications section
            if identifications:
                ui.label(f'Identifications ({len(identifications)})').classes('text-h6 font-bold mb-3')
                
                for ident in identifications:
                    with ui.card().classes('w-full p-4 mb-3'):
                        if ident.get('type'):
                            ui.label(f'Type: {ident["type"]}').classes('font-medium')
                        if ident.get('value'):
                            ui.label(f'Value: {ident["value"]}').classes('text-gray-900')
                        if ident.get('location'):
                            ui.label(f'Location: {ident["location"]}').classes('text-sm text-gray-600')
                        if ident.get('country'):
                            ui.label(f'Country: {ident["country"]}').classes('text-sm text-gray-600')
            
            # Aliases section
            if aliases:
                ui.label(f'Aliases ({len(aliases)})').classes('text-h6 font-bold mb-3 mt-6')
                
                for alias in aliases:
                    with ui.card().classes('w-full p-3 mb-2 bg-yellow-50'):
                        if isinstance(alias, dict):
                            ui.label(alias.get('alias_name', str(alias))).classes('font-medium')
                        else:
                            ui.label(str(alias)).classes('font-medium')
            
            if not identifications and not aliases:
                ui.label('No identification documents or aliases recorded.').classes('text-center text-gray-500 p-8')
    
    def _create_raw_data_panel(self, entity: Dict[str, Any]):
        """Create raw data panel"""
        with ui.column().classes('w-full gap-4 p-4'):
            ui.label('Raw Entity Data').classes('text-h6 font-bold mb-3')
            
            # JSON display with copy functionality
            json_str = json.dumps(entity, indent=2, default=str, ensure_ascii=False)
            
            with ui.row().classes('w-full justify-end mb-2'):
                ui.button('Copy JSON', 
                         on_click=lambda: ui.run_javascript(f'navigator.clipboard.writeText({json.dumps(json_str)})'),
                         icon='content_copy').props('size=sm')
            
            with ui.scroll_area().classes('w-full h-96 bg-gray-50 rounded border'):
                ui.code(json_str).classes('w-full text-xs')
    
    def _export_single_entity(self, entity: Dict[str, Any]):
        """Export single entity data"""
        try:
            if self.app_instance and hasattr(self.app_instance, 'exporter'):
                filename = self.app_instance.exporter.export_to_excel([entity])
                ui.notify(f'✅ Exported entity to {filename}', type='positive')
            else:
                # Fallback export
                from entity_exports import EntityExporter
                exporter = EntityExporter()
                filename = exporter.export_to_excel([entity])
                ui.notify(f'✅ Exported entity to Excel', type='positive')
                
        except Exception as e:
            logger.error(f"Single entity export error: {e}")
            ui.notify(f'❌ Export failed: {str(e)}', type='negative')

# Global instance
enhanced_entity_cards = EnhancedEntityCards()