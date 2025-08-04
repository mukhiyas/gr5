"""
Comprehensive Entity Export Module
Handles Excel, CSV, and JSON exports with complete data preservation
Based on actual entity data structure from search results
"""

import pandas as pd
import json
import tempfile
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class EntityExporter:
    """Complete entity data exporter with no data loss"""
    
    def __init__(self, app_instance=None):
        """Initialize the exporter with field mappings"""
        self.app_instance = app_instance
        self.core_fields = [
            'entity_id', 'risk_id', 'entity_name', 'entity_type', 'system_id', 
            'entity_date', 'created_date', 'source_item_id', 'bvd_id'
        ]
        
        self.risk_fields = [
            'risk_score', 'risk_level', 'risk_severity', 'calculated_risk_score', 
            'final_risk_score', 'risk_factors'
        ]
        
        self.pep_fields = [
            'is_pep', 'pep_status', 'pep_type', 'pep_level', 'highest_pep_level',
            'pep_codes', 'pep_levels', 'pep_descriptions', 'pep_associations'
        ]
        
        self.event_fields = [
            'event_count', 'event_codes', 'critical_events_count', 'latest_event_date'
        ]
        
        self.relationship_fields = [
            'relationship_count', 'source_count', 'alias_count'
        ]
        
        self.geographic_fields = [
            'primary_country', 'primary_city', 'country'
        ]
        
        self.additional_fields = [
            'prt_ratings', 'date_of_birth'
        ]
    
    def export_to_excel(self, entities: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
        """
        Export entities to Excel with comprehensive data coverage
        
        Args:
            entities: List of entity dictionaries
            filename: Optional filename, will generate if not provided
            
        Returns:
            str: Path to created Excel file
        """
        try:
            logger.info(f"Starting Excel export for {len(entities)} entities")
            
            # Flatten all entities to tabular format
            flattened_data = []
            for i, entity in enumerate(entities):
                try:
                    flat_entity = self._flatten_entity_for_excel(entity)
                    flattened_data.append(flat_entity)
                    logger.debug(f"Flattened entity {i+1}/{len(entities)}: {entity.get('entity_name', 'Unknown')}")
                except Exception as e:
                    logger.error(f"Error flattening entity {i+1}: {e}")
                    # Create minimal row to prevent complete failure
                    minimal_row = {
                        'Entity ID': entity.get('entity_id', ''),
                        'Entity Name': entity.get('entity_name', ''),
                        'Error': f'Data processing error: {str(e)}'
                    }
                    flattened_data.append(minimal_row)
            
            # Create DataFrame
            df = pd.DataFrame(flattened_data)
            
            # Generate filename if not provided
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"entity_export_{timestamp}.xlsx"
            
            # Create temporary file
            temp_dir = tempfile.gettempdir()
            filepath = Path(temp_dir) / filename
            
            # Write to Excel with multiple sheets
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name='Entity Data', index=False)
                
                # Summary sheet
                summary_df = self._create_summary_sheet(entities)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Events detail sheet (if entities have events)
                events_df = self._create_events_sheet(entities)
                if not events_df.empty:
                    events_df.to_excel(writer, sheet_name='Events Detail', index=False)
                
                # Relationships detail sheet
                relationships_df = self._create_relationships_sheet(entities)
                if not relationships_df.empty:
                    relationships_df.to_excel(writer, sheet_name='Relationships', index=False)
            
            logger.info(f"Excel export completed: {filepath} ({len(flattened_data)} entities)")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Excel export failed: {e}", exc_info=True)
            raise
    
    def export_to_csv(self, entities: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
        """
        Export entities to CSV with complete data flattening
        
        Args:
            entities: List of entity dictionaries
            filename: Optional filename, will generate if not provided
            
        Returns:
            str: Path to created CSV file
        """
        try:
            logger.info(f"Starting CSV export for {len(entities)} entities")
            
            # Flatten all entities
            flattened_data = []
            for i, entity in enumerate(entities):
                try:
                    flat_entity = self._flatten_entity_for_csv(entity)
                    flattened_data.append(flat_entity)
                    logger.debug(f"Flattened entity {i+1}/{len(entities)}: {entity.get('entity_name', 'Unknown')}")
                except Exception as e:
                    logger.error(f"Error flattening entity {i+1}: {e}")
                    # Create minimal row
                    minimal_row = {
                        'entity_id': entity.get('entity_id', ''),
                        'entity_name': entity.get('entity_name', ''),
                        'error': f'Data processing error: {str(e)}'
                    }
                    flattened_data.append(minimal_row)
            
            # Create DataFrame
            df = pd.DataFrame(flattened_data)
            
            # Generate filename if not provided
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"entity_export_{timestamp}.csv"
            
            # Create temporary file
            temp_dir = tempfile.gettempdir()
            filepath = Path(temp_dir) / filename
            
            # Write to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            logger.info(f"CSV export completed: {filepath} ({len(flattened_data)} entities)")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"CSV export failed: {e}", exc_info=True)
            raise
    
    def export_to_json(self, entities: List[Dict[str, Any]], filename: Optional[str] = None) -> str:
        """
        Export entities to JSON with complete data preservation
        
        Args:
            entities: List of entity dictionaries
            filename: Optional filename, will generate if not provided
            
        Returns:
            str: Path to created JSON file
        """
        try:
            logger.info(f"Starting JSON export for {len(entities)} entities")
            
            # Clean entities for JSON serialization
            clean_entities = []
            for entity in entities:
                clean_entity = self._clean_entity_for_json(entity)
                clean_entities.append(clean_entity)
            
            # Create export structure
            export_data = {
                'export_metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'entity_count': len(entities),
                    'format_version': '2.0',
                    'data_completeness': 'full'
                },
                'entities': clean_entities
            }
            
            # Generate filename if not provided
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"entity_export_{timestamp}.json"
            
            # Create temporary file
            temp_dir = tempfile.gettempdir()
            filepath = Path(temp_dir) / filename
            
            # Write to JSON
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"JSON export completed: {filepath} ({len(entities)} entities)")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"JSON export failed: {e}", exc_info=True)
            raise
    
    def _flatten_entity_for_excel(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten entity data for Excel export with comprehensive field coverage"""
        flat = {}
        
        # Core entity information
        flat['Entity ID'] = entity.get('entity_id', '')
        flat['Risk ID'] = entity.get('risk_id', '')
        flat['Entity Name'] = entity.get('entity_name', '')
        flat['Entity Type'] = entity.get('entity_type', '')
        flat['System ID'] = entity.get('system_id', '')
        flat['Entity Date'] = self._format_date(entity.get('entity_date'))
        flat['Created Date'] = self._format_date(entity.get('created_date'))
        flat['BVD ID'] = entity.get('bvd_id', '')
        flat['Source Item ID'] = entity.get('source_item_id', '')
        
        # Risk assessment
        flat['Risk Score'] = entity.get('risk_score', 0)
        flat['Risk Level'] = entity.get('risk_level', '')
        flat['Risk Severity'] = entity.get('risk_severity', '')
        flat['Calculated Risk Score'] = entity.get('calculated_risk_score', 0)
        flat['Final Risk Score'] = entity.get('final_risk_score', 0)
        
        # Risk factors breakdown
        risk_factors = entity.get('risk_factors', {})
        if isinstance(risk_factors, dict):
            flat['Event Risk'] = risk_factors.get('event_risk', 0)
            flat['PEP Risk'] = risk_factors.get('pep_risk', 0)
            flat['Geographic Risk'] = risk_factors.get('geographic_risk', 0)
            flat['Temporal Risk'] = risk_factors.get('temporal_risk', 0)
        
        # PEP information
        flat['Is PEP'] = entity.get('is_pep', False)
        flat['PEP Status'] = entity.get('pep_status', '')
        flat['PEP Type'] = entity.get('pep_type', '')
        flat['PEP Level'] = entity.get('pep_level', '')
        flat['Highest PEP Level'] = entity.get('highest_pep_level', '')
        flat['PEP Codes'] = self._list_to_string(entity.get('pep_codes', []))
        flat['PEP Levels'] = self._list_to_string(entity.get('pep_levels', []))
        flat['PEP Descriptions'] = self._list_to_string(entity.get('pep_descriptions', []), separator='; ')
        flat['PEP Associations'] = self._list_to_string(entity.get('pep_associations', []), separator='; ')
        
        # Event information with database-driven descriptions
        flat['Event Count'] = entity.get('event_count', 0)
        flat['Critical Events Count'] = entity.get('critical_events_count', 0)
        flat['Event Codes'] = self._list_to_string(entity.get('event_codes', []))
        flat['Latest Event Date'] = entity.get('latest_event_date', '')
        
        # Add database-driven event code descriptions
        event_codes = entity.get('event_codes', [])
        if event_codes and isinstance(event_codes, list) and self.app_instance:
            try:
                event_descriptions = []
                for code in event_codes[:5]:  # First 5 codes
                    desc = self.app_instance.get_event_description(code)
                    event_descriptions.append(f"{code}: {desc}")
                
                flat['Event Code Descriptions'] = self._list_to_string(event_descriptions, separator='; ')
            except Exception as e:
                logger.warning(f"Failed to get database-driven descriptions for export: {e}")
        
        # Add PEP descriptions
        pep_codes = entity.get('pep_codes', [])
        if pep_codes and isinstance(pep_codes, list) and self.app_instance:
            try:
                pep_descriptions = []
                for code in pep_codes[:3]:  # First 3 PEP codes
                    desc = self.app_instance.get_pep_description(code)
                    pep_descriptions.append(f"{code}: {desc}")
                
                flat['PEP Code Descriptions'] = self._list_to_string(pep_descriptions, separator='; ')
            except Exception as e:
                logger.warning(f"Failed to get PEP descriptions for export: {e}")
        
        # Events detail (first 5 events)
        events = entity.get('events', [])
        if events and isinstance(events, list):
            for i, event in enumerate(events[:5]):  # First 5 events
                if isinstance(event, dict):
                    event_code = event.get('event_category_code', '')
                    flat[f'Event {i+1} Code'] = event_code
                    flat[f'Event {i+1} Sub Code'] = event.get('event_sub_category_code', '')
                    flat[f'Event {i+1} Date'] = event.get('event_date', '')
                    flat[f'Event {i+1} Description'] = event.get('event_description', event.get('event_description_short', ''))
                    
                    # Add database-driven description and risk score
                    try:
                        from database_driven_codes import get_event_description, get_event_risk_score
                        flat[f'Event {i+1} DB Description'] = get_event_description(event_code)
                        flat[f'Event {i+1} Risk Score'] = get_event_risk_score(event_code)
                    except ImportError:
                        pass
        
        # Geographic information
        flat['Primary Country'] = entity.get('primary_country', '')
        flat['Primary City'] = entity.get('primary_city', '')
        flat['Country'] = entity.get('country', '')
        
        # Address information
        addresses = entity.get('addresses', [])
        if addresses and isinstance(addresses, list):
            for i, addr in enumerate(addresses[:3]):  # First 3 addresses
                if isinstance(addr, dict):
                    flat[f'Address {i+1} Country'] = addr.get('address_country', '')
                    flat[f'Address {i+1} City'] = addr.get('address_city', '')
                    flat[f'Address {i+1} Raw'] = addr.get('address_raw_format', '')
        
        # Aliases
        flat['Alias Count'] = entity.get('alias_count', 0)
        aliases = entity.get('aliases', [])
        if aliases:
            if isinstance(aliases, list):
                flat['All Aliases'] = '; '.join([str(alias) for alias in aliases])
            else:
                flat['All Aliases'] = str(aliases)
        
        # Key aliases with types
        key_aliases = entity.get('key_aliases', [])
        if key_aliases and isinstance(key_aliases, list):
            alias_details = []
            for alias in key_aliases[:10]:  # First 10 key aliases
                if isinstance(alias, dict):
                    name = alias.get('alias_name', '')
                    code_type = alias.get('alias_code_type', '')
                    if name:
                        alias_details.append(f"{name} ({code_type})" if code_type else name)
            flat['Key Aliases Detail'] = '; '.join(alias_details)
        
        # Relationships
        flat['Relationship Count'] = entity.get('relationship_count', 0)
        relationships = entity.get('relationships', [])
        if relationships and isinstance(relationships, list):
            rel_details = []
            for rel in relationships[:10]:  # First 10 relationships
                if isinstance(rel, dict):
                    name = rel.get('related_entity_name', '')
                    rel_type = rel.get('type', '')
                    direction = rel.get('direction', '')
                    if name:
                        rel_details.append(f"{name} ({rel_type}-{direction})")
            flat['Relationships Detail'] = '; '.join(rel_details)
        
        # Date of birth
        dob = entity.get('date_of_birth', {})
        if isinstance(dob, dict):
            year = dob.get('date_of_birth_year', '')
            month = dob.get('date_of_birth_month', '')
            day = dob.get('date_of_birth_day', '')
            if year:
                flat['Birth Year'] = year
                flat['Birth Month'] = month
                flat['Birth Day'] = day
                if year and month and day:
                    # Safe date formatting - convert strings to integers before formatting
                    try:
                        if str(month).isdigit() and str(day).isdigit():
                            flat['Date of Birth'] = f"{year}-{int(month):02d}-{int(day):02d}"
                        else:
                            flat['Date of Birth'] = f"{year}-{month}-{day}"
                    except (ValueError, TypeError):
                        flat['Date of Birth'] = f"{year}-{month}-{day}"
        
        # PRT ratings
        prt_ratings = entity.get('prt_ratings', [])
        if prt_ratings:
            flat['PRT Ratings'] = self._list_to_string(prt_ratings)
        
        # Source information
        flat['Source Count'] = entity.get('source_count', 0)
        sources = entity.get('sources', [])
        if sources and isinstance(sources, list):
            source_names = []
            for source in sources[:5]:  # First 5 sources
                if isinstance(source, dict):
                    name = source.get('name', '')
                    if name:
                        source_names.append(name)
            flat['Source Names'] = '; '.join(source_names)
        
        return flat
    
    def _flatten_entity_for_csv(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten entity data for CSV export - similar to Excel but more compact"""
        flat = {}
        
        # Core fields
        flat['entity_id'] = entity.get('entity_id', '')
        flat['risk_id'] = entity.get('risk_id', '')
        flat['entity_name'] = entity.get('entity_name', '')
        flat['entity_type'] = entity.get('entity_type', '')
        flat['system_id'] = entity.get('system_id', '')
        flat['entity_date'] = self._format_date(entity.get('entity_date'))
        flat['created_date'] = self._format_date(entity.get('created_date'))
        
        # Risk data
        flat['risk_score'] = entity.get('risk_score', 0)
        flat['risk_level'] = entity.get('risk_level', '')
        flat['risk_severity'] = entity.get('risk_severity', '')
        flat['calculated_risk_score'] = entity.get('calculated_risk_score', 0)
        flat['final_risk_score'] = entity.get('final_risk_score', 0)
        
        # Risk factors
        risk_factors = entity.get('risk_factors', {})
        if isinstance(risk_factors, dict):
            flat['event_risk'] = risk_factors.get('event_risk', 0)
            flat['pep_risk'] = risk_factors.get('pep_risk', 0)
            flat['geographic_risk'] = risk_factors.get('geographic_risk', 0)
            flat['temporal_risk'] = risk_factors.get('temporal_risk', 0)
        
        # PEP data
        flat['is_pep'] = entity.get('is_pep', False)
        flat['pep_status'] = entity.get('pep_status', '')
        flat['pep_type'] = entity.get('pep_type', '')
        flat['pep_level'] = entity.get('pep_level', '')
        flat['highest_pep_level'] = entity.get('highest_pep_level', '')
        flat['pep_codes'] = self._list_to_string(entity.get('pep_codes', []))
        flat['pep_levels'] = self._list_to_string(entity.get('pep_levels', []))
        flat['pep_descriptions'] = self._list_to_string(entity.get('pep_descriptions', []), separator=' | ')
        
        # Event data
        flat['event_count'] = entity.get('event_count', 0)
        flat['critical_events_count'] = entity.get('critical_events_count', 0)
        flat['event_codes'] = self._list_to_string(entity.get('event_codes', []))
        flat['latest_event_date'] = entity.get('latest_event_date', '')
        
        # Geographic
        flat['primary_country'] = entity.get('primary_country', '')
        flat['primary_city'] = entity.get('primary_city', '')
        flat['country'] = entity.get('country', '')
        
        # Counts
        flat['alias_count'] = entity.get('alias_count', 0)
        flat['relationship_count'] = entity.get('relationship_count', 0)
        flat['source_count'] = entity.get('source_count', 0)
        
        # Aliases (simplified)
        aliases = entity.get('aliases', [])
        if aliases:
            flat['aliases'] = self._list_to_string(aliases, separator=' | ')
        
        # Date of birth
        dob = entity.get('date_of_birth', {})
        if isinstance(dob, dict) and dob.get('date_of_birth_year'):
            year = dob.get('date_of_birth_year', '')
            month = dob.get('date_of_birth_month', '')
            day = dob.get('date_of_birth_day', '')
            flat['birth_year'] = year
            flat['birth_month'] = month
            flat['birth_day'] = day
        
        # BVD and other IDs
        flat['bvd_id'] = entity.get('bvd_id', '')
        flat['source_item_id'] = entity.get('source_item_id', '')
        
        return flat
    
    def _clean_entity_for_json(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Clean entity for JSON serialization"""
        clean = {}
        
        for key, value in entity.items():
            try:
                # Convert datetime objects to strings
                if hasattr(value, 'isoformat'):
                    clean[key] = value.isoformat()
                elif isinstance(value, (datetime,)):
                    clean[key] = value.isoformat()
                else:
                    clean[key] = value
            except Exception as e:
                logger.debug(f"Error cleaning field {key}: {e}")
                clean[key] = str(value)
        
        return clean
    
    def _create_summary_sheet(self, entities: List[Dict[str, Any]]) -> pd.DataFrame:
        """Create summary statistics sheet"""
        if not entities:
            return pd.DataFrame()
        
        summary_data = []
        
        # Basic statistics
        summary_data.append(['Total Entities', len(entities)])
        summary_data.append(['Export Timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        
        # Risk statistics
        risk_scores = [e.get('risk_score', 0) for e in entities if e.get('risk_score') is not None]
        if risk_scores:
            summary_data.append(['Average Risk Score', round(sum(risk_scores) / len(risk_scores), 2)])
            summary_data.append(['Max Risk Score', max(risk_scores)])
            summary_data.append(['Min Risk Score', min(risk_scores)])
        
        # PEP statistics
        pep_count = sum(1 for e in entities if e.get('is_pep', False))
        summary_data.append(['PEP Entities', pep_count])
        summary_data.append(['Non-PEP Entities', len(entities) - pep_count])
        
        # Event statistics
        total_events = sum(e.get('event_count', 0) for e in entities)
        summary_data.append(['Total Events', total_events])
        if entities:
            summary_data.append(['Average Events per Entity', round(total_events / len(entities), 2)])
        
        # Entity type breakdown
        entity_types = {}
        for entity in entities:
            entity_type = entity.get('entity_type', 'Unknown')
            entity_types[entity_type] = entity_types.get(entity_type, 0) + 1
        
        for entity_type, count in entity_types.items():
            summary_data.append([f'{entity_type} Entities', count])
        
        return pd.DataFrame(summary_data, columns=['Metric', 'Value'])
    
    def _create_events_sheet(self, entities: List[Dict[str, Any]]) -> pd.DataFrame:
        """Create detailed events sheet"""
        events_data = []
        
        for entity in entities:
            entity_id = entity.get('entity_id', '')
            entity_name = entity.get('entity_name', '')
            events = entity.get('events', [])
            
            if events and isinstance(events, list):
                for event in events:
                    if isinstance(event, dict):
                        events_data.append({
                            'Entity ID': entity_id,
                            'Entity Name': entity_name,
                            'Event Category Code': event.get('event_category_code', ''),
                            'Event Sub Category Code': event.get('event_sub_category_code', ''),
                            'Event Date': event.get('event_date', ''),
                            'Event Description': event.get('event_description', event.get('event_description_short', ''))
                        })
        
        return pd.DataFrame(events_data)
    
    def _create_relationships_sheet(self, entities: List[Dict[str, Any]]) -> pd.DataFrame:
        """Create detailed relationships sheet"""
        relationships_data = []
        
        for entity in entities:
            entity_id = entity.get('entity_id', '')
            entity_name = entity.get('entity_name', '')
            relationships = entity.get('relationships', [])
            
            if relationships and isinstance(relationships, list):
                for rel in relationships:
                    if isinstance(rel, dict):
                        relationships_data.append({
                            'Entity ID': entity_id,
                            'Entity Name': entity_name,
                            'Related Entity ID': rel.get('related_entity_id', ''),
                            'Related Entity Name': rel.get('related_entity_name', ''),
                            'Relationship Type': rel.get('type', ''),
                            'Direction': rel.get('direction', '')
                        })
        
        return pd.DataFrame(relationships_data)
    
    def _list_to_string(self, items: list, separator: str = ', ') -> str:
        """Convert list to string safely"""
        if not items:
            return ''
        
        if isinstance(items, list):
            return separator.join([str(item) for item in items if item])
        else:
            return str(items)
    
    def _format_date(self, date_obj) -> str:
        """Format date object to string with comprehensive error handling"""
        if not date_obj:
            return ''
        
        try:
            # If it's already a string, return as-is (avoid format operations)
            if isinstance(date_obj, str):
                return date_obj
            
            # Handle datetime/date objects
            if hasattr(date_obj, 'isoformat'):
                return date_obj.isoformat()
            elif hasattr(date_obj, 'strftime'):
                return date_obj.strftime('%Y-%m-%d')
            else:
                # Convert to string safely
                return str(date_obj)
        except Exception as e:
            logger.warning(f"Date formatting error for {date_obj} (type: {type(date_obj)}): {e}")
            # Safe conversion without any formatting operations
            try:
                return str(date_obj)
            except:
                return ''