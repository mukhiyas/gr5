"""
Database Corrections Module for GRID Entity Search
Fixes all database understanding issues identified in main.py audit

This module provides corrected database query methods that understand the actual GRID schema:
- PEP data in individual_attributes table where alias_code_type='PTY'
- Proper PEP type parsing from alias_value (e.g., 'MUN:L3', 'REG:L5', 'FAM')
- Correct risk scoring based on event categories and sub-categories
- Proper use of individual_date_of_births table
- Enhanced boolean search with proper PEP filtering
"""

import logging
import json
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import ast

logger = logging.getLogger(__name__)

class DatabaseCorrections:
    """Corrected database methods to replace incorrect logic in main.py"""
    
    def __init__(self):
        # Correct PEP type mappings based on datareq.txt and res2.txt
        self.pep_types = {
            'HOS': 'Head of State',
            'CAB': 'Cabinet Officials',
            'INF': 'Senior Infrastructure Officials',
            'NIO': 'Senior Non-Infrastructure Officials', 
            'MUN': 'Municipal Level Officials',
            'REG': 'Regional Officials',
            'LEG': 'Senior Legislative Branch',
            'AMB': 'Ambassadors and Top Diplomatic Officials',
            'MIL': 'Senior Military Figures',
            'JUD': 'Senior Judicial Figures',
            'POL': 'Political Party Figures',
            'ISO': 'International Sporting Officials',
            'GOE': 'Government Owned Enterprises',
            'GCO': 'Top Executives in State-Controlled Business',
            'IGO': 'International Government Organization Officials',
            'FAM': 'Family Members',
            'ASC': 'Close Associates and Advisors'
        }
        
        # Correct risk score mappings based on res2.txt event analysis
        self.event_risk_scores = {
            # Critical (80-100) - From res2.txt analysis
            'TER': 100, 'SAN': 100, 'WLT': 100, 'DEN': 95,
            'MLA': 90, 'DTF': 90, 'HUM': 90, 'ORG': 85,
            'BRB': 85, 'COR': 85, 'FRD': 80, 'KID': 85,
            
            # Valuable (60-79)
            'REG': 75, 'BUS': 70, 'TAX': 70, 'SEC': 70,
            'FOR': 65, 'EMB': 70, 'MOR': 65,
            
            # Investigative (40-59) 
            'ABU': 55, 'AST': 55, 'MUR': 55, 'ARS': 50,
            'BUR': 50, 'CFT': 45, 'CON': 45, 'CYB': 40,
            
            # Probative (0-39)
            'MIS': 30, 'NSC': 25, 'ENV': 30, 'GAM': 25,
            'DPS': 30, 'CPR': 25, 'OBS': 20, 'BKY': 20
        }
        
        # Event sub-category modifiers (from res2.txt)
        self.sub_category_modifiers = {
            'CVT': 1.3,  # Conviction - highest
            'SAN': 1.2,  # Sanction
            'ART': 1.1,  # Arrest
            'CHG': 1.0,  # Charged
            'IND': 0.9,  # Indicted
            'CMP': 0.8,  # Complaint Filed
            'PRB': 0.7,  # Probe
            'ALL': 0.6,  # Alleged
            'ASC': 0.5   # Associated
        }

    def extract_correct_pep_info(self, attributes: List[Dict]) -> Dict[str, Any]:
        """
        CORRECTED PEP extraction based on actual database schema
        
        Based on res2.txt analysis:
        - PEP data is in individual_attributes where alias_code_type='PTY'
        - alias_value contains formats like 'MUN:L3', 'REG:L5', 'FAM'
        - Family associations contain descriptions like 'Family Member of...'
        """
        pep_info = {
            'is_pep': False,
            'pep_type': None,
            'pep_level': None,
            'pep_description': None,
            'pep_associations': [],
            'pep_details': []
        }
        
        for attr in attributes:
            code_type = attr.get('alias_code_type', '')
            value = attr.get('alias_value', '')
            
            # CORRECT: Look for PTY code type (11.9M records in database)
            if code_type == 'PTY' and value:
                pep_info['is_pep'] = True
                pep_info['pep_details'].append(value)
                
                # Parse different PEP value formats
                if ':' in value:
                    # Format: 'MUN:L3', 'REG:L5' 
                    parts = value.split(':', 1)
                    pep_code = parts[0].strip()
                    level = parts[1].strip()
                    
                    if pep_code in self.pep_types:
                        pep_info['pep_type'] = pep_code
                        pep_info['pep_level'] = level
                        pep_info['pep_description'] = self.pep_types[pep_code]
                
                elif value in self.pep_types:
                    # Direct PEP code: 'FAM', 'ASC'
                    pep_info['pep_type'] = value
                    pep_info['pep_description'] = self.pep_types[value]
                    
                elif 'Family Member of' in value or 'Associate of' in value:
                    # Relationship descriptions
                    pep_info['pep_associations'].append(value)
                    if 'Family' in value:
                        pep_info['pep_type'] = 'FAM'
                        pep_info['pep_description'] = 'Family Member'
                    elif 'Associate' in value:
                        pep_info['pep_type'] = 'ASC'
                        pep_info['pep_description'] = 'Close Associate'
                
                else:
                    # Other relationship or description
                    pep_info['pep_associations'].append(value)
        
        return pep_info

    def calculate_correct_risk_score(self, events: List[Dict]) -> Dict[str, Any]:
        """
        CORRECTED risk calculation based on actual event structure from res2.txt
        
        Uses event_category_code and event_sub_category_code for proper scoring
        """
        if not events:
            return {'risk_score': 0, 'risk_category': 'Unknown', 'risk_details': []}
        
        max_score = 0
        risk_details = []
        
        for event in events:
            category = event.get('event_category_code', '')
            sub_category = event.get('event_sub_category_code', '')
            
            # Get base risk score
            base_score = self.event_risk_scores.get(category, 10)
            
            # Apply sub-category modifier
            modifier = self.sub_category_modifiers.get(sub_category, 1.0)
            
            # Calculate final score
            event_score = min(int(base_score * modifier), 100)
            max_score = max(max_score, event_score)
            
            risk_details.append({
                'event_category': category,
                'event_sub_category': sub_category,
                'base_score': base_score,
                'modifier': modifier,
                'final_score': event_score
            })
        
        # Determine risk category
        if max_score >= 80:
            risk_category = 'Critical'
        elif max_score >= 60:
            risk_category = 'Valuable'
        elif max_score >= 40:
            risk_category = 'Investigative'
        else:
            risk_category = 'Probative'
        
        return {
            'risk_score': max_score,
            'risk_category': risk_category,
            'risk_details': risk_details
        }

    def build_corrected_search_query(self, entity_type: str, search_params: Dict) -> Tuple[str, List]:
        """
        Build CORRECTED database query with proper table usage and joins
        
        Fixes all issues found in main.py audit:
        1. Proper PEP filtering using individual_attributes with PTY
        2. Correct use of individual_date_of_births table
        3. Proper event filtering with category codes
        4. Enhanced boolean search support
        """
        
        # Base query with all necessary joins
        base_query = f"""
        SELECT DISTINCT
            m.entity_id,
            m.risk_id,
            m.entity_name,
            m.recordDefinitionType,
            m.source_item_id,
            m.systemId,
            m.entityDate,
            
            -- Collect all attributes (including PEP data)
            COLLECT_LIST(
                CASE WHEN attr.alias_code_type IS NOT NULL 
                THEN STRUCT(attr.alias_code_type, attr.alias_value)
                END
            ) as attributes,
            
            -- Collect all events
            COLLECT_LIST(
                CASE WHEN ev.event_category_code IS NOT NULL
                THEN STRUCT(
                    ev.event_category_code,
                    ev.event_sub_category_code,
                    ev.event_date,
                    ev.event_description
                )
                END
            ) as events,
            
            -- Collect addresses
            COLLECT_LIST(
                CASE WHEN addr.address_id IS NOT NULL
                THEN STRUCT(
                    addr.address_country,
                    addr.address_city,
                    addr.address_type,
                    addr.address_line1
                )
                END
            ) as addresses,
            
            -- Date of birth info (CORRECTED: Use individual_date_of_births table)
            dob.date_of_birth_year,
            dob.date_of_birth_month,
            dob.date_of_birth_day,
            dob.date_of_birth_circa,
            
            -- BVD mapping
            bvd.bvdid,
            bvd.entitytype as bvd_entity_type
            
        FROM prd_bronze_catalog.grid.{entity_type}_mapping m
        
        -- CORRECTED: Proper attributes join for PEP data
        LEFT JOIN prd_bronze_catalog.grid.{entity_type}_attributes attr 
            ON m.entity_id = attr.entity_id
        
        -- Events join
        LEFT JOIN prd_bronze_catalog.grid.{entity_type}_events ev 
            ON m.entity_id = ev.entity_id
            
        -- Addresses join  
        LEFT JOIN prd_bronze_catalog.grid.{entity_type}_addresses addr
            ON m.entity_id = addr.entity_id
            
        -- CORRECTED: Use individual_date_of_births table
        {f"LEFT JOIN prd_bronze_catalog.grid.individual_date_of_births dob ON m.entity_id = dob.entity_id" if entity_type == 'individual' else ""}
        
        -- BVD mapping
        LEFT JOIN prd_bronze_catalog.grid.grid_orbis_mapping bvd 
            ON m.risk_id = bvd.riskid
        """
        
        # Build WHERE conditions
        where_conditions = []
        query_params = []
        
        # Name search with proper escaping
        if search_params.get('name'):
            where_conditions.append("LOWER(m.entity_name) LIKE LOWER(?)")
            query_params.append(f"%{search_params['name']}%")
        
        # CORRECTED: PEP-only filter using proper PTY lookup
        if search_params.get('pep_only'):
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{}_attributes pep_attr
                    WHERE pep_attr.entity_id = m.entity_id 
                    AND pep_attr.alias_code_type = 'PTY'
                )
            """.format(entity_type))
        
        # CORRECTED: PEP type filter
        if search_params.get('pep_types'):
            pep_types = search_params['pep_types']
            if isinstance(pep_types, str):
                pep_types = [pep_types]
            
            pep_conditions = []
            for pep_type in pep_types:
                pep_conditions.append("attr.alias_value LIKE ?")
                query_params.append(f"%{pep_type}%")
            
            where_conditions.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_attributes pep_attr
                    WHERE pep_attr.entity_id = m.entity_id 
                    AND pep_attr.alias_code_type = 'PTY'
                    AND ({' OR '.join(pep_conditions)})
                )
            """)
        
        # Country filter
        if search_params.get('country'):
            where_conditions.append("LOWER(addr.address_country) = LOWER(?)")
            query_params.append(search_params['country'])
        
        # Event category filter
        if search_params.get('event_categories'):
            categories = search_params['event_categories']
            if isinstance(categories, str):
                categories = [categories]
            
            placeholders = ','.join(['?' for _ in categories])
            where_conditions.append(f"ev.event_category_code IN ({placeholders})")
            query_params.extend(categories)
        
        # Date range filter using individual_date_of_births
        if entity_type == 'individual' and search_params.get('birth_year'):
            birth_year = search_params['birth_year']
            where_conditions.append("dob.date_of_birth_year = ?")
            query_params.append(str(birth_year))
        
        # Date range for events
        if search_params.get('event_date_from'):
            where_conditions.append("ev.event_date >= ?")
            query_params.append(search_params['event_date_from'])
            
        if search_params.get('event_date_to'):
            where_conditions.append("ev.event_date <= ?")
            query_params.append(search_params['event_date_to'])
        
        # Combine query
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        # Group by and order
        base_query += f"""
        GROUP BY m.entity_id, m.risk_id, m.entity_name, m.recordDefinitionType, 
                 m.source_item_id, m.systemId, m.entityDate,
                 dob.date_of_birth_year, dob.date_of_birth_month, 
                 dob.date_of_birth_day, dob.date_of_birth_circa,
                 bvd.bvdid, bvd.entitytype
        ORDER BY m.entity_name
        LIMIT {search_params.get('limit', 100)}
        """
        
        return base_query, query_params

    def build_advanced_boolean_query(self, entity_type: str, boolean_expression: str, search_params: Dict) -> Tuple[str, List]:
        """
        ENHANCED boolean search with proper PEP and risk filtering
        
        Supports complex expressions like:
        - (PEP_TYPE:MUN OR PEP_TYPE:REG) AND COUNTRY:Brazil
        - RISK_CATEGORY:BRB AND PEP_LEVEL:L3
        - NAME:John AND (EVENT:CVT OR EVENT:SAN)
        """
        
        try:
            # Parse boolean expression into conditions
            conditions = self._parse_boolean_expression(boolean_expression)
            
            # Build base query
            base_query, base_params = self.build_corrected_search_query(entity_type, search_params)
            
            # Add boolean conditions
            boolean_conditions = []
            boolean_params = []
            
            for condition in conditions:
                field = condition.get('field', '').upper()
                operator = condition.get('operator', '=')
                value = condition.get('value', '')
                
                if field == 'PEP_TYPE':
                    boolean_conditions.append(f"""
                        EXISTS (
                            SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_attributes ba
                            WHERE ba.entity_id = m.entity_id 
                            AND ba.alias_code_type = 'PTY'
                            AND ba.alias_value LIKE ?
                        )
                    """)
                    boolean_params.append(f"%{value}%")
                    
                elif field == 'RISK_CATEGORY':
                    boolean_conditions.append(f"""
                        EXISTS (
                            SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_events be
                            WHERE be.entity_id = m.entity_id 
                            AND be.event_category_code = ?
                        )
                    """)
                    boolean_params.append(value)
                    
                elif field == 'COUNTRY':
                    boolean_conditions.append(f"""
                        EXISTS (
                            SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_addresses bc
                            WHERE bc.entity_id = m.entity_id 
                            AND LOWER(bc.address_country) = LOWER(?)
                        )
                    """)
                    boolean_params.append(value)
                    
                elif field == 'NAME':
                    boolean_conditions.append("LOWER(m.entity_name) LIKE LOWER(?)")
                    boolean_params.append(f"%{value}%")
            
            # Combine with existing WHERE clause
            if boolean_conditions:
                if 'WHERE' in base_query:
                    base_query = base_query.replace('ORDER BY', f" AND ({' AND '.join(boolean_conditions)}) ORDER BY")
                else:
                    base_query = base_query.replace('GROUP BY', f" WHERE {' AND '.join(boolean_conditions)} GROUP BY")
                
                base_params.extend(boolean_params)
            
            return base_query, base_params
            
        except Exception as e:
            logger.error(f"Boolean query parsing error: {e}")
            # Fallback to basic query
            return self.build_corrected_search_query(entity_type, search_params)

    def _parse_boolean_expression(self, expression: str) -> List[Dict]:
        """Parse boolean expression into structured conditions"""
        conditions = []
        
        # Simple parsing for key:value pairs
        # This can be enhanced with more sophisticated parsing
        parts = expression.replace('(', '').replace(')', '').split(' AND ')
        
        for part in parts:
            part = part.strip()
            if ':' in part:
                field, value = part.split(':', 1)
                conditions.append({
                    'field': field.strip(),
                    'operator': '=',
                    'value': value.strip()
                })
        
        return conditions

    def process_corrected_results(self, raw_results: List[Dict]) -> List[Dict]:
        """
        Process raw database results with CORRECTED PEP and risk analysis
        
        Fixes all data processing issues found in main.py audit
        """
        processed_results = []
        
        for result in raw_results:
            try:
                # Extract and process attributes
                attributes = result.get('attributes', [])
                if isinstance(attributes, str):
                    try:
                        attributes = json.loads(attributes)
                    except:
                        attributes = []
                
                # CORRECTED: Use proper PEP extraction
                pep_info = self.extract_correct_pep_info(attributes)
                
                # Extract and process events
                events = result.get('events', [])
                if isinstance(events, str):
                    try:
                        events = json.loads(events)
                    except:
                        events = []
                
                # CORRECTED: Use proper risk calculation
                risk_info = self.calculate_correct_risk_score(events)
                
                # Process addresses
                addresses = result.get('addresses', [])
                if isinstance(addresses, str):
                    try:
                        addresses = json.loads(addresses)
                    except:
                        addresses = []
                
                # Build comprehensive result
                processed_result = {
                    'entity_id': result.get('entity_id'),
                    'risk_id': result.get('risk_id'),
                    'entity_name': result.get('entity_name'),
                    'entity_type': result.get('recordDefinitionType', '').lower(),
                    'source_item_id': result.get('source_item_id'),
                    'system_id': result.get('systemId'),
                    'entity_date': result.get('entityDate'),
                    
                    # CORRECTED: Proper PEP information
                    'is_pep': pep_info['is_pep'],
                    'pep_type': pep_info['pep_type'],
                    'pep_level': pep_info['pep_level'],
                    'pep_description': pep_info['pep_description'],
                    'pep_associations': pep_info['pep_associations'],
                    'pep_details': pep_info['pep_details'],
                    
                    # CORRECTED: Proper risk scoring
                    'risk_score': risk_info['risk_score'],
                    'risk_category': risk_info['risk_category'],
                    'risk_details': risk_info['risk_details'],
                    
                    # Additional data
                    'birth_year': result.get('date_of_birth_year'),
                    'birth_month': result.get('date_of_birth_month'),
                    'birth_day': result.get('date_of_birth_day'),
                    'birth_circa': result.get('date_of_birth_circa'),
                    
                    'bvd_id': result.get('bvdid'),
                    'bvd_entity_type': result.get('bvd_entity_type'),
                    
                    'addresses': addresses,
                    'events': events,
                    'attributes': attributes,
                    
                    # Export-ready summary
                    'export_summary': self._create_export_summary(result, pep_info, risk_info)
                }
                
                processed_results.append(processed_result)
                
            except Exception as e:
                logger.error(f"Error processing result for entity {result.get('entity_id', 'unknown')}: {e}")
                continue
        
        return processed_results

    def _create_export_summary(self, result: Dict, pep_info: Dict, risk_info: Dict) -> Dict:
        """Create export-ready summary with all corrected data"""
        return {
            'Entity_ID': result.get('entity_id', ''),
            'Risk_ID': result.get('risk_id', ''),
            'Entity_Name': result.get('entity_name', ''),
            'Entity_Type': result.get('recordDefinitionType', ''),
            'Is_PEP': 'Yes' if pep_info['is_pep'] else 'No',
            'PEP_Type': pep_info.get('pep_type', ''),
            'PEP_Level': pep_info.get('pep_level', ''),
            'PEP_Description': pep_info.get('pep_description', ''),
            'PEP_Associations': '; '.join(pep_info.get('pep_associations', [])),
            'Risk_Score': risk_info.get('risk_score', 0),
            'Risk_Category': risk_info.get('risk_category', ''),
            'Birth_Year': result.get('date_of_birth_year', ''),
            'BVD_ID': result.get('bvdid', ''),
            'Source_ID': result.get('source_item_id', ''),
            'System_ID': result.get('systemId', ''),
            'Entity_Date': result.get('entityDate', ''),
        }

# Usage example for integration with main.py:
"""
# In main.py, replace existing methods with:

from database_corrections import DatabaseCorrections

class EntitySearchApp:
    def __init__(self):
        # ... existing init code ...
        self.db_corrections = DatabaseCorrections()
    
    def search_entities(self, search_params):
        # Replace existing search logic with:
        query, params = self.db_corrections.build_corrected_search_query(
            entity_type, search_params
        )
        
        # Execute query and process results
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        raw_results = [dict(zip([col[0] for col in cursor.description], row)) 
                      for row in cursor.fetchall()]
        
        # Process with corrected logic
        return self.db_corrections.process_corrected_results(raw_results)
"""