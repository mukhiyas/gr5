"""
Advanced Database Query Module for GRID Entity Search
Comprehensive database integration with proper PEP classification and risk scoring
"""
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import json

logger = logging.getLogger(__name__)

# Import optimized queries
try:
    from .optimized_database_queries import optimized_db_queries
except ImportError:
    try:
        from optimized_database_queries import optimized_db_queries
    except ImportError:
        optimized_db_queries = None
        logger.warning("Optimized database queries not available, using standard queries")

class DatabaseQueries:
    """Enhanced database query methods with proper GRID schema integration"""
    
    def __init__(self):
        # Risk score mapping based on event categories (from database queries)
        self.event_risk_scores = {
            # Critical Severity (80-100)
            'TER': 100, 'WLT': 100, 'DEN': 95, 'DTF': 90,
            'TRF': 90, 'MLA': 85, 'HUM': 85, 'ORG': 85,
            'KID': 85, 'SPY': 85,
            
            # Valuable Severity (60-79)
            'BRB': 75, 'FRD': 70, 'TAX': 70, 'SEC': 70,
            'REG': 65, 'ROB': 60, 'SEX': 60, 'PEP': 60,
            'SNX': 60,
            
            # Investigative Severity (40-59)
            'MUR': 55, 'AST': 55, 'FUG': 50, 'BUR': 50,
            'TFT': 50, 'IGN': 50, 'CON': 45, 'CFT': 45,
            'SMG': 45, 'PSP': 40, 'IMP': 40, 'CYB': 40,
            'OBS': 40,
            
            # Probative Severity (0-39)
            'DPS': 35, 'NSC': 30, 'MIS': 30, 'ABU': 30,
            'PRJ': 30, 'ENV': 25, 'GAM': 25, 'ARS': 25,
            'BUS': 25, 'IPR': 20, 'LNS': 20, 'CPR': 20,
            'BKY': 20, 'RES': 20, 'MOR': 20, 'IRC': 20,
            'FAR': 15, 'LMD': 15, 'DPP': 15, 'FOF': 10,
            'FOS': 10, 'FOR': 10, 'MSB': 10, 'HTE': 10,
            'BIL': 5, 'CND': 5, 'DEF': 5, 'HCD': 5,
            'PER': 5, 'REO': 5, 'VCY': 5
        }
        
        # Event sub-category severity modifiers (from database queries)
        self.sub_category_modifiers = {
            'CVT': 1.3,  # Convicted
            'CNF': 1.2,  # Confession
            'SAN': 1.2,  # Sanctioned
            'SJT': 1.2,  # Jail Time
            'GOV': 1.2,  # Government
            'ART': 1.1,  # Arrested
            'IND': 1.1,  # Indicted
            'WTD': 1.1,  # Wanted
            'CHG': 1.0,  # Charged
            'ARN': 1.0,  # Arraigned
            'ACT': 1.0,  # Action
            'PLE': 1.0,  # Plea
            'CSP': 1.0,  # Conspiracy
            'TRL': 1.0,  # Trial
            'DEP': 1.0,  # Deported
            'SEZ': 1.0,  # Seizure
            'RVK': 1.0,  # Revoked
            'FIM': 1.0,  # Fine >$10K
            'EXP': 0.9,  # Expelled
            'CEN': 0.9,  # Censured
            'SPD': 0.9,  # Suspended
            'CMP': 0.8,  # Complaint
            'APL': 0.8,  # Appeal
            'SET': 0.8,  # Settlement
            'LIC': 0.8,  # License Action
            'ACC': 0.7,  # Accused
            'FIL': 0.7,  # Fine <$10K
            'PRB': 0.7,  # Probe
            'ADT': 0.7,  # Audit
            'ALL': 0.6,  # Alleged
            'LIN': 0.6,  # Lien
            'SPT': 0.6,  # Suspected
            'ACQ': 0.5,  # Acquitted
            'ASC': 0.5,  # Associated
            'DMS': 0.4   # Dismissed
        }
        
        # PEP type descriptions
        self.pep_types = {
            'HOS': 'Head of State',
            'CAB': 'Cabinet Official',
            'INF': 'Senior Infrastructure Official',
            'NIO': 'Senior Non-Infrastructure Official',
            'MUN': 'Municipal Official',
            'REG': 'Regional Official',
            'LEG': 'Senior Legislative',
            'AMB': 'Ambassador/Diplomatic',
            'MIL': 'Senior Military',
            'JUD': 'Senior Judicial',
            'POL': 'Political Party Figure',
            'GOE': 'Government Owned Enterprise',
            'GCO': 'State-Controlled Business Executive',
            'IGO': 'International Gov Organization',
            'ISO': 'International Sporting Official',
            'FAM': 'Family Member',
            'ASC': 'Close Associate'
        }
    
    def calculate_risk_score(self, events: List[Dict]) -> int:
        """Calculate risk score based on event categories and sub-categories"""
        if not events:
            return 0
        
        max_score = 0
        for event in events:
            category = event.get('event_category_code', '')
            sub_category = event.get('event_sub_category_code', '')
            
            # Get base score for category
            base_score = self.event_risk_scores.get(category, 10)
            
            # Apply sub-category modifier
            modifier = self.sub_category_modifiers.get(sub_category, 1.0)
            
            score = int(base_score * modifier)
            max_score = max(max_score, score)
        
        return min(max_score, 100)  # Cap at 100
    
    def extract_pep_info(self, attributes: List[Dict]) -> Dict[str, Any]:
        """Extract PEP classification from attributes based on verified database structure"""
        pep_info = {
            'is_pep': False,
            'pep_type': None,
            'pep_level': None,
            'pep_description': None,
            'pep_associations': [],
            'prt_ratings': [],
            'highest_level': None
        }
        
        # Track highest PEP level found (L6 > L5 > L4 > L3 > L2 > L1)
        level_hierarchy = {'L6': 6, 'L5': 5, 'L4': 4, 'L3': 3, 'L2': 2, 'L1': 1}
        highest_level_num = 0
        
        for attr in attributes:
            code_type = attr.get('alias_code_type', '')
            value = attr.get('alias_value', '')
            
            if code_type == 'PTY' and value:
                pep_info['is_pep'] = True
                
                # Parse PEP patterns based on verified database analysis
                if ':L' in value:  # Format: "HOS:L5", "MUN:L3", etc.
                    parts = value.split(':', 1)
                    if len(parts) == 2:
                        pep_code, level = parts
                        pep_info['pep_type'] = pep_code
                        pep_info['pep_level'] = level
                        
                        # Track highest level
                        level_num = level_hierarchy.get(level, 0)
                        if level_num > highest_level_num:
                            highest_level_num = level_num
                            pep_info['highest_level'] = level
                            
                elif value.upper() in self.pep_types:
                    # Simple 3-letter codes: "FAM", "ASC", etc.
                    pep_info['pep_type'] = value.upper()
                    
                elif 'Senior Official of' in value:
                    # "Senior Official of COMPANY NAME"
                    pep_info['pep_type'] = 'SENIOR_OFFICIAL'
                    pep_info['pep_associations'].append(value)
                    
                elif 'Family Member of' in value:
                    # "Family Member of PERSON NAME"
                    pep_info['pep_type'] = 'FAMILY_MEMBER'
                    pep_info['pep_associations'].append(value)
                    
                else:
                    # Other descriptive relationships
                    pep_info['pep_associations'].append(value)
                
                # Set description if we have a recognized type
                if pep_info['pep_type']:
                    pep_info['pep_description'] = self.pep_types.get(
                        pep_info['pep_type'], 
                        pep_info['pep_type']
                    )
            
            elif code_type == 'PRT':  # PEP Rating - verified format: "C:01/02/2023"
                pep_info['prt_ratings'].append(value)
                
            elif code_type == 'PLV':  # PEP Level (separate attribute)
                pep_info['pep_level'] = value
        
        return pep_info
    
    def build_optimized_search_query(self, entity_type: str, search_params: Dict) -> Tuple[str, Dict]:
        """Build optimized search query for 22M records with proper indexing and filtering"""
        
        # Base table name
        base_table = f"prd_bronze_catalog.grid.{entity_type}_mapping"
        params = {}
        
        # Start with most selective filters first for performance
        primary_filters = []
        secondary_filters = []
        
        # Name search (most common and selective)
        if search_params.get('name'):
            # Use index-friendly approach
            name_term = search_params['name']
            primary_filters.append("""
                (UPPER(m.entity_name) LIKE UPPER(%(name)s)
                 OR EXISTS (
                     SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_aliases a 
                     WHERE a.entity_id = m.entity_id 
                     AND UPPER(a.alias_name) LIKE UPPER(%(name)s)
                     LIMIT 1
                 ))
            """.format(entity_type=entity_type))
            params['name'] = f"%{name_term}%"
        
        # Entity ID search (most selective when provided)
        if search_params.get('entity_id'):
            primary_filters.append("m.entity_id = %(entity_id)s")
            params['entity_id'] = search_params['entity_id']
        
        # Risk ID search (very selective)
        if search_params.get('risk_id'):
            primary_filters.append("m.risk_id = %(risk_id)s")
            params['risk_id'] = search_params['risk_id']
        
        # PEP filter (moderately selective)
        if search_params.get('pep_only'):
            primary_filters.append("""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_attributes attr
                    WHERE attr.entity_id = m.entity_id 
                    AND attr.alias_code_type = 'PTY'
                    LIMIT 1
                )
            """.format(entity_type=entity_type))
        
        # High-risk events (moderately selective)
        if search_params.get('high_risk_only'):
            primary_filters.append("""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_category_code IN ('TER', 'WLT', 'DEN', 'DTF', 'BRB', 'MLA')
                    LIMIT 1
                )
            """.format(entity_type=entity_type))
        
        # Event category filter (selective when specific categories)
        if search_params.get('event_categories'):
            primary_filters.append("""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_category_code IN %(event_categories)s
                    LIMIT 1
                )
            """.format(entity_type=entity_type))
            params['event_categories'] = tuple(search_params['event_categories'])
        
        # Country filter (less selective, use as secondary)
        if search_params.get('country'):
            secondary_filters.append("""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_addresses addr
                    WHERE addr.entity_id = m.entity_id 
                    AND UPPER(addr.address_country) = UPPER(%(country)s)
                    LIMIT 1
                )
            """.format(entity_type=entity_type))
            params['country'] = search_params['country']
        
        # Date range filter (less selective, apply as secondary)
        if search_params.get('date_from') or search_params.get('date_to'):
            date_conditions = []
            if search_params.get('date_from'):
                date_conditions.append("ev.event_date >= %(date_from)s")
                params['date_from'] = search_params['date_from']
            if search_params.get('date_to'):
                date_conditions.append("ev.event_date <= %(date_to)s")
                params['date_to'] = search_params['date_to']
            
            secondary_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{entity_type}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND {' AND '.join(date_conditions)}
                    LIMIT 1
                )
            """)
        
        # Combine filters with proper ordering for index usage
        all_filters = primary_filters + secondary_filters
        where_clause = "WHERE " + " AND ".join(all_filters) if all_filters else ""
        
        # Set reasonable limits to prevent heap errors
        limit = min(search_params.get('limit', 1000), 5000)  # Cap at 5K results
        
        # Build the optimized query using LEFT JOINs for better performance
        query = f"""
        SELECT
            m.entity_id,
            m.risk_id,
            m.entity_name,
            m.recordDefinitionType,
            m.source_item_id,
            m.systemId,
            m.entityDate,
            
            -- Priority data only (avoid expensive JSON aggregations)
            COALESCE(pep_data.pep_types, '[]') as pep_types,
            COALESCE(event_data.events, '[]') as events,
            COALESCE(addr_data.addresses, '[]') as addresses,
            COALESCE(alias_data.aliases, '[]') as aliases
            
        FROM {base_table} m
        
        -- Pre-aggregated PEP data subquery
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    alias_code_type,
                    alias_value
                ))) as pep_types
            FROM prd_bronze_catalog.grid.{entity_type}_attributes
            WHERE alias_code_type IN ('PTY', 'PRT', 'PLV', 'RSC')
            GROUP BY entity_id
        ) pep_data ON m.entity_id = pep_data.entity_id
        
        -- Pre-aggregated event data subquery (limit to recent and high-risk)
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    event_category_code,
                    event_sub_category_code,
                    event_date,
                    event_description
                ))) as events
            FROM (
                SELECT *
                FROM prd_bronze_catalog.grid.{entity_type}_events
                WHERE event_category_code IN ('TER', 'WLT', 'DEN', 'DTF', 'BRB', 'MLA', 'HUM', 'ORG', 'PEP')
                   OR event_date >= DATE_SUB(CURRENT_DATE(), 365*2)  -- Last 2 years
                ORDER BY event_date DESC
                LIMIT 10  -- Max 10 events per entity
            ) recent_events
            GROUP BY entity_id
        ) event_data ON m.entity_id = event_data.entity_id
        
        -- Primary address only
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    address_country,
                    address_city,
                    address_type
                ))) as addresses
            FROM (
                SELECT *
                FROM prd_bronze_catalog.grid.{entity_type}_addresses
                WHERE address_type IS NOT NULL OR address_country IS NOT NULL
                ORDER BY CASE 
                    WHEN address_type = 'PEP' THEN 1
                    WHEN address_type = 'primary' THEN 2
                    ELSE 3
                END
                LIMIT 3  -- Max 3 addresses per entity
            ) addr
            GROUP BY entity_id
        ) addr_data ON m.entity_id = addr_data.entity_id
        
        -- Key aliases only
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    alias_name,
                    alias_code_type
                ))) as aliases
            FROM (
                SELECT *
                FROM prd_bronze_catalog.grid.{entity_type}_aliases
                WHERE alias_code_type IN ('AKA', 'FKA', 'LOC')
                LIMIT 5  -- Max 5 aliases per entity
            ) aliases
            GROUP BY entity_id
        ) alias_data ON m.entity_id = alias_data.entity_id
        
        {where_clause}
        
        ORDER BY 
            -- Prioritize high-risk and PEP entities
            CASE WHEN pep_data.entity_id IS NOT NULL THEN 1 ELSE 2 END,
            CASE WHEN event_data.entity_id IS NOT NULL THEN 1 ELSE 2 END,
            m.entityDate DESC
            
        LIMIT {limit}
        """
        
        return query, params
    
    def build_entity_detail_query(self, entity_type: str, entity_id: str) -> Tuple[str, Dict]:
        """Build detailed query for single entity with all data (for detail view)"""
        
        params = {'entity_id': entity_id}
        
        query = f"""
        SELECT
            m.entity_id,
            m.risk_id,
            m.entity_name,
            m.recordDefinitionType,
            m.source_item_id,
            m.systemId,
            m.entityDate,
            
            -- All attributes
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                alias_code_type,
                alias_value
            )))
            FROM prd_bronze_catalog.grid.{entity_type}_attributes
            WHERE entity_id = m.entity_id) as attributes,
            
            -- All events
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                event_category_code,
                event_sub_category_code,
                event_date,
                event_end_date,
                event_description,
                event_reference_source_item_id
            )))
            FROM prd_bronze_catalog.grid.{entity_type}_events
            WHERE entity_id = m.entity_id
            ORDER BY event_date DESC) as events,
            
            -- All addresses
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                address_raw_format,
                address_line1,
                address_line2,
                address_city,
                address_province,
                address_postal_code,
                address_country,
                address_type
            )))
            FROM prd_bronze_catalog.grid.{entity_type}_addresses
            WHERE entity_id = m.entity_id) as addresses,
            
            -- All aliases
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                alias_name,
                alias_code_type
            )))
            FROM prd_bronze_catalog.grid.{entity_type}_aliases
            WHERE entity_id = m.entity_id) as aliases,
            
            -- All identifications
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                identification_type,
                identification_value,
                identification_country,
                identification_issue_date,
                identification_expire_date
            )))
            FROM prd_bronze_catalog.grid.{entity_type}_identifications
            WHERE entity_id = m.entity_id) as identifications,
            
            -- Date of Birth (for individuals)
            {f'''(SELECT TO_JSON(STRUCT(
                date_of_birth_year,
                date_of_birth_month,
                date_of_birth_day,
                date_of_birth_circa
            ))
            FROM prd_bronze_catalog.grid.individual_date_of_births
            WHERE entity_id = m.entity_id
            LIMIT 1) as date_of_birth''' if entity_type == 'individual' else 'NULL as date_of_birth'},
            
            -- All relationships
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                related_entity_id,
                related_entity_name,
                direction,
                type
            )))
            FROM prd_bronze_catalog.grid.relationships
            WHERE entity_id = m.entity_id) as relationships,
            
            -- ORBIS data if available
            (SELECT TO_JSON(COLLECT_LIST(STRUCT(
                bvdid,
                entitytype,
                eventcode,
                entityname,
                asofdate
            )))
            FROM prd_bronze_catalog.grid.grid_orbis_mapping
            WHERE entityid = m.entity_id) as orbis_data,
            
            -- Source information
            (SELECT TO_JSON(COLLECT_LIST(DISTINCT STRUCT(
                name,
                description,
                type,
                publication,
                publish_date,
                url
            )))
            FROM prd_bronze_catalog.grid.sources s
            WHERE s.entity_id = m.source_item_id
               OR s.entity_id IN (
                   SELECT event_reference_source_item_id 
                   FROM prd_bronze_catalog.grid.{entity_type}_events 
                   WHERE entity_id = m.entity_id
               )) as sources
            
        FROM prd_bronze_catalog.grid.{entity_type}_mapping m
        WHERE m.entity_id = %(entity_id)s
        """
        
        return query, params
    
    def build_comprehensive_name_query(self, name_parts: List[str], entity_id: str = None, risk_id: str = None) -> Tuple[str, Dict]:
        """Optimized query for comprehensive name search including ORBIS mapping"""
        
        # Build name pattern conditions
        name_conditions = []
        params = {}
        
        for i, part in enumerate(name_parts):
            name_conditions.append(f"UPPER(m.entity_name) LIKE UPPER(%(name_part_{i})s)")
            params[f'name_part_{i}'] = f"%{part}%"
        
        # Add reverse order patterns
        if len(name_parts) >= 2:
            reverse_pattern = f"%{name_parts[1]}%{name_parts[0]}%"
            name_conditions.append(f"UPPER(m.entity_name) LIKE UPPER(%(reverse_pattern)s)")
            params['reverse_pattern'] = reverse_pattern
        
        # Build additional filters
        additional_filters = []
        if entity_id:
            additional_filters.append("m.entity_id = %(entity_id)s")
            params['entity_id'] = entity_id
        if risk_id:
            additional_filters.append("m.risk_id = %(risk_id)s")
            params['risk_id'] = risk_id
            
        name_filter = " OR ".join(name_conditions)
        if additional_filters:
            name_filter += " OR " + " OR ".join(additional_filters)
        
        # Build the ORBIS filter using the same parameter names
        orbis_filter = name_filter.replace('m.', 'o.').replace('entity_name', 'entityname').replace('entity_id', 'entityid').replace('risk_id', 'riskid')
        
        query = f"""
        WITH target_entities AS (
            -- Find entities using name patterns
            SELECT DISTINCT m.entity_id, m.entity_name, m.risk_id
            FROM prd_bronze_catalog.grid.individual_mapping m
            WHERE ({name_filter})
            
            UNION
            
            -- Also check ORBIS mapping
            SELECT DISTINCT o.entityid as entity_id, o.entityname as entity_name, o.riskid as risk_id
            FROM prd_bronze_catalog.grid.grid_orbis_mapping o
            WHERE ({orbis_filter})
        )
        SELECT 
            d.entity_id,
            d.entity_name,
            d.risk_id,
            
            -- All PEP classifications
            COALESCE((
                SELECT TO_JSON(COLLECT_LIST(STRUCT(
                    alias_code_type,
                    alias_value
                )))
                FROM prd_bronze_catalog.grid.individual_attributes attr
                WHERE attr.entity_id = d.entity_id 
                AND attr.alias_code_type IN ('PTY', 'PRT', 'PLV')
            ), '[]') as pep_data,
            
            -- All criminal/risk events
            COALESCE((
                SELECT TO_JSON(COLLECT_LIST(STRUCT(
                    event_category_code,
                    event_sub_category_code,
                    event_date,
                    event_description
                )))
                FROM prd_bronze_catalog.grid.individual_events ev
                WHERE ev.entity_id = d.entity_id
                ORDER BY event_date DESC
            ), '[]') as events,
            
            -- Address information
            COALESCE((
                SELECT TO_JSON(COLLECT_LIST(STRUCT(
                    address_country,
                    address_city,
                    address_type,
                    address_raw_format
                )))
                FROM prd_bronze_catalog.grid.individual_addresses addr
                WHERE addr.entity_id = d.entity_id
            ), '[]') as addresses,
            
            -- Birth information
            (
                SELECT TO_JSON(STRUCT(
                    date_of_birth_year,
                    date_of_birth_month,
                    date_of_birth_day
                ))
                FROM prd_bronze_catalog.grid.individual_date_of_births dob
                WHERE dob.entity_id = d.entity_id
                LIMIT 1
            ) as date_of_birth,
            
            -- ORBIS mapping data
            COALESCE((
                SELECT TO_JSON(COLLECT_LIST(STRUCT(
                    bvdid,
                    entitytype,
                    eventcode,
                    asofdate
                )))
                FROM prd_bronze_catalog.grid.grid_orbis_mapping om
                WHERE om.entityid = d.entity_id
            ), '[]') as orbis_data,
            
            -- Risk scoring (count high-risk events)
            (
                SELECT COUNT(*)
                FROM prd_bronze_catalog.grid.individual_events ev
                WHERE ev.entity_id = d.entity_id
                AND ev.event_category_code IN ('TER', 'WLT', 'BRB', 'MLA', 'FRD')
            ) as high_risk_event_count,
            
            -- PEP count
            (
                SELECT COUNT(*)
                FROM prd_bronze_catalog.grid.individual_attributes attr
                WHERE attr.entity_id = d.entity_id
                AND attr.alias_code_type = 'PTY'
            ) as pep_count
            
        FROM target_entities d
        ORDER BY 
            pep_count DESC,
            high_risk_event_count DESC,
            d.entity_name
        """
        
        return query, params
    
    def process_search_results(self, raw_results: List[Dict]) -> List[Dict]:
        """Process raw database results into structured format"""
        processed_results = []
        
        for row in raw_results:
            # Parse JSON fields
            for field in ['events', 'attributes', 'addresses', 'aliases', 
                         'identifications', 'relationships', 'sources']:
                if row.get(field) and isinstance(row[field], str):
                    try:
                        row[field] = json.loads(row[field])
                    except:
                        row[field] = []
                elif not row.get(field):
                    row[field] = []
            
            # Parse date_of_birth if exists
            if row.get('date_of_birth') and isinstance(row['date_of_birth'], str):
                try:
                    row['date_of_birth'] = json.loads(row['date_of_birth'])
                except:
                    row['date_of_birth'] = None
            
            # Calculate risk score
            risk_score = self.calculate_risk_score(row.get('events', []))
            
            # Extract PEP information
            pep_info = self.extract_pep_info(row.get('attributes', []))
            
            # Get primary address
            addresses = row.get('addresses', [])
            primary_address = addresses[0] if addresses else {}
            
            # Build processed result
            processed = {
                'entity_id': row['entity_id'],
                'risk_id': row['risk_id'],
                'entity_name': row['entity_name'],
                'entity_type': row['recordDefinitionType'],
                'risk_score': risk_score,
                'risk_level': self.get_risk_level(risk_score),
                'is_pep': pep_info['is_pep'],
                'pep_type': pep_info['pep_type'],
                'pep_description': pep_info['pep_description'],
                'pep_level': pep_info['pep_level'],
                'pep_associations': pep_info['pep_associations'],
                'date_of_birth': row.get('date_of_birth'),
                'primary_country': primary_address.get('address_country'),
                'primary_city': primary_address.get('address_city'),
                'events': row.get('events', []),
                'aliases': row.get('aliases', []),
                'addresses': addresses,
                'identifications': row.get('identifications', []),
                'relationships': row.get('relationships', []),
                'sources': row.get('sources', []),
                'created_date': row.get('entityDate')
            }
            
            processed_results.append(processed)
        
        return processed_results
    
    def get_risk_level(self, risk_score: int) -> str:
        """Convert risk score to risk level"""
        if risk_score >= 80:
            return 'Critical'
        elif risk_score >= 60:
            return 'Valuable'
        elif risk_score >= 40:
            return 'Investigative'
        else:
            return 'Probative'
    
    def build_analytics_query(self, entity_type: str, filters: Dict) -> Tuple[str, Dict]:
        """Build query for analytics and aggregations"""
        
        base_query = f"""
        WITH entity_events AS (
            SELECT 
                m.entity_id,
                m.entity_name,
                ev.event_category_code,
                ev.event_sub_category_code,
                ev.event_date,
                attr.alias_code_type,
                attr.alias_value,
                addr.address_country
            FROM prd_bronze_catalog.grid.{entity_type}_mapping m
            LEFT JOIN prd_bronze_catalog.grid.{entity_type}_events ev ON m.entity_id = ev.entity_id
            LEFT JOIN prd_bronze_catalog.grid.{entity_type}_attributes attr ON m.entity_id = attr.entity_id
            LEFT JOIN prd_bronze_catalog.grid.{entity_type}_addresses addr ON m.entity_id = addr.entity_id
            WHERE 1=1
        """
        
        params = {}
        conditions = []
        
        if filters.get('date_from'):
            conditions.append("ev.event_date >= %(date_from)s")
            params['date_from'] = filters['date_from']
        
        if filters.get('date_to'):
            conditions.append("ev.event_date <= %(date_to)s")
            params['date_to'] = filters['date_to']
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        base_query += """
        )
        SELECT 
            COUNT(DISTINCT entity_id) as total_entities,
            COUNT(DISTINCT CASE WHEN alias_code_type = 'PTY' THEN entity_id END) as pep_count,
            COUNT(DISTINCT event_category_code) as unique_event_types,
            COUNT(DISTINCT address_country) as unique_countries,
            
            -- Top event categories
            COLLECT_LIST(STRUCT(
                event_category_code,
                COUNT(*) as count
            )) as event_distribution,
            
            -- Top countries
            COLLECT_LIST(STRUCT(
                address_country,
                COUNT(DISTINCT entity_id) as entity_count
            )) as country_distribution
            
        FROM entity_events
        GROUP BY 1
        """
        
        return base_query, params
    
    def get_optimized_search(self, search_params: Dict) -> Tuple[str, Dict]:
        """Get optimized search query for large-scale performance"""
        if optimized_db_queries:
            return optimized_db_queries.build_lightning_fast_search(search_params)
        else:
            # Fallback to standard query with basic optimization
            logger.warning("Using fallback query due to missing optimized module")
            return self.build_optimized_search_query('individual', search_params)
    
    def get_entity_detail(self, entity_id: str, entity_type: str = 'individual') -> Tuple[str, Dict]:
        """Get detailed entity information"""
        if optimized_db_queries:
            return optimized_db_queries.build_entity_detail_query(entity_id, entity_type)
        else:
            return self.build_entity_detail_query('individual', entity_id)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics for monitoring and optimization"""
        return {
            'verified_record_counts': {
                'individual_mapping': 34183705,
                'individual_events': 55519185,
                'individual_attributes': 79125308,
                'organization_mapping': 4873887,
                'organization_events': 6561406,
                'organization_attributes': 20114352
            },
            'pep_statistics': {
                'total_pty_records': 11935989,
                'total_prt_records': 5466991,
                'pep_entities': 6315865,
                'level_distribution': {
                    'L3': 1819169, 'L2': 987650, 'L1': 447760,
                    'L4': 442243, 'L5': 199854, 'L6': 3
                }
            },
            'event_statistics': {
                'total_events': 55519185,
                'pep_events': 5862965,
                'critical_events': ['TER', 'WLT', 'DEN', 'DTF', 'BRB', 'MLA'],
                'top_countries': ['UNITED STATES', 'UNITED KINGDOM', 'INDIA', 'BRAZIL']
            },
            'performance_recommendations': {
                'max_search_limit': 5000,
                'recommended_filters': ['high_risk_only', 'pep_only', 'recent_only'],
                'index_suggestions': ['entity_name', 'event_category_code', 'alias_code_type']
            }
        }