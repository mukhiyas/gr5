"""
RISK_ID GROUPED Database Query Module for GRID Entity Search
Groups entities by risk_id instead of entity_id to show version history
Based on analysis of david.txt showing Hunter Biden has 37 entity versions under risk_id R114468110
"""
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import json
from config import config

logger = logging.getLogger(__name__)

class RiskIdOptimizedQueries:
    """Risk_id grouped database queries showing entity version history"""
    
    def __init__(self):
        # Load risk scores from configuration (user-configurable)
        self.event_risk_scores = self._load_event_risk_scores()
        self.critical_events = self._get_events_by_score_range(80, 100)
        self.high_risk_events = self._get_events_by_score_range(60, 79)
        self.medium_risk_events = self._get_events_by_score_range(40, 59)
        self.low_risk_events = self._get_events_by_score_range(0, 39)
        
        # User-configurable PEP level scores (from config)
        self.pep_level_scores = self._load_pep_level_scores()
        
        # User-configurable PRT rating scores (from config)  
        self.prt_rating_scores = self._load_prt_rating_scores()
        
        # User-configurable priority countries (from config)
        self.priority_countries = self._load_priority_countries()
    
    def _load_event_risk_scores(self) -> Dict[str, int]:
        """Load event risk scores from configuration (user-configurable)"""
        risk_scores = {}
        
        # Get all risk score categories from config
        critical_events = config.get('risk_scores.critical_events', {})
        valuable_events = config.get('risk_scores.valuable_events', {})
        investigative_events = config.get('risk_scores.investigative_events', {})
        probative_events = config.get('risk_scores.probative_events', {})
        
        # Combine all into single dictionary
        risk_scores.update(critical_events)
        risk_scores.update(valuable_events)
        risk_scores.update(investigative_events)
        risk_scores.update(probative_events)
        
        logger.info(f"✅ Loaded {len(risk_scores)} event risk scores from configuration")
        return risk_scores
    
    def _get_events_by_score_range(self, min_score: int, max_score: int) -> set:
        """Get event codes within a specific score range"""
        events = set()
        for event_code, score in self.event_risk_scores.items():
            if min_score <= score <= max_score:
                events.add(event_code)
        return events
    
    def _load_pep_level_scores(self) -> Dict[str, int]:
        """Load PEP level scores from configuration (user-configurable)"""
        # Use configured PEP level multipliers to calculate scores
        base_score = config.get('pep_settings.base_score', 50)  # User-configurable base PEP score
        pep_multipliers = {
            'L1': config.get('pep_settings.level_multipliers.L1', 1.1),
            'L2': config.get('pep_settings.level_multipliers.L2', 1.2), 
            'L3': config.get('pep_settings.level_multipliers.L3', 1.3),
            'L4': config.get('pep_settings.level_multipliers.L4', 1.4),
            'L5': config.get('pep_settings.level_multipliers.L5', 1.5),
            'L6': config.get('pep_settings.level_multipliers.L6', 1.6)
        }
        
        pep_scores = {}
        for level, multiplier in pep_multipliers.items():
            pep_scores[level] = int(base_score * multiplier)
        
        logger.info(f"✅ Loaded PEP level scores from configuration: {pep_scores}")
        return pep_scores
    
    def _load_prt_rating_scores(self) -> Dict[str, int]:
        """Load PRT rating scores from configuration (user-configurable)"""
        # Default PRT ratings if not configured
        default_scores = {'A': 90, 'B': 75, 'C': 60, 'D': 45}
        
        # Allow override from config if needed
        prt_scores = {}
        for rating in ['A', 'B', 'C', 'D']:
            prt_scores[rating] = config.get(f'prt_ratings.{rating}', default_scores[rating])
        
        logger.info(f"✅ Loaded PRT rating scores: {prt_scores}")
        return prt_scores
    
    def _load_priority_countries(self) -> set:
        """Load priority countries from configuration (user-configurable)"""
        # Default priority countries
        default_countries = {
            'UNITED STATES', 'UNITED KINGDOM', 'INDIA', 'BRAZIL', 
            'MEXICO', 'ITALY', 'AUSTRALIA', 'CANADA', 'PAKISTAN',
            'NIGERIA', 'RUSSIAN FEDERATION', 'BANGLADESH', 'GERMANY', 'CHINA'
        }
        
        # Allow override from config
        config_countries = config.get('geographic_settings.priority_countries', list(default_countries))
        if isinstance(config_countries, list):
            priority_countries = set(config_countries)
        else:
            priority_countries = default_countries
        
        logger.info(f"✅ Loaded {len(priority_countries)} priority countries from configuration")
        return priority_countries

    def get_event_risk_score(self, event_code: str) -> int:
        """Get risk score for an event code (user-configurable)"""
        default_score = config.get('risk_scores.default_event_score', 20)
        return self.event_risk_scores.get(event_code, default_score)
    
    def build_risk_id_grouped_search(self, search_params: Dict) -> Tuple[str, Dict]:
        """Build search query that groups entities by risk_id and shows version history"""
        
        params = {}
        base_filters = []
        performance_filters = []
        
        # === FILTERS ADAPTED FOR RISK_ID GROUPING ===
        
        # Risk ID (most selective - single entity with all versions)
        if search_params.get('risk_id'):
            base_filters.append("grouped_entities.risk_id = %(risk_id)s")
            params['risk_id'] = search_params['risk_id']
        
        # Entity ID (find the risk_id for this entity_id)
        elif search_params.get('entity_id'):
            base_filters.append("""
                grouped_entities.risk_id = (
                    SELECT risk_id 
                    FROM prd_bronze_catalog.grid.individual_mapping 
                    WHERE entity_id = %(entity_id)s
                    LIMIT 1
                )
            """)
            params['entity_id'] = search_params['entity_id']
        
        # Name search across all entity versions for each risk_id
        elif search_params.get('name'):
            name_term = search_params['name'].strip()
            logger.info(f"🔍 Name search for: '{name_term}' (length: {len(name_term)})")
            if len(name_term) >= 3:
                base_filters.append("""
                    grouped_entities.risk_id IN (
                        SELECT DISTINCT m.risk_id
                        FROM prd_bronze_catalog.grid.individual_mapping m
                        WHERE (
                            UPPER(m.entity_name) = UPPER(%(name_exact)s)
                            OR UPPER(m.entity_name) LIKE UPPER(%(name_pattern)s)
                            OR EXISTS (
                                SELECT 1 FROM prd_bronze_catalog.grid.individual_aliases a 
                                WHERE a.entity_id = m.entity_id 
                                AND (UPPER(a.alias_name) = UPPER(%(name_exact)s)
                                     OR UPPER(a.alias_name) LIKE UPPER(%(name_pattern)s))
                                LIMIT 1
                            )
                        )
                    )
                """)
                params['name_exact'] = name_term
                params['name_pattern'] = f"%{name_term}%"
                logger.info(f"✅ Added risk_id name filters - exact: '{params['name_exact']}', pattern: '{params['name_pattern']}'")
            else:
                logger.warning(f"⚠️ Name search term too short: '{name_term}' (min 3 chars)")
        
        # High-risk events filter (check all entity versions)
        if search_params.get('high_risk_only'):
            critical_events_filter = "','".join(self.critical_events.union(self.high_risk_events))
            performance_filters.append(f"""
                grouped_entities.risk_id IN (
                    SELECT DISTINCT m.risk_id
                    FROM prd_bronze_catalog.grid.individual_mapping m
                    WHERE EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.individual_events ev
                        WHERE ev.entity_id = m.entity_id 
                        AND ev.event_category_code IN ('{critical_events_filter}')
                        LIMIT 1
                    )
                )
            """)
        
        # Event categories filter (check all entity versions)
        elif search_params.get('event_categories'):
            performance_filters.append("""
                grouped_entities.risk_id IN (
                    SELECT DISTINCT m.risk_id
                    FROM prd_bronze_catalog.grid.individual_mapping m
                    WHERE EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.individual_events ev
                        WHERE ev.entity_id = m.entity_id 
                        AND ev.event_category_code IN %(event_categories)s
                        LIMIT 1
                    )
                )
            """)
            params['event_categories'] = tuple(search_params['event_categories'])
        
        # PEP-only filter (check all entity versions)
        if search_params.get('pep_only'):
            performance_filters.append("""
                grouped_entities.risk_id IN (
                    SELECT DISTINCT m.risk_id
                    FROM prd_bronze_catalog.grid.individual_mapping m
                    WHERE EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.individual_attributes attr
                        WHERE attr.entity_id = m.entity_id 
                        AND attr.alias_code_type = 'PTY'
                        LIMIT 1
                    )
                )
            """)
        
        # High-level PEPs only (check all entity versions)
        if search_params.get('high_level_pep_only'):
            performance_filters.append("""
                grouped_entities.risk_id IN (
                    SELECT DISTINCT m.risk_id
                    FROM prd_bronze_catalog.grid.individual_mapping m
                    WHERE EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.individual_attributes attr
                        WHERE attr.entity_id = m.entity_id 
                        AND attr.alias_code_type = 'PTY'
                        AND (attr.alias_value LIKE '%:L4' OR attr.alias_value LIKE '%:L5' OR attr.alias_value LIKE '%:L6')
                        LIMIT 1
                    )
                )
            """)
        
        # Geographic filters (check all entity versions)
        if search_params.get('country'):
            country = search_params['country'].upper()
            performance_filters.append("""
                grouped_entities.risk_id IN (
                    SELECT DISTINCT m.risk_id
                    FROM prd_bronze_catalog.grid.individual_mapping m
                    WHERE EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.individual_addresses addr
                        WHERE addr.entity_id = m.entity_id 
                        AND UPPER(addr.address_country) = %(country)s
                        LIMIT 1
                    )
                )
            """)
            params['country'] = country
        
        # Additional filters can be added here following the same pattern...
        
        # Combine all filters
        all_filters = base_filters + performance_filters
        where_clause = "WHERE " + " AND ".join(all_filters) if all_filters else ""
        
        # Intelligent limit based on filters
        if search_params.get('entity_id') or search_params.get('risk_id'):
            limit = 1  # Single entity lookup
        elif search_params.get('high_level_pep_only'):
            limit = min(search_params.get('limit', 500), 1000)
        elif search_params.get('pep_only'):
            limit = min(search_params.get('limit', 1000), 2000)
        elif search_params.get('high_risk_only'):
            limit = min(search_params.get('limit', 1500), 3000)
        else:
            limit = min(search_params.get('limit', 500), 1000)
        
        # Prepare event sets for SQL
        critical_events_sql = "'" + "','".join(self.critical_events) + "'" if self.critical_events else "''"
        high_risk_events_sql = "'" + "','".join(self.high_risk_events) + "'" if self.high_risk_events else "''"
        medium_risk_events_sql = "'" + "','".join(self.medium_risk_events) + "'" if self.medium_risk_events else "''"
        low_risk_events_sql = "'" + "','".join(self.low_risk_events) + "'" if self.low_risk_events else "''"
        
        logger.info(f"🔧 Using config-based events - Critical: {len(self.critical_events)}, High: {len(self.high_risk_events)}, Medium: {len(self.medium_risk_events)}, Low: {len(self.low_risk_events)}")
        
        # === RISK_ID GROUPED QUERY ===
        query_template = """SELECT
            grouped_entities.risk_id,
            grouped_entities.latest_entity_name,
            grouped_entities.entity_type,
            grouped_entities.system_id,
            grouped_entities.latest_entity_date,
            grouped_entities.entity_count,
            grouped_entities.all_entity_ids,
            grouped_entities.all_entity_dates,
            
            -- Aggregated PEP data from all entity versions
            COALESCE(pep_summary.pep_data, '[]') as pep_data,
            
            -- Aggregated events from all entity versions (chronologically ordered)
            COALESCE(all_events.events, '[]') as critical_events,
            
            -- Primary address from latest entity version
            COALESCE(primary_addr.address, '{{}}') as primary_address,
            
            -- Key aliases from all entity versions
            COALESCE(key_aliases.aliases, '[]') as key_aliases,
            
            -- Relationships from all entity versions
            COALESCE(relationships.relationships, '[]') as relationships,
            
            -- Date of birth from latest entity version
            COALESCE(dob.date_of_birth, NULL) as date_of_birth,
            
            -- Maximum risk score across all entity versions
            COALESCE(risk_calc.risk_score, 0) as calculated_risk_score
            
        FROM (
            -- Group all entity versions by risk_id
            SELECT 
                risk_id,
                -- Get latest entity data (most recent version)
                MAX(entity_name) as latest_entity_name,
                MAX(recordDefinitionType) as entity_type,
                MAX(systemId) as system_id,
                MAX(entityDate) as latest_entity_date,
                COUNT(*) as entity_count,
                COLLECT_LIST(entity_id) as all_entity_ids,
                COLLECT_LIST(entityDate) as all_entity_dates
            FROM prd_bronze_catalog.grid.individual_mapping
            GROUP BY risk_id
        ) grouped_entities
        
        -- Aggregated PEP data from all entity versions for each risk_id
        LEFT JOIN (
            SELECT 
                m.risk_id,
                TO_JSON(COLLECT_SET(STRUCT(
                    attr.alias_code_type,
                    attr.alias_value,
                    attr.entity_id,
                    m.entityDate,
                    CASE 
                        WHEN attr.alias_code_type = 'PTY' AND attr.alias_value LIKE '%:L%' 
                        THEN SUBSTRING_INDEX(attr.alias_value, ':', -1)
                        ELSE NULL
                    END as pep_level,
                    CASE 
                        WHEN attr.alias_code_type = 'PTY' AND attr.alias_value LIKE '%:%' 
                        THEN SUBSTRING_INDEX(attr.alias_value, ':', 1)
                        WHEN attr.alias_code_type = 'PTY' AND attr.alias_value REGEXP '^[A-Z]{{3}}$'
                        THEN attr.alias_value
                        ELSE NULL
                    END as pep_code
                ))) as pep_data
            FROM prd_bronze_catalog.grid.individual_attributes attr
            JOIN prd_bronze_catalog.grid.individual_mapping m ON attr.entity_id = m.entity_id
            WHERE attr.alias_code_type IN ('PTY', 'PRT')
            GROUP BY m.risk_id
        ) pep_summary ON grouped_entities.risk_id = pep_summary.risk_id
        
        -- All events from all entity versions for each risk_id (chronologically ordered)
        LEFT JOIN (
            SELECT 
                m.risk_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    e.event_category_code,
                    e.event_sub_category_code,
                    e.event_date,
                    e.entity_id,
                    m.entityDate as entity_version_date,
                    SUBSTR(e.event_description, 1, 200) as event_description_short,
                    -- Enhanced timestamp information
                    s.createdDate as created_date,
                    s.modifiedDate as modified_date,
                    s.publish_date,
                    -- Enhanced source/datafeed information
                    s.name as source_name,
                    s.type as source_type,
                    s.publication as source_publication,
                    s.publisher as source_publisher,
                    s.source_key
                ))) as events
            FROM prd_bronze_catalog.grid.individual_events e
            JOIN prd_bronze_catalog.grid.individual_mapping m ON e.entity_id = m.entity_id
            LEFT JOIN prd_bronze_catalog.grid.sources s 
                ON e.event_reference_source_item_id = s.entity_id
            GROUP BY m.risk_id
        ) all_events ON grouped_entities.risk_id = all_events.risk_id
        
        -- Primary address from latest entity version
        LEFT JOIN (
            SELECT 
                latest_mapping.risk_id,
                TO_JSON(STRUCT(
                    addr.address_country,
                    addr.address_city,
                    addr.address_type,
                    addr.entity_id,
                    latest_mapping.entityDate as entity_version_date
                )) as address
            FROM (
                SELECT risk_id, entity_id, entityDate,
                    ROW_NUMBER() OVER (
                        PARTITION BY risk_id 
                        ORDER BY entityDate DESC
                    ) as rn
                FROM prd_bronze_catalog.grid.individual_mapping
            ) latest_mapping
            JOIN prd_bronze_catalog.grid.individual_addresses addr 
                ON latest_mapping.entity_id = addr.entity_id
            WHERE latest_mapping.rn = 1 AND addr.address_country IS NOT NULL
        ) primary_addr ON grouped_entities.risk_id = primary_addr.risk_id
        
        -- Key aliases from all entity versions
        LEFT JOIN (
            SELECT 
                m.risk_id,
                TO_JSON(COLLECT_SET(STRUCT(
                    aliases.alias_name,
                    aliases.alias_code_type,
                    aliases.entity_id,
                    m.entityDate as entity_version_date
                ))) as aliases
            FROM prd_bronze_catalog.grid.individual_aliases aliases
            JOIN prd_bronze_catalog.grid.individual_mapping m ON aliases.entity_id = m.entity_id
            WHERE aliases.alias_code_type IN ('AKA', 'FKA', 'LOC')
            GROUP BY m.risk_id
        ) key_aliases ON grouped_entities.risk_id = key_aliases.risk_id
        
        -- Relationships from all entity versions
        LEFT JOIN (
            SELECT 
                m.risk_id,
                TO_JSON(COLLECT_SET(STRUCT(
                    rel.related_entity_id,
                    rel.related_entity_name,
                    rel.type,
                    rel.direction,
                    rel.entity_id,
                    m.entityDate as entity_version_date
                ))) as relationships
            FROM prd_bronze_catalog.grid.relationships rel
            JOIN prd_bronze_catalog.grid.individual_mapping m ON rel.entity_id = m.entity_id
            GROUP BY m.risk_id
        ) relationships ON grouped_entities.risk_id = relationships.risk_id
        
        -- Date of birth from latest entity version
        LEFT JOIN (
            SELECT 
                latest_mapping.risk_id,
                TO_JSON(STRUCT(
                    dob.date_of_birth_year,
                    dob.date_of_birth_month,
                    dob.date_of_birth_day,
                    dob.date_of_birth_circa,
                    dob.entity_id,
                    latest_mapping.entityDate as entity_version_date
                )) as date_of_birth
            FROM (
                SELECT risk_id, entity_id, entityDate,
                    ROW_NUMBER() OVER (
                        PARTITION BY risk_id 
                        ORDER BY entityDate DESC
                    ) as rn
                FROM prd_bronze_catalog.grid.individual_mapping
            ) latest_mapping
            JOIN prd_bronze_catalog.grid.individual_date_of_births dob 
                ON latest_mapping.entity_id = dob.entity_id
            WHERE latest_mapping.rn = 1
        ) dob ON grouped_entities.risk_id = dob.risk_id
        
        -- Maximum risk score across all entity versions
        LEFT JOIN (
            SELECT 
                m.risk_id,
                MAX(GREATEST(
                    COALESCE(
                        (SELECT MAX(
                            CASE 
                                WHEN e.event_category_code IN ({critical_events_sql}) THEN 100
                                WHEN e.event_category_code IN ({high_risk_events_sql}) THEN 85
                                WHEN e.event_category_code IN ({medium_risk_events_sql}) THEN 65
                                WHEN e.event_category_code IN ({low_risk_events_sql}) THEN 40
                                ELSE 20
                            END
                        )
                        FROM prd_bronze_catalog.grid.individual_events e
                        WHERE e.entity_id = m.entity_id), 0
                    ),
                    CASE 
                        WHEN (
                            SELECT COUNT(*) 
                            FROM prd_bronze_catalog.grid.individual_events e2 
                            WHERE e2.entity_id = m.entity_id
                        ) > 5 THEN 10
                        ELSE 0
                    END
                )) as risk_score
            FROM prd_bronze_catalog.grid.individual_mapping m
            GROUP BY m.risk_id
        ) risk_calc ON grouped_entities.risk_id = risk_calc.risk_id
        
        {where_clause}
        
        ORDER BY 
            -- Prioritize by risk and PEP status
            COALESCE(risk_calc.risk_score, 0) DESC,
            CASE WHEN pep_summary.risk_id IS NOT NULL THEN 1 ELSE 2 END,
            CASE WHEN all_events.risk_id IS NOT NULL THEN 1 ELSE 2 END,
            grouped_entities.latest_entity_date DESC,
            grouped_entities.entity_count DESC
            
        LIMIT {limit}
        """
        
        # Apply formatting
        query = query_template.format(
            limit=limit,
            where_clause=where_clause,
            critical_events_sql=critical_events_sql,
            high_risk_events_sql=high_risk_events_sql,
            medium_risk_events_sql=medium_risk_events_sql,
            low_risk_events_sql=low_risk_events_sql
        )
        
        logger.info(f"📋 Risk_id grouped query built with {len(all_filters)} filters, limit: {limit}")
        logger.debug(f"All filters: {all_filters}")
        logger.debug(f"Final params: {params}")
        
        return query, params
    
    def process_risk_id_results(self, raw_results: List[Dict]) -> List[Dict]:
        """Process risk_id grouped results showing entity version history"""
        processed_results = []
        
        logger.info(f"🔄 Processing {len(raw_results) if raw_results else 0} risk_id grouped results")
        
        if not raw_results:
            logger.warning("🔍 No raw results to process")
            return processed_results
        
        for idx, row in enumerate(raw_results):
            try:
                # Parse JSON fields safely
                for field in ['pep_data', 'critical_events', 'primary_address', 'key_aliases', 'relationships', 'date_of_birth']:
                    if row.get(field) and isinstance(row[field], str):
                        try:
                            row[field] = json.loads(row[field])
                        except Exception as e:
                            logger.error(f"❌ Failed to parse {field} for risk_id {row.get('risk_id')}: {e}")
                            row[field] = [] if field.endswith('s') or field == 'pep_data' else {}
                    elif not row.get(field):
                        row[field] = [] if field.endswith('s') or field == 'pep_data' else {}
                
                # Calculate comprehensive risk score
                risk_analysis = self.calculate_comprehensive_risk_score(row)
                
                # Extract PEP summary
                pep_summary = self.extract_pep_summary(row.get('pep_data', []))
                
                # Extract events information and sort chronologically
                events = row.get('critical_events', [])
                if events and isinstance(events, list):
                    # Sort events by created_date (newest first) to show recently added events at top
                    events = sorted(events, 
                                   key=lambda x: x.get('created_date', x.get('event_date', '')), 
                                   reverse=True)
                
                event_codes = []
                latest_event_date = None
                if events and isinstance(events, list):
                    for event in events:
                        if isinstance(event, dict) and event.get('event_category_code'):
                            event_codes.append(event['event_category_code'])
                        if isinstance(event, dict) and event.get('event_date') and (not latest_event_date or event['event_date'] > latest_event_date):
                            latest_event_date = event['event_date']
                
                # Extract address information
                address = row.get('primary_address', {})
                primary_country = address.get('address_country') if isinstance(address, dict) else None
                primary_city = address.get('address_city') if isinstance(address, dict) else None
                
                # Extract aliases
                aliases = row.get('key_aliases', [])
                alias_names = []
                if isinstance(aliases, list):
                    alias_names = [alias.get('alias_name', '') for alias in aliases if isinstance(alias, dict) and alias.get('alias_name')]
                
                # Entity version information
                entity_count = row.get('entity_count', 1)
                all_entity_ids = row.get('all_entity_ids', [])
                all_entity_dates = row.get('all_entity_dates', [])
                
                # Build processed result with risk_id as primary identifier
                processed = {
                # Core entity information (risk_id based)
                'risk_id': row.get('risk_id', ''),
                'entity_id': row.get('risk_id', ''),  # Use risk_id as main identifier
                'entity_name': row.get('latest_entity_name', ''),
                'entity_type': row.get('entity_type', ''),
                'system_id': row.get('system_id', ''),
                'entity_date': row.get('latest_entity_date'),
                
                # Entity version information
                'entity_count': entity_count,
                'all_entity_ids': all_entity_ids,
                'all_entity_dates': all_entity_dates,
                'latest_entity_date': row.get('latest_entity_date'),
                
                # Risk information
                'risk_score': risk_analysis['final_risk_score'],
                'risk_level': risk_analysis['risk_level'],
                'risk_severity': risk_analysis['risk_level'],  # Alias for compatibility
                'calculated_risk_score': row.get('calculated_risk_score', 0),
                'final_risk_score': risk_analysis['final_risk_score'],
                'risk_factors': risk_analysis['risk_factors'],
                
                # PEP information
                'is_pep': len(pep_summary.get('pep_codes', [])) > 0,
                'pep_status': 'PEP' if len(pep_summary.get('pep_codes', [])) > 0 else 'NOT_PEP',
                'pep_codes': pep_summary.get('pep_codes', []),
                'pep_levels': pep_summary.get('pep_levels', []),
                'pep_descriptions': pep_summary.get('pep_descriptions', []),
                'pep_type': pep_summary.get('pep_codes', [None])[0] if pep_summary.get('pep_codes') else None,
                'pep_level': pep_summary.get('highest_pep_level'),
                'highest_pep_level': pep_summary.get('highest_pep_level'),
                'pep_associations': pep_summary.get('pep_descriptions', []),
                'prt_ratings': pep_summary.get('prt_ratings', []),
                
                # Event information (chronologically sorted)
                'events': events,
                'event_count': len(events),
                'event_codes': list(set(event_codes)),
                'critical_events_count': len(events),
                'critical_events': events,
                'latest_event_date': latest_event_date,
                
                # Geographic information
                'primary_country': primary_country,
                'primary_city': primary_city,
                'country': primary_country,  # Alias for compatibility
                'addresses': [address] if address else [],
                
                # Additional data
                'aliases': alias_names,
                'key_aliases': aliases,
                'alias_count': len(alias_names),
                
                # Compatibility fields
                'identifications': [],
                'relationships': row.get('relationships', []),
                'sources': [],
                'date_of_birth': row.get('date_of_birth'),
                'source_count': 0,
                'relationship_count': len(row.get('relationships', [])),
                'bvd_id': '',
                'source_item_id': '',
                'created_date': row.get('latest_entity_date')
                }
                
                processed_results.append(processed)
                
            except Exception as e:
                logger.error(f"❌ Error processing risk_id result {idx}: {str(e)}")
                # Add a minimal result to prevent complete failure
                minimal_result = {
                    'risk_id': row.get('risk_id', ''),
                    'entity_id': row.get('risk_id', ''),
                    'entity_name': row.get('latest_entity_name', ''),
                    'risk_score': 0,
                    'events': [],
                    'pep_codes': [],
                    'entity_count': row.get('entity_count', 1),
                    'error': f"Processing error: {str(e)}"
                }
                processed_results.append(minimal_result)
        
        logger.info(f"✅ Processed {len(processed_results)} risk_id grouped results successfully")
        return processed_results
    
    def calculate_comprehensive_risk_score(self, entity_data: Dict) -> Dict[str, Any]:
        """Calculate comprehensive risk score from entity data"""
        
        risk_factors = {
            'event_risk': 0,
            'pep_risk': 0,
            'geographic_risk': 0,
            'temporal_risk': 0
        }
        
        # Event-based risk calculation
        events = entity_data.get('critical_events', [])
        if isinstance(events, str):
            try:
                events = json.loads(events)
            except:
                events = []
        
        max_event_risk = 0
        for event in events:
            category = event.get('event_category_code', '')
            event_score = self.get_event_risk_score(category)
            max_event_risk = max(max_event_risk, event_score)
        
        risk_factors['event_risk'] = max_event_risk
        
        # PEP-based risk calculation
        pep_data = entity_data.get('pep_data', [])
        if isinstance(pep_data, str):
            try:
                pep_data = json.loads(pep_data)
            except:
                pep_data = []
        
        max_pep_risk = 0
        for pep in pep_data:
            if pep.get('alias_code_type') == 'PTY':
                level = pep.get('pep_level')
                if level in self.pep_level_scores:
                    max_pep_risk = max(max_pep_risk, self.pep_level_scores[level])
                else:
                    max_pep_risk = max(max_pep_risk, 50)  # Default PEP risk
            elif pep.get('alias_code_type') == 'PRT':
                value = pep.get('alias_value', '')
                rating = value.split(':')[0] if ':' in value else value
                if rating in self.prt_rating_scores:
                    max_pep_risk = max(max_pep_risk, self.prt_rating_scores[rating])
        
        risk_factors['pep_risk'] = max_pep_risk
        
        # Geographic risk (simplified)
        address = entity_data.get('primary_address')
        if isinstance(address, str):
            try:
                address = json.loads(address)
            except:
                address = {}
        
        country = address.get('address_country', '') if address else ''
        if country in self.priority_countries:
            risk_factors['geographic_risk'] = config.get('geographic_risk.priority_country_score', 20)
        else:
            risk_factors['geographic_risk'] = config.get('geographic_risk.default_score', 10)
        
        # Calculate final score (weighted average)
        final_score = (
            risk_factors['event_risk'] * 0.5 +      # Events are most important
            risk_factors['pep_risk'] * 0.3 +        # PEP status is significant
            risk_factors['geographic_risk'] * 0.1 + # Geographic context
            risk_factors['temporal_risk'] * 0.1     # Temporal factors
        )
        
        return {
            'final_risk_score': int(final_score),
            'risk_level': self.get_risk_level(int(final_score)),
            'risk_factors': risk_factors
        }
    
    def get_risk_level(self, score: int) -> str:
        """Convert risk score to risk level"""
        if score >= 90:
            return 'Critical'
        elif score >= 75:
            return 'High'
        elif score >= 60:
            return 'Medium-High'
        elif score >= 45:
            return 'Medium'
        else:
            return 'Low'
    
    def extract_pep_summary(self, pep_data: List[Dict]) -> Dict[str, Any]:
        """Extract and summarize PEP information"""
        summary = {
            'pep_codes': [],
            'pep_levels': [],
            'pep_descriptions': [],
            'highest_pep_level': None,
            'prt_ratings': []
        }
        
        if not pep_data or not isinstance(pep_data, list):
            return summary
        
        level_hierarchy = {'L6': 6, 'L5': 5, 'L4': 4, 'L3': 3, 'L2': 2, 'L1': 1}
        highest_level_num = 0
        
        for pep in pep_data:
            if not isinstance(pep, dict):
                continue
            if pep.get('alias_code_type') == 'PTY':
                pep_code = pep.get('pep_code')
                pep_level = pep.get('pep_level')
                
                if pep_code and pep_code not in summary['pep_codes']:
                    summary['pep_codes'].append(pep_code)
                
                if pep_level and pep_level not in summary['pep_levels']:
                    summary['pep_levels'].append(pep_level)
                    level_num = level_hierarchy.get(pep_level, 0)
                    if level_num > highest_level_num:
                        highest_level_num = level_num
                        summary['highest_pep_level'] = pep_level
                
                if pep.get('alias_value'):
                    summary['pep_descriptions'].append(pep['alias_value'])
            
            elif pep.get('alias_code_type') == 'PRT':
                summary['prt_ratings'].append(pep.get('alias_value', ''))
        
        return summary

# Global instance
risk_id_optimized_queries = RiskIdOptimizedQueries()