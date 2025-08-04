"""
ULTRA-OPTIMIZED Database Query Module for GRID Entity Search
Handles 79M+ attributes, 55M+ events, 34M+ entities without heap errors
Based on complete database analysis from complete_db.txt
"""
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import json
from config import config

logger = logging.getLogger(__name__)

class OptimizedDatabaseQueries:
    """Performance-optimized database queries for massive scale (79M+ records)"""
    
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
    
    def build_lightning_fast_search(self, search_params: Dict) -> Tuple[str, Dict]:
        """Lightning-fast search optimized for 79M+ records with intelligent filtering"""
        
        params = {}
        base_filters = []
        performance_filters = []
        
        # Determine entity type and set table prefix
        entity_type = search_params.get('entity_type', 'individual').lower()
        if entity_type not in ['individual', 'organization']:
            entity_type = 'individual'  # Default to individual
        
        table_prefix = entity_type
        logger.info(f"🔍 Searching {entity_type} tables with prefix: {table_prefix}")
        
        # === ULTRA-HIGH SELECTIVITY FILTERS (Apply First) ===
        
        # Entity ID (most selective - single record)
        if search_params.get('entity_id'):
            base_filters.append("m.entity_id = %(entity_id)s")
            params['entity_id'] = search_params['entity_id']
        
        # Risk ID (very selective)
        elif search_params.get('risk_id'):
            base_filters.append("m.risk_id = %(risk_id)s")
            params['risk_id'] = search_params['risk_id']
        
        # Name search with advanced matching logic for both individuals and organizations
        elif search_params.get('name'):
            name_term = search_params['name'].strip()
            logger.info(f"🔍 Name search for: '{name_term}' (length: {len(name_term)})")
            if len(name_term) >= 3:  # Minimum length for performance
                
                # Determine search strategy based on entity type and name characteristics
                if entity_type == 'organization':
                    # Organizations: Prioritize exact matches and whole word matches
                    base_filters.append(f"""
                        (
                            -- Exact match (highest priority)
                            UPPER(m.entity_name) = UPPER(%(name_exact)s)
                            
                            -- Starts with search term (e.g., "Enron" matches "Enron Corporation")
                            OR UPPER(m.entity_name) LIKE UPPER(%(name_starts_with)s)
                            
                            -- Contains as whole word (avoid partial matches like "Enron" in "Henronry")
                            OR (
                                UPPER(m.entity_name) LIKE UPPER(%(name_word_start)s)
                                OR UPPER(m.entity_name) LIKE UPPER(%(name_word_middle)s)
                                OR UPPER(m.entity_name) LIKE UPPER(%(name_word_end)s)
                                OR UPPER(m.entity_name) = UPPER(%(name_exact)s)
                            )
                            
                            -- Check aliases for exact or starts-with matches
                            OR EXISTS (
                                SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_aliases a 
                                WHERE a.entity_id = m.entity_id 
                                AND (
                                    UPPER(a.alias_name) = UPPER(%(name_exact)s)
                                    OR UPPER(a.alias_name) LIKE UPPER(%(name_starts_with)s)
                                )
                                LIMIT 1
                            )
                        )
                    """)
                    
                    # Organization-specific parameters for precise matching
                    params['name_exact'] = name_term
                    params['name_starts_with'] = f"{name_term}%"
                    params['name_word_start'] = f"{name_term} %"  # Word at start
                    params['name_word_middle'] = f"% {name_term} %"  # Word in middle
                    params['name_word_end'] = f"% {name_term}"  # Word at end
                    
                else:
                    # Individuals: More flexible matching for name variations
                    base_filters.append(f"""
                        (
                            -- Exact match
                            UPPER(m.entity_name) = UPPER(%(name_exact)s)
                            
                            -- Contains search term (more flexible for individuals)
                            OR UPPER(m.entity_name) LIKE UPPER(%(name_contains)s)
                            
                            -- First name or last name match (common for individuals)
                            OR UPPER(m.entity_name) LIKE UPPER(%(name_starts_with)s)
                            OR UPPER(m.entity_name) LIKE UPPER(%(name_ends_with)s)
                            
                            -- Check aliases (AKA, FKA) for individuals
                            OR EXISTS (
                                SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_aliases a 
                                WHERE a.entity_id = m.entity_id 
                                AND (
                                    UPPER(a.alias_name) = UPPER(%(name_exact)s)
                                    OR UPPER(a.alias_name) LIKE UPPER(%(name_contains)s)
                                )
                                LIMIT 1
                            )
                        )
                    """)
                    
                    # Individual-specific parameters for flexible matching
                    params['name_exact'] = name_term
                    params['name_contains'] = f"%{name_term}%"
                    params['name_starts_with'] = f"{name_term}%"
                    params['name_ends_with'] = f"%{name_term}"
                
                logger.info(f"✅ Added {entity_type} name filters for: '{name_term}'")
            else:
                logger.warning(f"⚠️ Name search term too short: '{name_term}' (min 3 chars)")
        
        # === HIGH SELECTIVITY FILTERS ===
        
        # Critical/High-risk events only (reduces from 55M to ~15M events)
        if search_params.get('high_risk_only'):
            critical_events_filter = "','".join(self.critical_events.union(self.high_risk_events))
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_category_code IN ('{critical_events_filter}')
                    LIMIT 1
                )
            """)
        
        # Specific event categories (highly selective when specific)
        elif search_params.get('event_categories'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_category_code IN %(event_categories)s
                    LIMIT 1
                )
            """)
            params['event_categories'] = tuple(search_params['event_categories'])
        
        # PEP-only filter (reduces to 6.3M entities)
        if search_params.get('pep_only'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_attributes attr
                    WHERE attr.entity_id = m.entity_id 
                    AND attr.alias_code_type = 'PTY'
                    LIMIT 1
                )
            """)
        
        # High-level PEPs only (L4, L5, L6 - very selective)
        if search_params.get('high_level_pep_only'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_attributes attr
                    WHERE attr.entity_id = m.entity_id 
                    AND attr.alias_code_type = 'PTY'
                    AND (attr.alias_value LIKE '%:L4' OR attr.alias_value LIKE '%:L5' OR attr.alias_value LIKE '%:L6')
                    LIMIT 1
                )
            """)
        
        # Specific PEP levels filter (HOS, CAB, MUN, FAM, etc.)
        if search_params.get('pep_levels'):
            pep_level_list = search_params['pep_levels']
            if isinstance(pep_level_list, str):
                pep_level_list = [pep_level_list]
            
            logger.info(f"🔍 DEBUG: PEP levels filter requested: {pep_level_list}")
            
            # Build PEP level conditions based on actual database patterns
            pep_conditions = []
            for pep_level in pep_level_list:
                logger.info(f"🔍 DEBUG: Processing PEP level: '{pep_level}'")
                # Based on query results, PEP values are like 'MUN:L3', 'HOS:L1', 'FAM', 'ASC'
                if pep_level in ['HOS', 'CAB', 'INF', 'MUN', 'REG', 'LEG', 'AMB', 'MIL', 'JUD', 'POL', 'GOE', 'GCO', 'IGO', 'ISO', 'NIO']:
                    # These have :L# patterns
                    condition = f"attr.alias_value LIKE '{pep_level}:%'"
                    pep_conditions.append(condition)
                    logger.info(f"🔍 DEBUG: Added pattern condition: {condition}")
                elif pep_level in ['FAM', 'ASC']:
                    # These are standalone values
                    condition = f"attr.alias_value = '{pep_level}'"
                    pep_conditions.append(condition)
                    logger.info(f"🔍 DEBUG: Added exact condition: {condition}")
                else:
                    # Handle any other PEP patterns
                    condition = f"attr.alias_value LIKE '%{pep_level}%'"
                    pep_conditions.append(condition)
                    logger.info(f"🔍 DEBUG: Added generic condition: {condition}")
            
            if pep_conditions:
                pep_query = " OR ".join(pep_conditions)
                logger.info(f"🔍 DEBUG: Final PEP query: {pep_query}")
                performance_filters.append(f"""
                    EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_attributes attr
                        WHERE attr.entity_id = m.entity_id 
                        AND attr.alias_code_type = 'PTY'
                        AND ({pep_query})
                        LIMIT 1
                    )
                """)
                logger.info(f"🔍 DEBUG: Added PEP filter to performance_filters")
            else:
                logger.warning(f"⚠️ DEBUG: No PEP conditions generated for: {pep_level_list}")
        
        # === MEDIUM SELECTIVITY FILTERS ===
        
        # Geographic filters
        if search_params.get('country'):
            country = search_params['country'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_addresses addr
                    WHERE addr.entity_id = m.entity_id 
                    AND UPPER(addr.address_country) = %(country)s
                    LIMIT 1
                )
            """)
            params['country'] = country
        
        # City filter
        if search_params.get('city'):
            city = search_params['city'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_addresses addr
                    WHERE addr.entity_id = m.entity_id 
                    AND UPPER(addr.address_city) LIKE %(city_pattern)s
                    LIMIT 1
                )
            """)
            params['city_pattern'] = f"%{city}%"
        
        # Province/State filter
        if search_params.get('province'):
            province = search_params['province'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_addresses addr
                    WHERE addr.entity_id = m.entity_id 
                    AND (UPPER(addr.address_line1) LIKE %(province_pattern)s 
                         OR UPPER(addr.address_line2) LIKE %(province_pattern)s)
                    LIMIT 1
                )
            """)
            params['province_pattern'] = f"%{province}%"
        
        # Address filter
        if search_params.get('address'):
            address = search_params['address'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_addresses addr
                    WHERE addr.entity_id = m.entity_id 
                    AND (UPPER(addr.address_line1) LIKE %(address_pattern)s 
                         OR UPPER(addr.address_line2) LIKE %(address_pattern)s
                         OR UPPER(addr.address_raw_format) LIKE %(address_pattern)s)
                    LIMIT 1
                )
            """)
            params['address_pattern'] = f"%{address}%"
        
        # Identification filters
        if search_params.get('identification_type'):
            id_type = search_params['identification_type'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_identifications iid
                    WHERE iid.entity_id = m.entity_id 
                    AND UPPER(iid.identification_type) LIKE %(id_type_pattern)s
                    LIMIT 1
                )
            """)
            params['id_type_pattern'] = f"%{id_type}%"
        
        if search_params.get('identification_value'):
            id_value = search_params['identification_value'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_identifications iid
                    WHERE iid.entity_id = m.entity_id 
                    AND UPPER(iid.identification_value) LIKE %(id_value_pattern)s
                    LIMIT 1
                )
            """)
            params['id_value_pattern'] = f"%{id_value}%"
        
        # Source filters  
        if search_params.get('source_name'):
            source_name = search_params['source_name'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.sources src
                    WHERE src.entity_id = m.entity_id 
                    AND UPPER(src.name) LIKE %(source_name_pattern)s
                    LIMIT 1
                )
            """)
            params['source_name_pattern'] = f"%{source_name}%"
        
        if search_params.get('source_key'):
            source_key = search_params['source_key'].upper()
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.sources src
                    WHERE src.entity_id = m.entity_id 
                    AND UPPER(src.url) LIKE %(source_key_pattern)s
                    LIMIT 1
                )
            """)
            params['source_key_pattern'] = f"%{source_key}%"
        
        # System and BVD ID filters (via mapping tables)
        if search_params.get('systemId'):
            performance_filters.append("m.systemId = %(system_id)s")
            params['system_id'] = search_params['systemId']
        
        if search_params.get('bvdid'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.grid_orbis_mapping bvd
                    WHERE bvd.entityid = m.entity_id 
                    AND bvd.bvdid = %(bvd_id)s
                    LIMIT 1
                )
            """)
            params['bvd_id'] = search_params['bvdid']
        
        if search_params.get('source_item_id'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_reference_source_item_id = %(source_item_id)s
                    LIMIT 1
                )
            """)
            params['source_item_id'] = search_params['source_item_id']
        
        # Single event category filter (different from event_categories list)
        if search_params.get('event_category'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_category_code = %(event_category_single)s
                    LIMIT 1
                )
            """)
            params['event_category_single'] = search_params['event_category']
        
        if search_params.get('event_sub_category'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND UPPER(ev.event_sub_category_code) LIKE %(event_sub_category_pattern)s
                    LIMIT 1
                )
            """)
            params['event_sub_category_pattern'] = f"%{search_params['event_sub_category'].upper()}%"
        
        # PEP Rating filter (A, B, C, D)
        if search_params.get('pep_ratings'):
            rating_list = search_params['pep_ratings']
            if isinstance(rating_list, str):
                rating_list = [rating_list]
            
            # Convert to SQL IN clause format
            rating_conditions = []
            for i, rating in enumerate(rating_list):
                param_key = f'pep_rating_{i}'
                rating_conditions.append(f"SUBSTRING(attr.alias_value, 1, 1) = %({param_key})s")
                params[param_key] = rating
            
            if rating_conditions:
                performance_filters.append(f"""
                    EXISTS (
                        SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_attributes attr
                        WHERE attr.entity_id = m.entity_id 
                        AND attr.alias_code_type = 'PRT'
                        AND ({' OR '.join(rating_conditions)})
                        LIMIT 1
                    )
                """)
        
        # Single event filter
        if search_params.get('single_event_only'):
            single_event_code = search_params.get('single_event_code')
            if single_event_code:
                # Filter for entities with exactly one event of specific type
                performance_filters.append(f"""
                    m.entity_id IN (
                        SELECT entity_id 
                        FROM prd_bronze_catalog.grid.{table_prefix}_events 
                        WHERE event_category_code = %(single_event_code)s
                        GROUP BY entity_id 
                        HAVING COUNT(*) = 1
                    )
                """)
                params['single_event_code'] = single_event_code.upper()
            else:
                # Filter for entities with exactly one event (any type)
                performance_filters.append(f"""
                    m.entity_id IN (
                        SELECT entity_id 
                        FROM prd_bronze_catalog.grid.{table_prefix}_events 
                        GROUP BY entity_id 
                        HAVING COUNT(*) = 1
                    )
                """)
        
        # Recent events only (last 5 years for performance)
        if search_params.get('recent_only'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
                    LIMIT 1
                )
            """)
        
        # Date range (apply as secondary filter)
        if search_params.get('date_from') or search_params.get('date_to'):
            date_conditions = []
            if search_params.get('date_from'):
                date_conditions.append("ev.event_date >= %(date_from)s")
                params['date_from'] = search_params['date_from']
            if search_params.get('date_to'):
                date_conditions.append("ev.event_date <= %(date_to)s")
                params['date_to'] = search_params['date_to']
            
            date_filter_clause = ' AND '.join(date_conditions)
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND {date_filter_clause}
                    LIMIT 1
                )
            """)
        
        # Risk codes filter (specific event categories)
        if search_params.get('risk_codes'):
            risk_codes = search_params['risk_codes']
            if isinstance(risk_codes, str):
                risk_codes = [risk_codes]
            
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND ev.event_category_code IN %(risk_codes)s
                    LIMIT 1
                )
            """)
            params['risk_codes'] = tuple(risk_codes)
        
        # Entity date filter
        if search_params.get('entity_date'):
            if isinstance(search_params['entity_date'], tuple) and len(search_params['entity_date']) == 2:
                year, range_years = search_params['entity_date']
                start_date = f"{int(year) - int(range_years)}-01-01"
                end_date = f"{int(year) + int(range_years)}-12-31"
                performance_filters.append("m.entityDate BETWEEN %(entity_date_start)s AND %(entity_date_end)s")
                params['entity_date_start'] = start_date
                params['entity_date_end'] = end_date
        
        # Event sub-category filter
        if search_params.get('event_sub_category'):
            performance_filters.append(f"""
                EXISTS (
                    SELECT 1 FROM prd_bronze_catalog.grid.{table_prefix}_events ev
                    WHERE ev.entity_id = m.entity_id 
                    AND UPPER(ev.event_sub_category_code) LIKE %(event_sub_category_pattern)s
                    LIMIT 1
                )
            """)
            params['event_sub_category_pattern'] = f"%{search_params['event_sub_category'].upper()}%"
        
        # Combine all filters with optimal order
        all_filters = base_filters + performance_filters
        where_clause = "WHERE " + " AND ".join(all_filters) if all_filters else ""
        
        # Ensure name parameters exist for ORDER BY clause (set to None if not searching by name)
        if 'name_exact' not in params:
            params['name_exact'] = None
        if 'name_starts_with' not in params:
            params['name_starts_with'] = None
        
        # Debug logging
        logger.info(f"🔍 Query filters count: base={len(base_filters)}, performance={len(performance_filters)}, total={len(all_filters)}")
        logger.info(f"🔍 Query parameters: {list(params.keys())}")
        if not all_filters:
            logger.warning("⚠️ No filters created - query will return all entities limited by LIMIT clause")
        
        # Intelligent limit based on filters (prevent heap errors)
        if search_params.get('entity_id') or search_params.get('risk_id'):
            limit = 1  # Single entity lookup
        elif search_params.get('high_level_pep_only'):
            limit = min(search_params.get('limit', 500), 1000)  # L4+ PEPs are rare
        elif search_params.get('pep_only'):
            limit = min(search_params.get('limit', 1000), 2000)  # PEPs are valuable
        elif search_params.get('high_risk_only'):
            limit = min(search_params.get('limit', 1500), 3000)  # High-risk entities
        else:
            limit = min(search_params.get('limit', 500), 1000)   # General search
        
        # === PREPARE CONFIGURATION-BASED EVENT SETS FOR SQL ===
        critical_events_sql = "'" + "','".join(self.critical_events) + "'" if self.critical_events else "''"
        high_risk_events_sql = "'" + "','".join(self.high_risk_events) + "'" if self.high_risk_events else "''"
        medium_risk_events_sql = "'" + "','".join(self.medium_risk_events) + "'" if self.medium_risk_events else "''"
        low_risk_events_sql = "'" + "','".join(self.low_risk_events) + "'" if self.low_risk_events else "''"
        
        logger.info(f"🔧 Using config-based events - Critical: {len(self.critical_events)}, High: {len(self.high_risk_events)}, Medium: {len(self.medium_risk_events)}, Low: {len(self.low_risk_events)}")
        
        # === OPTIMIZED QUERY WITH STRATEGIC DATA RETRIEVAL ===
        # Build select fields based on entity type
        select_fields = """
            m.entity_id,
            m.risk_id,
            m.entity_name,
            m.recordDefinitionType,
            m.systemId,
            m.entityDate,
            
            -- Optimized PEP data (only PTY, PRT for performance)
            COALESCE(pep_summary.pep_data, '[]') as pep_data,
            
            -- Optimized critical events only
            COALESCE(critical_events.events, '[]') as critical_events,
            
            -- Primary address only (performance optimization)
            COALESCE(primary_addr.address, '{{}}') as primary_address,
            
            -- Key aliases only (limit 3 for performance)
            COALESCE(key_aliases.aliases, '[]') as key_aliases,
            
            -- Relationships data
            COALESCE(relationships.relationships, '[]') as relationships,
        """
        
        # Add date_of_birth only for individuals
        if entity_type == 'individual':
            select_fields += """
            -- Date of birth (for individuals)
            COALESCE(dob.date_of_birth, NULL) as date_of_birth,
            """
        else:
            select_fields += """
            -- Date of birth (not applicable for organizations)
            NULL as date_of_birth,
            """
        
        select_fields += """
            -- Performance risk score (calculated efficiently)
            COALESCE(risk_calc.risk_score, 0) as calculated_risk_score"""
        
        # Build the joins based on entity type
        date_of_birth_join = ""
        if entity_type == 'individual':
            date_of_birth_join = """
        -- Date of birth (for individuals)
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(STRUCT(
                    date_of_birth_year,
                    date_of_birth_month,
                    date_of_birth_day,
                    date_of_birth_circa
                )) as date_of_birth
            FROM prd_bronze_catalog.grid.individual_date_of_births
        ) dob ON m.entity_id = dob.entity_id"""
        
        query_template = f"""SELECT{select_fields}
            
        FROM prd_bronze_catalog.grid.{table_prefix}_mapping m
        
        -- Optimized PEP data subquery
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    alias_code_type,
                    alias_value,
                    CASE 
                        WHEN alias_code_type = 'PTY' AND alias_value LIKE '%:L%' 
                        THEN SUBSTRING_INDEX(alias_value, ':', -1)
                        ELSE NULL
                    END as pep_level,
                    CASE 
                        WHEN alias_code_type = 'PTY' AND alias_value LIKE '%:%' 
                        THEN SUBSTRING_INDEX(alias_value, ':', 1)
                        WHEN alias_code_type = 'PTY' AND alias_value REGEXP '^[A-Z]__THREE_DIGITS__$'
                        THEN alias_value
                        ELSE NULL
                    END as pep_code
                ))) as pep_data
            FROM prd_bronze_catalog.grid.{table_prefix}_attributes
            WHERE alias_code_type IN ('PTY', 'PRT')
            GROUP BY entity_id
        ) pep_summary ON m.entity_id = pep_summary.entity_id
        
        -- All events with enhanced source and timestamp information
        LEFT JOIN (
            SELECT 
                e.entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    e.event_category_code,
                    e.event_sub_category_code,
                    e.event_date,
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
            FROM prd_bronze_catalog.grid.{table_prefix}_events e
            LEFT JOIN prd_bronze_catalog.grid.sources s 
                ON e.event_reference_source_item_id = s.entity_id
            GROUP BY e.entity_id
        ) critical_events ON m.entity_id = critical_events.entity_id
        
        -- Primary address (performance - single address)
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(STRUCT(
                    address_country,
                    address_city,
                    address_type
                )) as address
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY entity_id 
                        ORDER BY 
                            CASE 
                                WHEN address_type = 'pep' THEN 1
                                WHEN address_type = 'birth' THEN 2
                                WHEN address_country IN ('UNITED STATES', 'UNITED KINGDOM', 'CANADA', 'AUSTRALIA') THEN 3
                                ELSE 4
                            END
                    ) as rn
                FROM prd_bronze_catalog.grid.{table_prefix}_addresses
                WHERE address_country IS NOT NULL
            ) ranked_addr
            WHERE rn = 1
        ) primary_addr ON m.entity_id = primary_addr.entity_id
        
        -- Key aliases only (performance optimization)
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    alias_name,
                    alias_code_type
                ))) as aliases
            FROM prd_bronze_catalog.grid.{table_prefix}_aliases
            WHERE alias_code_type IN ('AKA', 'FKA', 'LOC')
            GROUP BY entity_id
        ) key_aliases ON m.entity_id = key_aliases.entity_id
        
        -- Relationships data (get all relationships for each entity)
        LEFT JOIN (
            SELECT 
                entity_id,
                TO_JSON(COLLECT_LIST(STRUCT(
                    related_entity_id,
                    related_entity_name,
                    type,
                    direction
                ))) as relationships
            FROM prd_bronze_catalog.grid.relationships
            GROUP BY entity_id
        ) relationships ON m.entity_id = relationships.entity_id
        {date_of_birth_join}
        
        -- Configuration-based risk score calculation (user-configurable)
        LEFT JOIN (
            SELECT 
                entity_id,
                GREATEST(
                    -- Configuration-based event risk scoring
                    MAX(
                        CASE 
                            WHEN event_category_code IN ({critical_events_sql}) THEN 100
                            WHEN event_category_code IN ({high_risk_events_sql}) THEN 85
                            WHEN event_category_code IN ({medium_risk_events_sql}) THEN 65
                            WHEN event_category_code IN ({low_risk_events_sql}) THEN 40
                            ELSE 20
                        END
                    ),
                    -- PEP level boost
                    CASE 
                        WHEN COUNT(*) > 5 THEN 10  -- Many events boost
                        ELSE 0
                    END
                ) as risk_score
            FROM prd_bronze_catalog.grid.{table_prefix}_events
            GROUP BY entity_id
        ) risk_calc ON m.entity_id = risk_calc.entity_id
        
        {where_clause}
        
        ORDER BY 
            -- Prioritize exact name matches when doing name search
            CASE 
                WHEN %(name_exact)s IS NOT NULL AND UPPER(m.entity_name) = UPPER(%(name_exact)s) THEN 0
                WHEN %(name_exact)s IS NOT NULL AND UPPER(m.entity_name) LIKE UPPER(%(name_starts_with)s) THEN 1
                ELSE 2
            END,
            -- Then prioritize by risk and PEP status
            COALESCE(risk_calc.risk_score, 0) DESC,
            CASE WHEN pep_summary.entity_id IS NOT NULL THEN 1 ELSE 2 END,
            CASE WHEN critical_events.entity_id IS NOT NULL THEN 1 ELSE 2 END,
            m.entityDate DESC
            
        LIMIT {limit}
        """
        
        # Apply formatting
        query = query_template.format(
            limit=limit,
            where_clause=where_clause,
            date_of_birth_join=date_of_birth_join,
            critical_events_sql=critical_events_sql,
            high_risk_events_sql=high_risk_events_sql,
            medium_risk_events_sql=medium_risk_events_sql,
            low_risk_events_sql=low_risk_events_sql
        )
        
        # Replace special patterns that can't be handled by .format()
        query = query.replace('__THREE_DIGITS__', '{3}')
        
        logger.info(f"📋 Query built with {len(all_filters)} filters, limit: {limit}")
        logger.debug(f"All filters: {all_filters}")
        logger.debug(f"Final params: {params}")
        
        return query, params
    
    def process_search_results(self, raw_results: List[Dict]) -> List[Dict]:
        """Process and enrich search results with comprehensive data extraction"""
        processed_results = []
        
        logger.info(f"🔄 Processing {len(raw_results) if raw_results else 0} raw results")
        
        if not raw_results:
            logger.warning("🔍 No raw results to process")
            return processed_results
        
        for idx, row in enumerate(raw_results):
            try:
                # Process JSON fields for all entities
                
                # Parse JSON fields safely
                for field in ['pep_data', 'critical_events', 'primary_address', 'key_aliases', 'relationships', 'date_of_birth']:
                    if row.get(field) and isinstance(row[field], str):
                        try:
                            row[field] = json.loads(row[field])
                        except Exception as e:
                            logger.error(f"❌ Failed to parse {field} for entity {row.get('entity_id')}: {e}")
                            row[field] = [] if field.endswith('s') or field == 'pep_data' else {}
                    elif not row.get(field):
                        row[field] = [] if field.endswith('s') or field == 'pep_data' else {}
                
                # Calculate comprehensive risk score
                risk_analysis = self.calculate_comprehensive_risk_score(row)
                
                # Extract PEP summary
                pep_summary = self.extract_pep_summary(row.get('pep_data', []))
                
                # Extract events information
                events = row.get('critical_events', [])
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
                
                # Build processed result with all required fields
                processed = {
                # Core entity information
                'entity_id': row.get('entity_id', ''),
                'risk_id': row.get('risk_id', ''),
                'entity_name': row.get('entity_name', ''),
                'entity_type': row.get('recordDefinitionType', ''),
                'system_id': row.get('systemId', ''),
                'entity_date': row.get('entityDate'),
                
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
                'pep_type': pep_summary.get('pep_codes', [None])[0] if pep_summary.get('pep_codes') else None,  # Primary PEP type (safe access)
                'pep_level': pep_summary.get('highest_pep_level'),
                'highest_pep_level': pep_summary.get('highest_pep_level'),
                'pep_associations': pep_summary.get('pep_descriptions', []),
                'prt_ratings': pep_summary.get('prt_ratings', []),
                
                # Event information
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
                
                # Placeholder fields for compatibility with UI
                'identifications': [],
                'relationships': row.get('relationships', []),
                'sources': [],
                'date_of_birth': row.get('date_of_birth'),
                'source_count': 0,
                'relationship_count': len(row.get('relationships', [])),
                'bvd_id': '',
                'source_item_id': '',
                'created_date': row.get('entityDate')
                }
                
                processed_results.append(processed)
                
            except Exception as e:
                logger.error(f"❌ Error processing result {idx}: {str(e)}")
                # Add a minimal result to prevent complete failure
                minimal_result = {
                    'entity_id': row.get('entity_id', ''),
                    'entity_name': row.get('entity_name', ''),
                    'risk_id': row.get('risk_id', ''),
                    'risk_score': 0,
                    'events': [],
                    'pep_codes': [],
                    'error': f"Processing error: {str(e)}"
                }
                processed_results.append(minimal_result)
        
        logger.info(f"✅ Processed {len(processed_results)} results successfully")
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
            # Use precise configuration-based scoring
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
                
                # Store full value for descriptions
                if pep.get('alias_value'):
                    summary['pep_descriptions'].append(pep['alias_value'])
            
            elif pep.get('alias_code_type') == 'PRT':
                summary['prt_ratings'].append(pep.get('alias_value', ''))
        
        return summary

# Global instance
optimized_db_queries = OptimizedDatabaseQueries()