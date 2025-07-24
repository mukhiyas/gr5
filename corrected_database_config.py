"""
CORRECTED Database Configuration for GRID Entity Search
Using ACTUAL database schema from comprehensive audit
"""
from typing import Dict,List, Any

class CorrectedDatabaseConfig:
    """Database configuration with ACTUAL table and column names"""
    
    def __init__(self):
        self.catalog = "prd_bronze_catalog"
        self.schema = "grid"
        
    # ==================== ACTUAL TABLE NAMES ====================
    
    @property 
    def tables(self):
        return {
            # Individual entity tables
            'individual_mapping': f"{self.catalog}.{self.schema}.individual_mapping",
            'individual_events': f"{self.catalog}.{self.schema}.individual_events", 
            'individual_attributes': f"{self.catalog}.{self.schema}.individual_attributes",
            'individual_addresses': f"{self.catalog}.{self.schema}.individual_addresses",
            'individual_aliases': f"{self.catalog}.{self.schema}.individual_aliases",
            'individual_identifications': f"{self.catalog}.{self.schema}.individual_identifications",
            'individual_date_of_births': f"{self.catalog}.{self.schema}.individual_date_of_births",
            
            # Organization entity tables  
            'organization_mapping': f"{self.catalog}.{self.schema}.organization_mapping",
            'organization_events': f"{self.catalog}.{self.schema}.organization_events",
            'organization_attributes': f"{self.catalog}.{self.schema}.organization_attributes", 
            'organization_addresses': f"{self.catalog}.{self.schema}.organization_addresses",
            'organization_aliases': f"{self.catalog}.{self.schema}.organization_aliases",
            'organization_identifications': f"{self.catalog}.{self.schema}.organization_identifications",
            
            # Shared/reference tables
            'relationships': f"{self.catalog}.{self.schema}.relationships",
            'sources': f"{self.catalog}.{self.schema}.sources",
            'code_dictionary': f"{self.catalog}.{self.schema}.code_dictionary",
            'grid_orbis_mapping': f"{self.catalog}.{self.schema}.grid_orbis_mapping"
        }
    
    # ==================== ACTUAL COLUMN NAMES ====================
    
    @property
    def columns(self):
        return {
            # Main entity columns (mapping tables)
            'entity_id': 'entity_id',
            'risk_id': 'risk_id', 
            'record_type': 'recordDefinitionType',
            'source_item_id': 'source_item_id',
            'entity_name': 'entity_name',
            'system_id': 'systemId',
            'entity_date': 'entityDate',
            
            # Event columns
            'event_category': 'event_category_code',
            'event_subcategory': 'event_sub_category_code', 
            'event_date': 'event_date',
            'event_end_date': 'event_end_date',
            'event_description': 'event_description',
            'event_source_id': 'event_reference_source_item_id',
            
            # Attribute columns (PEP data stored here)
            'attribute_type': 'alias_code_type',  # 'PTY' for PEP data
            'attribute_value': 'alias_value',      # Actual PEP values
            
            # Address columns
            'address_country': 'address_country',
            'address_city': 'address_city',
            'address_line1': 'address_line1', 
            'address_line2': 'address_line2',
            'address_province': 'address_province',
            'address_type': 'address_type',
            'address_raw': 'address_raw_format',
            
            # Alias columns
            'alias_name': 'alias_name',
            'alias_type': 'alias_code_type',
            
            # Identification columns
            'id_type': 'identification_type',
            'id_value': 'identification_value',
            'id_country': 'identification_country',
            
            # Date of birth columns
            'birth_year': 'date_of_birth_year',
            'birth_month': 'date_of_birth_month', 
            'birth_day': 'date_of_birth_day',
            
            # Relationship columns
            'related_entity_id': 'related_entity_id',
            'related_entity_name': 'related_entity_name',
            'relationship_type': 'type',
            'relationship_direction': 'direction',
            
            # Source columns
            'source_name': 'name',
            'source_url': 'url',
            'source_description': 'description',
            'source_type': 'type',
            'source_publication': 'publication',
            
            # ORBIS mapping columns
            'orbis_risk_id': 'riskid',
            'orbis_entity_id': 'entityid', 
            'orbis_bvd_id': 'bvdid',
            'orbis_entity_type': 'entitytype',
            'orbis_event_code': 'eventcode',
            'orbis_entity_name': 'entityname'
        }
    
    # ==================== PEP ATTRIBUTE TYPES ====================
    
    @property
    def pep_attribute_types(self):
        """PEP data is stored in attributes table with specific types"""
        return {
            'PEP_TYPE': 'PTY',        # Main PEP classification 
            'PEP_RATING': 'PRT',      # PEP rating/grade
            'PEP_LEVEL': 'PLV',       # PEP level (but rarely used)
            'RISK_SCORE': 'RSC',      # Risk score attribute
            'RISKOGRAPHY': 'RGP'      # Risk description
        }
    
    # ==================== REAL PEP TYPE CODES ====================
    
    @property
    def pep_types(self):
        """Actual PEP type codes from PEP.txt and database verification"""
        return {
            # Political Officials
            'HOS': {'name': 'Head of State', 'level': 'L6', 'risk_multiplier': 2.0},
            'CAB': {'name': 'Cabinet Officials', 'level': 'L5', 'risk_multiplier': 1.8},
            'INF': {'name': 'Senior Infrastructure Officials', 'level': 'L4', 'risk_multiplier': 1.6},
            'NIO': {'name': 'Senior Non-Infrastructure Officials', 'level': 'L4', 'risk_multiplier': 1.6},
            'MUN': {'name': 'Municipal Officials', 'level': 'L3', 'risk_multiplier': 1.4},
            'REG': {'name': 'Regional Officials', 'level': 'L3', 'risk_multiplier': 1.4},
            'LEG': {'name': 'Senior Legislative', 'level': 'L4', 'risk_multiplier': 1.6},
            'AMB': {'name': 'Ambassadors/Diplomatic', 'level': 'L4', 'risk_multiplier': 1.6},
            'MIL': {'name': 'Senior Military', 'level': 'L4', 'risk_multiplier': 1.6},
            'JUD': {'name': 'Senior Judicial', 'level': 'L4', 'risk_multiplier': 1.6},
            'POL': {'name': 'Political Party Figures', 'level': 'L3', 'risk_multiplier': 1.4},
            
            # Business/Organizations
            'GOE': {'name': 'Government Owned Enterprises', 'level': 'L3', 'risk_multiplier': 1.4},
            'GCO': {'name': 'State-Controlled Business', 'level': 'L3', 'risk_multiplier': 1.4},
            'IGO': {'name': 'International Gov Organization', 'level': 'L3', 'risk_multiplier': 1.4},
            'ISO': {'name': 'International Sporting Officials', 'level': 'L2', 'risk_multiplier': 1.2},
            
            # Family/Associates
            'FAM': {'name': 'Family Members', 'level': 'L2', 'risk_multiplier': 1.2},
            'ASC': {'name': 'Close Associates', 'level': 'L1', 'risk_multiplier': 1.1}
        }
    
    # ==================== REAL EVENT CATEGORIES ====================
    
    @property 
    def event_categories(self):
        """63 actual event categories from database code_dictionary"""
        return {
            # Critical Risk (90-100)
            'TER': {'name': 'Terrorism', 'risk_score': 100, 'severity': 'critical'},
            'WLT': {'name': 'Watch List', 'risk_score': 100, 'severity': 'critical'},
            'DEN': {'name': 'Denied Entity', 'risk_score': 95, 'severity': 'critical'},
            'DTF': {'name': 'Drug Trafficking', 'risk_score': 90, 'severity': 'critical'},
            'TRF': {'name': 'Human Trafficking', 'risk_score': 90, 'severity': 'critical'},
            
            # High Risk (70-89)  
            'MLA': {'name': 'Money Laundering', 'risk_score': 85, 'severity': 'high'},
            'HUM': {'name': 'Human Rights Violations', 'risk_score': 85, 'severity': 'high'},
            'ORG': {'name': 'Organized Crime', 'risk_score': 85, 'severity': 'high'},
            'KID': {'name': 'Kidnapping', 'risk_score': 85, 'severity': 'high'},
            'SPY': {'name': 'Espionage/Spying', 'risk_score': 85, 'severity': 'high'},
            'BRB': {'name': 'Bribery/Corruption', 'risk_score': 75, 'severity': 'high'},
            'FRD': {'name': 'Fraud', 'risk_score': 70, 'severity': 'high'},
            'TAX': {'name': 'Tax Offenses', 'risk_score': 70, 'severity': 'high'},
            'SEC': {'name': 'Securities Violations', 'risk_score': 70, 'severity': 'high'},
            
            # Medium Risk (40-69)
            'REG': {'name': 'Regulatory Action', 'risk_score': 65, 'severity': 'medium'},
            'ROB': {'name': 'Robbery', 'risk_score': 60, 'severity': 'medium'},
            'SEX': {'name': 'Sex Offenses', 'risk_score': 60, 'severity': 'medium'},
            'PEP': {'name': 'PEP Classification', 'risk_score': 60, 'severity': 'medium'},
            'SNX': {'name': 'Sanctions', 'risk_score': 60, 'severity': 'medium'},
            'MUR': {'name': 'Murder/Manslaughter', 'risk_score': 55, 'severity': 'medium'},
            'AST': {'name': 'Assault/Battery', 'risk_score': 55, 'severity': 'medium'},
            'FUG': {'name': 'Fugitive', 'risk_score': 50, 'severity': 'medium'},
            'BUR': {'name': 'Burglary', 'risk_score': 50, 'severity': 'medium'},
            'TFT': {'name': 'Theft', 'risk_score': 50, 'severity': 'medium'},
            'IGN': {'name': 'Weapons/Guns', 'risk_score': 50, 'severity': 'medium'},
            'CON': {'name': 'Conspiracy', 'risk_score': 45, 'severity': 'medium'},
            'CFT': {'name': 'Counterfeiting', 'risk_score': 45, 'severity': 'medium'},
            'SMG': {'name': 'Smuggling', 'risk_score': 45, 'severity': 'medium'},
            'PSP': {'name': 'Stolen Property', 'risk_score': 40, 'severity': 'medium'},
            'IMP': {'name': 'Identity Theft', 'risk_score': 40, 'severity': 'medium'},
            'CYB': {'name': 'Cybercrime', 'risk_score': 40, 'severity': 'medium'},
            'OBS': {'name': 'Obscenity', 'risk_score': 40, 'severity': 'medium'},
            
            # Low Risk (0-39)
            'DPS': {'name': 'Drug Possession', 'risk_score': 35, 'severity': 'low'},
            'NSC': {'name': 'Non-Specific Crime', 'risk_score': 30, 'severity': 'low'},
            'MIS': {'name': 'Misconduct', 'risk_score': 30, 'severity': 'low'},
            'ABU': {'name': 'Abuse', 'risk_score': 30, 'severity': 'low'},
            'PRJ': {'name': 'Perjury', 'risk_score': 30, 'severity': 'low'},
            'ENV': {'name': 'Environmental Crimes', 'risk_score': 25, 'severity': 'low'},
            'GAM': {'name': 'Illegal Gambling', 'risk_score': 25, 'severity': 'low'},
            'ARS': {'name': 'Arson', 'risk_score': 25, 'severity': 'low'},
            'BUS': {'name': 'Business Crimes', 'risk_score': 25, 'severity': 'low'},
            'IPR': {'name': 'Prostitution', 'risk_score': 20, 'severity': 'low'},
            'LNS': {'name': 'Loan Sharking', 'risk_score': 20, 'severity': 'low'},
            'CPR': {'name': 'Copyright Infringement', 'risk_score': 20, 'severity': 'low'},
            'BKY': {'name': 'Bankruptcy', 'risk_score': 20, 'severity': 'low'},
            'RES': {'name': 'Real Estate Actions', 'risk_score': 20, 'severity': 'low'},
            'MOR': {'name': 'Mortgage Related', 'risk_score': 20, 'severity': 'low'},
            'IRC': {'name': 'Iran Connect', 'risk_score': 20, 'severity': 'low'},
            'FAR': {'name': 'Foreign Agent Registration', 'risk_score': 15, 'severity': 'low'},
            'LMD': {'name': 'Legal Marijuana', 'risk_score': 15, 'severity': 'low'},
            'DPP': {'name': 'Data Privacy', 'risk_score': 15, 'severity': 'low'},
            'FOF': {'name': 'Former OFAC', 'risk_score': 10, 'severity': 'low'},
            'FOS': {'name': 'Former Sanctions', 'risk_score': 10, 'severity': 'low'},
            'FOR': {'name': 'Forfeiture', 'risk_score': 10, 'severity': 'low'},
            'MSB': {'name': 'Money Services Business', 'risk_score': 10, 'severity': 'low'},
            'HTE': {'name': 'Hate Crimes', 'risk_score': 10, 'severity': 'low'},
            'BIL': {'name': 'Billing Practices', 'risk_score': 5, 'severity': 'low'},
            'CND': {'name': 'Financial Condition', 'risk_score': 5, 'severity': 'low'},
            'DEF': {'name': 'Default Risk', 'risk_score': 5, 'severity': 'low'},
            'HCD': {'name': 'Healthcare Disciplines', 'risk_score': 5, 'severity': 'low'},
            'PER': {'name': 'Performance Risk', 'risk_score': 5, 'severity': 'low'},
            'REO': {'name': 'Restructuring Risk', 'risk_score': 5, 'severity': 'low'},
            'VCY': {'name': 'Virtual Currency', 'risk_score': 5, 'severity': 'low'}
        }
    
    # ==================== REAL EVENT SUB-CATEGORIES ====================
    
    @property
    def event_subcategories(self):
        """36 actual event sub-categories from database code_dictionary"""
        return {
            # High severity modifiers (1.2-1.3x)
            'CVT': {'name': 'Convicted', 'multiplier': 1.3},
            'CNF': {'name': 'Confession', 'multiplier': 1.2},
            'SAN': {'name': 'Sanctioned', 'multiplier': 1.2},
            'SJT': {'name': 'Jail Time', 'multiplier': 1.2},
            'GOV': {'name': 'Government Official', 'multiplier': 1.2},
            
            # Medium-high severity (1.0-1.1x)
            'ART': {'name': 'Arrested', 'multiplier': 1.1},
            'IND': {'name': 'Indicted', 'multiplier': 1.1}, 
            'WTD': {'name': 'Wanted', 'multiplier': 1.1},
            'CHG': {'name': 'Charged', 'multiplier': 1.0},
            'ARN': {'name': 'Arraigned', 'multiplier': 1.0},
            'ACT': {'name': 'Regulatory Action', 'multiplier': 1.0},
            'PLE': {'name': 'Plea', 'multiplier': 1.0},
            'CSP': {'name': 'Conspiracy', 'multiplier': 1.0},
            'TRL': {'name': 'Trial', 'multiplier': 1.0},
            'DEP': {'name': 'Deported', 'multiplier': 1.0},
            'SEZ': {'name': 'Seizure', 'multiplier': 1.0},
            'RVK': {'name': 'Revoked', 'multiplier': 1.0},
            'FIM': {'name': 'Fine >$10K', 'multiplier': 1.0},
            
            # Lower severity (0.8-0.9x)
            'EXP': {'name': 'Expelled', 'multiplier': 0.9},
            'CEN': {'name': 'Censured', 'multiplier': 0.9},
            'SPD': {'name': 'Suspended', 'multiplier': 0.9},
            'CMP': {'name': 'Complaint', 'multiplier': 0.8},
            'APL': {'name': 'Appeal', 'multiplier': 0.8},
            'SET': {'name': 'Settlement', 'multiplier': 0.8},
            'LIC': {'name': 'License Action', 'multiplier': 0.8},
            
            # Low severity (0.6-0.7x)
            'ACC': {'name': 'Accused', 'multiplier': 0.7},
            'FIL': {'name': 'Fine <$10K', 'multiplier': 0.7},
            'PRB': {'name': 'Probe', 'multiplier': 0.7},
            'ADT': {'name': 'Audit', 'multiplier': 0.7},
            'ALL': {'name': 'Alleged', 'multiplier': 0.6},
            'LIN': {'name': 'Lien', 'multiplier': 0.6},
            'SPT': {'name': 'Suspected', 'multiplier': 0.6},
            
            # Minimal severity (0.4-0.5x)
            'ACQ': {'name': 'Acquitted', 'multiplier': 0.5},
            'ASC': {'name': 'Associated', 'multiplier': 0.5},
            'DMS': {'name': 'Dismissed', 'multiplier': 0.4}
        }
    
    # ==================== SEARCH QUERY BUILDERS ====================
    
    def build_entity_search_query(self, entity_type='individual', search_term=None, filters=None):
        """Build proper JOIN query for entity search"""
        
        base_table = f"{entity_type}_mapping"
        events_table = f"{entity_type}_events"
        attributes_table = f"{entity_type}_attributes"
        addresses_table = f"{entity_type}_addresses"
        aliases_table = f"{entity_type}_aliases"
        
        query = f"""
        SELECT DISTINCT
            e.entity_id,
            e.entity_name,
            e.risk_id,
            e.recordDefinitionType,
            e.systemId,
            e.entityDate,
            
            -- Aggregate events
            COLLECT_LIST(STRUCT(
                ev.event_category_code,
                ev.event_sub_category_code, 
                ev.event_date,
                ev.event_description
            )) as events,
            
            -- Aggregate PEP attributes
            COLLECT_LIST(CASE 
                WHEN attr.alias_code_type = 'PTY' THEN attr.alias_value 
            END) as pep_types,
            
            -- Aggregate addresses
            COLLECT_LIST(STRUCT(
                addr.address_country,
                addr.address_city,
                addr.address_type
            )) as addresses,
            
            -- Aggregate aliases
            COLLECT_LIST(alias.alias_name) as aliases
            
        FROM {self.tables[base_table]} e
        LEFT JOIN {self.tables[events_table]} ev ON e.entity_id = ev.entity_id
        LEFT JOIN {self.tables[attributes_table]} attr ON e.entity_id = attr.entity_id  
        LEFT JOIN {self.tables[addresses_table]} addr ON e.entity_id = addr.entity_id
        LEFT JOIN {self.tables[aliases_table]} alias ON e.entity_id = alias.entity_id
        """
        
        # Add WHERE conditions
        conditions = []
        
        if search_term:
            conditions.append(f"""
            (UPPER(e.entity_name) LIKE UPPER('%{search_term}%')
             OR UPPER(alias.alias_name) LIKE UPPER('%{search_term}%'))
            """)
        
        if filters:
            if filters.get('countries'):
                countries = "','".join(filters['countries'])
                conditions.append(f"addr.address_country IN ('{countries}')")
                
            if filters.get('event_categories'):
                events = "','".join(filters['event_categories']) 
                conditions.append(f"ev.event_category_code IN ('{events}')")
                
            if filters.get('pep_types'):
                pep_conditions = []
                for pep_type in filters['pep_types']:
                    pep_conditions.append(f"attr.alias_value LIKE '{pep_type}%'")
                conditions.append(f"({' OR '.join(pep_conditions)})")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += """
        GROUP BY e.entity_id, e.entity_name, e.risk_id, e.recordDefinitionType, e.systemId, e.entityDate
        ORDER BY 
            COUNT(CASE WHEN ev.event_category_code IN ('TER', 'WLT', 'DEN', 'DTF', 'BRB', 'MLA') THEN 1 END) DESC,
            COUNT(CASE WHEN attr.alias_code_type = 'PTY' THEN 1 END) DESC,
            e.entity_name
        """
        
        return query
    
    def build_comprehensive_entity_query(self, name_filters: Dict):
        """Comprehensive query to find entities using all available data"""
        return f"""
        SELECT DISTINCT
            e.entity_id,
            e.entity_name,
            e.risk_id,
            om.bvdid,
            om.riskid as orbis_risk_id,
            
            -- All PEP classifications
            COLLECT_LIST(CASE WHEN attr.alias_code_type = 'PTY' THEN attr.alias_value END) as pep_classifications,
            
            -- All criminal/risk events
            COLLECT_LIST(STRUCT(
                ev.event_category_code,
                ev.event_sub_category_code,
                ev.event_description,
                ev.event_date
            )) as risk_events,
            
            -- All addresses
            COLLECT_LIST(STRUCT(
                addr.address_country,
                addr.address_city,
                addr.address_type,
                addr.address_raw_format
            )) as addresses,
            
            -- Birth information
            dob.date_of_birth_year,
            dob.date_of_birth_month,
            dob.date_of_birth_day,
            
            -- Sources
            COLLECT_LIST(src.name) as source_names
            
        FROM {self.tables['individual_mapping']} e
        LEFT JOIN {self.tables['grid_orbis_mapping']} om ON e.entity_id = om.entityid
        LEFT JOIN {self.tables['individual_attributes']} attr ON e.entity_id = attr.entity_id
        LEFT JOIN {self.tables['individual_events']} ev ON e.entity_id = ev.entity_id  
        LEFT JOIN {self.tables['individual_addresses']} addr ON e.entity_id = addr.entity_id
        LEFT JOIN {self.tables['individual_date_of_births']} dob ON e.entity_id = dob.entity_id
        LEFT JOIN {self.tables['sources']} src ON e.entity_id = src.entity_id
        
        WHERE (
            -- Name variations - dynamic based on input
            {' OR '.join([f"UPPER(e.entity_name) LIKE UPPER('%{name}%')" for name in name_filters.get('names', [])])}
            {' OR ' if name_filters.get('names') and name_filters.get('orbis_names') else ''}
            {' OR '.join([f"UPPER(om.entityname) LIKE UPPER('%{name}%')" for name in name_filters.get('orbis_names', [])])}
        )
        {f"AND ({' OR '.join(name_filters.get('additional_filters', []))})" if name_filters.get('additional_filters') else ''}
        
        GROUP BY e.entity_id, e.entity_name, e.risk_id, om.bvdid, om.riskid,
                 dob.date_of_birth_year, dob.date_of_birth_month, dob.date_of_birth_day
        
        ORDER BY 
            COUNT(CASE WHEN attr.alias_code_type = 'PTY' THEN 1 END) DESC,
            COUNT(CASE WHEN ev.event_category_code IN ('BRB', 'PEP', 'MLA') THEN 1 END) DESC,
            e.entity_name
        """

# Global instance
corrected_db_config = CorrectedDatabaseConfig()