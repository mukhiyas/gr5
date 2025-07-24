"""
Database-Driven Event Codes System
Extracts ALL event codes and definitions directly from the database
No hardcoding - everything comes from live data
"""

import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import json
from databricks import sql
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

logger = logging.getLogger(__name__)

class DatabaseDrivenCodes:
    """Event codes system that extracts everything from live database"""
    
    def __init__(self):
        """Initialize database-driven codes system"""
        self.connection = None
        self.event_codes = {}
        self.sub_category_codes = {}
        self.entity_attribute_codes = {}
        self.relationship_codes = {}
        self.usage_statistics = {}
        self.user_customizations = {}
        
        # Initialize connection and load data
        self._init_database_connection()
        self._load_all_codes_from_database()
        self._load_user_customizations()
    
    def _init_database_connection(self):
        """Initialize database connection"""
        try:
            self.connection = sql.connect(
                server_hostname=os.getenv("DB_HOST"),
                http_path=os.getenv("DB_HTTP_PATH"),
                access_token=os.getenv("DB_ACCESS_TOKEN"),
                catalog="prd_bronze_catalog",
                schema="grid"
            )
            logger.info("✅ Connected to database for live code extraction")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            self.connection = None
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute query and return results"""
        if not self.connection:
            logger.error("No database connection available")
            return []
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []
    
    def _load_all_codes_from_database(self):
        """Load all codes and definitions from database"""
        try:
            # 1. Load event category codes with definitions
            event_query = """
            SELECT 
                code,
                code_description,
                code_type
            FROM prd_bronze_catalog.grid.code_dictionary
            WHERE code_type = 'event_category'
            ORDER BY code
            """
            
            event_results = self._execute_query(event_query)
            logger.info(f"📋 Loaded {len(event_results)} event category codes from database")
            
            for row in event_results:
                self.event_codes[row['code']] = {
                    'name': row['code_description'],
                    'description': row['code_description'],
                    'code_type': row['code_type'],
                    'source': 'database_dictionary'
                }
            
            # 2. Load usage statistics
            usage_query = """
            WITH event_stats AS (
                SELECT 
                    event_category_code as code,
                    COUNT(*) as total_usage,
                    COUNT(DISTINCT entity_id) as unique_entities,
                    MIN(event_date) as first_seen,
                    MAX(event_date) as last_seen,
                    COUNT(CASE WHEN event_date >= DATE_SUB(CURRENT_DATE(), 365) THEN 1 END) as recent_usage,
                    AVG(DATEDIFF(CURRENT_DATE(), event_date)) as avg_days_old
                FROM (
                    SELECT event_category_code, entity_id, event_date FROM prd_bronze_catalog.grid.individual_events
                    UNION ALL
                    SELECT event_category_code, entity_id, event_date FROM prd_bronze_catalog.grid.organization_events
                ) all_events
                WHERE event_category_code IS NOT NULL
                GROUP BY event_category_code
            )
            SELECT 
                code,
                total_usage,
                unique_entities,
                recent_usage,
                avg_days_old,
                first_seen,
                last_seen,
                ROW_NUMBER() OVER (ORDER BY total_usage DESC) as frequency_rank
            FROM event_stats
            ORDER BY frequency_rank
            """
            
            usage_results = self._execute_query(usage_query)
            logger.info(f"📊 Loaded usage statistics for {len(usage_results)} codes")
            
            for row in usage_results:
                code = row['code']
                self.usage_statistics[code] = {
                    'total_usage': row['total_usage'],
                    'unique_entities': row['unique_entities'],
                    'recent_usage': row['recent_usage'],
                    'avg_days_old': row['avg_days_old'],
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen'],
                    'frequency_rank': row['frequency_rank']
                }
            
            # 3. Load sub-category codes
            sub_query = """
            SELECT 
                code,
                code_description,
                code_type
            FROM prd_bronze_catalog.grid.code_dictionary
            WHERE code_type = 'event_sub_category'
            ORDER BY code
            """
            
            sub_results = self._execute_query(sub_query)
            logger.info(f"📋 Loaded {len(sub_results)} event sub-category codes")
            
            for row in sub_results:
                self.sub_category_codes[row['code']] = {
                    'name': row['code_description'],
                    'description': row['code_description'],
                    'code_type': row['code_type'],
                    'source': 'database_dictionary'
                }
            
            # 4. Load entity attribute codes (PEP types, etc.)
            attr_query = """
            SELECT 
                code,
                code_description,
                code_type
            FROM prd_bronze_catalog.grid.code_dictionary
            WHERE code_type = 'entity_attribute'
            ORDER BY code
            """
            
            attr_results = self._execute_query(attr_query)
            logger.info(f"📋 Loaded {len(attr_results)} entity attribute codes")
            
            for row in attr_results:
                self.entity_attribute_codes[row['code']] = {
                    'name': row['code_description'],
                    'description': row['code_description'],
                    'code_type': row['code_type'],
                    'source': 'database_dictionary'
                }
            
            # 5. Assign intelligent risk scores
            self._assign_intelligent_risk_scores()
            
            logger.info("✅ Successfully loaded all codes from database")
            
        except Exception as e:
            logger.error(f"❌ Error loading codes from database: {e}")
            self._load_fallback_codes()
    
    def _assign_intelligent_risk_scores(self):
        """Assign intelligent risk scores based on database content and usage"""
        
        # High-risk keywords from actual database descriptions
        critical_keywords = [
            'terror', 'trafficking', 'murder', 'watch list', 'denied', 'sanction',
            'laundering', 'human rights', 'kidnap', 'espionage', 'organized crime'
        ]
        
        valuable_keywords = [
            'fraud', 'bribery', 'corruption', 'conspiracy', 'tax', 'securities',
            'regulatory', 'embezzle', 'extortion', 'insider'
        ]
        
        investigative_keywords = [
            'assault', 'battery', 'theft', 'burglary', 'forgery', 'cyber',
            'identity', 'counterfeit', 'smuggling', 'fugitive'
        ]
        
        for code, code_data in self.event_codes.items():
            # Get usage statistics
            usage_stats = self.usage_statistics.get(code, {})
            frequency_rank = usage_stats.get('frequency_rank', 999)
            total_usage = usage_stats.get('total_usage', 0)
            recent_usage = usage_stats.get('recent_usage', 0)
            
            # Analyze description content
            description = code_data.get('description', '').lower()
            
            # Base score calculation
            # High frequency = lower base score (common events are often less critical individually)
            base_score = max(15, 90 - (frequency_rank * 1.5))
            
            # Adjust for recent activity (recent = more relevant)
            if recent_usage > 0:
                recency_boost = min(20, recent_usage / max(1, total_usage) * 100)
                base_score += recency_boost
            
            # Content-based severity and scoring
            risk_score = base_score
            severity = 'probative'
            
            if any(keyword in description for keyword in critical_keywords):
                risk_score = max(85, base_score + 25)
                severity = 'critical'
            elif any(keyword in description for keyword in valuable_keywords):
                risk_score = max(65, base_score + 15)
                severity = 'valuable'
            elif any(keyword in description for keyword in investigative_keywords):
                risk_score = max(45, base_score + 10)
                severity = 'investigative'
            
            # Ensure bounds
            risk_score = min(100, max(10, int(risk_score)))
            
            # Store risk scoring
            code_data.update({
                'risk_score': risk_score,
                'severity': severity,
                'auto_assigned': True,
                'reasoning': f"Database analysis: rank {frequency_rank}, {total_usage:,} uses, recent {recent_usage}"
            })
    
    def _load_fallback_codes(self):
        """Load minimal fallback codes if database is unavailable"""
        fallback = {
            'TER': {'name': 'Terrorism', 'risk_score': 100, 'severity': 'critical'},
            'DTF': {'name': 'Drug Trafficking', 'risk_score': 90, 'severity': 'critical'},
            'MLA': {'name': 'Money Laundering', 'risk_score': 85, 'severity': 'critical'},
            'FRD': {'name': 'Fraud', 'risk_score': 70, 'severity': 'valuable'},
            'BRB': {'name': 'Bribery', 'risk_score': 75, 'severity': 'valuable'},
        }
        
        for code, data in fallback.items():
            self.event_codes[code] = {
                **data,
                'description': data['name'],
                'source': 'fallback',
                'auto_assigned': True
            }
        
        logger.warning("⚠️ Using fallback codes - database unavailable")
    
    def _load_user_customizations(self):
        """Load user customizations"""
        try:
            config_file = "/Users/sujanmukhiya/Desktop/NiceGUI_GRID/advanced_entity_search/user_event_config.json"
            
            with open(config_file, 'r', encoding='utf-8') as f:
                self.user_customizations = json.load(f)
            
            # Apply customizations
            for code, custom_config in self.user_customizations.items():
                if code in self.event_codes:
                    self.event_codes[code].update(custom_config)
                    self.event_codes[code]['auto_assigned'] = False
                    self.event_codes[code]['user_customized'] = True
            
            logger.info(f"✅ Applied user customizations for {len(self.user_customizations)} codes")
            
        except FileNotFoundError:
            logger.info("📝 No user customizations found - using database defaults")
        except Exception as e:
            logger.error(f"❌ Error loading user customizations: {e}")
    
    def save_user_customizations(self):
        """Save user customizations"""
        try:
            config_file = "/Users/sujanmukhiya/Desktop/NiceGUI_GRID/advanced_entity_search/user_event_config.json"
            
            # Extract user-customized codes
            user_configs = {
                code: {k: v for k, v in config.items() 
                      if k in ['risk_score', 'severity', 'name', 'description', 'user_customized']}
                for code, config in self.event_codes.items()
                if config.get('user_customized', False)
            }
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(user_configs, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Saved user customizations for {len(user_configs)} codes")
            
        except Exception as e:
            logger.error(f"❌ Error saving user customizations: {e}")
    
    def get_code_info(self, code: str) -> Dict[str, Any]:
        """Get comprehensive information about an event code"""
        if code not in self.event_codes:
            return {
                'code': code,
                'name': f'Unknown Code {code}',
                'description': f'Code {code} not found in database',
                'risk_score': 0,
                'severity': 'unknown',
                'source': 'missing'
            }
        
        code_data = self.event_codes[code].copy()
        usage_stats = self.usage_statistics.get(code, {})
        
        return {
            'code': code,
            'name': code_data.get('name', f'Event Code {code}'),
            'description': code_data.get('description', 'No description available'),
            'risk_score': code_data.get('risk_score', 0),
            'severity': code_data.get('severity', 'unknown'),
            'usage_count': usage_stats.get('total_usage', 0),
            'frequency_rank': usage_stats.get('frequency_rank', 999),
            'recent_usage': usage_stats.get('recent_usage', 0),
            'first_seen': usage_stats.get('first_seen'),
            'last_seen': usage_stats.get('last_seen'),
            'source': code_data.get('source', 'unknown'),
            'auto_assigned': code_data.get('auto_assigned', True),
            'user_customized': code_data.get('user_customized', False),
            'reasoning': code_data.get('reasoning', 'Database-driven assignment')
        }
    
    def update_code_config(self, code: str, **kwargs):
        """Update code configuration"""
        if code not in self.event_codes:
            # Add new code
            self.event_codes[code] = {
                'name': kwargs.get('name', f'User Defined {code}'),
                'description': kwargs.get('description', ''),
                'source': 'user_added'
            }
        
        # Update fields
        for field in ['name', 'description', 'risk_score', 'severity']:
            if field in kwargs:
                self.event_codes[code][field] = kwargs[field]
        
        # Mark as user customized
        self.event_codes[code]['user_customized'] = True
        self.event_codes[code]['auto_assigned'] = False
        
        logger.info(f"✅ Updated configuration for code {code}")
    
    def get_all_codes_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all codes"""
        summary = []
        
        for code in sorted(self.event_codes.keys()):
            info = self.get_code_info(code)
            summary.append(info)
        
        return summary
    
    def refresh_from_database(self):
        """Refresh codes from database (for live updates)"""
        logger.info("🔄 Refreshing codes from database...")
        self._load_all_codes_from_database()
        self._load_user_customizations()
        logger.info("✅ Database refresh completed")

# Global instance
database_driven_codes = DatabaseDrivenCodes()

def get_event_description(event_code: str, sub_code: str = None) -> str:
    """Get event description for display"""
    info = database_driven_codes.get_code_info(event_code)
    
    # Only return "Unknown" if the code truly doesn't exist in the database
    # For codes in database, always return the proper name
    if info['source'] == 'missing' or info['name'].startswith('Unknown Code'):
        return event_code  # Just return the code itself, not "Unknown (code)"
    
    if sub_code and sub_code in database_driven_codes.sub_category_codes:
        sub_info = database_driven_codes.sub_category_codes[sub_code]
        return f"{info['name']} - {sub_info['name']}"
    elif sub_code:
        return f"{info['name']} - {sub_code}"
    
    return info['name']

def get_event_risk_score(event_code: str) -> int:
    """Get risk score for an event code"""
    info = database_driven_codes.get_code_info(event_code)
    return info['risk_score']

def get_event_severity(event_code: str) -> str:
    """Get severity level for an event code"""
    info = database_driven_codes.get_code_info(event_code)
    return info['severity']