"""
Comprehensive Event Codes Configuration
Automatically extracts ALL event codes from database and definitions from documentation
User-configurable risk scoring for all codes with no hardcoding
"""

import re
import logging
from typing import Dict, Any, List, Tuple
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class ComprehensiveEventCodes:
    """Comprehensive event codes configuration with auto-extraction and user customization"""
    
    def __init__(self):
        """Initialize comprehensive event codes system"""
        self.all_codes = {}
        self.code_definitions = {}
        self.risk_scores = {}
        self.user_customizations = {}
        
        # Load all data
        self._load_database_codes()
        self._load_code_definitions()
        self._assign_default_risk_scores()
        self._load_user_customizations()
    
    def _load_database_codes(self):
        """Load ALL event codes from database query results"""
        try:
            codes_file = "/Users/sujanmukhiya/Desktop/NiceGUI_GRID/advanced_entity_search/codes.txt"
            
            with open(codes_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract event codes and usage counts
            # Pattern: CODE usage_count (e.g., "ABU 1255581")
            code_pattern = r'^([A-Z]{3})\s+(\d+)$'
            
            for line in content.split('\n'):
                line = line.strip()
                match = re.match(code_pattern, line)
                if match:
                    code = match.group(1)
                    usage_count = int(match.group(2))
                    
                    self.all_codes[code] = {
                        'usage_count': usage_count,
                        'frequency_rank': 0,  # Will be calculated later
                        'category': self._infer_category_from_code(code),
                        'name': self._get_default_name(code),
                        'source': 'database_query'
                    }
            
            # Calculate frequency ranks
            sorted_codes = sorted(self.all_codes.items(), key=lambda x: x[1]['usage_count'], reverse=True)
            for rank, (code, data) in enumerate(sorted_codes, 1):
                self.all_codes[code]['frequency_rank'] = rank
            
            logger.info(f"✅ Loaded {len(self.all_codes)} event codes from database")
            
        except Exception as e:
            logger.error(f"❌ Error loading database codes: {e}")
            self._load_fallback_codes()
    
    def _load_code_definitions(self):
        """Load code definitions from risk codes documentation"""
        try:
            risk_codes_file = "/Users/sujanmukhiya/Desktop/NiceGUI_GRID/advanced_entity_search/risk codes.txt"
            
            with open(risk_codes_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Extract code definitions from markdown table format
            # Pattern: | CODE | Category | Description |
            table_pattern = r'\\|\\s*([A-Z]{3})\\s*\\|\\s*([^|]+)\\s*\\|\\s*([^|]+)\\s*\\|'
            
            matches = re.findall(table_pattern, content)
            
            for code, category, description in matches:
                code = code.strip()
                category = category.strip()
                description = description.strip()
                
                self.code_definitions[code] = {
                    'name': category,
                    'description': description,
                    'source': 'documentation'
                }
                
                # Update the all_codes with better names if available
                if code in self.all_codes:
                    self.all_codes[code]['name'] = category
                    self.all_codes[code]['description'] = description
            
            logger.info(f"✅ Loaded {len(self.code_definitions)} code definitions from documentation")
            
        except Exception as e:
            logger.error(f"❌ Error loading code definitions: {e}")
    
    def _assign_default_risk_scores(self):
        """Assign default risk scores based on frequency, category, and severity indicators"""
        
        # High-risk keywords that indicate critical severity
        critical_keywords = [
            'terror', 'trafficking', 'murder', 'laundering', 'weapon', 'sanction', 'watch',
            'denied', 'espionage', 'kidnap', 'human rights', 'organized crime'
        ]
        
        # Medium-risk keywords
        valuable_keywords = [
            'fraud', 'bribery', 'conspiracy', 'robbery', 'tax', 'securities', 'regulatory',
            'corruption', 'embezzle', 'extortion'
        ]
        
        # Lower-risk keywords
        investigative_keywords = [
            'assault', 'theft', 'burglary', 'forgery', 'cyber', 'identity', 'counterfeit',
            'smuggling', 'fugitive'
        ]
        
        for code, data in self.all_codes.items():
            # Get name and description for analysis
            name = data.get('name', '').lower()
            description = data.get('description', '').lower()
            text_to_analyze = f"{name} {description}"
            
            # Default risk score based on frequency (more frequent = potentially lower individual risk)
            frequency_rank = data.get('frequency_rank', 50)
            base_score = max(20, 100 - (frequency_rank * 2))  # Higher frequency = lower base score
            
            # Adjust based on severity indicators
            risk_score = base_score
            severity = 'probative'
            
            # Check for critical indicators
            if any(keyword in text_to_analyze for keyword in critical_keywords):
                risk_score = max(85, base_score + 30)
                severity = 'critical'
            
            # Check for valuable indicators
            elif any(keyword in text_to_analyze for keyword in valuable_keywords):
                risk_score = max(65, base_score + 20)
                severity = 'valuable'
            
            # Check for investigative indicators
            elif any(keyword in text_to_analyze for keyword in investigative_keywords):
                risk_score = max(45, base_score + 10)
                severity = 'investigative'
            
            # Ensure score is within bounds
            risk_score = min(100, max(10, risk_score))
            
            self.risk_scores[code] = {
                'risk_score': risk_score,
                'severity': severity,
                'auto_assigned': True,
                'reasoning': f"Based on frequency rank {frequency_rank} and content analysis"
            }
        
        logger.info(f"✅ Assigned default risk scores for {len(self.risk_scores)} codes")
    
    def _infer_category_from_code(self, code: str) -> str:
        """Infer category from 3-letter code patterns"""
        # Common code patterns
        patterns = {
            'A': 'Assault/Abuse',
            'B': 'Business/Bribery', 
            'C': 'Conspiracy/Cyber',
            'D': 'Drug/Denied',
            'E': 'Environmental/Economic',
            'F': 'Fraud/Fugitive',
            'G': 'Government',
            'H': 'Human Rights/Trafficking',
            'I': 'Identity/Immigration',
            'K': 'Kidnapping',
            'L': 'Legal',
            'M': 'Money/Murder',
            'O': 'Organized Crime',
            'P': 'Political/Possession',
            'R': 'Regulatory/Robbery',
            'S': 'Sanctions/Securities',
            'T': 'Terrorism/Tax/Theft',
            'W': 'Weapons/Watch List'
        }
        
        first_letter = code[0] if code else 'U'
        return patterns.get(first_letter, 'Unknown Category')
    
    def _get_default_name(self, code: str) -> str:
        """Get default name for unknown codes"""
        return f"Event Code {code}"
    
    def _load_fallback_codes(self):
        """Load fallback codes if database file is not available"""
        fallback_codes = [
            'ABU', 'ARS', 'AST', 'BKY', 'BRB', 'BUR', 'CFT', 'CON', 'CYB', 'DEN',
            'DPS', 'DTF', 'ENV', 'FRD', 'FUG', 'GAM', 'HUM', 'IGN', 'IMP', 'KID',
            'MIS', 'MLA', 'ORG', 'REG', 'SEC', 'TER', 'TRF', 'WLT'
        ]
        
        for i, code in enumerate(fallback_codes):
            self.all_codes[code] = {
                'usage_count': 1000 - (i * 10),  # Simulated usage
                'frequency_rank': i + 1,
                'category': self._infer_category_from_code(code),
                'name': self._get_default_name(code),
                'source': 'fallback'
            }
        
        logger.warning(f"⚠️ Using fallback codes: {len(fallback_codes)} codes loaded")
    
    def _load_user_customizations(self):
        """Load user customizations from config file"""
        try:
            config_file = "/Users/sujanmukhiya/Desktop/NiceGUI_GRID/advanced_entity_search/user_event_config.json"
            
            with open(config_file, 'r', encoding='utf-8') as f:
                self.user_customizations = json.load(f)
            
            # Apply user customizations
            for code, custom_config in self.user_customizations.items():
                if code in self.risk_scores:
                    self.risk_scores[code].update(custom_config)
                    self.risk_scores[code]['auto_assigned'] = False
                    self.risk_scores[code]['user_customized'] = True
            
            logger.info(f"✅ Loaded user customizations for {len(self.user_customizations)} codes")
            
        except FileNotFoundError:
            logger.info("📝 No user customizations file found - using defaults")
        except Exception as e:
            logger.error(f"❌ Error loading user customizations: {e}")
    
    def save_user_customizations(self):
        """Save user customizations to config file"""
        try:
            config_file = "/Users/sujanmukhiya/Desktop/NiceGUI_GRID/advanced_entity_search/user_event_config.json"
            
            # Extract only user-customized codes
            user_configs = {
                code: config for code, config in self.risk_scores.items()
                if config.get('user_customized', False)
            }
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(user_configs, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Saved user customizations for {len(user_configs)} codes")
            
        except Exception as e:
            logger.error(f"❌ Error saving user customizations: {e}")
    
    def get_code_info(self, code: str) -> Dict[str, Any]:
        """Get comprehensive information about an event code"""
        if code not in self.all_codes:
            return {
                'code': code,
                'name': f'Unknown Code {code}',
                'description': 'Code not found in system',
                'risk_score': 0,
                'severity': 'unknown',
                'usage_count': 0,
                'frequency_rank': 999,
                'category': 'Unknown',
                'source': 'missing'
            }
        
        base_info = self.all_codes[code].copy()
        risk_info = self.risk_scores.get(code, {})
        
        return {
            'code': code,
            'name': base_info.get('name', f'Event Code {code}'),
            'description': base_info.get('description', 'No description available'),
            'risk_score': risk_info.get('risk_score', 0),
            'severity': risk_info.get('severity', 'unknown'),
            'usage_count': base_info.get('usage_count', 0),
            'frequency_rank': base_info.get('frequency_rank', 999),
            'category': base_info.get('category', 'Unknown'),
            'source': base_info.get('source', 'unknown'),
            'auto_assigned': risk_info.get('auto_assigned', True),
            'user_customized': risk_info.get('user_customized', False),
            'reasoning': risk_info.get('reasoning', 'No reasoning available')
        }
    
    def update_code_config(self, code: str, name: str = None, risk_score: int = None, 
                          severity: str = None, description: str = None):
        """Allow users to update code configuration"""
        if code not in self.all_codes:
            # Add new code
            self.all_codes[code] = {
                'usage_count': 0,
                'frequency_rank': 999,
                'category': self._infer_category_from_code(code),
                'name': name or f'User Defined {code}',
                'source': 'user_added'
            }
        
        if code not in self.risk_scores:
            self.risk_scores[code] = {}
        
        # Update configuration
        if name:
            self.all_codes[code]['name'] = name
        if description:
            self.all_codes[code]['description'] = description
        if risk_score is not None:
            self.risk_scores[code]['risk_score'] = max(0, min(100, risk_score))
        if severity:
            self.risk_scores[code]['severity'] = severity
        
        # Mark as user customized
        self.risk_scores[code]['user_customized'] = True
        self.risk_scores[code]['auto_assigned'] = False
        
        logger.info(f"✅ Updated configuration for code {code}")
    
    def get_all_codes_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all codes for UI display"""
        summary = []
        
        for code in sorted(self.all_codes.keys()):
            info = self.get_code_info(code)
            summary.append({
                'code': code,
                'name': info['name'],
                'risk_score': info['risk_score'],
                'severity': info['severity'],
                'usage_count': info['usage_count'],
                'frequency_rank': info['frequency_rank'],
                'user_customized': info['user_customized'],
                'description': info['description'][:100] + '...' if len(info['description']) > 100 else info['description']
            })
        
        return summary
    
    def export_configuration(self) -> Dict[str, Any]:
        """Export complete configuration for backup/sharing"""
        return {
            'metadata': {
                'version': '1.0.0',
                'exported_at': datetime.now().isoformat(),
                'total_codes': len(self.all_codes),
                'user_customizations': len([c for c in self.risk_scores.values() if c.get('user_customized')])
            },
            'all_codes': self.all_codes,
            'risk_scores': self.risk_scores,
            'code_definitions': self.code_definitions
        }
    
    def import_configuration(self, config: Dict[str, Any]):
        """Import configuration from backup/sharing"""
        try:
            if 'all_codes' in config:
                self.all_codes.update(config['all_codes'])
            if 'risk_scores' in config:
                self.risk_scores.update(config['risk_scores'])
            if 'code_definitions' in config:
                self.code_definitions.update(config['code_definitions'])
            
            logger.info("✅ Configuration imported successfully")
            
        except Exception as e:
            logger.error(f"❌ Error importing configuration: {e}")

# Global instance
comprehensive_event_codes = ComprehensiveEventCodes()

def get_event_description(event_code: str, sub_code: str = None) -> str:
    """Get event description for display"""
    info = comprehensive_event_codes.get_code_info(event_code)
    
    if info['name'] == f'Unknown Code {event_code}':
        return f"Unknown ({event_code})"
    
    if sub_code:
        return f"{info['name']} - {sub_code}"
    
    return info['name']

def get_event_risk_score(event_code: str) -> int:
    """Get risk score for an event code"""
    info = comprehensive_event_codes.get_code_info(event_code)
    return info['risk_score']

def get_event_severity(event_code: str) -> str:
    """Get severity level for an event code"""
    info = comprehensive_event_codes.get_code_info(event_code)
    return info['severity']