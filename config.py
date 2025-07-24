"""
Configuration Management for GRID Entity Search
Centralizes all hardcoded values for production readiness
"""
import json
import os
from typing import Dict, Any, Optional
from pathlib import Path

class ConfigManager:
    """Centralized configuration management"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or 'config.json'
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or use defaults"""
        config_path = Path(self.config_file)
        
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load config file {self.config_file}: {e}")
        
        return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Default configuration values"""
        return {
            "risk_thresholds": {
                "critical": 80,
                "valuable": 60,
                "investigative": 40,
                "probative": 0
            },
            
            "system_settings": {
                "cache_ttl": 300,
                "batch_size": 1000,
                "max_parallel_queries": 4,
                "query_timeout": 30,
                "default_limit": 100
            },
            
            "risk_scores": {
                "default_event_score": 20,
                "critical_events": {
                    "TER": 95, "SAN": 90, "MLA": 85, "DTF": 85, "HUM": 85,
                    "BRB": 80, "COR": 75, "FRD": 70, "KID": 80, "WLT": 90
                },
                "valuable_events": {
                    "REG": 65, "BUS": 60, "TAX": 65, "SEC": 65, "FOR": 60,
                    "EMB": 65, "MOR": 60, "IND": 70, "CVT": 75
                },
                "investigative_events": {
                    "ENV": 55, "WCC": 50, "REG": 45, "ABU": 50, "AST": 50,
                    "MUR": 55, "ARS": 45, "BUR": 45, "CFT": 40, "CON": 40
                },
                "probative_events": {
                    "MIS": 25, "NSC": 20, "GAM": 20, "DPS": 25, "CPR": 20,
                    "OBS": 15, "BKY": 15, "CYB": 35
                }
            },
            
            "geographic_risk": {
                "priority_country_score": 20,
                "default_score": 10,
                "high_risk": {
                    "RU": 1.5, "BY": 1.5, "VE": 1.5, "NI": 1.5, "CU": 1.5,
                    "IR": 1.4, "SY": 1.4, "AF": 1.4
                },
                "medium_risk": {
                    "CN": 1.3, "TR": 1.2, "EG": 1.2, "IN": 1.2, "BR": 1.2,
                    "MX": 1.2, "ZA": 1.2, "PK": 1.2
                },
                "low_risk": {
                    "US": 0.95, "CA": 0.95, "GB": 0.95, "DE": 0.95,
                    "FR": 0.95, "AU": 0.95, "JP": 0.95, "CH": 0.9,
                    "SE": 0.9, "NO": 0.9, "DK": 0.9
                },
                "default_multiplier": 1.0
            },
            
            "ui_theme": {
                "colors": {
                    "primary": "#1976d2",
                    "secondary": "#424242", 
                    "accent": "#82b1ff",
                    "success": "#4caf50",
                    "warning": "#ff9800",
                    "error": "#f44336",
                    "info": "#2196f3"
                },
                "risk_colors": {
                    "critical": "#d32f2f",
                    "valuable": "#f57c00",
                    "investigative": "#fbc02d",
                    "probative": "#1976d2"
                }
            },
            
            "database": {
                "schema_prefix": "prd_bronze_catalog.grid",
                "timeout": 30,
                "retry_attempts": 3,
                "connection_pool_size": 10
            },
            
            "export_settings": {
                "default_directory": "./exports",
                "max_file_size": "50MB",
                "supported_formats": ["csv", "excel", "json", "pdf"],
                "include_metadata": True
            },
            
            "pep_settings": {
                "base_score": 50,
                "level_multipliers": {
                    "L1": 1.1, "L2": 1.2, "L3": 1.3, "L4": 1.4, "L5": 1.5, "L6": 1.6
                },
                "high_level_positions": ["HOS", "CAB", "INF"],
                "family_codes": ["FAM", "SPO", "CHI", "PAR", "SIB"],
                "associate_codes": ["ASC", "CAS", "BUS", "POL"]
            },
            
            "prt_ratings": {
                "A": 90, "B": 75, "C": 60, "D": 45
            },
            
            "geographic_settings": {
                "priority_countries": [
                    "UNITED STATES", "UNITED KINGDOM", "INDIA", "BRAZIL",
                    "MEXICO", "ITALY", "AUSTRALIA", "CANADA", "PAKISTAN",
                    "NIGERIA", "RUSSIAN FEDERATION", "BANGLADESH", "GERMANY", "CHINA"
                ]
            },
            
            "api_settings": {
                "copilot_base_url": "https://copilot.moodys.com/api/v1",
                "request_timeout": 30,
                "max_retries": 3
            },
            
            "server": {
                "host": "localhost",
                "port": 8080,
                "debug": False,
                "reload": False
            }
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation"""
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value using dot notation"""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save_config(self) -> None:
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def get_risk_threshold(self, category: str) -> int:
        """Get risk threshold for category"""
        return self.get(f'risk_thresholds.{category}', 0)
    
    def get_risk_score(self, event_category: str) -> int:
        """Get risk score for event category"""
        # Check each risk level category
        for level in ['critical_events', 'valuable_events', 'investigative_events', 'probative_events']:
            score = self.get(f'risk_scores.{level}.{event_category}')
            if score is not None:
                return score
        return 10  # Default low risk
    
    def get_geographic_multiplier(self, country_code: str) -> float:
        """Get geographic risk multiplier for country"""
        # Check each risk level
        for level in ['high_risk', 'medium_risk', 'low_risk']:
            multiplier = self.get(f'geographic_risk.{level}.{country_code}')
            if multiplier is not None:
                return multiplier
        return self.get('geographic_risk.default_multiplier', 1.0)
    
    def get_pep_multiplier(self, pep_level: str) -> float:
        """Get PEP level multiplier"""
        return self.get(f'pep_settings.level_multipliers.{pep_level}', 1.0)
    
    def get_ui_color(self, color_type: str) -> str:
        """Get UI color value"""
        return self.get(f'ui_theme.colors.{color_type}', '#000000')
    
    def get_risk_color(self, risk_level: str) -> str:
        """Get risk level color"""
        return self.get(f'ui_theme.risk_colors.{risk_level}', '#666666')

# Global configuration instance
config = ConfigManager()

# Environment variable overrides
def apply_env_overrides():
    """Apply environment variable overrides"""
    env_mappings = {
        'DB_TIMEOUT': 'database.timeout',
        'CACHE_TTL': 'system_settings.cache_ttl',
        'SERVER_PORT': 'server.port',
        'SERVER_HOST': 'server.host',
        'DEBUG': 'server.debug',
        'COPILOT_URL': 'api_settings.copilot_base_url',
        'EXPORT_DIR': 'export_settings.default_directory'
    }
    
    for env_var, config_key in env_mappings.items():
        value = os.getenv(env_var)
        if value is not None:
            # Convert to appropriate type
            if config_key.endswith(('.port', '.timeout', '.cache_ttl')):
                value = int(value)
            elif config_key.endswith('.debug'):
                value = value.lower() in ('true', '1', 'yes', 'on')
            
            config.set(config_key, value)

# Apply environment overrides on import
apply_env_overrides()