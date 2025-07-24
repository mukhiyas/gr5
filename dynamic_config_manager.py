"""
Dynamic Configuration Manager for GRID Entity Search
Provides user-configurable settings with database persistence
No hardcoded values - everything is configurable through UI
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from databricks import sql
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

logger = logging.getLogger(__name__)

class DynamicConfigManager:
    """Dynamic configuration manager with database and file persistence"""
    
    def __init__(self, connection=None):
        self.connection = connection
        self.config_file = Path("user_config.json")
        self.config = self._load_configuration()
        
    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration from file or database, with fallback to minimal defaults"""
        config = {}
        
        # Try to load from file first
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                logger.info(f"✅ Loaded configuration from {self.config_file}")
            except Exception as e:
                logger.warning(f"Failed to load config file: {e}")
        
        # Try to load from database if available
        try:
            db_config = self._load_from_database()
            if db_config:
                config.update(db_config)
                logger.info("✅ Loaded configuration from database")
        except Exception as e:
            logger.warning(f"Database configuration load failed: {e}")
        
        # If no config loaded, create minimal structure
        if not config:
            config = self._create_minimal_config()
            logger.info("✅ Created minimal default configuration")
        
        return config
    
    def _create_minimal_config(self) -> Dict[str, Any]:
        """Create minimal configuration structure - no hardcoded risk scores"""
        return {
            "risk_scores": {
                "event_codes": {},  # Empty - user will configure
                "default_score": 50
            },
            "pep_settings": {
                "base_score": 50,
                "level_multipliers": {},  # Empty - user will configure
                "custom_levels": {}
            },
            "prt_ratings": {},  # Empty - user will configure
            "geographic_risk": {
                "country_multipliers": {},  # Empty - user will configure
                "default_multiplier": 1.0
            },
            "risk_thresholds": {
                "critical_min": 80,
                "valuable_min": 60,
                "investigative_min": 40,
                "probative_min": 0
            },
            "system_settings": {
                "cache_ttl": 300,
                "batch_size": 1000,
                "max_parallel_queries": 4,
                "query_timeout": 30,
                "default_limit": 5  # Updated default
            },
            "ui_preferences": {
                "theme": "light",
                "default_view": "grid"
            },
            "metadata": {
                "created": datetime.now().isoformat(),
                "last_modified": datetime.now().isoformat(),
                "version": "1.0"
            }
        }
    
    def _load_from_database(self) -> Optional[Dict[str, Any]]:
        """Load configuration from database if available"""
        if not self.connection:
            return None
            
        try:
            cursor = self.connection.cursor()
            # Try to load from a configuration table (if it exists)
            cursor.execute("""
                SELECT config_key, config_value, last_modified
                FROM prd_bronze_catalog.grid.user_configurations
                WHERE is_active = true
                ORDER BY last_modified DESC
            """)
            
            config = {}
            for row in cursor.fetchall():
                try:
                    config[row['config_key']] = json.loads(row['config_value'])
                except:
                    config[row['config_key']] = row['config_value']
            
            return config if config else None
            
        except Exception as e:
            # Table might not exist - that's ok, we'll use file config
            logger.debug(f"Database config load failed (table may not exist): {e}")
            return None
    
    def save_configuration(self) -> bool:
        """Save configuration to file and database"""
        self.config["metadata"]["last_modified"] = datetime.now().isoformat()
        
        # Save to file
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2, default=str)
            logger.info(f"✅ Saved configuration to {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to save config file: {e}")
            return False
        
        # Save to database if available
        try:
            self._save_to_database()
        except Exception as e:
            logger.warning(f"Database save failed: {e}")
        
        return True
    
    def _save_to_database(self):
        """Save configuration to database"""
        if not self.connection:
            return
            
        cursor = self.connection.cursor()
        timestamp = datetime.now().isoformat()
        
        for key, value in self.config.items():
            try:
                cursor.execute("""
                    MERGE INTO prd_bronze_catalog.grid.user_configurations AS target
                    USING (SELECT ? as config_key, ? as config_value, ? as last_modified) AS source
                    ON target.config_key = source.config_key
                    WHEN MATCHED THEN UPDATE SET 
                        config_value = source.config_value,
                        last_modified = source.last_modified,
                        is_active = true
                    WHEN NOT MATCHED THEN INSERT 
                        (config_key, config_value, last_modified, is_active)
                        VALUES (source.config_key, source.config_value, source.last_modified, true)
                """, (key, json.dumps(value, default=str), timestamp))
            except:
                # Table might not exist - continue anyway
                pass
    
    def get(self, key: str, default=None):
        """Get configuration value using dot notation"""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value using dot notation"""
        keys = key.split('.')
        config = self.config
        
        # Navigate to the parent dictionary
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        # Set the value
        config[keys[-1]] = value
    
    def update_risk_score(self, event_code: str, score: int):
        """Update individual risk score"""
        if "risk_scores" not in self.config:
            self.config["risk_scores"] = {"event_codes": {}}
        if "event_codes" not in self.config["risk_scores"]:
            self.config["risk_scores"]["event_codes"] = {}
            
        self.config["risk_scores"]["event_codes"][event_code] = score
        logger.info(f"Updated risk score for {event_code}: {score}")
    
    def update_pep_setting(self, level: str, multiplier: float):
        """Update PEP level multiplier"""
        if "pep_settings" not in self.config:
            self.config["pep_settings"] = {"level_multipliers": {}}
        if "level_multipliers" not in self.config["pep_settings"]:
            self.config["pep_settings"]["level_multipliers"] = {}
            
        self.config["pep_settings"]["level_multipliers"][level] = multiplier
        logger.info(f"Updated PEP level {level}: {multiplier}")
    
    def update_prt_rating(self, rating: str, score: int):
        """Update PRT rating score"""
        if "prt_ratings" not in self.config:
            self.config["prt_ratings"] = {}
            
        self.config["prt_ratings"][rating] = score
        logger.info(f"Updated PRT rating {rating}: {score}")
    
    def update_geographic_risk(self, country: str, multiplier: float):
        """Update geographic risk multiplier"""
        if "geographic_risk" not in self.config:
            self.config["geographic_risk"] = {"country_multipliers": {}}
        if "country_multipliers" not in self.config["geographic_risk"]:
            self.config["geographic_risk"]["country_multipliers"] = {}
            
        self.config["geographic_risk"]["country_multipliers"][country] = multiplier
        logger.info(f"Updated geographic risk for {country}: {multiplier}")
    
    def get_all_risk_scores(self) -> Dict[str, int]:
        """Get all configured risk scores"""
        return self.config.get("risk_scores", {}).get("event_codes", {})
    
    def get_all_pep_multipliers(self) -> Dict[str, float]:
        """Get all PEP level multipliers"""
        return self.config.get("pep_settings", {}).get("level_multipliers", {})
    
    def get_all_prt_ratings(self) -> Dict[str, int]:
        """Get all PRT ratings"""
        return self.config.get("prt_ratings", {})
    
    def get_all_geographic_multipliers(self) -> Dict[str, float]:
        """Get all geographic risk multipliers"""
        return self.config.get("geographic_risk", {}).get("country_multipliers", {})
    
    def get_all_subcategory_multipliers(self) -> Dict[str, float]:
        """Get all event sub-category multipliers"""
        return self.config.get("sub_category_multipliers", {})
    
    def export_configuration(self) -> str:
        """Export configuration as JSON string"""
        return json.dumps(self.config, indent=2, default=str)
    
    def import_configuration(self, config_json: str) -> bool:
        """Import configuration from JSON string"""
        try:
            imported_config = json.loads(config_json)
            # Validate structure
            if not isinstance(imported_config, dict):
                raise ValueError("Invalid configuration format")
            
            self.config = imported_config
            self.config["metadata"]["last_modified"] = datetime.now().isoformat()
            
            return self.save_configuration()
        except Exception as e:
            logger.error(f"Configuration import failed: {e}")
            return False
    
    def reset_to_minimal(self):
        """Reset configuration to minimal defaults"""
        self.config = self._create_minimal_config()
        self.save_configuration()
        logger.info("Configuration reset to minimal defaults")
    
    def add_event_code(self, code: str, description: str, score: int):
        """Add new event code with description and score"""
        if "risk_scores" not in self.config:
            self.config["risk_scores"] = {"event_codes": {}, "event_descriptions": {}}
        if "event_codes" not in self.config["risk_scores"]:
            self.config["risk_scores"]["event_codes"] = {}
        if "event_descriptions" not in self.config["risk_scores"]:
            self.config["risk_scores"]["event_descriptions"] = {}
            
        self.config["risk_scores"]["event_codes"][code] = score
        self.config["risk_scores"]["event_descriptions"][code] = description
        logger.info(f"Added new event code {code}: {description} (score: {score})")
    
    def remove_event_code(self, code: str):
        """Remove event code"""
        if "risk_scores" in self.config:
            self.config["risk_scores"].get("event_codes", {}).pop(code, None)
            self.config["risk_scores"].get("event_descriptions", {}).pop(code, None)
        logger.info(f"Removed event code {code}")

# Global instance (will be initialized when needed)
dynamic_config = None

def get_dynamic_config(connection=None) -> DynamicConfigManager:
    """Get the global dynamic configuration instance"""
    global dynamic_config
    if dynamic_config is None:
        dynamic_config = DynamicConfigManager(connection)
    return dynamic_config