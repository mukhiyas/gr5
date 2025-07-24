"""
Database-Verified Configuration for GRID Entity Search
Uses ONLY actual codes from database queries - NO mix and match
"""
import json
import os
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

class DatabaseVerifiedConfigManager:
    """Configuration manager using ONLY database-verified codes"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or 'database_verified_config.json'
        self.config = self._load_config()
        self.verification_info = self._get_verification_info()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or use database-verified defaults"""
        config_path = Path(self.config_file)
        
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                return self._merge_with_defaults(config)
            except Exception as e:
                print(f"Warning: Could not load config file {self.config_file}: {e}")
        
        return self._get_database_verified_defaults()
    
    def _merge_with_defaults(self, user_config: Dict) -> Dict:
        """Merge user config with database-verified defaults"""
        defaults = self._get_database_verified_defaults()
        return self._deep_merge(defaults, user_config)
    
    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries"""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def _get_database_verified_defaults(self) -> Dict[str, Any]:
        """Get defaults using ONLY database-verified codes"""
        return {
            "version": "3.0.0-database-verified",
            "last_updated": datetime.now().isoformat(),
            "source": "Actual database query results from codes.txt",
            
            # ACTUAL Event Categories from database query (63 codes)
            "event_categories": {
                # Critical Severity (80-100)
                "TER": {"name": "Terrorism", "risk_score": 100, "severity": "critical"},
                "WLT": {"name": "Watch List", "risk_score": 100, "severity": "critical"},
                "DEN": {"name": "Denied Entity", "risk_score": 95, "severity": "critical"},
                "DTF": {"name": "Drug Trafficking", "risk_score": 90, "severity": "critical"},
                "TRF": {"name": "Human Trafficking", "risk_score": 90, "severity": "critical"},
                "MLA": {"name": "Money Laundering", "risk_score": 85, "severity": "critical"},
                "HUM": {"name": "Human Rights", "risk_score": 85, "severity": "critical"},
                "ORG": {"name": "Organized Crime", "risk_score": 85, "severity": "critical"},
                "KID": {"name": "Kidnapping", "risk_score": 85, "severity": "critical"},
                "SPY": {"name": "Espionage", "risk_score": 85, "severity": "critical"},
                
                # Valuable Severity (60-79)
                "BRB": {"name": "Bribery", "risk_score": 75, "severity": "valuable"},
                "FRD": {"name": "Fraud", "risk_score": 70, "severity": "valuable"},
                "TAX": {"name": "Tax Crimes", "risk_score": 70, "severity": "valuable"},
                "SEC": {"name": "Securities Violations", "risk_score": 70, "severity": "valuable"},
                "REG": {"name": "Regulatory Action", "risk_score": 65, "severity": "valuable"},
                "ROB": {"name": "Robbery", "risk_score": 60, "severity": "valuable"},
                "SEX": {"name": "Sex Offenses", "risk_score": 60, "severity": "valuable"},
                "PEP": {"name": "Politically Exposed Person", "risk_score": 60, "severity": "valuable"},
                "SNX": {"name": "Sanctions Connect", "risk_score": 60, "severity": "valuable"},
                
                # Investigative Severity (40-59)
                "MUR": {"name": "Murder", "risk_score": 55, "severity": "investigative"},
                "AST": {"name": "Assault", "risk_score": 55, "severity": "investigative"},
                "FUG": {"name": "Fugitive", "risk_score": 50, "severity": "investigative"},
                "BUR": {"name": "Burglary", "risk_score": 50, "severity": "investigative"},
                "TFT": {"name": "Theft", "risk_score": 50, "severity": "investigative"},
                "IGN": {"name": "Illegal Weapons", "risk_score": 50, "severity": "investigative"},
                "CON": {"name": "Conspiracy", "risk_score": 45, "severity": "investigative"},
                "CFT": {"name": "Counterfeiting", "risk_score": 45, "severity": "investigative"},
                "SMG": {"name": "Smuggling", "risk_score": 45, "severity": "investigative"},
                "PSP": {"name": "Possession Stolen Property", "risk_score": 40, "severity": "investigative"},
                "IMP": {"name": "Identity Theft", "risk_score": 40, "severity": "investigative"},
                "CYB": {"name": "Cybercrime", "risk_score": 40, "severity": "investigative"},
                "OBS": {"name": "Obscenity", "risk_score": 40, "severity": "investigative"},
                
                # Probative Severity (0-39)
                "DPS": {"name": "Drug Possession", "risk_score": 35, "severity": "probative"},
                "NSC": {"name": "Non-Specific Crime", "risk_score": 30, "severity": "probative"},
                "MIS": {"name": "Misconduct", "risk_score": 30, "severity": "probative"},
                "ABU": {"name": "Abuse", "risk_score": 30, "severity": "probative"},
                "PRJ": {"name": "Perjury", "risk_score": 30, "severity": "probative"},
                "ENV": {"name": "Environmental", "risk_score": 25, "severity": "probative"},
                "GAM": {"name": "Gambling", "risk_score": 25, "severity": "probative"},
                "ARS": {"name": "Arson", "risk_score": 25, "severity": "probative"},
                "BUS": {"name": "Business Crimes", "risk_score": 25, "severity": "probative"},
                "IPR": {"name": "Illegal Prostitution", "risk_score": 20, "severity": "probative"},
                "LNS": {"name": "Loan Sharking", "risk_score": 20, "severity": "probative"},
                "CPR": {"name": "Copyright", "risk_score": 20, "severity": "probative"},
                "BKY": {"name": "Bankruptcy", "risk_score": 20, "severity": "probative"},
                "RES": {"name": "Real Estate", "risk_score": 20, "severity": "probative"},
                "MOR": {"name": "Mortgage", "risk_score": 20, "severity": "probative"},
                "IRC": {"name": "Iran Connect", "risk_score": 20, "severity": "probative"},
                "FAR": {"name": "Foreign Agent", "risk_score": 15, "severity": "probative"},
                "LMD": {"name": "Legal Marijuana", "risk_score": 15, "severity": "probative"},
                "DPP": {"name": "Data Privacy", "risk_score": 15, "severity": "probative"},
                "FOF": {"name": "Former OFAC", "risk_score": 10, "severity": "probative"},
                "FOS": {"name": "Former Sanctions", "risk_score": 10, "severity": "probative"},
                "FOR": {"name": "Forfeiture", "risk_score": 10, "severity": "probative"},
                "MSB": {"name": "Money Services Business", "risk_score": 10, "severity": "probative"},
                "HTE": {"name": "Hate Crimes", "risk_score": 10, "severity": "probative"},
                "BIL": {"name": "Billing", "risk_score": 5, "severity": "probative"},
                "CND": {"name": "Financial Condition", "risk_score": 5, "severity": "probative"},
                "DEF": {"name": "Default", "risk_score": 5, "severity": "probative"},
                "HCD": {"name": "Healthcare", "risk_score": 5, "severity": "probative"},
                "PER": {"name": "Performance", "risk_score": 5, "severity": "probative"},
                "REO": {"name": "Restructuring", "risk_score": 5, "severity": "probative"},
                "VCY": {"name": "Virtual Currency", "risk_score": 5, "severity": "probative"}
            },
            
            # ACTUAL Event Sub-Categories from database query (36 codes)
            "event_sub_categories": {
                "ACC": {"name": "Accused", "multiplier": 0.7},
                "ACQ": {"name": "Acquitted", "multiplier": 0.5},
                "ACT": {"name": "Action", "multiplier": 1.0},
                "ADT": {"name": "Audit", "multiplier": 0.7},
                "ALL": {"name": "Alleged", "multiplier": 0.6},
                "APL": {"name": "Appeal", "multiplier": 0.8},
                "ARB": {"name": "Arbitration", "multiplier": 0.7},
                "ARN": {"name": "Arraigned", "multiplier": 1.0},
                "ART": {"name": "Arrested", "multiplier": 1.1},
                "ASC": {"name": "Associated", "multiplier": 0.5},
                "CEN": {"name": "Censured", "multiplier": 0.9},
                "CHG": {"name": "Charged", "multiplier": 1.0},
                "CMP": {"name": "Complaint", "multiplier": 0.8},
                "CNF": {"name": "Confession", "multiplier": 1.2},
                "CSP": {"name": "Conspiracy", "multiplier": 1.0},
                "CVT": {"name": "Convicted", "multiplier": 1.3},
                "DEP": {"name": "Deported", "multiplier": 1.0},
                "DMS": {"name": "Dismissed", "multiplier": 0.4},
                "EXP": {"name": "Expelled", "multiplier": 0.9},
                "FIL": {"name": "Fine <$10K", "multiplier": 0.7},
                "FIM": {"name": "Fine >$10K", "multiplier": 1.0},
                "GOV": {"name": "Government", "multiplier": 1.2},
                "IND": {"name": "Indicted", "multiplier": 1.1},
                "LIC": {"name": "License Action", "multiplier": 0.8},
                "LIN": {"name": "Lien", "multiplier": 0.6},
                "PLE": {"name": "Plea", "multiplier": 1.0},
                "PRB": {"name": "Probe", "multiplier": 0.7},
                "RVK": {"name": "Revoked", "multiplier": 1.0},
                "SAN": {"name": "Sanctioned", "multiplier": 1.2},
                "SET": {"name": "Settlement", "multiplier": 0.8},
                "SEZ": {"name": "Seizure", "multiplier": 1.0},
                "SJT": {"name": "Jail Time", "multiplier": 1.2},
                "SPD": {"name": "Suspended", "multiplier": 0.9},
                "SPT": {"name": "Suspected", "multiplier": 0.6},
                "TRL": {"name": "Trial", "multiplier": 1.0},
                "WTD": {"name": "Wanted", "multiplier": 1.1}
            },
            
            # ACTUAL PEP Types from PEP.txt (17 codes)
            "pep_types": {
                "HOS": {"name": "Head of State", "risk_multiplier": 2.0, "level": "L6"},
                "CAB": {"name": "Cabinet Officials", "risk_multiplier": 1.8, "level": "L5"},
                "INF": {"name": "Infrastructure Officials", "risk_multiplier": 1.6, "level": "L4"},
                "NIO": {"name": "Non-Infrastructure Officials", "risk_multiplier": 1.5, "level": "L4"},
                "MUN": {"name": "Municipal Officials", "risk_multiplier": 1.3, "level": "L3"},
                "REG": {"name": "Regional Officials", "risk_multiplier": 1.4, "level": "L3"},
                "LEG": {"name": "Legislative Branch", "risk_multiplier": 1.5, "level": "L4"},
                "AMB": {"name": "Ambassadors", "risk_multiplier": 1.6, "level": "L4"},
                "MIL": {"name": "Military Figures", "risk_multiplier": 1.7, "level": "L5"},
                "JUD": {"name": "Judicial Figures", "risk_multiplier": 1.6, "level": "L4"},
                "POL": {"name": "Political Figures", "risk_multiplier": 1.4, "level": "L3"},
                "ISO": {"name": "Sporting Officials", "risk_multiplier": 1.2, "level": "L2"},
                "GOE": {"name": "Government Enterprises", "risk_multiplier": 1.5, "level": "L4"},
                "GCO": {"name": "State-Controlled Business", "risk_multiplier": 1.4, "level": "L3"},
                "IGO": {"name": "International Organizations", "risk_multiplier": 1.3, "level": "L3"},
                "FAM": {"name": "Family Members", "risk_multiplier": 1.2, "level": "L2"},
                "ASC": {"name": "Close Associates", "risk_multiplier": 1.1, "level": "L1"}
            },
            
            # ACTUAL Entity Attributes from database query (23 codes)
            "entity_attributes": {
                "BLD": {"name": "Build", "data_type": "string"},
                "CPX": {"name": "Complexion", "data_type": "string"},
                "DDT": {"name": "Deceased Date", "data_type": "date"},
                "DED": {"name": "Deceased", "data_type": "boolean"},
                "ECL": {"name": "Eye Color", "data_type": "string"},
                "HCL": {"name": "Hair Color", "data_type": "string"},
                "HGT": {"name": "Height", "data_type": "measurement"},
                "IMG": {"name": "Image URL", "data_type": "url"},
                "LNG": {"name": "Language", "data_type": "string"},
                "NAT": {"name": "Nationality", "data_type": "string"},
                "NIN": {"name": "NI Number", "data_type": "string"},
                "OCU": {"name": "Occupation", "data_type": "string"},
                "PHD": {"name": "Physical Description", "data_type": "text"},
                "PRT": {"name": "PEP Rating", "data_type": "string"},
                "PTY": {"name": "PEP Type", "data_type": "string"},
                "RAC": {"name": "Race", "data_type": "string"},
                "RGP": {"name": "Riskography", "data_type": "string"},
                "RID": {"name": "Risk ID", "data_type": "string"},
                "RMK": {"name": "Remarks", "data_type": "text"},
                "SEX": {"name": "Sex", "data_type": "string"},
                "SMK": {"name": "Scars/Marks", "data_type": "text"},
                "URL": {"name": "Entity URL", "data_type": "url"},
                "WGT": {"name": "Weight", "data_type": "measurement"}
            },
            
            # Geographic Risk (keeping existing as these are business logic)
            "geographic_risk": {
                "critical_risk": {
                    "AF": {"name": "Afghanistan", "multiplier": 2.5, "reason": "High security risk"},
                    "SY": {"name": "Syria", "multiplier": 2.5, "reason": "Conflict zone"},
                    "KP": {"name": "North Korea", "multiplier": 2.5, "reason": "International sanctions"},
                    "IR": {"name": "Iran", "multiplier": 2.3, "reason": "International sanctions"}
                },
                "high_risk": {
                    "RU": {"name": "Russia", "multiplier": 1.8, "reason": "Geopolitical tensions"},
                    "VE": {"name": "Venezuela", "multiplier": 1.7, "reason": "Economic crisis"},
                    "CN": {"name": "China", "multiplier": 1.4, "reason": "Regulatory complexity"}
                },
                "medium_risk": {
                    "TR": {"name": "Turkey", "multiplier": 1.2, "reason": "Economic volatility"},
                    "BR": {"name": "Brazil", "multiplier": 1.2, "reason": "Corruption concerns"},
                    "IN": {"name": "India", "multiplier": 1.2, "reason": "Regulatory complexity"}
                },
                "low_risk": {
                    "US": {"name": "United States", "multiplier": 0.95, "reason": "Strong regulatory framework"},
                    "GB": {"name": "United Kingdom", "multiplier": 0.95, "reason": "Robust regulation"},
                    "CH": {"name": "Switzerland", "multiplier": 0.9, "reason": "Financial center"}
                },
                "default_multiplier": 1.0
            },
            
            # Risk Thresholds
            "risk_thresholds": {
                "critical": {"min": 80, "max": 100, "color": "#d32f2f", "description": "Highest risk entities"},
                "valuable": {"min": 60, "max": 79, "color": "#f57c00", "description": "High risk entities"},
                "investigative": {"min": 40, "max": 59, "color": "#fbc02d", "description": "Medium risk entities"},
                "probative": {"min": 0, "max": 39, "color": "#1976d2", "description": "Lower risk entities"}
            },
            
            # System Settings
            "system_settings": {
                "cache_ttl": 300,
                "batch_size": 1000,
                "max_parallel_queries": 4,
                "query_timeout": 30,
                "default_limit": 100
            },
            
            # Server Configuration
            "server": {
                "host": "localhost",
                "port": 8080,
                "debug": False,
                "reload": False
            }
        }
    
    def _get_verification_info(self) -> Dict[str, Any]:
        """Get database verification information"""
        return {
            "event_categories": {
                "source": "Database query from codes.txt lines 10-73",
                "count": 63,
                "verified": True
            },
            "event_sub_categories": {
                "source": "Database query from codes.txt lines 85-121", 
                "count": 36,
                "verified": True
            },
            "pep_types": {
                "source": "PEP.txt lines 7-26",
                "count": 17,
                "verified": True,
                "note": "PTY attribute contains actual names/relationships, not type codes"
            },
            "entity_attributes": {
                "source": "Database query from codes.txt lines 144-167",
                "count": 23,
                "verified": True
            },
            "verification_date": datetime.now().isoformat()
        }
    
    # Keep all the existing methods from the original class
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
        self.config["last_updated"] = datetime.now().isoformat()
    
    def save_config(self) -> None:
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def reset_to_defaults(self) -> None:
        """Reset configuration to database-verified defaults"""
        try:
            self.config = self._get_database_verified_defaults()
            self.save_config()
        except Exception as e:
            print(f"Error resetting to defaults: {e}")
            raise
    
    def get_event_category(self, code: str) -> Dict[str, Any]:
        """Get event category information"""
        return self.get(f'event_categories.{code}', {
            "name": code,
            "description": f"Unknown event category: {code}",
            "risk_score": 50,
            "severity": "investigative"
        })
    
    def get_event_sub_category(self, code: str) -> Dict[str, Any]:
        """Get event sub-category information"""
        return self.get(f'event_sub_categories.{code}', {
            "name": code,
            "description": f"Unknown sub-category: {code}",
            "multiplier": 1.0
        })
    
    def get_pep_type(self, code: str) -> Dict[str, Any]:
        """Get PEP type information"""
        return self.get(f'pep_types.{code}', {
            "name": code,
            "description": f"Unknown PEP type: {code}",
            "risk_multiplier": 1.0,
            "level": "L1"
        })
    
    def get_geographic_multiplier(self, country_code: str) -> float:
        """Get geographic risk multiplier"""
        for level in ['critical_risk', 'high_risk', 'medium_risk', 'low_risk']:
            multiplier_info = self.get(f'geographic_risk.{level}.{country_code}')
            if multiplier_info:
                return multiplier_info.get('multiplier', 1.0)
        return self.get('geographic_risk.default_multiplier', 1.0)

# Global database-verified configuration instance
database_verified_config = DatabaseVerifiedConfigManager()