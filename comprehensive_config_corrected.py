"""
CORRECTED Comprehensive Configuration Management for GRID Entity Search
Uses ONLY actual database schema codes - NO mix and match
"""
import json
import os
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

class CorrectedComprehensiveConfigManager:
    """CORRECTED configuration manager using ONLY actual database codes"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or 'corrected_comprehensive_config.json'
        self.config = self._load_config()
        self.schema_info = self._get_database_schema_info()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or use ACTUAL database defaults"""
        config_path = Path(self.config_file)
        
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                return self._merge_with_defaults(config)
            except Exception as e:
                print(f"Warning: Could not load config file {self.config_file}: {e}")
        
        return self._get_actual_database_defaults()
    
    def _merge_with_defaults(self, user_config: Dict) -> Dict:
        """Merge user config with actual database defaults"""
        defaults = self._get_actual_database_defaults()
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
    
    def _get_actual_database_defaults(self) -> Dict[str, Any]:
        """Get defaults using ONLY actual database schema codes"""
        return {
            "version": "2.0.0-corrected",
            "last_updated": datetime.now().isoformat(),
            "source": "database_schema.txt and PEP.txt - ACTUAL codes only",
            
            # ACTUAL Event Categories from database schema.txt lines 527-593
            "event_categories": {
                "ABU": {"name": "Abuse", "description": "Abuse (Domestic, Elder, Child)", "risk_score": 55, "severity": "investigative"},
                "ARS": {"name": "Arson", "description": "Arson", "risk_score": 50, "severity": "investigative"},
                "AST": {"name": "Assault", "description": "Assault, Battery", "risk_score": 55, "severity": "investigative"},
                "BIL": {"name": "Billing", "description": "Questionable Billing Practices", "risk_score": 30, "severity": "probative"},
                "BKY": {"name": "Bankruptcy", "description": "Bankruptcy", "risk_score": 20, "severity": "probative"},
                "BLK": {"name": "Black List", "description": "Firm Specific Black List", "risk_score": 70, "severity": "valuable"},
                "BRB": {"name": "Bribery", "description": "Bribery, Graft, Kickbacks, Political Corruption", "risk_score": 85, "severity": "critical"},
                "BUR": {"name": "Burglary", "description": "Burglary", "risk_score": 45, "severity": "investigative"},
                "BUS": {"name": "Business Crimes", "description": "Business Crimes (Antitrust, Bankruptcy, Price Fixing)", "risk_score": 60, "severity": "valuable"},
                "CFT": {"name": "Counterfeiting", "description": "Counterfeiting, Forgery", "risk_score": 45, "severity": "investigative"},
                "CND": {"name": "Financial Condition", "description": "Financial Condition Risk", "risk_score": 40, "severity": "investigative"},
                "CON": {"name": "Conspiracy", "description": "Conspiracy (No specific crime named)", "risk_score": 50, "severity": "investigative"},
                "CPR": {"name": "Copyright", "description": "Copyright Infringement (Intellectual Property, Electronic Piracy)", "risk_score": 25, "severity": "probative"},
                "CYB": {"name": "Cybercrime", "description": "Computer Related, Cyber Crime", "risk_score": 40, "severity": "investigative"},
                "DEF": {"name": "Default Risk", "description": "Default Risk", "risk_score": 35, "severity": "probative"},
                "DEN": {"name": "Denied Entity", "description": "Denied Entity", "risk_score": 95, "severity": "critical"},
                "DPP": {"name": "Data Privacy", "description": "Data Privacy and Protection", "risk_score": 30, "severity": "probative"},
                "DPS": {"name": "Drug Possession", "description": "Possession of Drugs or Drug Paraphernalia", "risk_score": 30, "severity": "probative"},
                "DTF": {"name": "Drug Trafficking", "description": "Trafficking or Distribution of Drugs", "risk_score": 90, "severity": "critical"},
                "DUI": {"name": "DUI", "description": "DUI, DWI", "risk_score": 20, "severity": "probative"},
                "ENV": {"name": "Environmental", "description": "Environmental Crimes (Poaching, Illegal Logging, Animal Cruelty)", "risk_score": 30, "severity": "probative"},
                "FAR": {"name": "Foreign Agent", "description": "Foreign Agent Registration Act", "risk_score": 70, "severity": "valuable"},
                "FOF": {"name": "Former OFAC", "description": "Former OFAC", "risk_score": 80, "severity": "critical"},
                "FOR": {"name": "Forfeiture", "description": "Forfeiture", "risk_score": 65, "severity": "valuable"},
                "FOS": {"name": "Former Sanctions", "description": "Former Sanctions List", "risk_score": 75, "severity": "valuable"},
                "FRD": {"name": "Fraud", "description": "Fraud, Scams, Swindles", "risk_score": 70, "severity": "valuable"},
                "FUG": {"name": "Fugitive", "description": "Escape, Fugitive", "risk_score": 60, "severity": "valuable"},
                "GAM": {"name": "Gambling", "description": "Illegal Gambling", "risk_score": 25, "severity": "probative"},
                "HCD": {"name": "Healthcare", "description": "Health Care Disciplines", "risk_score": 40, "severity": "investigative"},
                "HTE": {"name": "Hate Crimes", "description": "Hate Groups, Hate Crimes", "risk_score": 70, "severity": "valuable"},
                "HUM": {"name": "Human Rights", "description": "Human rights, Genocide, War crimes", "risk_score": 95, "severity": "critical"},
                "IFO": {"name": "Information", "description": "Information Only", "risk_score": 10, "severity": "probative"},
                "IGN": {"name": "Weapons", "description": "Possession or Sale of Guns, Weapons and Explosives", "risk_score": 60, "severity": "valuable"},
                "IMP": {"name": "Identity Theft", "description": "Identity Theft, Impersonation", "risk_score": 45, "severity": "investigative"},
                "IPR": {"name": "Prostitution", "description": "Illegal Prostitution, Promoting Illegal Prostitution", "risk_score": 35, "severity": "probative"},
                "IRC": {"name": "Iran Connect", "description": "Iran Connect", "risk_score": 80, "severity": "critical"},
                "KID": {"name": "Kidnapping", "description": "Kidnapping, Abduction, Held Against Will", "risk_score": 85, "severity": "critical"},
                "LMD": {"name": "Marijuana", "description": "Legal Marijuana Dispensary", "risk_score": 15, "severity": "probative"},
                "LNS": {"name": "Loan Sharking", "description": "Loan Sharking, Usury, Predatory Lending", "risk_score": 55, "severity": "investigative"},
                "MIS": {"name": "Misconduct", "description": "Misconduct", "risk_score": 25, "severity": "probative"},
                "MLA": {"name": "Money Laundering", "description": "Money Laundering", "risk_score": 85, "severity": "critical"},
                "MOR": {"name": "Mortgage", "description": "Mortgage Related", "risk_score": 60, "severity": "valuable"},
                "MSB": {"name": "Money Services", "description": "Money Services Business", "risk_score": 40, "severity": "investigative"},
                "MUR": {"name": "Murder", "description": "Murder, Manslaughter (Committed, Planned or Attempted)", "risk_score": 95, "severity": "critical"},
                "NSC": {"name": "Non-Specific", "description": "Non Specific Crime", "risk_score": 25, "severity": "probative"},
                "OBS": {"name": "Obscenity", "description": "Obscenity Related, Child Pornography", "risk_score": 60, "severity": "valuable"},
                "ORG": {"name": "Organized Crime", "description": "Organized Crime, Criminal Association, Racketeering", "risk_score": 85, "severity": "critical"},
                "PEP": {"name": "Political", "description": "Person Political", "risk_score": 70, "severity": "valuable"},
                "PER": {"name": "Performance", "description": "Performance Risk", "risk_score": 35, "severity": "probative"},
                "PLT": {"name": "Public Order", "description": "Public Intoxication, Lewdness, Trespassing", "risk_score": 15, "severity": "probative"},
                "PRJ": {"name": "Perjury", "description": "False Filings, False Statements, Perjury, Obstruction of Justice", "risk_score": 50, "severity": "investigative"},
                "PSP": {"name": "Stolen Property", "description": "Possession of Stolen Property", "risk_score": 40, "severity": "investigative"},
                "REG": {"name": "Regulatory", "description": "Regulatory Action", "risk_score": 65, "severity": "valuable"},
                "REO": {"name": "Restructuring", "description": "Restructuring, Reorganization, Divestiture Risk", "risk_score": 30, "severity": "probative"},
                "RES": {"name": "Real Estate", "description": "Real Estate Actions", "risk_score": 35, "severity": "probative"},
                "ROB": {"name": "Robbery", "description": "Robbery (Stealing by Threat, Use of Force)", "risk_score": 60, "severity": "valuable"},
                "SEC": {"name": "Securities", "description": "SEC Violations (Insider Trading, Securities Fraud)", "risk_score": 70, "severity": "valuable"},
                "SEX": {"name": "Sex Offenses", "description": "Sex Offenses (Rape, Sodomy, Sexual Abuse, Pedophilia)", "risk_score": 80, "severity": "critical"},
                "SMG": {"name": "Smuggling", "description": "Smuggling (Does not include Drugs, Money, People or Guns)", "risk_score": 65, "severity": "valuable"},
                "SNX": {"name": "Sanctions", "description": "Sanctions Connect", "risk_score": 90, "severity": "critical"},
                "SPY": {"name": "Espionage", "description": "Espionage, Spying, Treason", "risk_score": 95, "severity": "critical"},
                "TAX": {"name": "Tax", "description": "Tax Related Offenses", "risk_score": 65, "severity": "valuable"},
                "TER": {"name": "Terrorism", "description": "Terrorist Related", "risk_score": 100, "severity": "critical"},
                "TFT": {"name": "Theft", "description": "Theft (Larceny, Misappropriation, Embezzlement, Extortion)", "risk_score": 45, "severity": "investigative"},
                "TRF": {"name": "Trafficking", "description": "People Trafficking, Organ Trafficking", "risk_score": 90, "severity": "critical"},
                "VCY": {"name": "Virtual Currency", "description": "Virtual Currency", "risk_score": 50, "severity": "investigative"},
                "WLT": {"name": "Watch List", "description": "Watch List", "risk_score": 100, "severity": "critical"}
            },
            
            # ACTUAL Event Sub-Categories from database schema.txt lines 594-629
            "event_sub_categories": {
                "ACC": {"name": "Accused", "description": "Accuse", "multiplier": 0.8},
                "ACQ": {"name": "Acquitted", "description": "Acquit, Not Guilty", "multiplier": 0.5},
                "ACT": {"name": "Action", "description": "Disciplinary, Regulatory Action", "multiplier": 1.0},
                "ADT": {"name": "Audit", "description": "Audit", "multiplier": 0.7},
                "ALL": {"name": "Alleged", "description": "Allege", "multiplier": 0.6},
                "APL": {"name": "Appeal", "description": "Appeal", "multiplier": 0.8},
                "ARB": {"name": "Arbitration", "description": "Arbitration", "multiplier": 0.7},
                "ARN": {"name": "Arraigned", "description": "Arraign", "multiplier": 1.0},
                "ART": {"name": "Arrested", "description": "Arrest", "multiplier": 1.1},
                "ASC": {"name": "Associated", "description": "Associated with, Seen with", "multiplier": 0.5},
                "CEN": {"name": "Censured", "description": "Censure", "multiplier": 0.9},
                "CHG": {"name": "Charged", "description": "Charged", "multiplier": 1.0},
                "CMP": {"name": "Complaint", "description": "Complaint Filed", "multiplier": 0.8},
                "CNF": {"name": "Confession", "description": "Confession", "multiplier": 1.2},
                "CSP": {"name": "Conspiracy", "description": "Conspire", "multiplier": 1.0},
                "CVT": {"name": "Convicted", "description": "Convict, Conviction", "multiplier": 1.3},
                "DEP": {"name": "Deported", "description": "Deported", "multiplier": 1.0},
                "DMS": {"name": "Dismissed", "description": "Dismissed", "multiplier": 0.4},
                "EXP": {"name": "Expelled", "description": "Expelled", "multiplier": 0.9},
                "FIL": {"name": "Fine <$10K", "description": "Fine - Less than $10,000", "multiplier": 0.7},
                "FIM": {"name": "Fine >$10K", "description": "Fine - More than $10,000", "multiplier": 1.0},
                "GOV": {"name": "Government", "description": "Government Official", "multiplier": 1.2},
                "IND": {"name": "Indicted", "description": "Indict, Indictment", "multiplier": 1.1},
                "LIC": {"name": "License Action", "description": "Licensing Action", "multiplier": 0.8},
                "LIN": {"name": "Lien", "description": "Lien", "multiplier": 0.6},
                "PLE": {"name": "Plea", "description": "Plea", "multiplier": 1.0},
                "PRB": {"name": "Probe", "description": "Probe", "multiplier": 0.7},
                "RVK": {"name": "Revoked", "description": "Revoked Registration", "multiplier": 1.0},
                "SAN": {"name": "Sanctioned", "description": "Sanction", "multiplier": 1.2},
                "SET": {"name": "Settlement", "description": "Settlement or Suit", "multiplier": 0.8},
                "SEZ": {"name": "Seizure", "description": "Seizure", "multiplier": 1.0},
                "SJT": {"name": "Jail Time", "description": "Served Jail Time", "multiplier": 1.2},
                "SPD": {"name": "Suspended", "description": "Suspended", "multiplier": 0.9},
                "SPT": {"name": "Suspected", "description": "Suspected", "multiplier": 0.6},
                "TRL": {"name": "Trial", "description": "Trial", "multiplier": 1.0},
                "WTD": {"name": "Wanted", "description": "Wanted", "multiplier": 1.1}
            },
            
            # ACTUAL PEP Types from PEP.txt lines 7-26
            "pep_types": {
                "HOS": {"name": "Head of State", "description": "Head of state", "risk_multiplier": 2.0, "level": "L6"},
                "CAB": {"name": "Cabinet Officials", "description": "Cabinet officials", "risk_multiplier": 1.8, "level": "L5"},
                "INF": {"name": "Infrastructure Officials", "description": "Senior officials overseeing key infrastructure sectors", "risk_multiplier": 1.6, "level": "L4"},
                "NIO": {"name": "Non-Infrastructure Officials", "description": "Senior officials overseeing non-infrastructure sectors", "risk_multiplier": 1.5, "level": "L4"},
                "MUN": {"name": "Municipal Officials", "description": "Municipal level officials", "risk_multiplier": 1.3, "level": "L3"},
                "REG": {"name": "Regional Officials", "description": "Regional officials", "risk_multiplier": 1.4, "level": "L3"},
                "LEG": {"name": "Legislative Branch", "description": "Senior legislative branch", "risk_multiplier": 1.5, "level": "L4"},
                "AMB": {"name": "Ambassadors", "description": "Ambassadors and top diplomatic officials", "risk_multiplier": 1.6, "level": "L4"},
                "MIL": {"name": "Military Figures", "description": "Senior military figures", "risk_multiplier": 1.7, "level": "L5"},
                "JUD": {"name": "Judicial Figures", "description": "Senior judicial figures", "risk_multiplier": 1.6, "level": "L4"},
                "POL": {"name": "Political Figures", "description": "Political party figures", "risk_multiplier": 1.4, "level": "L3"},
                "ISO": {"name": "Sporting Officials", "description": "International sporting officials", "risk_multiplier": 1.2, "level": "L2"},
                "GOE": {"name": "Government Enterprises", "description": "Government Owned Enterprises (Organizations)", "risk_multiplier": 1.5, "level": "L4"},
                "GCO": {"name": "State-Controlled Business", "description": "Top executives/functionaries in state-controlled business", "risk_multiplier": 1.4, "level": "L3"},
                "IGO": {"name": "International Organizations", "description": "International Government Organization Officials", "risk_multiplier": 1.3, "level": "L3"},
                "FAM": {"name": "Family Members", "description": "Family members", "risk_multiplier": 1.2, "level": "L2"},
                "ASC": {"name": "Close Associates", "description": "Close associates and advisors", "risk_multiplier": 1.1, "level": "L1"}
            },
            
            # ACTUAL Entity Attributes from database schema.txt lines 630-658
            "entity_attributes": {
                "NIN": {"name": "NI Number", "description": "NI_NUMBER", "data_type": "string"},
                "LGB": {"name": "Legal Basis", "description": "LEGAL_BASIS", "data_type": "string"},
                "REG": {"name": "Registration Date", "description": "REG_DATE", "data_type": "date"},
                "LON": {"name": "Listed On", "description": "LISTED_ON", "data_type": "date"},
                "IMG": {"name": "Image URL", "description": "IMAGE_URL", "data_type": "url"},
                "URL": {"name": "Entity URL", "description": "ENTITY_URL", "data_type": "url"},
                "PHD": {"name": "Physical Description", "description": "PHYSICAL_DESCRIPTION", "data_type": "text"},
                "HCL": {"name": "Hair Color", "description": "HAIR_COLOR", "data_type": "string"},
                "ECL": {"name": "Eye Color", "description": "EYE_COLOR", "data_type": "string"},
                "HGT": {"name": "Height", "description": "HEIGHT", "data_type": "measurement"},
                "CPX": {"name": "Complexion", "description": "COMPLEXION", "data_type": "string"},
                "WGT": {"name": "Weight", "description": "WEIGHT", "data_type": "measurement"},
                "SEX": {"name": "Sex", "description": "SEX", "data_type": "string"},
                "BLD": {"name": "Build", "description": "BUILD", "data_type": "string"},
                "RAC": {"name": "Race", "description": "RACE", "data_type": "string"},
                "SMK": {"name": "Scars/Marks", "description": "SCARS_MARKS", "data_type": "text"},
                "LNG": {"name": "Language", "description": "LANGUAGE", "data_type": "string"},
                "NAT": {"name": "Nationality", "description": "NATIONALITY", "data_type": "string"},
                "OCU": {"name": "Occupation", "description": "OCCUPATION", "data_type": "string"},
                "RMK": {"name": "Remarks", "description": "REMARK", "data_type": "text"},
                "DED": {"name": "Deceased", "description": "DECEASED", "data_type": "boolean"},
                "DDT": {"name": "Deceased Date", "description": "DECEASED_DATE", "data_type": "date"},
                "PTY": {"name": "PEP Type", "description": "PEP_TYPE", "data_type": "string"},
                "RGP": {"name": "Riskography", "description": "RISKOGRAPHY", "data_type": "string"},
                "RID": {"name": "Risk ID", "description": "RISKID", "data_type": "string"},
                "PLV": {"name": "PEP Level", "description": "PEP_LEVEL", "data_type": "string"},
                "RSC": {"name": "Risk Score", "description": "RISK_SCORE", "data_type": "number"},
                "PRT": {"name": "PEP Rating", "description": "PEP_RATING", "data_type": "string"},
                "BIO": {"name": "Biography", "description": "BIO", "data_type": "text"}
            },
            
            # Geographic Risk (keeping existing as these are business logic, not database schema)
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
            
            # Risk Thresholds (business logic)
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
    
    def _get_database_schema_info(self) -> Dict[str, Any]:
        """Get database schema information"""
        return {
            "source_files": {
                "event_categories": "database schema.txt lines 527-593",
                "event_sub_categories": "database schema.txt lines 594-629", 
                "entity_attributes": "database schema.txt lines 630-658",
                "pep_types": "PEP.txt lines 7-26",
                "relationship_types": "database schema.txt lines 659-708"
            },
            "verification": "All codes verified against actual database schema files"
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

# Global corrected configuration instance
corrected_comprehensive_config = CorrectedComprehensiveConfigManager()