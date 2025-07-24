"""
Calculation Engine
Uses comprehensive configuration for all risk calculations and business logic
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import math
from database_verified_config import database_verified_config

class DatabaseVerifiedCalculationEngine:
    """Risk calculation engine using database-verified configuration"""
    
    def __init__(self):
        self.config = database_verified_config
    
    def calculate_entity_risk_score(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive risk score for an entity"""
        
        # Extract entity information
        events = entity_data.get('events', [])
        attributes = entity_data.get('attributes', [])
        addresses = entity_data.get('addresses', [])
        relationships = entity_data.get('relationships', [])
        
        # Base risk calculation from events
        event_risk = self._calculate_event_risk(events)
        
        # PEP risk calculation from attributes
        pep_risk = self._calculate_pep_risk(attributes)
        
        # Geographic risk from addresses
        geo_risk = self._calculate_geographic_risk(addresses)
        
        # Relationship risk
        relationship_risk = self._calculate_relationship_risk(relationships)
        
        # Temporal decay factor
        temporal_factor = self._calculate_temporal_decay(events)
        
        # Combine all risk factors
        combined_score = self._combine_risk_scores({
            'event_risk': event_risk,
            'pep_risk': pep_risk,
            'geographic_risk': geo_risk,
            'relationship_risk': relationship_risk,
            'temporal_factor': temporal_factor
        })
        
        # Get risk level classification
        risk_level_info = self._get_risk_level_info(combined_score['total_score'])
        
        return {
            'total_score': combined_score['total_score'],
            'risk_level': risk_level_info['level'],
            'risk_color': risk_level_info['color'],
            'risk_description': risk_level_info['description'],
            'component_scores': combined_score['components'],
            'calculation_details': combined_score['details']
        }
    
    def _calculate_event_risk(self, events: List[Dict]) -> Dict[str, Any]:
        """Calculate risk score from events using configuration"""
        if not events:
            return {'score': 0, 'max_event': None, 'event_count': 0}
        
        max_score = 0
        max_event = None
        total_weighted_score = 0
        processed_events = []
        
        for event in events:
            category = event.get('event_category_code', '')
            sub_category = event.get('event_sub_category_code', '')
            event_date = event.get('event_date')
            
            # Get category information from config
            category_info = self.config.get_event_category(category)
            base_score = category_info['risk_score']
            
            # Apply sub-category multiplier
            if sub_category:
                sub_info = self.config.get_event_sub_category(sub_category)
                modified_score = base_score * sub_info['multiplier']
            else:
                modified_score = base_score
            
            # Apply temporal decay
            if event_date:
                temporal_multiplier = self._get_temporal_multiplier(event_date)
                final_score = modified_score * temporal_multiplier
            else:
                final_score = modified_score
            
            # Track maximum and accumulate weighted score
            if final_score > max_score:
                max_score = final_score
                max_event = {
                    'category': category,
                    'sub_category': sub_category,
                    'date': event_date,
                    'score': final_score
                }
            
            total_weighted_score += final_score
            processed_events.append({
                'category': category,
                'sub_category': sub_category,
                'base_score': base_score,
                'final_score': final_score
            })
        
        # Use maximum event score as primary, with boost for multiple events
        event_count_multiplier = min(1.0 + (len(events) - 1) * 0.1, 2.0)  # Max 2x for multiple events
        final_score = min(max_score * event_count_multiplier, 100)
        
        return {
            'score': final_score,
            'max_event': max_event,
            'event_count': len(events),
            'processed_events': processed_events,
            'event_count_multiplier': event_count_multiplier
        }
    
    def _calculate_pep_risk(self, attributes: List[Dict]) -> Dict[str, Any]:
        """Calculate PEP risk using configuration"""
        pep_score = 0
        pep_details = []
        highest_pep_multiplier = 1.0
        
        for attr in attributes:
            code_type = attr.get('alias_code_type', '')
            value = attr.get('alias_value', '')
            
            if code_type == 'PTY' and value:  # PEP Type attribute
                # Parse PEP type from value
                pep_type = self._extract_pep_type_from_value(value)
                
                if pep_type:
                    pep_info = self.config.get_pep_type(pep_type)
                    pep_multiplier = pep_info['risk_multiplier']
                    
                    if pep_multiplier > highest_pep_multiplier:
                        highest_pep_multiplier = pep_multiplier
                    
                    pep_details.append({
                        'type': pep_type,
                        'value': value,
                        'multiplier': pep_multiplier,
                        'level': pep_info['level'],
                        'description': pep_info['description']
                    })
        
        # Base PEP score from configuration
        if pep_details:
            base_pep_score = 60  # Base PEP risk score
            pep_score = base_pep_score * highest_pep_multiplier
        
        return {
            'score': pep_score,
            'is_pep': len(pep_details) > 0,
            'highest_multiplier': highest_pep_multiplier,
            'pep_details': pep_details
        }
    
    def _calculate_geographic_risk(self, addresses: List[Dict]) -> Dict[str, Any]:
        """Calculate geographic risk using configuration"""
        if not addresses:
            return {'score': 0, 'countries': [], 'highest_multiplier': 1.0}
        
        highest_multiplier = 1.0
        countries_analyzed = []
        
        for address in addresses:
            country = address.get('address_country', '').upper()
            if country:
                multiplier = self.config.get_geographic_multiplier(country)
                
                if multiplier > highest_multiplier:
                    highest_multiplier = multiplier
                
                countries_analyzed.append({
                    'country': country,
                    'multiplier': multiplier
                })
        
        # Apply geographic multiplier to base risk
        base_geo_score = 50  # Base geographic consideration score
        geo_score = base_geo_score * highest_multiplier
        
        return {
            'score': geo_score if highest_multiplier > 1.0 else 0,
            'countries': countries_analyzed,
            'highest_multiplier': highest_multiplier
        }
    
    def _calculate_relationship_risk(self, relationships: List[Dict]) -> Dict[str, Any]:
        """Calculate relationship risk using configuration"""
        if not relationships:
            return {'score': 0, 'relationships': [], 'highest_factor': 1.0}
        
        highest_factor = 1.0
        relationships_analyzed = []
        
        # Simple relationship risk mapping
        high_risk_types = ['BUSINESS_PARTNER', 'ASSOCIATE', 'BENEFICIAL_OWNER', 'DIRECTOR', 'SHAREHOLDER']
        medium_risk_types = ['FAMILY_MEMBER', 'SPOUSE', 'RELATIVE']
        
        for rel in relationships:
            rel_type = rel.get('type', '').upper()
            if rel_type:
                if rel_type in high_risk_types:
                    risk_factor = 1.5
                    description = "High-risk business relationship"
                elif rel_type in medium_risk_types:
                    risk_factor = 1.2
                    description = "Family/personal relationship"
                else:
                    risk_factor = 1.1
                    description = "General relationship"
                
                if risk_factor > highest_factor:
                    highest_factor = risk_factor
                
                relationships_analyzed.append({
                    'type': rel_type,
                    'related_entity': rel.get('related_entity_name', ''),
                    'risk_factor': risk_factor,
                    'description': description
                })
        
        # Relationship risk boost
        base_rel_score = 30  # Base relationship consideration score
        rel_score = base_rel_score * highest_factor if highest_factor > 1.0 else 0
        
        return {
            'score': rel_score,
            'relationships': relationships_analyzed,
            'highest_factor': highest_factor
        }
    
    def _calculate_temporal_decay(self, events: List[Dict]) -> Dict[str, Any]:
        """Calculate temporal decay factor"""
        if not events:
            return {'factor': 1.0, 'most_recent_date': None}
        
        most_recent_date = None
        decay_factor = 1.0
        
        for event in events:
            event_date_str = event.get('event_date')
            if event_date_str:
                try:
                    event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
                    if not most_recent_date or event_date > most_recent_date:
                        most_recent_date = event_date
                except:
                    continue
        
        if most_recent_date:
            days_since = (datetime.now() - most_recent_date).days
            years_since = days_since / 365.25
            
            # Apply temporal decay from system settings
            decay_rate = self.config.get('system_settings.temporal_decay_rate', 0.1)
            max_age_years = self.config.get('system_settings.max_age_years', 10)
            min_weight = self.config.get('system_settings.minimum_weight', 0.1)
            
            if years_since <= max_age_years:
                decay_factor = max(1.0 - (years_since * decay_rate), min_weight)
            else:
                decay_factor = min_weight
        
        return {
            'factor': decay_factor,
            'most_recent_date': most_recent_date.isoformat() if most_recent_date else None,
            'years_since': years_since if most_recent_date else None
        }
    
    def _combine_risk_scores(self, risk_components: Dict[str, Any]) -> Dict[str, Any]:
        """Combine all risk components into final score"""
        
        # Extract component scores
        event_score = risk_components['event_risk']['score']
        pep_score = risk_components['pep_risk']['score']
        geo_score = risk_components['geographic_risk']['score']
        rel_score = risk_components['relationship_risk']['score']
        temporal_factor = risk_components['temporal_factor']['factor']
        
        # Weight configuration from system settings
        weights = {
            'event_weight': self.config.get('system_settings.event_weight', 0.6),
            'pep_weight': self.config.get('system_settings.pep_weight', 0.25),
            'geographic_weight': self.config.get('system_settings.geographic_weight', 0.1),
            'relationship_weight': self.config.get('system_settings.relationship_weight', 0.05)
        }
        
        # Calculate weighted score
        weighted_score = (
            event_score * weights['event_weight'] +
            pep_score * weights['pep_weight'] +
            geo_score * weights['geographic_weight'] +
            rel_score * weights['relationship_weight']
        )
        
        # Apply temporal decay
        temporal_adjusted_score = weighted_score * temporal_factor
        
        # Ensure score is within bounds
        final_score = min(max(int(temporal_adjusted_score), 0), 100)
        
        return {
            'total_score': final_score,
            'components': {
                'event_score': event_score,
                'pep_score': pep_score,
                'geographic_score': geo_score,
                'relationship_score': rel_score,
                'weighted_score': weighted_score,
                'temporal_adjusted_score': temporal_adjusted_score
            },
            'details': {
                'weights_used': weights,
                'temporal_factor': temporal_factor,
                'component_breakdown': risk_components
            }
        }
    
    def _extract_pep_type_from_value(self, value: str) -> Optional[str]:
        """Extract PEP type code from attribute value"""
        if not value:
            return None
        
        # Handle different PEP value formats
        if ':' in value:
            # Format: 'MUN:L3', 'REG:L5'
            return value.split(':', 1)[0].strip()
        elif value in self.config.get('pep_types', {}):
            # Direct PEP code: 'FAM', 'ASC'
            return value
        else:
            # Try to extract from description
            for pep_code in self.config.get('pep_types', {}):
                if pep_code in value.upper():
                    return pep_code
        
        return None
    
    def _get_temporal_multiplier(self, event_date_str: str) -> float:
        """Get temporal multiplier for event date"""
        try:
            event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
            days_since = (datetime.now() - event_date).days
            years_since = days_since / 365.25
            
            # Configuration-based temporal decay
            if years_since <= 1:
                return 1.0  # No decay for recent events
            elif years_since <= 3:
                return 0.9  # Slight decay
            elif years_since <= 5:
                return 0.8  # Moderate decay
            elif years_since <= 10:
                return 0.6  # Significant decay
            else:
                return 0.3  # Heavy decay for old events
        except:
            return 1.0  # Default if date parsing fails
    
    def calculate_name_match_score(self, search_name: str, entity_name: str, 
                                 entity_data: Dict[str, Any]) -> int:
        """Calculate name match score using configuration"""
        
        # Base name similarity (simplified - would use more sophisticated matching)
        base_score = self._calculate_name_similarity(search_name, entity_name)
        
        # Adjustments based on additional data
        adjustments = 0
        
        # DOB adjustment (if available)
        if 'date_of_birth' in entity_data:
            dob_match = self._calculate_dob_match(entity_data.get('search_dob'), 
                                                entity_data['date_of_birth'])
            adjustments += dob_match
        
        # Address adjustments
        if 'addresses' in entity_data:
            address_match = self._calculate_address_match(entity_data.get('search_address'),
                                                        entity_data['addresses'])
            adjustments += address_match
        
        # Final score with bounds
        final_score = min(max(base_score + adjustments, 0), 100)
        
        return final_score
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> int:
        """Calculate name similarity score"""
        # Simplified implementation - would use sophisticated string matching
        if not name1 or not name2:
            return 0
        
        name1_clean = name1.lower().strip()
        name2_clean = name2.lower().strip()
        
        if name1_clean == name2_clean:
            return 100
        elif name1_clean in name2_clean or name2_clean in name1_clean:
            return 80
        else:
            # Would implement more sophisticated matching (Levenshtein, etc.)
            return 50
    
    def _calculate_dob_match(self, search_dob: Optional[str], entity_dob: str) -> int:
        """Calculate date of birth match adjustment"""
        if not search_dob or not entity_dob:
            return 0
        
        try:
            search_date = datetime.strptime(search_dob, '%Y-%m-%d')
            entity_date = datetime.strptime(entity_dob, '%Y-%m-%d')
            
            year_diff = abs(search_date.year - entity_date.year)
            
            if year_diff == 0:
                return 2  # Exact match
            elif year_diff <= 2:
                return 1  # Close match
            else:
                return -1  # Poor match
        except:
            return 0
    
    def _calculate_address_match(self, search_address: Optional[Dict], 
                               entity_addresses: List[Dict]) -> int:
        """Calculate address match adjustment"""
        if not search_address or not entity_addresses:
            return 0
        
        search_country = search_address.get('country', '').upper()
        search_city = search_address.get('city', '').lower()
        
        for address in entity_addresses:
            entity_country = address.get('address_country', '').upper()
            entity_city = address.get('address_city', '').lower()
            
            if search_country == entity_country:
                if search_city and entity_city and search_city == entity_city:
                    return 2  # Country and city match
                else:
                    return 1  # Country match only
            elif search_country and entity_country and search_country != entity_country:
                return -1  # Country mismatch
        
        return 0
    
    def get_configurable_thresholds(self) -> Dict[str, Any]:
        """Get all configurable thresholds for UI display"""
        return {
            'risk_thresholds': self.config.get('risk_thresholds'),
            'system_settings': self.config.get('system_settings'),
            'geographic_multipliers': self.config.get('geographic_risk'),
            'event_categories': self.config.get('event_categories'),
            'pep_types': self.config.get('pep_types')
        }
    
    def _get_risk_level_info(self, score: float) -> Dict[str, str]:
        """Get risk level information based on score"""
        thresholds = self.config.get('risk_thresholds')
        
        if score >= thresholds['critical']['min']:
            return {
                'level': 'Critical',
                'color': thresholds['critical']['color'],
                'description': thresholds['critical']['description']
            }
        elif score >= thresholds['valuable']['min']:
            return {
                'level': 'Valuable', 
                'color': thresholds['valuable']['color'],
                'description': thresholds['valuable']['description']
            }
        elif score >= thresholds['investigative']['min']:
            return {
                'level': 'Investigative',
                'color': thresholds['investigative']['color'],
                'description': thresholds['investigative']['description']
            }
        else:
            return {
                'level': 'Probative',
                'color': thresholds['probative']['color'],
                'description': thresholds['probative']['description']
            }

# Global calculation engine instance
calculation_engine = DatabaseVerifiedCalculationEngine()