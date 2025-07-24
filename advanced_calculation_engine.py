"""
Advanced Risk Calculation Engine
Ultra-sophisticated risk calculation with machine learning-inspired features
"""
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import math
import statistics
from database_verified_config import database_verified_config

class AdvancedRiskCalculationEngine:
    """Ultra-advanced risk calculation engine with ML-inspired features"""
    
    def __init__(self):
        self.config = database_verified_config
        self.calculation_history = []  # For pattern learning
        
    def calculate_entity_risk_score(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate ultra-sophisticated risk score for an entity"""
        
        # Extract entity information
        events = entity_data.get('events', [])
        attributes = entity_data.get('attributes', [])
        addresses = entity_data.get('addresses', [])
        relationships = entity_data.get('relationships', [])
        aliases = entity_data.get('aliases', [])
        identifications = entity_data.get('identifications', [])
        
        # Advanced risk calculations
        event_analysis = self._calculate_advanced_event_risk(events)
        pep_analysis = self._calculate_advanced_pep_risk(attributes)
        geographic_analysis = self._calculate_advanced_geographic_risk(addresses)
        relationship_analysis = self._calculate_advanced_relationship_risk(relationships)
        temporal_analysis = self._calculate_advanced_temporal_patterns(events)
        behavior_analysis = self._calculate_behavioral_patterns(entity_data)
        network_analysis = self._calculate_network_effects(relationships, events)
        
        # Anomaly detection
        anomaly_score = self._detect_anomalies(entity_data)
        
        # Risk correlation analysis
        correlation_factors = self._analyze_risk_correlations({
            'events': event_analysis,
            'pep': pep_analysis,
            'geographic': geographic_analysis,
            'relationships': relationship_analysis,
            'temporal': temporal_analysis,
            'behavior': behavior_analysis,
            'network': network_analysis,
            'anomalies': anomaly_score
        })
        
        # ML-inspired ensemble scoring
        ensemble_score = self._ensemble_risk_calculation(correlation_factors)
        
        # Confidence scoring
        confidence_metrics = self._calculate_confidence_metrics(entity_data, ensemble_score)
        
        # Risk trajectory prediction
        trajectory = self._predict_risk_trajectory(events, ensemble_score['total_score'])
        
        # Final risk classification with uncertainty
        risk_classification = self._advanced_risk_classification(
            ensemble_score['total_score'], 
            confidence_metrics
        )
        
        result = {
            'total_score': round(ensemble_score['total_score'], 2),
            'confidence_score': round(confidence_metrics['overall_confidence'], 2),
            'risk_level': risk_classification['level'],
            'risk_color': risk_classification['color'],
            'risk_description': risk_classification['description'],
            'uncertainty_range': risk_classification['uncertainty_range'],
            'risk_trajectory': trajectory,
            'component_scores': ensemble_score['components'],
            'advanced_analytics': {
                'behavioral_patterns': behavior_analysis,
                'network_effects': network_analysis,
                'anomaly_indicators': anomaly_score,
                'correlation_factors': correlation_factors,
                'confidence_breakdown': confidence_metrics
            },
            'calculation_metadata': {
                'algorithm_version': '3.0.0-advanced',
                'calculation_timestamp': datetime.now().isoformat(),
                'data_completeness': self._assess_data_completeness(entity_data),
                'feature_importance': ensemble_score['feature_importance']
            }
        }
        
        # Store for pattern learning
        self._store_calculation_history(entity_data, result)
        
        return result
    
    def _calculate_advanced_event_risk(self, events: List[Dict]) -> Dict[str, Any]:
        """Advanced event risk analysis with pattern detection"""
        if not events:
            return {
                'score': 0, 'event_count': 0, 'severity_distribution': {},
                'temporal_clustering': {}, 'escalation_pattern': None
            }
        
        event_scores = []
        severity_counts = {'critical': 0, 'valuable': 0, 'investigative': 0, 'probative': 0}
        event_timeline = []
        category_patterns = {}
        
        for event in events:
            category = event.get('event_category_code', '')
            sub_category = event.get('event_sub_category_code', '')
            event_date = event.get('event_date')
            
            # Get category information
            category_info = self.config.get_event_category(category)
            base_score = category_info['risk_score']
            severity = category_info['severity']
            
            # Apply sub-category multiplier with advanced logic
            if sub_category:
                sub_info = self.config.get_event_sub_category(sub_category)
                multiplier = sub_info['multiplier']
                
                # Advanced multiplier adjustment based on category-subcategory synergy
                synergy_boost = self._calculate_category_synergy(category, sub_category)
                final_multiplier = multiplier * synergy_boost
                
                modified_score = base_score * final_multiplier
            else:
                modified_score = base_score
            
            # Advanced temporal weighting
            if event_date:
                temporal_weight = self._calculate_advanced_temporal_weight(event_date, category)
                final_score = modified_score * temporal_weight
                
                event_timeline.append({
                    'date': event_date,
                    'category': category,
                    'score': final_score,
                    'severity': severity
                })
            else:
                final_score = modified_score
            
            event_scores.append(final_score)
            severity_counts[severity] += 1
            
            # Track category patterns
            if category not in category_patterns:
                category_patterns[category] = 0
            category_patterns[category] += 1
        
        # Calculate sophisticated event risk metrics
        max_score = max(event_scores) if event_scores else 0
        avg_score = statistics.mean(event_scores) if event_scores else 0
        score_variance = statistics.variance(event_scores) if len(event_scores) > 1 else 0
        
        # Event clustering analysis
        temporal_clusters = self._analyze_temporal_clustering(event_timeline)
        
        # Escalation pattern detection
        escalation_pattern = self._detect_escalation_patterns(event_timeline)
        
        # Diversity penalty/bonus
        diversity_factor = self._calculate_event_diversity_factor(category_patterns)
        
        # Calculate final event risk score using ensemble method
        event_risk_score = self._ensemble_event_scoring(
            max_score, avg_score, score_variance, diversity_factor, 
            escalation_pattern, temporal_clusters
        )
        
        return {
            'score': event_risk_score,
            'max_event_score': max_score,
            'average_event_score': avg_score,
            'score_variance': score_variance,
            'event_count': len(events),
            'severity_distribution': severity_counts,
            'category_patterns': category_patterns,
            'temporal_clustering': temporal_clusters,
            'escalation_pattern': escalation_pattern,
            'diversity_factor': diversity_factor,
            'timeline_analysis': event_timeline
        }
    
    def _calculate_advanced_pep_risk(self, attributes: List[Dict]) -> Dict[str, Any]:
        """Advanced PEP risk analysis with relationship mapping"""
        pep_details = []
        pep_levels = []
        pep_categories = []
        highest_multiplier = 1.0
        
        for attr in attributes:
            code_type = attr.get('alias_code_type', '')
            value = attr.get('alias_value', '')
            
            if code_type == 'PTY':
                # Extract PEP type codes from PEP.txt (not the names in alias_value)
                for pep_code in ['HOS', 'CAB', 'INF', 'NIO', 'MUN', 'REG', 'LEG', 
                               'AMB', 'MIL', 'JUD', 'POL', 'ISO', 'GOE', 'GCO', 'IGO', 'FAM', 'ASC']:
                    if pep_code in value or self._fuzzy_match_pep_type(value, pep_code):
                        pep_info = self.config.get_pep_type(pep_code)
                        multiplier = pep_info['risk_multiplier']
                        level = pep_info['level']
                        
                        if multiplier > highest_multiplier:
                            highest_multiplier = multiplier
                        
                        pep_details.append({
                            'type': pep_code,
                            'description': pep_info['name'],
                            'level': level,
                            'multiplier': multiplier,
                            'raw_value': value
                        })
                        
                        pep_levels.append(level)
                        pep_categories.append(pep_code)
        
        # Advanced PEP risk calculation
        if pep_details:
            base_pep_score = 60  # Base PEP risk score
            
            # Multi-level PEP analysis
            level_weights = {'L6': 2.0, 'L5': 1.8, 'L4': 1.6, 'L3': 1.4, 'L2': 1.2, 'L1': 1.0}
            weighted_level_score = sum(level_weights.get(level, 1.0) for level in pep_levels)
            
            # PEP category diversity factor
            category_diversity = len(set(pep_categories)) / len(pep_categories) if pep_categories else 0
            diversity_bonus = 1.0 + (category_diversity * 0.2)  # Up to 20% bonus for diverse PEP types
            
            # Family/Associate connection analysis
            family_connections = len([p for p in pep_details if p['type'] in ['FAM', 'ASC']])
            direct_political = len([p for p in pep_details if p['type'] not in ['FAM', 'ASC']])
            
            # Connection risk multiplier
            if direct_political > 0 and family_connections > 0:
                connection_multiplier = 1.3  # Both direct and indirect connections
            elif direct_political > 0:
                connection_multiplier = 1.2  # Direct political connections
            elif family_connections > 0:
                connection_multiplier = 1.1  # Only family/associate connections
            else:
                connection_multiplier = 1.0
            
            # Calculate sophisticated PEP score
            pep_score = (base_pep_score * highest_multiplier * diversity_bonus * 
                        connection_multiplier * (weighted_level_score / len(pep_levels)))
        else:
            pep_score = 0
            weighted_level_score = 0
            diversity_bonus = 1.0
            connection_multiplier = 1.0
        
        return {
            'score': min(pep_score, 100),  # Cap at 100
            'pep_details': pep_details,
            'highest_multiplier': highest_multiplier,
            'level_distribution': dict((level, pep_levels.count(level)) for level in set(pep_levels)),
            'category_diversity': len(set(pep_categories)),
            'weighted_level_score': weighted_level_score,
            'diversity_bonus': diversity_bonus,
            'connection_analysis': {
                'direct_political': len([p for p in pep_details if p['type'] not in ['FAM', 'ASC']]),
                'family_associate': len([p for p in pep_details if p['type'] in ['FAM', 'ASC']]),
                'connection_multiplier': connection_multiplier if 'connection_multiplier' in locals() else 1.0
            }
        }
    
    def _calculate_advanced_geographic_risk(self, addresses: List[Dict]) -> Dict[str, Any]:
        """Advanced geographic risk with sanctions and conflict zone analysis"""
        if not addresses:
            return {'score': 0, 'countries': [], 'risk_distribution': {}}
        
        country_risks = []
        unique_countries = set()
        risk_distribution = {'critical': 0, 'high': 0, 'medium': 0, 'low': 0}
        sanctions_exposure = 0
        conflict_zones = 0
        
        for addr in addresses:
            country = addr.get('address_country', '').upper()
            if country:
                unique_countries.add(country)
                multiplier = self.config.get_geographic_multiplier(country)
                
                # Advanced risk classification
                if multiplier >= 2.0:
                    risk_level = 'critical'
                    risk_distribution['critical'] += 1
                    if country in ['IR', 'KP', 'SY']:  # Sanctions countries
                        sanctions_exposure += 1
                    if country in ['AF', 'SY', 'YE']:  # Conflict zones
                        conflict_zones += 1
                elif multiplier >= 1.5:
                    risk_level = 'high'
                    risk_distribution['high'] += 1
                elif multiplier >= 1.1:
                    risk_level = 'medium'
                    risk_distribution['medium'] += 1
                else:
                    risk_level = 'low'
                    risk_distribution['low'] += 1
                
                country_risks.append({
                    'country': country,
                    'multiplier': multiplier,
                    'risk_level': risk_level
                })
        
        # Calculate geographic risk score
        if country_risks:
            max_multiplier = max(risk['multiplier'] for risk in country_risks)
            avg_multiplier = sum(risk['multiplier'] for risk in country_risks) / len(country_risks)
            
            # Base geographic score
            base_geo_score = 20
            
            # Apply advanced multipliers
            geo_score = base_geo_score * max_multiplier
            
            # Sanctions and conflict zone penalties
            sanctions_penalty = sanctions_exposure * 15
            conflict_penalty = conflict_zones * 10
            
            # Multi-country risk (diversification vs concentration)
            country_count = len(unique_countries)
            if country_count > 3:
                multi_country_factor = 1.2  # Risk increases with more countries
            else:
                multi_country_factor = 1.0
            
            final_geo_score = (geo_score + sanctions_penalty + conflict_penalty) * multi_country_factor
        else:
            final_geo_score = 0
            max_multiplier = 1.0
            avg_multiplier = 1.0
        
        return {
            'score': min(final_geo_score, 100),
            'countries': list(unique_countries),
            'country_risks': country_risks,
            'risk_distribution': risk_distribution,
            'sanctions_exposure': sanctions_exposure,
            'conflict_zones': conflict_zones,
            'max_multiplier': max_multiplier if 'max_multiplier' in locals() else 1.0,
            'avg_multiplier': avg_multiplier if 'avg_multiplier' in locals() else 1.0,
            'multi_country_factor': multi_country_factor if 'multi_country_factor' in locals() else 1.0
        }
    
    def _calculate_advanced_relationship_risk(self, relationships: List[Dict]) -> Dict[str, Any]:
        """Advanced relationship risk with network analysis"""
        if not relationships:
            return {'score': 0, 'network_metrics': {}, 'risk_connections': []}
        
        # Enhanced relationship categorization
        relationship_categories = {
            'ownership': ['BENEFICIAL_OWNER', 'SHAREHOLDER', 'DIRECTOR', 'OFFICER'],
            'business': ['BUSINESS_PARTNER', 'ASSOCIATE', 'CONTRACTOR', 'VENDOR', 'CLIENT'],
            'personal': ['FAMILY_MEMBER', 'SPOUSE', 'RELATIVE', 'FRIEND'],
            'legal': ['ATTORNEY', 'LEGAL_REPRESENTATIVE', 'TRUSTEE'],
            'financial': ['BANK', 'LENDER', 'BORROWER', 'GUARANTOR']
        }
        
        risk_weights = {
            'ownership': 2.0,   # Highest risk - control relationships
            'business': 1.5,    # High risk - business connections
            'legal': 1.3,       # Medium-high risk - legal connections
            'financial': 1.2,   # Medium risk - financial connections
            'personal': 1.1     # Lower risk - personal connections
        }
        
        network_nodes = set()
        relationship_analysis = []
        category_counts = {cat: 0 for cat in relationship_categories.keys()}
        max_risk_weight = 1.0
        
        for rel in relationships:
            rel_type = rel.get('type', '').upper()
            related_entity = rel.get('related_entity_name', '')
            
            # Categorize relationship
            risk_category = 'unknown'
            risk_weight = 1.0
            
            for category, types in relationship_categories.items():
                if rel_type in types:
                    risk_category = category
                    risk_weight = risk_weights[category]
                    category_counts[category] += 1
                    break
            
            if risk_weight > max_risk_weight:
                max_risk_weight = risk_weight
            
            # Network analysis
            network_nodes.add(related_entity)
            
            relationship_analysis.append({
                'type': rel_type,
                'category': risk_category,
                'risk_weight': risk_weight,
                'related_entity': related_entity,
                'direction': rel.get('direction', 'bidirectional')
            })
        
        # Network metrics
        network_density = len(relationships) / len(network_nodes) if network_nodes else 0
        relationship_diversity = len([cat for cat, count in category_counts.items() if count > 0])
        
        # Calculate relationship risk score
        base_rel_score = 25
        
        # Apply maximum risk weight
        weighted_score = base_rel_score * max_risk_weight
        
        # Network complexity bonus/penalty
        if network_density > 2.0:  # Many connections to same entities
            complexity_factor = 1.3
        elif network_density > 1.5:
            complexity_factor = 1.2
        else:
            complexity_factor = 1.0
        
        # Diversity factor
        diversity_factor = 1.0 + (relationship_diversity * 0.1)  # 10% per relationship category
        
        final_rel_score = weighted_score * complexity_factor * diversity_factor
        
        return {
            'score': min(final_rel_score, 100),
            'relationship_count': len(relationships),
            'network_nodes': len(network_nodes),
            'network_density': network_density,
            'category_distribution': category_counts,
            'relationship_diversity': relationship_diversity,
            'max_risk_weight': max_risk_weight,
            'complexity_factor': complexity_factor if 'complexity_factor' in locals() else 1.0,
            'diversity_factor': diversity_factor if 'diversity_factor' in locals() else 1.0,
            'risk_connections': relationship_analysis
        }
    
    def _calculate_advanced_temporal_patterns(self, events: List[Dict]) -> Dict[str, Any]:
        """Advanced temporal pattern analysis"""
        if not events:
            return {'decay_factor': 1.0, 'patterns': {}}
        
        event_dates = []
        for event in events:
            event_date_str = event.get('event_date')
            if event_date_str:
                try:
                    event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
                    event_dates.append(event_date)
                except:
                    continue
        
        if not event_dates:
            return {'decay_factor': 1.0, 'patterns': {}}
        
        event_dates.sort()
        most_recent = event_dates[-1]
        oldest = event_dates[0]
        
        # Advanced temporal metrics
        days_since_recent = (datetime.now() - most_recent).days
        years_since_recent = days_since_recent / 365.25
        time_span_years = (most_recent - oldest).days / 365.25 if len(event_dates) > 1 else 0
        
        # Activity frequency analysis
        if time_span_years > 0:
            activity_rate = len(event_dates) / time_span_years  # Events per year
        else:
            activity_rate = len(event_dates)  # All events in same period
        
        # Recency scoring with advanced curve
        if years_since_recent <= 1:
            recency_factor = 1.0  # Very recent
        elif years_since_recent <= 2:
            recency_factor = 0.9  # Recent
        elif years_since_recent <= 5:
            recency_factor = 0.7  # Moderately recent
        elif years_since_recent <= 10:
            recency_factor = 0.5  # Old
        else:
            recency_factor = 0.3  # Very old
        
        # Activity pattern analysis
        if activity_rate > 2:
            activity_factor = 1.2  # High activity increases risk
        elif activity_rate > 1:
            activity_factor = 1.1  # Moderate activity
        else:
            activity_factor = 1.0   # Low activity
        
        # Temporal clustering
        temporal_clusters = self._detect_temporal_clusters(event_dates)
        
        # Calculate final temporal factor
        temporal_factor = recency_factor * activity_factor
        
        return {
            'decay_factor': temporal_factor,
            'years_since_recent': years_since_recent,
            'time_span_years': time_span_years,
            'activity_rate': activity_rate,
            'recency_factor': recency_factor,
            'activity_factor': activity_factor,
            'temporal_clusters': temporal_clusters,
            'event_count': len(event_dates),
            'patterns': {
                'most_recent': most_recent.isoformat(),
                'oldest': oldest.isoformat(),
                'activity_intensity': 'high' if activity_rate > 1 else 'moderate' if activity_rate > 0.5 else 'low'
            }
        }
    
    def _calculate_behavioral_patterns(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze behavioral patterns from entity data"""
        
        # Alias analysis
        aliases = entity_data.get('aliases', [])
        alias_count = len(aliases)
        alias_diversity = len(set(alias.get('alias_name', '') for alias in aliases))
        
        # Multiple identity risk
        if alias_count > 5:
            identity_risk = 'high'
            identity_factor = 1.3
        elif alias_count > 2:
            identity_risk = 'medium'
            identity_factor = 1.1
        else:
            identity_risk = 'low'
            identity_factor = 1.0
        
        # Identification document analysis
        identifications = entity_data.get('identifications', [])
        id_countries = set(id_doc.get('identification_country', '') for id_doc in identifications)
        multi_jurisdiction = len(id_countries) > 2
        
        # Address mobility analysis
        addresses = entity_data.get('addresses', [])
        address_countries = set(addr.get('address_country', '') for addr in addresses)
        geographic_mobility = len(address_countries)
        
        # Behavioral risk score
        behavior_score = 0
        
        if alias_count > 3:
            behavior_score += 15  # Multiple aliases
        if multi_jurisdiction:
            behavior_score += 10  # Multiple ID jurisdictions
        if geographic_mobility > 3:
            behavior_score += 10  # High geographic mobility
        
        return {
            'behavior_score': behavior_score,
            'identity_risk': identity_risk,
            'identity_factor': identity_factor,
            'alias_analysis': {
                'alias_count': alias_count,
                'alias_diversity': alias_diversity,
                'risk_level': identity_risk
            },
            'jurisdiction_analysis': {
                'id_countries': len(id_countries),
                'multi_jurisdiction': multi_jurisdiction
            },
            'mobility_analysis': {
                'address_countries': len(address_countries),
                'geographic_mobility': geographic_mobility
            }
        }
    
    def _calculate_network_effects(self, relationships: List[Dict], events: List[Dict]) -> Dict[str, Any]:
        """Calculate network effect amplification"""
        
        if not relationships:
            return {'network_amplification': 1.0, 'network_risk': 0}
        
        # Simple network risk calculation
        high_risk_connections = 0
        total_connections = len(relationships)
        
        for rel in relationships:
            rel_type = rel.get('type', '').upper()
            if rel_type in ['BUSINESS_PARTNER', 'ASSOCIATE', 'BENEFICIAL_OWNER']:
                high_risk_connections += 1
        
        # Network amplification factor
        if high_risk_connections > 3:
            network_amplification = 1.3
        elif high_risk_connections > 1:
            network_amplification = 1.2
        else:
            network_amplification = 1.1
        
        network_risk = (high_risk_connections / total_connections) * 100 if total_connections > 0 else 0
        
        return {
            'network_amplification': network_amplification,
            'network_risk': network_risk,
            'high_risk_connections': high_risk_connections,
            'total_connections': total_connections
        }
    
    def _detect_anomalies(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Detect anomalous patterns in entity data"""
        
        anomaly_indicators = []
        anomaly_score = 0
        
        # Check for unusual patterns
        events = entity_data.get('events', [])
        addresses = entity_data.get('addresses', [])
        
        # Temporal anomalies
        if events:
            event_dates = []
            for event in events:
                if event.get('event_date'):
                    try:
                        event_dates.append(datetime.strptime(event['event_date'], '%Y-%m-%d'))
                    except:
                        continue
            
            if len(event_dates) > 1:
                # Check for unusual clustering
                date_gaps = []
                for i in range(1, len(event_dates)):
                    gap = (event_dates[i] - event_dates[i-1]).days
                    date_gaps.append(gap)
                
                if date_gaps:
                    avg_gap = sum(date_gaps) / len(date_gaps)
                    if any(gap > avg_gap * 3 for gap in date_gaps):
                        anomaly_indicators.append('temporal_clustering')
                        anomaly_score += 10
        
        # Geographic anomalies
        if addresses:
            countries = [addr.get('address_country') for addr in addresses]
            unique_countries = set(filter(None, countries))
            if len(unique_countries) > 5:  # Many countries
                anomaly_indicators.append('excessive_geographic_spread')
                anomaly_score += 15
        
        return {
            'anomaly_score': anomaly_score,
            'anomaly_indicators': anomaly_indicators,
            'total_anomalies': len(anomaly_indicators)
        }
    
    def _analyze_risk_correlations(self, risk_factors: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze correlations between different risk factors"""
        
        # Extract scores for correlation analysis
        event_score = risk_factors['events']['score']
        pep_score = risk_factors['pep']['score'] 
        geo_score = risk_factors['geographic']['score']
        rel_score = risk_factors['relationships']['score']
        
        # Correlation bonuses
        correlation_bonus = 0
        correlation_factors = []
        
        # High event + High PEP = Dangerous combination
        if event_score > 70 and pep_score > 50:
            correlation_bonus += 15
            correlation_factors.append('high_risk_pep_with_serious_events')
        
        # High geographic + High relationship = International criminal network
        if geo_score > 50 and rel_score > 40:
            correlation_bonus += 10
            correlation_factors.append('international_network_indicators')
        
        # Multiple moderate risks = Cumulative concern
        moderate_risks = sum(1 for score in [event_score, pep_score, geo_score, rel_score] if 30 <= score <= 60)
        if moderate_risks >= 3:
            correlation_bonus += 8
            correlation_factors.append('multiple_moderate_risks')
        
        return {
            'correlation_bonus': correlation_bonus,
            'correlation_factors': correlation_factors,
            'risk_synergies': len(correlation_factors)
        }
    
    def _ensemble_risk_calculation(self, risk_factors: Dict[str, Any]) -> Dict[str, Any]:
        """ML-inspired ensemble calculation"""
        
        # Extract component scores
        event_score = risk_factors['events']['score']
        pep_score = risk_factors['pep']['score']
        geo_score = risk_factors['geographic']['score']
        rel_score = risk_factors['relationships']['score']
        temporal_factor = risk_factors['temporal']['decay_factor']
        behavior_score = risk_factors['behavior']['behavior_score']
        network_amp = risk_factors['network']['network_amplification']
        anomaly_score = risk_factors['anomalies']['anomaly_score']
        correlation_bonus = risk_factors.get('correlation_bonus', 0)
        
        # Advanced ensemble weights (based on feature importance)
        weights = {
            'events': 0.45,      # Highest weight - actual criminal activity
            'pep': 0.20,         # High weight - political exposure
            'geographic': 0.15,   # Moderate weight - jurisdictional risk
            'relationships': 0.10, # Lower weight - network effects
            'behavior': 0.05,     # Low weight - behavioral patterns
            'anomalies': 0.05     # Low weight - anomaly detection
        }
        
        # Calculate weighted base score
        base_score = (
            event_score * weights['events'] +
            pep_score * weights['pep'] +
            geo_score * weights['geographic'] +
            rel_score * weights['relationships'] +
            behavior_score * weights['behavior'] +
            anomaly_score * weights['anomalies']
        )
        
        # Apply temporal decay
        temporally_adjusted = base_score * temporal_factor
        
        # Apply network amplification
        network_adjusted = temporally_adjusted * network_amp
        
        # Add correlation bonus
        final_score = network_adjusted + correlation_bonus
        
        # Ensure score is within bounds
        final_score = max(0, min(100, final_score))
        
        return {
            'total_score': final_score,
            'components': {
                'event_score': event_score,
                'pep_score': pep_score,
                'geographic_score': geo_score,
                'relationship_score': rel_score,
                'behavior_score': behavior_score,
                'anomaly_score': anomaly_score,
                'temporal_factor': temporal_factor,
                'network_amplification': network_amp,
                'correlation_bonus': correlation_bonus
            },
            'feature_importance': weights,
            'calculation_steps': {
                'base_weighted_score': base_score,
                'temporal_adjustment': temporally_adjusted,
                'network_adjustment': network_adjusted,
                'final_with_correlations': final_score
            }
        }
    
    def _calculate_confidence_metrics(self, entity_data: Dict[str, Any], score: float) -> Dict[str, Any]:
        """Calculate confidence in the risk score"""
        
        data_completeness = self._assess_data_completeness(entity_data)
        
        # Base confidence from data completeness
        base_confidence = data_completeness['completeness_percentage']
        
        # Adjust confidence based on score extremes
        if score > 80 or score < 20:
            # Very high or very low scores are more confident
            extreme_confidence_boost = 10
        else:
            extreme_confidence_boost = 0
        
        # Event count confidence
        event_count = len(entity_data.get('events', []))
        if event_count > 5:
            event_confidence = 10
        elif event_count > 2:
            event_confidence = 5
        else:
            event_confidence = 0
        
        overall_confidence = min(100, base_confidence + extreme_confidence_boost + event_confidence)
        
        return {
            'overall_confidence': overall_confidence,
            'data_completeness_factor': base_confidence,
            'extreme_score_boost': extreme_confidence_boost,
            'event_count_boost': event_confidence,
            'confidence_level': 'high' if overall_confidence > 80 else 'medium' if overall_confidence > 60 else 'low'
        }
    
    def _predict_risk_trajectory(self, events: List[Dict], current_score: float) -> Dict[str, Any]:
        """Predict risk trajectory based on event patterns"""
        
        if not events:
            return {'trend': 'stable', 'predicted_change': 0}
        
        # Analyze event recency trends
        recent_events = []
        older_events = []
        cutoff_date = datetime.now() - timedelta(days=365*2)  # 2 years ago
        
        for event in events:
            event_date_str = event.get('event_date')
            if event_date_str:
                try:
                    event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
                    if event_date > cutoff_date:
                        recent_events.append(event)
                    else:
                        older_events.append(event)
                except:
                    continue
        
        # Calculate trend
        if len(recent_events) > len(older_events):
            trend = 'increasing'
            predicted_change = 5  # Risk likely to increase
        elif len(recent_events) < len(older_events):
            trend = 'decreasing'
            predicted_change = -3  # Risk likely to decrease
        else:
            trend = 'stable'
            predicted_change = 0
        
        return {
            'trend': trend,
            'predicted_change': predicted_change,
            'recent_events': len(recent_events),
            'historical_events': len(older_events),
            'trajectory_confidence': 'medium'
        }
    
    def _advanced_risk_classification(self, score: float, confidence: Dict[str, Any]) -> Dict[str, Any]:
        """Advanced risk classification with uncertainty bounds"""
        
        # Get base classification
        thresholds = self.config.get('risk_thresholds')
        confidence_level = confidence['overall_confidence']
        
        # Calculate uncertainty range based on confidence
        if confidence_level > 80:
            uncertainty = 2  # High confidence, low uncertainty
        elif confidence_level > 60:
            uncertainty = 5  # Medium confidence, medium uncertainty
        else:
            uncertainty = 8  # Low confidence, high uncertainty
        
        lower_bound = max(0, score - uncertainty)
        upper_bound = min(100, score + uncertainty)
        
        # Base classification
        if score >= thresholds['critical']['min']:
            level = 'Critical'
            color = thresholds['critical']['color']
            description = thresholds['critical']['description']
        elif score >= thresholds['valuable']['min']:
            level = 'Valuable'
            color = thresholds['valuable']['color']
            description = thresholds['valuable']['description']
        elif score >= thresholds['investigative']['min']:
            level = 'Investigative'
            color = thresholds['investigative']['color']
            description = thresholds['investigative']['description']
        else:
            level = 'Probative'
            color = thresholds['probative']['color']
            description = thresholds['probative']['description']
        
        return {
            'level': level,
            'color': color,
            'description': description,
            'uncertainty_range': {
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'uncertainty': uncertainty
            },
            'confidence_qualified_level': f"{level} ({confidence['confidence_level']} confidence)"
        }
    
    def _assess_data_completeness(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess completeness of entity data"""
        
        required_fields = ['events', 'attributes', 'addresses']
        optional_fields = ['relationships', 'aliases', 'identifications']
        
        field_scores = {}
        total_score = 0
        max_score = 0
        
        for field in required_fields:
            max_score += 20  # Required fields worth 20 points each
            data = entity_data.get(field, [])
            if data and len(data) > 0:
                field_scores[field] = 20
                total_score += 20
            else:
                field_scores[field] = 0
        
        for field in optional_fields:
            max_score += 10  # Optional fields worth 10 points each
            data = entity_data.get(field, [])
            if data and len(data) > 0:
                field_scores[field] = 10
                total_score += 10
            else:
                field_scores[field] = 0
        
        completeness_percentage = (total_score / max_score) * 100 if max_score > 0 else 0
        
        return {
            'completeness_percentage': completeness_percentage,
            'field_scores': field_scores,
            'missing_fields': [field for field, score in field_scores.items() if score == 0],
            'data_quality': 'excellent' if completeness_percentage > 80 else 'good' if completeness_percentage > 60 else 'fair' if completeness_percentage > 40 else 'poor'
        }
    
    def _store_calculation_history(self, entity_data: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Store calculation for pattern learning"""
        
        history_entry = {
            'timestamp': datetime.now().isoformat(),
            'entity_id': entity_data.get('entity_id', 'unknown'),
            'final_score': result['total_score'],
            'confidence': result['confidence_score'],
            'data_completeness': result['calculation_metadata']['data_completeness']['completeness_percentage']
        }
        
        self.calculation_history.append(history_entry)
        
        # Keep only last 1000 calculations
        if len(self.calculation_history) > 1000:
            self.calculation_history = self.calculation_history[-1000:]
    
    # Helper methods for advanced calculations
    def _calculate_category_synergy(self, category: str, sub_category: str) -> float:
        """Calculate synergy between category and sub-category"""
        
        # High-impact combinations
        high_synergy = {
            ('TER', 'CVT'): 1.2,  # Convicted terrorist
            ('MLA', 'SAN'): 1.2,  # Sanctioned money launderer
            ('ORG', 'ART'): 1.1,  # Arrested organized crime
            ('DTF', 'IND'): 1.1,  # Indicted drug trafficker
        }
        
        return high_synergy.get((category, sub_category), 1.0)
    
    def _calculate_advanced_temporal_weight(self, event_date: str, category: str) -> float:
        """Advanced temporal weighting based on event type"""
        
        try:
            event_dt = datetime.strptime(event_date, '%Y-%m-%d')
            years_ago = (datetime.now() - event_dt).days / 365.25
            
            # Different decay rates for different event types
            if category in ['TER', 'WLT', 'DEN']:  # Never fully decay critical events
                min_weight = 0.5
                decay_rate = 0.05
            elif category in ['MLA', 'DTF', 'ORG']:  # Slow decay for serious crimes
                min_weight = 0.3
                decay_rate = 0.08
            else:  # Normal decay for other events
                min_weight = 0.1
                decay_rate = 0.12
            
            weight = max(1.0 - (years_ago * decay_rate), min_weight)
            return weight
            
        except:
            return 1.0
    
    def _analyze_temporal_clustering(self, event_timeline: List[Dict]) -> Dict[str, Any]:
        """Analyze temporal clustering of events"""
        
        if len(event_timeline) < 2:
            return {'clusters': 0, 'clustering_score': 0}
        
        # Sort by date
        sorted_events = sorted(event_timeline, key=lambda x: x['date'])
        
        clusters = 0
        current_cluster_size = 1
        clustering_score = 0
        
        for i in range(1, len(sorted_events)):
            try:
                current_date = datetime.strptime(sorted_events[i]['date'], '%Y-%m-%d')
                prev_date = datetime.strptime(sorted_events[i-1]['date'], '%Y-%m-%d')
                days_diff = (current_date - prev_date).days
                
                if days_diff <= 90:  # Events within 90 days are clustered
                    current_cluster_size += 1
                else:
                    if current_cluster_size > 1:
                        clusters += 1
                        clustering_score += current_cluster_size * 5  # Points for cluster size
                    current_cluster_size = 1
            except:
                continue
        
        # Check final cluster
        if current_cluster_size > 1:
            clusters += 1
            clustering_score += current_cluster_size * 5
        
        return {
            'clusters': clusters,
            'clustering_score': min(clustering_score, 50),  # Cap at 50
            'pattern': 'clustered' if clusters > 0 else 'distributed'
        }
    
    def _detect_escalation_patterns(self, event_timeline: List[Dict]) -> Dict[str, Any]:
        """Detect escalation patterns in events"""
        
        if len(event_timeline) < 2:
            return {'escalation': False, 'pattern': 'insufficient_data'}
        
        # Sort by date and analyze score progression
        sorted_events = sorted(event_timeline, key=lambda x: x['date'])
        scores = [event['score'] for event in sorted_events]
        
        # Look for increasing trend
        increasing_count = 0
        decreasing_count = 0
        
        for i in range(1, len(scores)):
            if scores[i] > scores[i-1]:
                increasing_count += 1
            elif scores[i] < scores[i-1]:
                decreasing_count += 1
        
        if increasing_count > decreasing_count and increasing_count > len(scores) * 0.6:
            return {'escalation': True, 'pattern': 'escalating', 'trend_strength': increasing_count / len(scores)}
        elif decreasing_count > increasing_count and decreasing_count > len(scores) * 0.6:
            return {'escalation': False, 'pattern': 'de-escalating', 'trend_strength': decreasing_count / len(scores)}
        else:
            return {'escalation': False, 'pattern': 'stable', 'trend_strength': 0.5}
    
    def _calculate_event_diversity_factor(self, category_patterns: Dict[str, int]) -> float:
        """Calculate diversity factor for event categories"""
        
        if not category_patterns:
            return 1.0
        
        total_events = sum(category_patterns.values())
        unique_categories = len(category_patterns)
        
        # Diversity can be good (varied criminal activity) or bad (specialized criminal)
        if unique_categories == 1:
            # Specialized in one type of crime
            return 1.1  # Slight increase for specialization
        elif unique_categories > 5:
            # Very diverse criminal activity
            return 1.2  # Increase for criminal versatility
        else:
            # Moderate diversity
            return 1.0
    
    def _ensemble_event_scoring(self, max_score: float, avg_score: float, 
                               variance: float, diversity_factor: float,
                               escalation: Dict, clustering: Dict) -> float:
        """Ensemble method for event scoring"""
        
        # Start with maximum score (most important factor)
        base_score = max_score * 0.7
        
        # Add average score component (for volume effect)
        volume_component = avg_score * 0.2
        
        # Add variance component (high variance = unpredictable behavior)
        variance_component = min(math.sqrt(variance) * 0.1, 10)
        
        # Apply diversity factor
        diversity_adjusted = (base_score + volume_component + variance_component) * diversity_factor
        
        # Apply escalation bonus
        if escalation.get('escalation', False):
            escalation_bonus = 10 * escalation.get('trend_strength', 0)
        else:
            escalation_bonus = 0
        
        # Apply clustering bonus
        clustering_bonus = clustering.get('clustering_score', 0) * 0.2
        
        final_score = diversity_adjusted + escalation_bonus + clustering_bonus
        
        return min(final_score, 100)  # Cap at 100
    
    def _fuzzy_match_pep_type(self, value: str, pep_code: str) -> bool:
        """Fuzzy matching for PEP types in text"""
        
        pep_keywords = {
            'HOS': ['head of state', 'president', 'prime minister', 'king', 'queen'],
            'CAB': ['cabinet', 'minister', 'secretary'],
            'MIL': ['military', 'general', 'admiral', 'colonel'],
            'JUD': ['judge', 'judicial', 'court', 'justice'],
            'FAM': ['family', 'wife', 'husband', 'son', 'daughter', 'father', 'mother'],
            'ASC': ['associate', 'friend', 'advisor', 'consultant']
        }
        
        keywords = pep_keywords.get(pep_code, [])
        value_lower = value.lower()
        
        return any(keyword in value_lower for keyword in keywords)
    
    def _detect_temporal_clusters(self, event_dates: List[datetime]) -> Dict[str, Any]:
        """Detect temporal clusters in event dates"""
        
        if len(event_dates) < 2:
            return {'cluster_count': 0, 'largest_cluster': 0}
        
        sorted_dates = sorted(event_dates)
        clusters = []
        current_cluster = [sorted_dates[0]]
        
        for i in range(1, len(sorted_dates)):
            days_diff = (sorted_dates[i] - sorted_dates[i-1]).days
            
            if days_diff <= 180:  # 6 months clustering window
                current_cluster.append(sorted_dates[i])
            else:
                if len(current_cluster) > 1:
                    clusters.append(current_cluster)
                current_cluster = [sorted_dates[i]]
        
        # Add final cluster
        if len(current_cluster) > 1:
            clusters.append(current_cluster)
        
        return {
            'cluster_count': len(clusters),
            'largest_cluster': max(len(cluster) for cluster in clusters) if clusters else 0,
            'total_clustered_events': sum(len(cluster) for cluster in clusters)
        }

# Global advanced calculation engine instance
advanced_calculation_engine = AdvancedRiskCalculationEngine()