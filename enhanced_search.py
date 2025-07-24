"""
Enhanced Search Module with Proper Database Integration
Fixes all data retrieval and classification issues
"""
import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from databricks import sql
import os
import json
from database_queries import DatabaseQueries

logger = logging.getLogger(__name__)

class EnhancedEntitySearch:
    """Enhanced search with proper GRID database integration"""
    
    def __init__(self, connection=None):
        self.connection = connection
        self.db_queries = DatabaseQueries()
        self.query_cache = {}
        self.cache_ttl = 300  # 5 minutes
        
    def set_connection(self, connection):
        """Set database connection"""
        self.connection = connection
    
    async def search_entities(self, search_params: Dict) -> List[Dict]:
        """
        Execute comprehensive entity search with proper data retrieval
        
        Args:
            search_params: Dictionary containing search criteria
                - entity_type: 'individual' or 'organization'
                - name: Entity name to search
                - country: Country filter
                - risk_level: Risk level filter ('Critical', 'Valuable', etc.)
                - pep_only: Boolean to filter only PEPs
                - event_categories: List of event category codes
                - date_from: Start date for events
                - date_to: End date for events
                - limit: Maximum results (default 100)
        
        Returns:
            List of processed entity results with proper classifications
        """
        try:
            # Default entity type
            entity_type = search_params.get('entity_type', 'individual').lower()
            
            # Build comprehensive query
            query, params = self.db_queries.build_comprehensive_query(entity_type, search_params)
            
            # Check cache
            cache_key = f"{query}:{json.dumps(params, sort_keys=True)}"
            cached_result = self._get_cached_result(cache_key)
            if cached_result is not None:
                logger.info("Returning cached results")
                return cached_result
            
            # Execute query
            logger.info(f"Executing search query for {entity_type} entities")
            cursor = self.connection.cursor()
            
            # Convert params for Databricks SQL (uses ? placeholders)
            query_formatted = self._format_query_for_databricks(query, params)
            cursor.execute(query_formatted)
            
            # Fetch raw results
            raw_results = []
            columns = [desc[0] for desc in cursor.description]
            
            for row in cursor:
                raw_results.append(dict(zip(columns, row)))
            
            cursor.close()
            
            # Process results
            processed_results = self.db_queries.process_search_results(raw_results)
            
            # Apply post-processing filters
            filtered_results = self._apply_post_filters(processed_results, search_params)
            
            # Cache results
            self._cache_result(cache_key, filtered_results)
            
            logger.info(f"Found {len(filtered_results)} entities matching criteria")
            return filtered_results
            
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            raise
    
    def _format_query_for_databricks(self, query: str, params: Dict) -> str:
        """Convert parameterized query to Databricks format"""
        # Databricks uses direct string formatting
        for param_name, param_value in params.items():
            placeholder = f"%({param_name})s"
            if isinstance(param_value, str):
                value = f"'{param_value}'"
            elif isinstance(param_value, (list, tuple)):
                value = f"({','.join(repr(v) for v in param_value)})"
            else:
                value = str(param_value)
            query = query.replace(placeholder, value)
        return query
    
    def _apply_post_filters(self, results: List[Dict], search_params: Dict) -> List[Dict]:
        """Apply filters that couldn't be done in SQL"""
        filtered = results
        
        # Filter by risk level
        if search_params.get('risk_level'):
            risk_level = search_params['risk_level']
            filtered = [r for r in filtered if r['risk_level'] == risk_level]
        
        # Filter by risk score range
        if search_params.get('risk_score_min'):
            min_score = search_params['risk_score_min']
            filtered = [r for r in filtered if r['risk_score'] >= min_score]
        
        if search_params.get('risk_score_max'):
            max_score = search_params['risk_score_max']
            filtered = [r for r in filtered if r['risk_score'] <= max_score]
        
        # Filter by minimum relationships
        if search_params.get('min_relationships'):
            min_rels = search_params['min_relationships']
            filtered = [r for r in filtered if len(r.get('relationships', [])) >= min_rels]
        
        # Filter by recent activity
        if search_params.get('recent_activity_days'):
            days = search_params['recent_activity_days']
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered = [
                r for r in filtered
                if any(
                    self._parse_date(event.get('event_date')) >= cutoff_date
                    for event in r.get('events', [])
                    if event.get('event_date')
                )
            ]
        
        # Sort by risk score (highest first)
        filtered.sort(key=lambda x: x['risk_score'], reverse=True)
        
        return filtered
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime"""
        if not date_str:
            return datetime.min
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except:
                return datetime.min
    
    def _get_cached_result(self, cache_key: str) -> Optional[List[Dict]]:
        """Get cached result if valid"""
        if cache_key in self.query_cache:
            cached_data, timestamp = self.query_cache[cache_key]
            if datetime.now() - timestamp < timedelta(seconds=self.cache_ttl):
                return cached_data
            else:
                del self.query_cache[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, result: List[Dict]):
        """Cache search result"""
        self.query_cache[cache_key] = (result, datetime.now())
        
        # Clean old cache entries
        if len(self.query_cache) > 100:
            oldest_keys = sorted(
                self.query_cache.keys(),
                key=lambda k: self.query_cache[k][1]
            )[:20]
            for key in oldest_keys:
                del self.query_cache[key]
    
    async def get_entity_details(self, entity_id: str, entity_type: str = 'individual') -> Dict:
        """Get comprehensive details for a specific entity"""
        search_params = {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'limit': 1
        }
        
        results = await self.search_entities(search_params)
        return results[0] if results else None
    
    async def search_by_name_and_country(self, name: str, country: Optional[str] = None, 
                                       entity_type: str = 'individual', 
                                       pep_only: bool = False) -> List[Dict]:
        """Convenience method for name/country search"""
        search_params = {
            'entity_type': entity_type,
            'name': name,
            'pep_only': pep_only,
            'limit': 100
        }
        
        if country:
            search_params['country'] = country
        
        return await self.search_entities(search_params)
    
    async def search_high_risk_entities(self, 
                                      min_risk_score: int = 80,
                                      event_categories: Optional[List[str]] = None,
                                      days_back: int = 30) -> List[Dict]:
        """Search for high-risk entities with recent activity"""
        search_params = {
            'entity_type': 'individual',
            'risk_score_min': min_risk_score,
            'recent_activity_days': days_back,
            'limit': 500
        }
        
        if event_categories:
            search_params['event_categories'] = event_categories
        
        # Search both individuals and organizations
        individuals = await self.search_entities(search_params)
        
        search_params['entity_type'] = 'organization'
        organizations = await self.search_entities(search_params)
        
        # Combine and sort by risk score
        all_results = individuals + organizations
        all_results.sort(key=lambda x: x['risk_score'], reverse=True)
        
        return all_results[:500]  # Limit total results
    
    async def get_analytics_data(self, entity_type: str = 'individual', 
                               filters: Optional[Dict] = None) -> Dict:
        """Get analytics and aggregation data"""
        try:
            filters = filters or {}
            query, params = self.db_queries.build_analytics_query(entity_type, filters)
            
            cursor = self.connection.cursor()
            query_formatted = self._format_query_for_databricks(query, params)
            cursor.execute(query_formatted)
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                columns = [desc[0] for desc in cursor.description]
                analytics_data = dict(zip(columns, result))
                
                # Parse JSON fields
                for field in ['event_distribution', 'country_distribution']:
                    if analytics_data.get(field) and isinstance(analytics_data[field], str):
                        try:
                            analytics_data[field] = json.loads(analytics_data[field])
                        except:
                            analytics_data[field] = []
                
                return analytics_data
            
            return {
                'total_entities': 0,
                'pep_count': 0,
                'unique_event_types': 0,
                'unique_countries': 0,
                'event_distribution': [],
                'country_distribution': []
            }
            
        except Exception as e:
            logger.error(f"Analytics query error: {str(e)}")
            return {}
    
    def get_risk_category_info(self, event_code: str) -> Dict:
        """Get information about a risk category"""
        base_score = self.db_queries.event_risk_scores.get(event_code, 0)
        
        if base_score >= 80:
            severity = 'Critical'
            color = 'red'
        elif base_score >= 60:
            severity = 'Valuable'
            color = 'orange'
        elif base_score >= 40:
            severity = 'Investigative'
            color = 'yellow'
        else:
            severity = 'Probative'
            color = 'blue'
        
        return {
            'code': event_code,
            'base_score': base_score,
            'severity': severity,
            'color': color
        }
    
    def get_pep_type_info(self, pep_code: str) -> Dict:
        """Get information about a PEP type"""
        return {
            'code': pep_code,
            'description': self.db_queries.pep_types.get(pep_code, pep_code),
            'is_family': pep_code == 'FAM',
            'is_associate': pep_code == 'ASC',
            'is_government': pep_code in ['HOS', 'CAB', 'INF', 'NIO', 'MUN', 'REG', 'LEG', 'AMB', 'MIL', 'JUD'],
            'is_business': pep_code in ['GOE', 'GCO'],
            'is_political': pep_code == 'POL',
            'is_international': pep_code in ['IGO', 'ISO']
        }