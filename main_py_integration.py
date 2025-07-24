"""
Main.py Integration Module
Modular replacement for all incorrect database methods in main.py

This module provides drop-in replacements for methods in main.py that have
incorrect database understanding. Import this module and replace the 
incorrect methods to fix ALL database issues.

Usage in main.py:
from main_py_integration import MainPyIntegration

class EntitySearchApp:
    def __init__(self):
        # ... existing code ...
        self.integration = MainPyIntegration(self)
        
    # Replace incorrect methods with:
    def search_entities(self, search_params):
        return self.integration.search_entities_corrected(search_params)
"""

import asyncio
import logging
import json
import re
import ast
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from databricks import sql
from comprehensive_database_integration import ComprehensiveDatabaseIntegration

logger = logging.getLogger(__name__)

class BooleanQueryParser:
    """Advanced Boolean Query Parser for complex search expressions"""
    
    def __init__(self):
        # Supported operators
        self.operators = {
            'CONTAINS': 'LIKE',
            'STARTS_WITH': 'LIKE', 
            'ENDS_WITH': 'LIKE',
            'EQUALS': '=',
            'NOT_EQUALS': '!=',
            'GREATER_THAN': '>',
            'LESS_THAN': '<',
            'GREATER_EQUAL': '>=',
            'LESS_EQUAL': '<=',
            'IN': 'IN',
            'NOT_IN': 'NOT IN',
            'IS_NULL': 'IS NULL',
            'IS_NOT_NULL': 'IS NOT NULL',
            'REGEX': 'REGEXP_LIKE'
        }
        
        # Field mappings for boolean queries
        self.field_mappings = {
            'entity_name': 'm.entity_name',
            'name': 'm.entity_name',
            'entity_id': 'm.entity_id',
            'risk_id': 'm.risk_id',
            'country': 'addr.address_country',
            'city': 'addr.address_city',
            'source_id': 'm.source_item_id',
            'system_id': 'm.systemId',
            'pep_role': 'pep_attr.alias_value',
            'pep_level': 'pep_attr.alias_value',
            'pep_type': 'pep_attr.alias_value',
            'event_category': 'ev.event_category_code',
            'event_subcategory': 'ev.event_sub_category_code',
            'birth_year': 'dob.date_of_birth_year',
            'birth_month': 'dob.date_of_birth_month',
            'address_type': 'addr.address_type',
            'bvd_id': 'bvd.bvdid'
        }

    def parse_boolean_expression(self, expression: str) -> Dict[str, Any]:
        """
        Parse boolean expression into SQL-compatible conditions
        
        Supported syntax:
        - entity_name CONTAINS "John" AND country = "US"
        - (pep_role = "MUN" OR pep_role = "LEG") AND pep_level CONTAINS "L3"
        - NOT event_category = "BRB" AND birth_year >= 1980
        - entity_name REGEX "^John.*Smith$"
        """
        try:
            # Clean and normalize expression
            cleaned_expr = self._clean_expression(expression)
            
            # Tokenize the expression
            tokens = self._tokenize(cleaned_expr)
            
            # Parse into conditions and logical operators
            parsed = self._parse_tokens(tokens)
            
            # Convert to SQL conditions
            sql_conditions = self._build_sql_conditions(parsed)
            
            return {
                'success': True,
                'sql_conditions': sql_conditions,
                'required_joins': self._get_required_joins(parsed),
                'parameters': self._extract_parameters(parsed),
                'original_expression': expression
            }
            
        except Exception as e:
            logger.error(f"Boolean query parsing error: {e}")
            return {
                'success': False,
                'error': str(e),
                'sql_conditions': [],
                'required_joins': [],
                'parameters': [],
                'original_expression': expression
            }

    def _clean_expression(self, expr: str) -> str:
        """Clean and normalize boolean expression"""
        # Remove extra whitespace
        expr = ' '.join(expr.split())
        
        # Normalize boolean operators
        expr = re.sub(r'\bAND\b', ' AND ', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bOR\b', ' OR ', expr, flags=re.IGNORECASE)
        expr = re.sub(r'\bNOT\b', ' NOT ', expr, flags=re.IGNORECASE)
        
        # Normalize parentheses
        expr = re.sub(r'\s*\(\s*', ' ( ', expr)
        expr = re.sub(r'\s*\)\s*', ' ) ', expr)
        
        return expr.strip()

    def _tokenize(self, expr: str) -> List[str]:
        """Tokenize boolean expression"""
        # Split on spaces but preserve quoted strings
        tokens = []
        current_token = ''
        in_quotes = False
        quote_char = None
        
        i = 0
        while i < len(expr):
            char = expr[i]
            
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
                current_token += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                current_token += char
                quote_char = None
            elif char == ' ' and not in_quotes:
                if current_token.strip():
                    tokens.append(current_token.strip())
                    current_token = ''
            else:
                current_token += char
            
            i += 1
        
        if current_token.strip():
            tokens.append(current_token.strip())
        
        return tokens

    def _parse_tokens(self, tokens: List[str]) -> List[Dict]:
        """Parse tokens into structured conditions"""
        conditions = []
        i = 0
        
        while i < len(tokens):
            token = tokens[i]
            
            # Handle logical operators
            if token.upper() in ['AND', 'OR', 'NOT']:
                conditions.append({
                    'type': 'operator',
                    'operator': token.upper()
                })
                i += 1
                
            # Handle parentheses
            elif token in ['(', ')']:
                conditions.append({
                    'type': 'parenthesis',
                    'value': token
                })
                i += 1
                
            # Handle field conditions
            elif i + 2 < len(tokens) and tokens[i + 1].upper() in self.operators:
                field = token
                operator = tokens[i + 1].upper()
                value = tokens[i + 2]
                
                conditions.append({
                    'type': 'condition',
                    'field': field,
                    'operator': operator,
                    'value': self._clean_value(value)
                })
                i += 3
                
            else:
                i += 1
        
        return conditions

    def _clean_value(self, value: str) -> str:
        """Clean quoted values"""
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        elif value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        return value

    def _build_sql_conditions(self, parsed: List[Dict]) -> List[str]:
        """Convert parsed conditions to SQL"""
        sql_parts = []
        
        for item in parsed:
            if item['type'] == 'condition':
                sql_condition = self._build_single_condition(item)
                sql_parts.append(sql_condition)
            elif item['type'] == 'operator':
                sql_parts.append(item['operator'])
            elif item['type'] == 'parenthesis':
                sql_parts.append(item['value'])
        
        return sql_parts

    def _build_single_condition(self, condition: Dict) -> str:
        """Build single SQL condition"""
        field = condition['field'].lower()
        operator = condition['operator']
        value = condition['value']
        
        # Map field to database column
        db_field = self.field_mappings.get(field, field)
        
        # Handle special PEP fields
        if field in ['pep_role', 'pep_type']:
            return f"""EXISTS (
                SELECT 1 FROM prd_bronze_catalog.grid.individual_attributes pep_attr
                WHERE pep_attr.entity_id = m.entity_id 
                AND pep_attr.alias_code_type = 'PTY'
                AND {self._build_operator_condition('pep_attr.alias_value', operator, value)}
            )"""
            
        elif field == 'pep_level':
            return f"""EXISTS (
                SELECT 1 FROM prd_bronze_catalog.grid.individual_attributes pep_attr
                WHERE pep_attr.entity_id = m.entity_id 
                AND pep_attr.alias_code_type = 'PTY'
                AND pep_attr.alias_value LIKE '%:{value}%'
            )"""
            
        elif field in ['country', 'city', 'address_type']:
            return f"""EXISTS (
                SELECT 1 FROM prd_bronze_catalog.grid.individual_addresses addr
                WHERE addr.entity_id = m.entity_id 
                AND {self._build_operator_condition(self.field_mappings[field], operator, value)}
            )"""
            
        elif field in ['event_category', 'event_subcategory']:
            return f"""EXISTS (
                SELECT 1 FROM prd_bronze_catalog.grid.individual_events ev
                WHERE ev.entity_id = m.entity_id 
                AND {self._build_operator_condition(self.field_mappings[field], operator, value)}
            )"""
            
        elif field in ['birth_year', 'birth_month']:
            return f"""EXISTS (
                SELECT 1 FROM prd_bronze_catalog.grid.individual_date_of_births dob
                WHERE dob.entity_id = m.entity_id 
                AND {self._build_operator_condition(self.field_mappings[field], operator, value)}
            )"""
            
        else:
            # Direct field mapping
            return self._build_operator_condition(db_field, operator, value)

    def _build_operator_condition(self, db_field: str, operator: str, value: str) -> str:
        """Build operator-specific condition"""
        if operator == 'CONTAINS':
            return f"LOWER({db_field}) LIKE LOWER('%{value}%')"
        elif operator == 'STARTS_WITH':
            return f"LOWER({db_field}) LIKE LOWER('{value}%')"
        elif operator == 'ENDS_WITH':
            return f"LOWER({db_field}) LIKE LOWER('%{value}')"
        elif operator == 'EQUALS':
            return f"LOWER({db_field}) = LOWER('{value}')"
        elif operator == 'NOT_EQUALS':
            return f"LOWER({db_field}) != LOWER('{value}')"
        elif operator in ['GREATER_THAN', 'LESS_THAN', 'GREATER_EQUAL', 'LESS_EQUAL']:
            sql_op = self.operators[operator]
            return f"{db_field} {sql_op} '{value}'"
        elif operator == 'IN':
            values = [f"'{v.strip()}'" for v in value.split(',')]
            return f"{db_field} IN ({','.join(values)})"
        elif operator == 'NOT_IN':
            values = [f"'{v.strip()}'" for v in value.split(',')]
            return f"{db_field} NOT IN ({','.join(values)})"
        elif operator == 'IS_NULL':
            return f"{db_field} IS NULL"
        elif operator == 'IS_NOT_NULL':
            return f"{db_field} IS NOT NULL"
        elif operator == 'REGEX':
            return f"REGEXP_LIKE({db_field}, '{value}')"
        else:
            return f"{db_field} = '{value}'"

    def _get_required_joins(self, parsed: List[Dict]) -> List[str]:
        """Determine required table joins based on fields used"""
        joins = set()
        
        for item in parsed:
            if item['type'] == 'condition':
                field = item['field'].lower()
                
                if field in ['country', 'city', 'address_type']:
                    joins.add('addresses')
                elif field in ['event_category', 'event_subcategory']:
                    joins.add('events')
                elif field in ['birth_year', 'birth_month']:
                    joins.add('date_of_births')
                elif field in ['pep_role', 'pep_type', 'pep_level']:
                    joins.add('attributes')
                elif field == 'bvd_id':
                    joins.add('bvd_mapping')
        
        return list(joins)

    def _extract_parameters(self, parsed: List[Dict]) -> List[str]:
        """Extract parameter values for prepared statements"""
        params = []
        
        for item in parsed:
            if item['type'] == 'condition':
                params.append(item['value'])
        
        return params

class MainPyIntegration:
    """Main integration class that replaces incorrect methods in main.py"""
    
    def __init__(self, app_instance):
        """Initialize with reference to main EntitySearchApp instance"""
        self.app = app_instance
        self.db_integration = ComprehensiveDatabaseIntegration(app_instance.connection)
        self.boolean_parser = BooleanQueryParser()
        
        # Connect to app's connection updates
        if hasattr(app_instance, 'connection'):
            self.db_integration.connection = app_instance.connection

    def search_entities_corrected(self, search_params: Dict) -> List[Dict]:
        """
        CORRECTED search_entities method replacing main.py implementation
        
        Fixes ALL database understanding issues:
        - Correct PEP detection using PTY codes
        - Proper risk calculation from events  
        - Date of birth integration
        - Relationships extraction
        - Enhanced boolean search parsing
        """
        try:
            # Set connection if available
            if hasattr(self.app, 'connection') and self.app.connection:
                self.db_integration.connection = self.app.connection
            
            entity_type = search_params.get('entity_type', 'individual').lower()
            
            # Handle boolean query if present
            boolean_query = search_params.get('boolean_query', '').strip()
            if boolean_query:
                return self._execute_boolean_search(entity_type, boolean_query, search_params)
            
            # Build corrected query
            query, params = self.db_integration.build_comprehensive_search_query(
                entity_type, search_params
            )
            
            # Execute query
            cursor = self.app.connection.cursor()
            cursor.execute(query, params)
            
            # Get raw results
            raw_results = []
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                raw_results.append(dict(zip(columns, row)))
            
            cursor.close()
            
            # Process with ALL corrections
            processed_results = self.db_integration.process_comprehensive_results(raw_results)
            
            # Apply post-filters
            filtered_results = self._apply_post_filters_corrected(processed_results, search_params)
            
            logger.info(f"Search completed: {len(filtered_results)} results")
            return filtered_results
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []

    def _execute_boolean_search(self, entity_type: str, boolean_query: str, search_params: Dict) -> List[Dict]:
        """Execute boolean search with advanced parsing"""
        try:
            # Parse boolean expression
            parsed_query = self.boolean_parser.parse_boolean_expression(boolean_query)
            
            if not parsed_query['success']:
                logger.error(f"Boolean query parsing failed: {parsed_query.get('error')}")
                return []
            
            # Build query with boolean conditions
            query, params = self.db_integration.build_advanced_boolean_query(
                entity_type, boolean_query, search_params
            )
            
            # Execute query
            cursor = self.app.connection.cursor()
            cursor.execute(query, params)
            
            # Get results
            raw_results = []
            columns = [desc[0] for desc in cursor.description]
            for row in cursor:
                raw_results.append(dict(zip(columns, row)))
            
            cursor.close()
            
            # Process results
            return self.db_integration.process_comprehensive_results(raw_results)
            
        except Exception as e:
            logger.error(f"Boolean search error: {e}")
            return []

    def _apply_post_filters_corrected(self, results: List[Dict], search_params: Dict) -> List[Dict]:
        """Apply post-processing filters with corrected logic"""
        filtered = results
        
        # Risk score filter
        min_risk = search_params.get('min_risk_score', 0)
        max_risk = search_params.get('max_risk_score', 100)
        if min_risk > 0 or max_risk < 100:
            filtered = [r for r in filtered if min_risk <= r.get('risk_score', 0) <= max_risk]
        
        # PEP-only filter
        if search_params.get('pep_only'):
            filtered = [r for r in filtered if r.get('is_pep', False)]
        
        # Risk category filter
        risk_categories = search_params.get('risk_categories', [])
        if risk_categories:
            filtered = [r for r in filtered if r.get('risk_category') in risk_categories]
        
        # Relationship filter
        if search_params.get('has_relationships'):
            filtered = [r for r in filtered if r.get('relationship_count', 0) > 0]
        
        # Date range filter for entity dates
        date_from = search_params.get('date_from')
        date_to = search_params.get('date_to')
        if date_from or date_to:
            filtered = self._filter_by_date_range(filtered, date_from, date_to)
        
        # Sort by relevance (risk score descending)
        filtered.sort(key=lambda x: x.get('risk_score', 0), reverse=True)
        
        # Apply limit
        limit = search_params.get('limit', 100)
        return filtered[:limit]

    def _filter_by_date_range(self, results: List[Dict], date_from: str, date_to: str) -> List[Dict]:
        """Filter results by date range"""
        filtered = []
        
        for result in results:
            entity_date = result.get('entity_date')
            if not entity_date:
                continue
                
            try:
                if isinstance(entity_date, str):
                    entity_date = datetime.strptime(entity_date, '%Y-%m-%d').date()
                elif isinstance(entity_date, datetime):
                    entity_date = entity_date.date()
                
                include_result = True
                
                if date_from:
                    from_date = datetime.strptime(date_from, '%Y-%m-%d').date()
                    if entity_date < from_date:
                        include_result = False
                
                if date_to and include_result:
                    to_date = datetime.strptime(date_to, '%Y-%m-%d').date()
                    if entity_date > to_date:
                        include_result = False
                
                if include_result:
                    filtered.append(result)
                    
            except (ValueError, TypeError):
                # Include results with invalid dates
                filtered.append(result)
        
        return filtered

    def extract_pep_info_corrected(self, attributes: List[Dict], events: List[Dict] = None) -> Dict[str, Any]:
        """CORRECTED PEP extraction replacing main.py implementation"""
        if events is None:
            events = []
        return self.db_integration.extract_comprehensive_pep_info(attributes, events)

    def calculate_risk_score_corrected(self, events: List[Dict], pep_info: Dict = None) -> Dict[str, Any]:
        """CORRECTED risk calculation replacing main.py implementation"""
        if pep_info is None:
            pep_info = {'is_pep': False, 'risk_multiplier': 1.0}
        return self.db_integration.calculate_comprehensive_risk_score(events, pep_info)

    def build_query_corrected(self, entity_type: str, search_params: Dict) -> Tuple[str, List]:
        """CORRECTED query building replacing main.py implementation"""
        return self.db_integration.build_comprehensive_search_query(entity_type, search_params)

    def validate_boolean_query(self, query: str) -> Dict[str, Any]:
        """Validate boolean query syntax"""
        parsed = self.boolean_parser.parse_boolean_expression(query)
        
        return {
            'valid': parsed['success'],
            'error': parsed.get('error'),
            'required_joins': parsed.get('required_joins', []),
            'field_count': len([p for p in parsed.get('sql_conditions', []) if 'EXISTS' in str(p)]),
            'suggestions': self._get_query_suggestions(query) if not parsed['success'] else []
        }

    def _get_query_suggestions(self, query: str) -> List[str]:
        """Provide query suggestions for invalid queries"""
        suggestions = []
        
        # Common field suggestions
        if 'name' in query.lower():
            suggestions.append('Try: entity_name CONTAINS "value"')
        if 'pep' in query.lower():
            suggestions.append('Try: pep_role = "MUN" AND pep_level = "L3"')
        if 'country' in query.lower():
            suggestions.append('Try: country = "United States"')
        if 'event' in query.lower():
            suggestions.append('Try: event_category = "BRB"')
        
        # Syntax suggestions
        suggestions.extend([
            'Use quotes around values: field = "value"',
            'Combine with AND/OR: condition1 AND condition2',
            'Use parentheses for grouping: (A OR B) AND C',
            'Available operators: CONTAINS, EQUALS, IN, GREATER_THAN'
        ])
        
        return suggestions

    def get_export_data_corrected(self, results: List[Dict], format_type: str = 'csv') -> Dict[str, Any]:
        """CORRECTED export data generation with all fixed fields"""
        export_data = []
        
        for result in results:
            export_summary = result.get('export_summary', {})
            
            # Enhanced export record with ALL corrected data
            export_record = {
                'Entity_ID': export_summary.get('Entity_ID', ''),
                'Risk_ID': export_summary.get('Risk_ID', ''),
                'Entity_Name': export_summary.get('Entity_Name', ''),
                'Entity_Type': export_summary.get('Entity_Type', ''),
                
                # CORRECTED: Complete PEP data
                'Is_PEP': export_summary.get('Is_PEP', 'No'),
                'PEP_Roles': export_summary.get('PEP_Roles', ''),
                'PEP_Levels': export_summary.get('PEP_Levels', ''),
                'PEP_Descriptions': export_summary.get('PEP_Descriptions', ''),
                'PEP_Associations': export_summary.get('PEP_Associations', ''),
                'PEP_Companies': export_summary.get('PEP_Companies', ''),
                'PRT_Codes': export_summary.get('PRT_Codes', ''),
                
                # CORRECTED: Complete risk data
                'Risk_Score': export_summary.get('Risk_Score', 0),
                'Base_Risk_Score': export_summary.get('Base_Risk_Score', 0),
                'Risk_Category': export_summary.get('Risk_Category', ''),
                'PEP_Risk_Multiplier': export_summary.get('PEP_Risk_Multiplier', 1.0),
                
                # CORRECTED: Date of birth data
                'Birth_Date': export_summary.get('Birth_Date', ''),
                'Birth_Year': export_summary.get('Birth_Year', ''),
                'Birth_Circa': export_summary.get('Birth_Circa', 'No'),
                
                # CORRECTED: Relationships data
                'Relationship_Count': export_summary.get('Relationship_Count', 0),
                'Relationships': export_summary.get('Relationships', ''),
                
                # Additional fields
                'BVD_ID': export_summary.get('BVD_ID', ''),
                'Source_ID': export_summary.get('Source_ID', ''),
                'System_ID': export_summary.get('System_ID', ''),
                'Entity_Date': export_summary.get('Entity_Date', ''),
                
                # Event details
                'Event_Count': len(result.get('events', [])),
                'Event_Categories': '; '.join(list(set([e.get('event_category_code', '') for e in result.get('events', []) if e.get('event_category_code')]))),
                'Has_PEP_Events': 'Yes' if any(e.get('event_category_code') == 'PEP' for e in result.get('events', [])) else 'No',
                
                # Address details
                'Countries': '; '.join(list(set([a.get('address_country', '') for a in result.get('addresses', []) if a.get('address_country')]))),
                'Cities': '; '.join(list(set([a.get('address_city', '') for a in result.get('addresses', []) if a.get('address_city')])))
            }
            
            export_data.append(export_record)
        
        return {
            'data': export_data,
            'total_records': len(export_data),
            'format': format_type,
            'timestamp': datetime.now().isoformat(),
            'columns': list(export_data[0].keys()) if export_data else []
        }