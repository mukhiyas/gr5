-- COMPREHENSIVE DEBUG QUERIES FOR GR3 ENTITY SEARCH
-- Run these queries to understand the actual data structure and patterns
-- for all filters including PEP, boolean, and clustering analysis

-- =============================================================================
-- 1. PEP RATING (PRT) DATA STRUCTURE ANALYSIS
-- =============================================================================

-- Query 1A: PEP Rating patterns and distribution
SELECT 
    'PRT_PATTERNS' as query_type,
    alias_code_type,
    alias_value,
    COUNT(DISTINCT entity_id) as entity_count,
    LEFT(alias_value, 10) as pattern_sample
FROM prd_bronze_catalog.grid.individual_attributes 
WHERE alias_code_type = 'PRT'
GROUP BY alias_code_type, alias_value
ORDER BY entity_count DESC
LIMIT 50;

-- Query 1B: PEP Rating letter distribution (A,B,C,D)
SELECT 
    'PRT_LETTERS' as query_type,
    CASE 
        WHEN alias_value LIKE 'A%' THEN 'A'
        WHEN alias_value LIKE 'B%' THEN 'B' 
        WHEN alias_value LIKE 'C%' THEN 'C'
        WHEN alias_value LIKE 'D%' THEN 'D'
        ELSE 'OTHER'
    END as rating_letter,
    COUNT(DISTINCT entity_id) as entity_count,
    COLLECT_SET(LEFT(alias_value, 20)) as sample_values
FROM prd_bronze_catalog.grid.individual_attributes 
WHERE alias_code_type = 'PRT'
GROUP BY CASE 
    WHEN alias_value LIKE 'A%' THEN 'A'
    WHEN alias_value LIKE 'B%' THEN 'B' 
    WHEN alias_value LIKE 'C%' THEN 'C'
    WHEN alias_value LIKE 'D%' THEN 'D'
    ELSE 'OTHER'
END
ORDER BY entity_count DESC;

-- =============================================================================
-- 2. PEP LEVEL (PTY) DATA STRUCTURE ANALYSIS  
-- =============================================================================

-- Query 2A: PEP Level patterns and distribution
SELECT 
    'PTY_PATTERNS' as query_type,
    alias_code_type,
    alias_value,
    COUNT(DISTINCT entity_id) as entity_count
FROM prd_bronze_catalog.grid.individual_attributes 
WHERE alias_code_type = 'PTY'
GROUP BY alias_code_type, alias_value
ORDER BY entity_count DESC
LIMIT 50;

-- Query 2B: PEP Level categories (HOS, CAB, etc.) with L1-L6 breakdown
SELECT 
    'PTY_CATEGORIES' as query_type,
    CASE 
        WHEN alias_value LIKE 'HOS:%' THEN 'HOS'
        WHEN alias_value LIKE 'CAB:%' THEN 'CAB'
        WHEN alias_value LIKE 'MUN:%' THEN 'MUN'
        WHEN alias_value LIKE 'LEG:%' THEN 'LEG'
        WHEN alias_value = 'FAM' THEN 'FAM'
        WHEN alias_value = 'ASC' THEN 'ASC'
        ELSE 'OTHER'
    END as pep_category,
    CASE 
        WHEN alias_value LIKE '%:L1' THEN 'L1'
        WHEN alias_value LIKE '%:L2' THEN 'L2'
        WHEN alias_value LIKE '%:L3' THEN 'L3'
        WHEN alias_value LIKE '%:L4' THEN 'L4'
        WHEN alias_value LIKE '%:L5' THEN 'L5'
        WHEN alias_value LIKE '%:L6' THEN 'L6'
        ELSE 'NO_LEVEL'
    END as risk_level,
    COUNT(DISTINCT entity_id) as entity_count,
    COLLECT_SET(alias_value) as sample_values
FROM prd_bronze_catalog.grid.individual_attributes 
WHERE alias_code_type = 'PTY'
GROUP BY 
    CASE 
        WHEN alias_value LIKE 'HOS:%' THEN 'HOS'
        WHEN alias_value LIKE 'CAB:%' THEN 'CAB'
        WHEN alias_value LIKE 'MUN:%' THEN 'MUN'
        WHEN alias_value LIKE 'LEG:%' THEN 'LEG'
        WHEN alias_value = 'FAM' THEN 'FAM'
        WHEN alias_value = 'ASC' THEN 'ASC'
        ELSE 'OTHER'
    END,
    CASE 
        WHEN alias_value LIKE '%:L1' THEN 'L1'
        WHEN alias_value LIKE '%:L2' THEN 'L2'
        WHEN alias_value LIKE '%:L3' THEN 'L3'
        WHEN alias_value LIKE '%:L4' THEN 'L4'
        WHEN alias_value LIKE '%:L5' THEN 'L5'
        WHEN alias_value LIKE '%:L6' THEN 'L6'
        ELSE 'NO_LEVEL'
    END
ORDER BY entity_count DESC;

-- =============================================================================
-- 3. PEP RATING TO LEVEL CORRELATION ANALYSIS
-- =============================================================================

-- Query 3: Entities with both PRT and PTY data - correlation analysis
SELECT 
    'PRT_PTY_CORRELATION' as query_type,
    prt.alias_value as prt_rating,
    pty.alias_value as pty_level,
    COUNT(DISTINCT m.entity_id) as entity_count,
    COLLECT_SET(m.entity_name) as sample_names
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_attributes prt ON m.entity_id = prt.entity_id AND prt.alias_code_type = 'PRT'
INNER JOIN prd_bronze_catalog.grid.individual_attributes pty ON m.entity_id = pty.entity_id AND pty.alias_code_type = 'PTY'
GROUP BY prt.alias_value, pty.alias_value
ORDER BY entity_count DESC
LIMIT 100;

-- =============================================================================
-- 4. EVENT CATEGORY AND RISK CODE ANALYSIS
-- =============================================================================

-- Query 4A: Event categories and risk codes distribution
SELECT 
    'EVENT_CATEGORIES' as query_type,
    event_category_code,
    event_sub_category_code,
    COUNT(DISTINCT entity_id) as entity_count,
    COUNT(*) as event_count
FROM prd_bronze_catalog.grid.individual_events
GROUP BY event_category_code, event_sub_category_code
ORDER BY entity_count DESC
LIMIT 50;

-- Query 4B: Boolean search patterns for events
SELECT 
    'EVENT_BOOLEAN_PATTERNS' as query_type,
    event_category_code,
    CASE 
        WHEN event_sub_category_code IN ('ACQ', 'DMS') THEN 'ACQUITTED'
        WHEN event_category_code IN ('SAN', 'ENF', 'PEP') THEN 'HIGH_RISK'
        WHEN event_category_code IN ('REG', 'LIC') THEN 'REGULATORY'
        ELSE 'OTHER'
    END as risk_pattern,
    COUNT(DISTINCT entity_id) as entity_count
FROM prd_bronze_catalog.grid.individual_events
GROUP BY event_category_code, CASE 
    WHEN event_sub_category_code IN ('ACQ', 'DMS') THEN 'ACQUITTED'
    WHEN event_category_code IN ('SAN', 'ENF', 'PEP') THEN 'HIGH_RISK'
    WHEN event_category_code IN ('REG', 'LIC') THEN 'REGULATORY'
    ELSE 'OTHER'
END
ORDER BY entity_count DESC;

-- =============================================================================
-- 5. GEOGRAPHIC AND ADDRESS FILTERING ANALYSIS
-- =============================================================================

-- Query 5: Geographic distribution and patterns
SELECT 
    'GEOGRAPHIC_PATTERNS' as query_type,
    address_country,
    address_city,
    address_province,
    COUNT(DISTINCT entity_id) as entity_count
FROM prd_bronze_catalog.grid.individual_addresses
WHERE address_country IS NOT NULL
GROUP BY address_country, address_city, address_province
ORDER BY entity_count DESC
LIMIT 50;

-- =============================================================================
-- 6. IDENTIFICATION AND SOURCE ANALYSIS
-- =============================================================================

-- Query 6A: Identification types and patterns
SELECT 
    'IDENTIFICATION_PATTERNS' as query_type,
    identification_type,
    COUNT(DISTINCT entity_id) as entity_count,
    COUNT(*) as id_count
FROM prd_bronze_catalog.grid.individual_identifications
GROUP BY identification_type
ORDER BY entity_count DESC;

-- Query 6B: Source systems and keys (Fixed schema)
SELECT 
    'SOURCE_PATTERNS' as query_type,
    s.source_key,
    COUNT(DISTINCT s.entity_id) as entity_count,
    COLLECT_SET(s.url) as sample_sources
FROM prd_bronze_catalog.grid.sources s
WHERE s.source_key IS NOT NULL
GROUP BY s.source_key
ORDER BY entity_count DESC
LIMIT 30;

-- =============================================================================
-- 7. CLUSTERING ANALYSIS DATA STRUCTURE
-- =============================================================================

-- Query 7A: Entity relationships for clustering
SELECT 
    'CLUSTERING_RELATIONSHIPS' as query_type,
    relationship_type,
    COUNT(*) as relationship_count,
    COUNT(DISTINCT entity_id) as unique_entities,
    COUNT(DISTINCT related_entity_id) as unique_related_entities
FROM prd_bronze_catalog.grid.relationships
GROUP BY relationship_type
ORDER BY relationship_count DESC;

-- Query 7B: Sample clustering data structure
SELECT 
    'CLUSTERING_SAMPLE' as query_type,
    m.entity_name,
    COUNT(r.related_entity_id) as relationship_count,
    COLLECT_SET(r.relationship_type) as relationship_types,
    COLLECT_SET(rm.entity_name) as related_entities
FROM prd_bronze_catalog.grid.individual_mapping m
LEFT JOIN prd_bronze_catalog.grid.relationships r ON m.entity_id = r.entity_id
LEFT JOIN prd_bronze_catalog.grid.individual_mapping rm ON r.related_entity_id = rm.entity_id
GROUP BY m.entity_name, m.entity_id
HAVING COUNT(r.related_entity_id) > 0
ORDER BY relationship_count DESC
LIMIT 20;

-- =============================================================================
-- 8. DATE AND TIME FILTERING ANALYSIS
-- =============================================================================

-- Query 8: Date patterns for filtering
SELECT 
    'DATE_PATTERNS' as query_type,
    date_of_birth_year,
    COUNT(DISTINCT entity_id) as entity_count
FROM prd_bronze_catalog.grid.individual_date_of_births
WHERE date_of_birth_year IS NOT NULL
GROUP BY date_of_birth_year
ORDER BY date_of_birth_year DESC
LIMIT 50;

-- =============================================================================
-- 9. BOOLEAN LOGIC AND COMBINED FILTER TESTING
-- =============================================================================

-- Query 9A: Test combined PEP rating + level filtering
SELECT 
    'COMBINED_PEP_TEST' as query_type,
    'A_RATING_WITH_LEVELS' as test_case,
    COUNT(DISTINCT m.entity_id) as entity_count,
    COLLECT_SET(CONCAT(m.entity_name, ' (PRT:', prt.alias_value, ', PTY:', pty.alias_value, ')')) as sample_entities
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_attributes prt ON m.entity_id = prt.entity_id 
    AND prt.alias_code_type = 'PRT' 
    AND (prt.alias_value LIKE 'A:%' OR prt.alias_value = 'A' OR prt.alias_value LIKE 'A-%')
INNER JOIN prd_bronze_catalog.grid.individual_attributes pty ON m.entity_id = pty.entity_id 
    AND pty.alias_code_type = 'PTY'
GROUP BY 1, 2
LIMIT 1;

-- Query 9B: Test name + PEP filter combination
SELECT 
    'NAME_PEP_COMBINATION' as query_type,
    COUNT(DISTINCT m.entity_id) as entity_count,
    COLLECT_SET(CONCAT(m.entity_name, ' (PTY:', attr.alias_value, ')')) as sample_entities
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_attributes attr ON m.entity_id = attr.entity_id 
    AND attr.alias_code_type = 'PTY'
WHERE LOWER(m.entity_name) LIKE '%john%'
LIMIT 1;

-- =============================================================================
-- 10. PERFORMANCE AND OPTIMIZATION ANALYSIS
-- =============================================================================

-- Query 10: Large dataset sampling for performance testing
SELECT 
    'PERFORMANCE_SAMPLE' as query_type,
    COUNT(DISTINCT m.entity_id) as total_entities,
    COUNT(DISTINCT attr.entity_id) as entities_with_attributes,
    COUNT(DISTINCT ev.entity_id) as entities_with_events,
    COUNT(DISTINCT r.entity_id) as entities_with_relationships
FROM prd_bronze_catalog.grid.individual_mapping m
LEFT JOIN prd_bronze_catalog.grid.individual_attributes attr ON m.entity_id = attr.entity_id
LEFT JOIN prd_bronze_catalog.grid.individual_events ev ON m.entity_id = ev.entity_id
LEFT JOIN prd_bronze_catalog.grid.relationships r ON m.entity_id = r.entity_id
LIMIT 1;