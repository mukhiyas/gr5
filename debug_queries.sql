-- SQL Queries to Debug PEP Filter Issues
-- Please run these queries directly in your database and provide the results

-- Query 1: Check if PEP data exists and what formats it's in
SELECT 
    alias_code_type,
    alias_value,
    COUNT(DISTINCT entity_id) as entity_count,
    COUNT(*) as total_records
FROM prd_bronze_catalog.grid.individual_attributes 
WHERE alias_code_type = 'PTY'
GROUP BY alias_code_type, alias_value
ORDER BY entity_count DESC
LIMIT 20;

-- Query 2: Find entities with PEP attributes (any PTY type)
SELECT 
    m.entity_id,
    m.entity_name,
    attr.alias_code_type,
    attr.alias_value
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_attributes attr ON m.entity_id = attr.entity_id
WHERE attr.alias_code_type = 'PTY'
LIMIT 10;

-- Query 3: Test specific PEP level filter (HOS - Head of State)
SELECT 
    m.entity_id,
    m.entity_name,
    attr.alias_code_type,
    attr.alias_value
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_attributes attr ON m.entity_id = attr.entity_id
WHERE attr.alias_code_type = 'PTY' 
AND attr.alias_value LIKE 'HOS:%'
LIMIT 5;

-- Query 4: Test name + PEP filter combination
SELECT 
    m.entity_id,
    m.entity_name,
    attr.alias_code_type,
    attr.alias_value
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_attributes attr ON m.entity_id = attr.entity_id
WHERE attr.alias_code_type = 'PTY'
AND UPPER(m.entity_name) LIKE '%TRUMP%'
LIMIT 5;

-- Query 5: Check what attribute types exist (to understand the data structure)
SELECT 
    alias_code_type,
    COUNT(DISTINCT entity_id) as entity_count,
    COUNT(*) as total_records,
    MIN(alias_value) as sample_value
FROM prd_bronze_catalog.grid.individual_attributes
GROUP BY alias_code_type
ORDER BY entity_count DESC
LIMIT 15;

-- Query 6: Test country filter
SELECT 
    m.entity_id,
    m.entity_name,
    addr.address_country,
    addr.address_city
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_addresses addr ON m.entity_id = addr.entity_id
WHERE UPPER(addr.address_country) = 'UNITED STATES'
LIMIT 5;

-- Query 7: Test name + country filter combination
SELECT 
    m.entity_id,
    m.entity_name,
    addr.address_country
FROM prd_bronze_catalog.grid.individual_mapping m
INNER JOIN prd_bronze_catalog.grid.individual_addresses addr ON m.entity_id = addr.entity_id
WHERE UPPER(addr.address_country) = 'UNITED STATES'
AND UPPER(m.entity_name) LIKE '%SMITH%'
LIMIT 5;

-- Query 8: Check what countries exist
SELECT 
    address_country,
    COUNT(DISTINCT entity_id) as entity_count
FROM prd_bronze_catalog.grid.individual_addresses
WHERE address_country IS NOT NULL
GROUP BY address_country
ORDER BY entity_count DESC
LIMIT 10;