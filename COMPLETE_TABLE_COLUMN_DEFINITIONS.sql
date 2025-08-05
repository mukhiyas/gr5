-- ================================================================================
-- COMPLETE DATABRICKS TABLE AND COLUMN DEFINITIONS
-- Full Schema Documentation with Data Types and Descriptions
-- ================================================================================

-- ****************************************************************************
-- CATALOG AND SCHEMA INFORMATION
-- ****************************************************************************

-- Catalog: prd_bronze_catalog
-- Schema: grid
-- Full path format: prd_bronze_catalog.grid.[table_name]

-- ****************************************************************************
-- METHOD TO GET ALL TABLE DEFINITIONS
-- ****************************************************************************

-- Get all tables in the schema
SHOW TABLES IN prd_bronze_catalog.grid;

-- Get complete schema information for all tables
SELECT 
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_catalog = 'prd_bronze_catalog' 
AND table_schema = 'grid'
ORDER BY table_name;

-- ****************************************************************************
-- DETAILED TABLE AND COLUMN DEFINITIONS
-- ****************************************************************************

-- ============================================================================
-- 1. INDIVIDUAL_MAPPING - Core entity table for individuals
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_mapping;

-- Expected columns:
-- entity_id          STRING      NOT NULL    Primary key, unique entity identifier
-- risk_id            STRING      NULL        Risk management identifier
-- entity_name        STRING      NULL        Full name of the individual
-- recordDefinitionType STRING    NULL        Type of entity record
-- systemId           STRING      NULL        Source system identifier
-- entityDate         TIMESTAMP   NULL        Entity creation/modification date
-- source_item_id     STRING      NULL        Source record identifier
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

-- Get actual column definitions with comments
SHOW COLUMNS IN prd_bronze_catalog.grid.individual_mapping;

-- ============================================================================
-- 2. INDIVIDUAL_ATTRIBUTES - Entity attributes and characteristics
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_attributes;

-- Expected columns:
-- entity_id          STRING      NOT NULL    Foreign key to individual_mapping
-- risk_id            STRING      NULL        Risk identifier
-- alias_code_type    STRING      NULL        Type of attribute (PTY, PRT, PLV, RSC, etc.)
-- alias_value        STRING      NULL        Value of the attribute
-- alias_name         STRING      NULL        Descriptive name for attribute
-- source_item_id     STRING      NULL        Source reference
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

-- Show all columns with data types
SHOW COLUMNS IN prd_bronze_catalog.grid.individual_attributes;

-- ============================================================================
-- 3. INDIVIDUAL_EVENTS - Risk events and activities
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_events;

-- Expected columns:
-- entity_id                      STRING      NOT NULL    Foreign key to individual_mapping
-- risk_id                        STRING      NULL        Risk identifier
-- event_category_code            STRING      NULL        Primary event category (TER, MLA, BRB, etc.)
-- event_sub_category_code        STRING      NULL        Sub-category of event
-- event_date                     DATE        NULL        Date when event occurred
-- event_end_date                 DATE        NULL        End date for ongoing events
-- event_description              STRING      NULL        Full description of event
-- event_reference_source_item_id STRING      NULL        Reference to source
-- source_item_id                 STRING      NULL        Source identifier
-- entity_name                    STRING      NULL        Entity name at time of event
-- systemId                       STRING      NULL        System identifier
-- entityDate                     TIMESTAMP   NULL        Entity date reference
-- created_date                   TIMESTAMP   NULL        Record creation timestamp
-- modified_date                  TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.individual_events;

-- ============================================================================
-- 4. INDIVIDUAL_ADDRESSES - Geographic and location information
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_addresses;

-- Expected columns:
-- entity_id          STRING      NOT NULL    Foreign key to individual_mapping
-- risk_id            STRING      NULL        Risk identifier
-- address_type       STRING      NULL        Type of address (pep, birth, residence, etc.)
-- address_line1      STRING      NULL        Primary street address
-- address_line2      STRING      NULL        Secondary address line
-- address_city       STRING      NULL        City name
-- address_province   STRING      NULL        State/Province
-- address_country    STRING      NULL        Country code or name
-- address_postal_code STRING     NULL        Postal/ZIP code
-- address_raw_format STRING      NULL        Unstructured address text
-- source_item_id     STRING      NULL        Source reference
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.individual_addresses;

-- ============================================================================
-- 5. INDIVIDUAL_ALIASES - Alternative names and AKAs
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_aliases;

-- Expected columns:
-- entity_id          STRING      NOT NULL    Foreign key to individual_mapping
-- risk_id            STRING      NULL        Risk identifier
-- alias_name         STRING      NULL        Alternative name
-- alias_code_type    STRING      NULL        Type of alias (AKA, FKA, LOC, etc.)
-- alias_value        STRING      NULL        Additional alias information
-- source_item_id     STRING      NULL        Source reference
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.individual_aliases;

-- ============================================================================
-- 6. INDIVIDUAL_IDENTIFICATIONS - Identity documents and numbers
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_identifications;

-- Expected columns:
-- entity_id                  STRING      NOT NULL    Foreign key to individual_mapping
-- risk_id                    STRING      NULL        Risk identifier
-- identification_type        STRING      NULL        Type of ID (passport, national_id, etc.)
-- identification_value       STRING      NULL        ID number/value
-- identification_expire_date DATE        NULL        Expiration date
-- identification_issue_date  DATE        NULL        Issue date
-- identification_country     STRING      NULL        Issuing country
-- source_item_id            STRING      NULL        Source reference
-- created_date              TIMESTAMP   NULL        Record creation timestamp
-- modified_date             TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.individual_identifications;

-- ============================================================================
-- 7. INDIVIDUAL_DATE_OF_BIRTHS - Birth date information
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.individual_date_of_births;

-- Expected columns:
-- entity_id              STRING      NOT NULL    Foreign key to individual_mapping
-- risk_id                STRING      NULL        Risk identifier
-- date_of_birth_year     INT         NULL        Birth year
-- date_of_birth_month    INT         NULL        Birth month (1-12)
-- date_of_birth_day      INT         NULL        Birth day (1-31)
-- date_of_birth_circa    STRING      NULL        Approximate date flag (Y/N)
-- date_of_birth_text     STRING      NULL        Textual birth date
-- source_item_id         STRING      NULL        Source reference
-- created_date           TIMESTAMP   NULL        Record creation timestamp
-- modified_date          TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.individual_date_of_births;

-- ============================================================================
-- 8. ORGANIZATION_MAPPING - Core entity table for organizations
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.organization_mapping;

-- Expected columns (parallel to individual_mapping):
-- entity_id          STRING      NOT NULL    Primary key, unique entity identifier
-- risk_id            STRING      NULL        Risk management identifier
-- entity_name        STRING      NULL        Organization name
-- recordDefinitionType STRING    NULL        Type of entity record
-- systemId           STRING      NULL        Source system identifier
-- entityDate         TIMESTAMP   NULL        Entity creation/modification date
-- source_item_id     STRING      NULL        Source record identifier
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.organization_mapping;

-- ============================================================================
-- 9. ORGANIZATION_ATTRIBUTES - Organization characteristics
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.organization_attributes;

-- Expected columns (parallel to individual_attributes):
-- entity_id          STRING      NOT NULL    Foreign key to organization_mapping
-- risk_id            STRING      NULL        Risk identifier
-- alias_code_type    STRING      NULL        Type of attribute
-- alias_value        STRING      NULL        Value of the attribute
-- alias_name         STRING      NULL        Descriptive name
-- source_item_id     STRING      NULL        Source reference
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.organization_attributes;

-- ============================================================================
-- 10. ORGANIZATION_EVENTS - Organization risk events
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.organization_events;

-- Columns parallel to individual_events
SHOW COLUMNS IN prd_bronze_catalog.grid.organization_events;

-- ============================================================================
-- 11. ORGANIZATION_ADDRESSES - Organization locations
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.organization_addresses;

-- Columns parallel to individual_addresses
SHOW COLUMNS IN prd_bronze_catalog.grid.organization_addresses;

-- ============================================================================
-- 12. RELATIONSHIPS - Entity relationship network
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.relationships;

-- Expected columns:
-- entity_id              STRING      NOT NULL    Source entity ID
-- related_entity_id      STRING      NOT NULL    Target entity ID
-- related_entity_name    STRING      NULL        Name of related entity
-- type                   STRING      NULL        Relationship type code
-- relationship_type      STRING      NULL        Detailed relationship type
-- direction              STRING      NULL        Direction of relationship (TO/FROM)
-- strength               STRING      NULL        Relationship strength
-- start_date             DATE        NULL        Relationship start date
-- end_date               DATE        NULL        Relationship end date
-- source_item_id         STRING      NULL        Source reference
-- created_date           TIMESTAMP   NULL        Record creation timestamp
-- modified_date          TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.relationships;

-- ============================================================================
-- 13. CODE_DICTIONARY - Master reference table
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.code_dictionary;

-- Expected columns:
-- code               STRING      NOT NULL    Code value
-- code_description   STRING      NULL        Human-readable description
-- code_type          STRING      NOT NULL    Type of code (event_category, relationship_type, etc.)
-- parent_code        STRING      NULL        Parent code for hierarchical codes
-- is_active          BOOLEAN     NULL        Whether code is currently active
-- sort_order         INT         NULL        Display sort order
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.code_dictionary;

-- ============================================================================
-- 14. SOURCES - Data provenance and source tracking
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.sources;

-- Expected columns:
-- entity_id          STRING      NOT NULL    Entity this source relates to
-- source_id          STRING      NULL        Unique source identifier
-- source_key         STRING      NULL        Source system key
-- risk_id            STRING      NULL        Risk identifier
-- url                STRING      NULL        Source URL
-- name               STRING      NULL        Source name
-- type               STRING      NULL        Source type
-- publication        STRING      NULL        Publication name
-- publisher          STRING      NULL        Publisher name
-- author             STRING      NULL        Author information
-- createdDate        TIMESTAMP   NULL        Source creation date
-- modifiedDate       TIMESTAMP   NULL        Source modification date
-- publish_date       DATE        NULL        Publication date
-- source_item_id     STRING      NULL        Source item reference

SHOW COLUMNS IN prd_bronze_catalog.grid.sources;

-- ============================================================================
-- 15. GRID_ORBIS_MAPPING - External system integration
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.grid_orbis_mapping;

-- Expected columns:
-- entityid           STRING      NOT NULL    Grid entity ID
-- entityname         STRING      NULL        Entity name in Orbis
-- riskid             STRING      NULL        Risk ID in Orbis
-- bvdid              STRING      NULL        Bureau van Dijk ID
-- asofdate           DATE        NULL        As-of date for mapping
-- confidence_score   FLOAT       NULL        Match confidence score
-- mapping_type       STRING      NULL        Type of mapping
-- created_date       TIMESTAMP   NULL        Record creation timestamp
-- modified_date      TIMESTAMP   NULL        Last modification timestamp

SHOW COLUMNS IN prd_bronze_catalog.grid.grid_orbis_mapping;

-- ============================================================================
-- 16. USER_CONFIGURATIONS - System configuration settings
-- ============================================================================
DESCRIBE TABLE EXTENDED prd_bronze_catalog.grid.user_configurations;

-- Expected columns:
-- config_key         STRING      NOT NULL    Configuration key
-- config_value       STRING      NULL        Configuration value (usually JSON)
-- config_type        STRING      NULL        Type of configuration
-- description        STRING      NULL        Configuration description
-- is_active          BOOLEAN     NULL        Whether config is active
-- last_modified      TIMESTAMP   NULL        Last modification timestamp
-- modified_by        STRING      NULL        User who modified

SHOW COLUMNS IN prd_bronze_catalog.grid.user_configurations;

-- ****************************************************************************
-- COMPREHENSIVE COLUMN INFORMATION QUERIES
-- ****************************************************************************

-- ============================================================================
-- Get all columns across all tables with full metadata
-- ============================================================================
SELECT 
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    is_nullable,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    datetime_precision,
    column_comment
FROM information_schema.columns
WHERE table_catalog = 'prd_bronze_catalog' 
AND table_schema = 'grid'
ORDER BY table_name, ordinal_position;

-- ============================================================================
-- Get table relationships and foreign keys
-- ============================================================================
-- Note: Databricks may not enforce foreign keys, but logical relationships exist:

SELECT 
    'LOGICAL_RELATIONSHIPS' as relationship_type,
    'individual_attributes.entity_id -> individual_mapping.entity_id' as relationship
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'individual_events.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'individual_addresses.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'individual_aliases.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'individual_identifications.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'individual_date_of_births.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'relationships.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'relationships.related_entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'sources.entity_id -> individual_mapping.entity_id'
UNION ALL
SELECT 'LOGICAL_RELATIONSHIPS', 'grid_orbis_mapping.entityid -> individual_mapping.entity_id';

-- ============================================================================
-- Get table statistics and row counts
-- ============================================================================
WITH table_stats AS (
    SELECT 'individual_mapping' as table_name, COUNT(*) as row_count FROM prd_bronze_catalog.grid.individual_mapping
    UNION ALL
    SELECT 'individual_attributes', COUNT(*) FROM prd_bronze_catalog.grid.individual_attributes
    UNION ALL
    SELECT 'individual_events', COUNT(*) FROM prd_bronze_catalog.grid.individual_events
    UNION ALL
    SELECT 'individual_addresses', COUNT(*) FROM prd_bronze_catalog.grid.individual_addresses
    UNION ALL
    SELECT 'individual_aliases', COUNT(*) FROM prd_bronze_catalog.grid.individual_aliases
    UNION ALL
    SELECT 'individual_identifications', COUNT(*) FROM prd_bronze_catalog.grid.individual_identifications
    UNION ALL
    SELECT 'individual_date_of_births', COUNT(*) FROM prd_bronze_catalog.grid.individual_date_of_births
    UNION ALL
    SELECT 'organization_mapping', COUNT(*) FROM prd_bronze_catalog.grid.organization_mapping
    UNION ALL
    SELECT 'organization_attributes', COUNT(*) FROM prd_bronze_catalog.grid.organization_attributes
    UNION ALL
    SELECT 'organization_events', COUNT(*) FROM prd_bronze_catalog.grid.organization_events
    UNION ALL
    SELECT 'organization_addresses', COUNT(*) FROM prd_bronze_catalog.grid.organization_addresses
    UNION ALL
    SELECT 'relationships', COUNT(*) FROM prd_bronze_catalog.grid.relationships
    UNION ALL
    SELECT 'code_dictionary', COUNT(*) FROM prd_bronze_catalog.grid.code_dictionary
    UNION ALL
    SELECT 'sources', COUNT(*) FROM prd_bronze_catalog.grid.sources
    UNION ALL
    SELECT 'grid_orbis_mapping', COUNT(*) FROM prd_bronze_catalog.grid.grid_orbis_mapping
)
SELECT 
    table_name,
    row_count,
    CASE 
        WHEN row_count > 1000000000 THEN CONCAT(ROUND(row_count/1000000000.0, 2), 'B')
        WHEN row_count > 1000000 THEN CONCAT(ROUND(row_count/1000000.0, 2), 'M')
        WHEN row_count > 1000 THEN CONCAT(ROUND(row_count/1000.0, 2), 'K')
        ELSE CAST(row_count AS STRING)
    END as formatted_count
FROM table_stats
ORDER BY row_count DESC;

-- ============================================================================
-- Column usage patterns and data quality checks
-- ============================================================================

-- Check nullable columns and data completeness
SELECT 
    table_name,
    column_name,
    is_nullable,
    CASE 
        WHEN table_name = 'individual_mapping' THEN 
            CASE column_name
                WHEN 'entity_id' THEN 'Primary Key - Always populated'
                WHEN 'entity_name' THEN 'Core field - Should be populated'
                WHEN 'risk_id' THEN 'Risk tracking - May be null'
                ELSE 'Standard field'
            END
        WHEN table_name = 'individual_attributes' THEN
            CASE column_name
                WHEN 'entity_id' THEN 'Foreign Key - Always populated'
                WHEN 'alias_code_type' THEN 'PTY, PRT, PLV, RSC, etc.'
                WHEN 'alias_value' THEN 'Contains actual attribute value'
                ELSE 'Standard field'
            END
        WHEN table_name = 'individual_events' THEN
            CASE column_name
                WHEN 'entity_id' THEN 'Foreign Key - Always populated'
                WHEN 'event_category_code' THEN 'TER, MLA, BRB, DTF, etc.'
                WHEN 'event_date' THEN 'Critical for risk assessment'
                ELSE 'Standard field'
            END
        ELSE 'Check table documentation'
    END as usage_notes
FROM information_schema.columns
WHERE table_catalog = 'prd_bronze_catalog' 
AND table_schema = 'grid'
AND table_name IN ('individual_mapping', 'individual_attributes', 'individual_events')
ORDER BY table_name, ordinal_position;

-- ****************************************************************************
-- ATTRIBUTE AND EVENT CODE REFERENCES
-- ****************************************************************************

-- Common alias_code_type values in attributes tables:
-- PTY = PEP Type (Political Exposure Type)
-- PRT = PEP Rating (Risk Rating with date)
-- PLV = PEP Level (L1-L6)
-- RSC = Risk Score
-- NAT = Nationality
-- OCU = Occupation
-- URL = Entity URL
-- IMG = Image URL
-- BIO = Biography
-- RMK = Remarks

-- Common event_category_code values:
-- TER = Terrorist Related
-- MLA = Money Laundering
-- BRB = Bribery, Graft, Kickbacks
-- DTF = Drug Trafficking
-- WLT = Watch List
-- DEN = Denied Entity
-- SEC = SEC Violations
-- FRD = Fraud, Scams, Swindles
-- TAX = Tax Related Offenses
-- PEP = Person Political
-- AST = Assault, Battery
-- MUR = Murder, Manslaughter
-- TFT = Theft, Larceny, Embezzlement

-- Common event_sub_category_code values:
-- CVT = Conviction
-- SAN = Sanctions
-- IND = Indictment
-- CHG = Charged
-- ART = Arrest
-- SPT = Suspected
-- ALL = Alleged
-- ACQ = Acquitted
-- DMS = Dismissed

-- ****************************************************************************
-- END OF TABLE AND COLUMN DEFINITIONS
-- ****************************************************************************