# Complete Databricks Table Schema Reference
## Detailed Column Definitions and Data Types

---

## 📊 **Database Structure**
- **Catalog**: `prd_bronze_catalog`
- **Schema**: `grid`
- **Full Path**: `prd_bronze_catalog.grid.[table_name]`

---

## 🗂️ **Core Individual Entity Tables**

### **1. individual_mapping** - Main Entity Registry
Primary table for individual entities with core identification information.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Primary Key** - Unique entity identifier |
| `risk_id` | STRING | NULL | Risk management system identifier |
| `entity_name` | STRING | NULL | Full name of the individual |
| `recordDefinitionType` | STRING | NULL | Type/category of entity record |
| `systemId` | STRING | NULL | Source system identifier |
| `entityDate` | TIMESTAMP | NULL | Entity creation/modification date |
| `source_item_id` | STRING | NULL | Source record identifier |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Key Characteristics:**
- **Primary Key**: `entity_id`
- **Estimated Rows**: 34M+ entities
- **Business Purpose**: Core entity registry for all individuals

---

### **2. individual_attributes** - Entity Characteristics & PEP Information
Stores various attributes including PEP status, risk scores, and entity characteristics.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Foreign Key** to individual_mapping |
| `risk_id` | STRING | NULL | Risk identifier |
| `alias_code_type` | STRING | NULL | **Attribute Type Code** (see codes below) |
| `alias_value` | STRING | NULL | **Attribute Value** (see patterns below) |
| `alias_name` | STRING | NULL | Descriptive name for attribute |
| `source_item_id` | STRING | NULL | Source reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Critical Attribute Types (`alias_code_type`):**
- **`PTY`** - PEP Type (Political Exposure)
  - Values: `HOS:L1` (Head of State), `CAB:L2` (Cabinet), `MUN:L3` (Municipal), `FAM` (Family), `ASC` (Associate)
- **`PRT`** - PEP Rating (Risk Rating with Date)
  - Values: `A:MM/DD/YYYY` (Highest), `B:MM/DD/YYYY` (High), `C:MM/DD/YYYY` (Medium), `D:MM/DD/YYYY` (Lower)
- **`PLV`** - PEP Level (L1-L6 scale)
- **`RSC`** - Risk Score (Computed risk values)
- **`NAT`** - Nationality
- **`OCU`** - Occupation
- **`URL`** - Entity URL
- **`IMG`** - Image URL
- **`RMK`** - Remarks/Notes

**Key Characteristics:**
- **Foreign Key**: `entity_id` → `individual_mapping.entity_id`
- **Estimated Rows**: 79M+ attributes
- **Business Purpose**: PEP identification, risk scoring, entity profiling

---

### **3. individual_events** - Risk Events & Activities
Records all risk-related events, compliance incidents, and activities associated with entities.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Foreign Key** to individual_mapping |
| `risk_id` | STRING | NULL | Risk identifier |
| `event_category_code` | STRING | NULL | **Primary Event Category** (see codes below) |
| `event_sub_category_code` | STRING | NULL | **Event Sub-Category** (see codes below) |
| `event_date` | DATE | NULL | **Event Occurrence Date** |
| `event_end_date` | DATE | NULL | End date for ongoing events |
| `event_description` | STRING | NULL | Full description of the event |
| `event_reference_source_item_id` | STRING | NULL | Reference to source |
| `source_item_id` | STRING | NULL | Source identifier |
| `entity_name` | STRING | NULL | Entity name at time of event |
| `systemId` | STRING | NULL | System identifier |
| `entityDate` | TIMESTAMP | NULL | Entity date reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Critical Event Categories (`event_category_code`):**
- **`TER`** - Terrorist Related (Critical Risk: 100)
- **`MLA`** - Money Laundering (Critical Risk: 100)
- **`BRB`** - Bribery, Graft, Kickbacks (High Risk: 85)
- **`DTF`** - Drug Trafficking/Distribution (High Risk: 85)
- **`WLT`** - Watch List (High Risk: 85)
- **`DEN`** - Denied Entity (High Risk: 85)
- **`SEC`** - SEC Violations (Medium Risk: 65)
- **`FRD`** - Fraud, Scams, Swindles (Medium Risk: 65)
- **`TAX`** - Tax Related Offenses (Medium Risk: 65)
- **`PEP`** - Person Political (Variable Risk)
- **`AST`** - Assault, Battery
- **`MUR`** - Murder, Manslaughter
- **`TFT`** - Theft, Larceny, Embezzlement

**Event Sub-Categories (`event_sub_category_code`):**
- **`CVT`** - Conviction (Legal conviction)
- **`SAN`** - Sanctions (International sanctions)
- **`IND`** - Indictment (Formal charge)
- **`CHG`** - Charged (Formal charges filed)
- **`ART`** - Arrest (Arrested by authorities)
- **`SPT`** - Suspected (Under suspicion)
- **`ALL`** - Alleged (Allegations made)
- **`ACQ`** - Acquitted (Found not guilty)
- **`DMS`** - Dismissed (Charges dismissed)

**Key Characteristics:**
- **Foreign Key**: `entity_id` → `individual_mapping.entity_id`
- **Estimated Rows**: 55M+ events
- **Business Purpose**: Risk assessment, compliance monitoring, event tracking

---

### **4. individual_addresses** - Geographic Information
Geographic and location data for entities including addresses and jurisdictions.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Foreign Key** to individual_mapping |
| `risk_id` | STRING | NULL | Risk identifier |
| `address_type` | STRING | NULL | Type (pep, birth, residence, business) |
| `address_line1` | STRING | NULL | Primary street address |
| `address_line2` | STRING | NULL | Secondary address line |
| `address_city` | STRING | NULL | City name |
| `address_province` | STRING | NULL | State/Province |
| `address_country` | STRING | NULL | **Country** (critical for risk assessment) |
| `address_postal_code` | STRING | NULL | Postal/ZIP code |
| `address_raw_format` | STRING | NULL | Unstructured address text |
| `source_item_id` | STRING | NULL | Source reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Key Characteristics:**
- **Foreign Key**: `entity_id` → `individual_mapping.entity_id`
- **Business Purpose**: Geographic risk assessment, jurisdiction analysis
- **Coverage**: 200+ countries globally

---

### **5. individual_aliases** - Alternative Names
Alternative names, AKAs, and name variations for entities.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Foreign Key** to individual_mapping |
| `risk_id` | STRING | NULL | Risk identifier |
| `alias_name` | STRING | NULL | **Alternative Name** |
| `alias_code_type` | STRING | NULL | Type (AKA, FKA, LOC, etc.) |
| `alias_value` | STRING | NULL | Additional alias information |
| `source_item_id` | STRING | NULL | Source reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Alias Types:**
- **`AKA`** - Also Known As
- **`FKA`** - Formerly Known As
- **`LOC`** - Local Name

---

### **6. individual_identifications** - Identity Documents
Identity documents, passport numbers, national IDs, and other identification.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Foreign Key** to individual_mapping |
| `risk_id` | STRING | NULL | Risk identifier |
| `identification_type` | STRING | NULL | **ID Type** (passport, national_id, etc.) |
| `identification_value` | STRING | NULL | **ID Number/Value** |
| `identification_expire_date` | DATE | NULL | Expiration date |
| `identification_issue_date` | DATE | NULL | Issue date |
| `identification_country` | STRING | NULL | Issuing country |
| `source_item_id` | STRING | NULL | Source reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

---

### **7. individual_date_of_births** - Birth Information
Birth date information with support for partial and approximate dates.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Foreign Key** to individual_mapping |
| `risk_id` | STRING | NULL | Risk identifier |
| `date_of_birth_year` | INT | NULL | **Birth Year** |
| `date_of_birth_month` | INT | NULL | Birth Month (1-12) |
| `date_of_birth_day` | INT | NULL | Birth Day (1-31) |
| `date_of_birth_circa` | STRING | NULL | Approximate date flag (Y/N) |
| `date_of_birth_text` | STRING | NULL | Textual birth date |
| `source_item_id` | STRING | NULL | Source reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

---

## 🏢 **Organization Entity Tables**

### **8. organization_mapping** - Organization Registry
Main table for organization entities (parallel structure to individual_mapping).

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Primary Key** - Unique organization identifier |
| `risk_id` | STRING | NULL | Risk management identifier |
| `entity_name` | STRING | NULL | Organization name |
| `recordDefinitionType` | STRING | NULL | Type of organization record |
| `systemId` | STRING | NULL | Source system identifier |
| `entityDate` | TIMESTAMP | NULL | Entity creation/modification date |
| `source_item_id` | STRING | NULL | Source record identifier |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

### **9-12. Organization Support Tables**
Parallel structure to individual tables:
- **`organization_attributes`** - Organization characteristics
- **`organization_events`** - Organization risk events  
- **`organization_addresses`** - Organization locations
- **`organization_aliases`** - Organization alternative names

---

## 🔗 **Relationship and Network Tables**

### **13. relationships** - Entity Network Mapping
Maps relationships and connections between entities for network analysis.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | **Source Entity ID** |
| `related_entity_id` | STRING | NOT NULL | **Target Entity ID** |
| `related_entity_name` | STRING | NULL | Name of related entity |
| `type` | STRING | NULL | **Relationship Type Code** |
| `relationship_type` | STRING | NULL | Detailed relationship type |
| `direction` | STRING | NULL | Direction (TO/FROM/BIDIRECTIONAL) |
| `strength` | STRING | NULL | Relationship strength |
| `start_date` | DATE | NULL | Relationship start date |
| `end_date` | DATE | NULL | Relationship end date |
| `source_item_id` | STRING | NULL | Source reference |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Common Relationship Types:**
- **Family**: MOTHER, FATHER, WIFE, HUSBAND, SON, DAUGHTER, BROTHER, SISTER
- **Business**: EMPLOYEE, EMPLOYER, SHAREHOLDER_OWNER, COLLEAGUE
- **Associates**: ASSOCIATE, FRIEND, AGENT_REPRESENTATIVE
- **Legal**: LEGAL_ADVISER, FINANCIAL_ADVISER

---

### **14. grid_orbis_mapping** - External System Integration
Integration mapping with Orbis (Bureau van Dijk) database.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entityid` | STRING | NOT NULL | **Grid Entity ID** |
| `entityname` | STRING | NULL | Entity name in Orbis |
| `riskid` | STRING | NULL | Risk ID in Orbis |
| `bvdid` | STRING | NULL | **Bureau van Dijk ID** |
| `asofdate` | DATE | NULL | As-of date for mapping |
| `confidence_score` | FLOAT | NULL | Match confidence score |
| `mapping_type` | STRING | NULL | Type of mapping |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

---

## 📚 **Reference and Lookup Tables**

### **15. code_dictionary** - Master Reference Table
Central repository for all code definitions and descriptions.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `code` | STRING | NOT NULL | **Code Value** |
| `code_description` | STRING | NULL | **Human-readable Description** |
| `code_type` | STRING | NOT NULL | **Code Category** (see types below) |
| `parent_code` | STRING | NULL | Parent code for hierarchical codes |
| `is_active` | BOOLEAN | NULL | Whether code is currently active |
| `sort_order` | INT | NULL | Display sort order |
| `created_date` | TIMESTAMP | NULL | Record creation timestamp |
| `modified_date` | TIMESTAMP | NULL | Last modification timestamp |

**Code Types and Counts:**
- **`event_category`** (67 codes) - Primary event classifications
- **`relationship_type`** (48 codes) - Relationship classifications
- **`event_sub_category`** (36 codes) - Event sub-classifications
- **`entity_attribute`** (29 codes) - Entity attribute types
- **`relationship_direction`** (2 codes) - FROM/TO

---

## 📍 **Source and Metadata Tables**

### **16. sources** - Data Provenance Tracking
Tracks data sources, publishers, and provenance information.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `entity_id` | STRING | NOT NULL | Entity this source relates to |
| `source_id` | STRING | NULL | Unique source identifier |
| `source_key` | STRING | NULL | **Source System Key** |
| `risk_id` | STRING | NULL | Risk identifier |
| `url` | STRING | NULL | Source URL |
| `name` | STRING | NULL | Source name |
| `type` | STRING | NULL | Source type |
| `publication` | STRING | NULL | Publication name |
| `publisher` | STRING | NULL | Publisher name |
| `author` | STRING | NULL | Author information |
| `createdDate` | TIMESTAMP | NULL | Source creation date |
| `modifiedDate` | TIMESTAMP | NULL | Source modification date |
| `publish_date` | DATE | NULL | **Publication Date** |
| `source_item_id` | STRING | NULL | Source item reference |

---

### **17. user_configurations** - System Configuration
User-defined system configurations and settings.

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| `config_key` | STRING | NOT NULL | **Configuration Key** |
| `config_value` | STRING | NULL | Configuration value (JSON format) |
| `config_type` | STRING | NULL | Type of configuration |
| `description` | STRING | NULL | Configuration description |
| `is_active` | BOOLEAN | NULL | Whether config is active |
| `last_modified` | TIMESTAMP | NULL | Last modification timestamp |
| `modified_by` | STRING | NULL | User who modified |

---

## 🔍 **Entity Relationships and Foreign Keys**

### **Logical Relationships** (not enforced constraints):

```
individual_mapping (1) ←→ (many) individual_attributes
individual_mapping (1) ←→ (many) individual_events  
individual_mapping (1) ←→ (many) individual_addresses
individual_mapping (1) ←→ (many) individual_aliases
individual_mapping (1) ←→ (many) individual_identifications
individual_mapping (1) ←→ (many) individual_date_of_births
individual_mapping (1) ←→ (many) relationships (source)
individual_mapping (1) ←→ (many) relationships (target)
individual_mapping (1) ←→ (many) sources
individual_mapping (1) ←→ (many) grid_orbis_mapping
```

---

## 📊 **Data Scale and Performance**

| Table Name | Estimated Rows | Description |
|------------|----------------|-------------|
| `individual_attributes` | **79M+** | Entity characteristics and PEP data |
| `individual_events` | **55M+** | Risk events and compliance incidents |
| `individual_mapping` | **34M+** | Core individual entities |
| `individual_addresses` | **20M+** | Geographic information |
| `relationships` | **15M+** | Entity network connections |
| `sources` | **10M+** | Data provenance records |
| `individual_aliases` | **8M+** | Alternative names |
| `organization_mapping` | **5M+** | Organization entities |
| `individual_identifications` | **3M+** | Identity documents |
| `code_dictionary` | **180+** | Reference codes |

---

## 🎯 **Business Usage Patterns**

### **High-Frequency Queries:**
- Entity search by name/ID
- Risk assessment queries
- PEP status lookups
- Event filtering by category/date
- Geographic filtering

### **Complex Analytics:**
- Network analysis across relationships
- Risk scoring calculations
- Compliance reporting
- Trend analysis over time
- Cross-entity pattern detection

### **Data Quality Checks:**
- Entity completeness validation
- Source freshness monitoring
- Code reference integrity
- Relationship consistency

---

**This schema supports enterprise-scale compliance operations with industry-leading performance and comprehensive data coverage across 200+ countries and 1000+ data sources.**