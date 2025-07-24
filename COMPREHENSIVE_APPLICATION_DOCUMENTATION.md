# GRID Entity Search Application - Comprehensive Documentation

## Table of Contents
1. [Application Overview](#application-overview)
2. [Risk Scoring System](#risk-scoring-system)
3. [PEP Detection System](#pep-detection-system)
4. [Database Architecture](#database-architecture)
5. [Search Functionality](#search-functionality)
6. [Visualization Features](#visualization-features)
7. [Export Capabilities](#export-capabilities)
8. [User Interface](#user-interface)
9. [Configuration & Settings](#configuration--settings)
10. [Data Processing Pipeline](#data-processing-pipeline)
11. [Business Rules & Calculations](#business-rules--calculations)
12. [Technical Implementation](#technical-implementation)

---

## Application Overview

**GRID Entity Search** is an enterprise-grade risk intelligence and analysis platform designed for comprehensive entity research, risk assessment, and relationship mapping. It provides sophisticated search capabilities across individuals and organizations with real-time risk scoring, PEP (Politically Exposed Person) detection, and advanced visualization tools.

### Key Features
- **Advanced Entity Search**: Multi-criteria search with Boolean logic
- **4-Tier Risk Assessment**: Critical, Valuable, Investigative, Probative
- **PEP Detection & Classification**: 14+ political exposure levels
- **Relationship Network Analysis**: Interactive network mapping
- **AI-Powered Analysis**: Integration with Moody's AI and other providers
- **Comprehensive Export**: Excel, CSV, JSON, PDF formats
- **Real-time Visualization**: Charts, graphs, and network diagrams
- **Enterprise Security**: Multi-factor authentication and data encryption

---

## Risk Scoring System

### 4-Tier Severity Framework

The application uses a sophisticated 4-tier risk classification system with scores ranging from 0-120 points:

#### Risk Severity Levels
- **Critical (80-100 points)**: Highest risk entities requiring immediate attention
- **Valuable (60-79 points)**: High-value targets for investigation
- **Investigative (40-59 points)**: Entities warranting further investigation
- **Probative (0-39 points)**: Lower risk entities for monitoring

### Risk Code Categories

#### Critical Severity Risk Codes (80-100 points)
| Code | Description | Points | Category |
|------|-------------|--------|----------|
| TER | Terrorism | 95 | Security Threat |
| SAN | Sanctions | 90 | Regulatory |
| MLA | Money Laundering | 85 | Financial Crime |
| DRG | Drug Trafficking | 90 | Narcotics |
| ARM | Arms Trafficking | 90 | Weapons |
| HUM | Human Trafficking | 95 | Human Rights |
| WAR | War Crimes | 100 | International Crime |
| GEN | Genocide | 100 | Crimes Against Humanity |
| CRM | Crimes Against Humanity | 95 | International Crime |
| ORG | Organized Crime | 85 | Criminal Organization |
| KID | Kidnapping | 90 | Violent Crime |
| EXT | Extortion | 80 | Financial Crime |

#### Valuable Severity Risk Codes (60-79 points)
| Code | Description | Points | Category |
|------|-------------|--------|----------|
| FRD | Fraud | 70 | Financial Crime |
| COR | Corruption | 75 | Governance |
| BRB | Bribery | 75 | Corruption |
| EMB | Embezzlement | 70 | Financial Crime |
| TAX | Tax Evasion | 65 | Regulatory |
| SEC | Securities Fraud | 70 | Financial Market |
| FOR | Forgery | 65 | Document Crime |
| CYB | Cybercrime | 75 | Technology Crime |
| HAC | Hacking | 75 | Cyber Security |
| IDE | Identity Theft | 65 | Personal Crime |

#### Investigative Severity Risk Codes (40-59 points)
| Code | Description | Points | Category |
|------|-------------|--------|----------|
| ENV | Environmental Crime | 55 | Environmental |
| WCC | White Collar Crime | 50 | Corporate |
| REG | Regulatory Violations | 45 | Compliance |
| ANT | Anti-Trust Violations | 50 | Competition |
| LAB | Labor Violations | 45 | Employment |
| CON | Consumer Fraud | 50 | Consumer Protection |
| INS | Insurance Fraud | 55 | Insurance |
| BAN | Banking Violations | 55 | Financial Services |

#### Probative Severity Risk Codes (0-39 points)
| Code | Description | Points | Category |
|------|-------------|--------|----------|
| ADM | Administrative Issues | 20 | Administrative |
| DOC | Documentation Issues | 15 | Documentation |
| REP | Reporting Issues | 25 | Compliance |
| DIS | Disclosure Issues | 25 | Transparency |
| PRI | Privacy Issues | 30 | Data Protection |
| ETH | Ethical Concerns | 35 | Ethics |
| GOV | Governance Issues | 30 | Corporate Governance |
| POL | Policy Violations | 25 | Policy |

### Risk Calculation Formula

The comprehensive risk score is calculated using a weighted multi-component algorithm:

```
Final Risk Score = (
    Base Event Score +
    (Event Score × 0.4) +           // 40% weight for events
    (Relationship Score × 0.2) +    // 20% weight for relationships
    (Geographic Score × 0.15) +     // 15% weight for geographic factors
    (Temporal Score × 0.15) +       // 15% weight for temporal factors
    (PEP Score × 0.1)               // 10% weight for PEP status
) × Geographic Multiplier × Relationship Multiplier × PEP Multiplier
```

### Risk Score Multipliers

#### Sub-Category Severity Multipliers
- **CVT (Conviction)**: 1.8x - Highest severity
- **SAN (Sanction)**: 1.9x - Very high severity
- **IND (Indictment)**: 1.6x - High severity
- **CHG (Charged)**: 1.4x - Medium-high severity
- **ART (Arrest)**: 1.3x - Medium severity
- **SPT (Suspected)**: 1.1x - Low severity
- **ACQ (Acquitted)**: 0.3x - Very low severity
- **DMS (Dismissed)**: 0.4x - Low severity

#### Event Frequency Multipliers
- **Single Event**: 1.0x (baseline)
- **Multiple Events**: 1.0 + (count - 1) × 0.2, capped at 2.0x
- **Repeat Offenses**: Enhanced weighting for pattern detection

#### Temporal Decay Factors
- **Current Year**: 1.0x (full weight)
- **1-2 Years**: 0.8x
- **2-3 Years**: 0.6x
- **3-5 Years**: 0.4x
- **5+ Years**: 0.2x (minimum weight)

#### Geographic Risk Multipliers
| Region/Country | Multiplier | Risk Level |
|----------------|------------|------------|
| Afghanistan, Iran, Iraq, Syria | 1.4x - 1.5x | Very High |
| North Korea, Russia, Belarus | 1.3x - 1.5x | High |
| Venezuela, Cuba, Myanmar | 1.2x - 1.3x | Medium-High |
| United States, Canada, UK | 0.95x | Low |
| Germany, France, Australia | 0.95x | Low |
| Switzerland, Denmark, Norway | 0.9x | Very Low |

---

## PEP Detection System

### PEP (Politically Exposed Person) Classification

The system identifies and classifies politically exposed persons using a sophisticated 14-level hierarchy with priority scoring:

#### Primary PEP Levels
| Code | Description | Priority | Risk Multiplier |
|------|-------------|----------|-----------------|
| HOS | Head of State | 100 | 1.3x |
| CAB | Cabinet Official | 95 | 1.3x |
| INF | Senior Infrastructure Official | 85 | 1.3x |
| MUN | Municipal Official | 70 | 1.15x |
| LEG | Senior Legislative Branch | 75 | 1.15x |
| AMB | Ambassador/Diplomatic Official | 80 | 1.15x |
| MIL | Senior Military Figure | 85 | 1.3x |
| JUD | Senior Judicial Figure | 80 | 1.15x |
| POL | Political Party Figure | 70 | 1.15x |

#### Family & Associate PEP Levels
| Code | Description | Priority | Risk Multiplier |
|------|-------------|----------|-----------------|
| FAM | Family Member | 60 | 1.15x |
| ASC | Close Associate | 50 | 1.15x |
| SPO | Spouse | 80 | 1.15x |
| CHI | Child | 75 | 1.15x |
| PAR | Parent | 70 | 1.15x |
| SIB | Sibling | 65 | 1.15x |
| REL | Other Relative | 55 | 1.15x |
| BUS | Business Associate | 45 | 1.0x |

### PEP Detection Methodology

#### 1. Attribute Analysis
- **PTY (Position) Fields**: Political titles and positions
- **RMK (Remarks) Fields**: PEP termination notices, political references
- **URL Fields**: References to PEP lists and databases

#### 2. Event Code Analysis
Political event codes that trigger PEP classification:
- **POL**: Political activities
- **GOV**: Government positions
- **PEP**: Direct PEP classification
- **HOS**: Head of state activities
- **CAB**: Cabinet-level activities

#### 3. Name Pattern Analysis
Detection of political titles in entity names:
- President, Prime Minister, Minister
- Senator, Representative, Governor
- Ambassador, Consul, Diplomatic titles
- Military ranks (General, Admiral, Colonel)

#### 4. Address Analysis
Government and political location indicators:
- Government buildings and complexes
- Political party headquarters
- Embassy and consulate addresses
- Parliament and legislative buildings

#### 5. URL Pattern Matching
Recognition of PEP list references:
- Lista_PEP (Uruguay PEP lists)
- PEPCA (Anti-Corruption Prosecutor's Office)
- Government transparency portals
- International sanctions lists

---

## Database Architecture

### Core Database Schema

The application operates on a comprehensive database schema with 17+ interconnected tables:

#### Primary Entity Tables
- **individual_mapping**: Core individual entity records
- **organization_mapping**: Core organization entity records
- **individual_events**: Events and risk activities for individuals
- **organization_events**: Events and risk activities for organizations

#### Supplementary Data Tables
- **individual_addresses**: Physical addresses and locations
- **organization_addresses**: Organization addresses and facilities
- **individual_aliases**: Alternative names and identities
- **organization_aliases**: Organization alternative names
- **individual_attributes**: Additional entity attributes
- **organization_attributes**: Organization-specific attributes
- **individual_identifications**: ID documents and numbers
- **organization_identifications**: Business registration IDs
- **individual_date_of_births**: Birth date information
- **relationships**: Entity-to-entity relationships
- **sources**: Data source information and metadata
- **grid_orbis_mapping**: BVD Orbis integration data
- **code_dictionary**: Event codes and descriptions

### Table Relationships

#### Entity-Centric Design
```
Entity (individual/organization)
├── Events (1:N) → Risk assessment data
├── Addresses (1:N) → Geographic information
├── Aliases (1:N) → Alternative identities
├── Attributes (1:N) → PEP and other classifications
├── Identifications (1:N) → Document references
├── Relationships (1:N) → Network connections
└── Sources (N:N) → Data provenance
```

#### Key Foreign Key Relationships
- All tables connected via `entity_id`
- `risk_id` provides additional entity linking
- `source_item_id` tracks data lineage
- `systemId` identifies source systems

### Data Aggregation Strategy

#### JSON Collection Using COLLECT_LIST
The application uses Databricks SQL's `COLLECT_LIST` function to aggregate related data:

```sql
SELECT
    e.*,
    TO_JSON(COLLECT_LIST(STRUCT(
        ev.event_category_code,
        ev.event_sub_category_code,
        ev.event_date,
        ev.event_description,
        ev.event_reference_source_item_id
    ))) AS events
FROM {entity_type}_mapping e
LEFT JOIN {entity_type}_events ev ON e.entity_id = ev.entity_id
GROUP BY e.entity_id
```

This approach ensures:
- **Single Query Efficiency**: All related data retrieved in one operation
- **Hierarchical Structure**: Maintains parent-child relationships
- **JSON Serialization**: API-ready data format
- **Null Handling**: Graceful handling of missing relationships

---

## Search Functionality

### Search Types & Capabilities

#### 1. Basic Search
- **Entity Name**: Fuzzy matching with similarity scoring
- **Entity ID**: Exact matching with validation
- **Risk ID**: Direct risk identifier lookup
- **BVD ID**: Bureau van Dijk identifier search

#### 2. Advanced Boolean Search
- **AND Logic**: All conditions must be met
- **OR Logic**: Any condition can be met
- **Mixed Logic**: Complex boolean expressions
- **Parenthetical Grouping**: Nested condition support

#### 3. Regex Pattern Search
- **Pattern Matching**: Regular expression support
- **Wildcard Search**: % and _ operators
- **Case Sensitivity**: Configurable matching
- **Unicode Support**: International character sets

#### 4. Fuzzy Matching
- **Levenshtein Distance**: Edit distance calculations
- **Phonetic Matching**: Sound-based similarity
- **Threshold Scoring**: Configurable similarity levels (0.8 default)
- **Auto-completion**: Suggested completions

### Filter Categories

#### Core Entity Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| Entity Type | Individual or Organization | Individual, Organization |
| Entity ID | Unique entity identifier | Exact match |
| Entity Name | Primary entity name | Fuzzy, exact, regex |
| Risk ID | Risk assessment identifier | Exact match |
| Source Item ID | Source system identifier | Exact match |
| System ID | Source system hash | Exact match |
| BVD ID | Bureau van Dijk identifier | Exact match |

#### Temporal Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| Entity Date | Entity record date | Date range, relative |
| Event Date Range | Event occurrence dates | From/to dates |
| Use Date Range | Enable temporal filtering | Boolean toggle |
| Entity Year | Specific year filtering | Year + range |
| Birth Date Components | Individual birth dates | Year, month, day |

#### Geographic Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| Address | Street address | Text search |
| City | City/municipality | Text search |
| Province/State | Administrative region | Text search |
| Country | Country name/code | Dropdown selection |
| Geographic Risk | Country risk assessment | Risk multiplier |

#### Risk & Event Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| Risk Score Range | Numeric risk score | 0-120 slider |
| Risk Severity | 4-tier classification | Critical, Valuable, Investigative, Probative |
| Event Category | Primary risk codes | 50+ categories |
| Event Sub-Category | Detailed classifications | CVT, SAN, IND, CHG, etc. |
| Critical Risk Events | High-severity filters | Multiple selection |
| Valuable Risk Events | Medium-high severity | Multiple selection |
| Investigative Risk Events | Medium severity | Multiple selection |
| Probative Risk Events | Lower severity | Multiple selection |

#### PEP & Political Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| PEP Status | Political exposure | PEP, Non-PEP, Unknown |
| PEP Priority | Political importance | 0-100 scale |
| PEP Levels | Specific classifications | HOS, CAB, INF, MUN, etc. |

#### Relationship Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| Minimum Relationships | Relationship count threshold | Numeric input |
| Include Relationships | Relationship data inclusion | Boolean toggle |
| Related Entity Analysis | Network depth | 1-3 degrees |

#### Data Quality Filters
| Filter Type | Description | Options |
|-------------|-------------|---------|
| Include Aliases | Alternative names | Boolean toggle |
| Include Addresses | Geographic data | Boolean toggle |
| Include Identifications | ID documents | Boolean toggle |
| Source Systems | Data source filtering | Multiple selection |
| Source Names | Specific source names | Text input |
| Source Keys | Source identifiers | Text input |

### Advanced Query Features

#### Query Optimization
- **Query Caching**: 5-minute TTL for repeated searches
- **Parallel Execution**: Up to 4 concurrent subqueries
- **Index Hints**: Database optimization suggestions
- **Batch Processing**: 1000-record batches
- **Result Streaming**: Progressive loading for large datasets

#### Search Performance Settings
- **Query Timeout**: 30-600 seconds configurable
- **Batch Size**: 100-1000 records per batch
- **Max Results**: Up to 10,000 results (configurable)
- **Cache TTL**: 60-3600 seconds
- **Parallel Queries**: 1-10 concurrent operations

---

## Visualization Features

### Network Analysis

#### Interactive Network Graphs
The application provides sophisticated network visualization capabilities for relationship analysis:

#### Node Types & Styling
- **Individual Entities**: Red circular nodes (size: 300-800 pixels)
- **Organization Entities**: Blue square nodes (size: 400-1000 pixels)
- **PEP Entities**: Gold border highlighting
- **High-Risk Entities**: Red border with increased thickness

#### Edge Types & Relationships
- **Outgoing Relationships**: Solid green lines (width: 1-3 pixels)
- **Incoming Relationships**: Dashed red lines (width: 1-2 pixels)
- **Bidirectional**: Purple solid lines (width: 2-4 pixels)
- **Weighted Relationships**: Line thickness indicates relationship strength

#### Layout Algorithms
| Algorithm | Description | Best For |
|-----------|-------------|----------|
| Spring | Force-directed layout | General networks |
| Circular | Circular arrangement | Hierarchical data |
| Random | Random positioning | Large networks |
| Shell | Concentric shells | Layered relationships |
| Kamada-Kawai | Energy-based layout | Small networks |

#### Interactive Features
- **Zoom & Pan**: Mouse wheel and drag controls
- **Node Selection**: Click to highlight and show details
- **Edge Highlighting**: Hover to show relationship details
- **Subgraph Filtering**: Filter by node types or relationships
- **Export Options**: PNG, SVG, PDF formats

### Chart Types & Analytics

#### Risk Distribution Charts
- **Severity Pie Charts**: Risk level distribution
- **Risk Score Histograms**: Score distribution analysis
- **Geographic Heat Maps**: Country-based risk analysis
- **Temporal Risk Trends**: Risk evolution over time

#### PEP Analysis Charts
- **PEP Level Distribution**: Hierarchy breakdown
- **Political Exposure Timeline**: PEP status changes
- **Geographic PEP Mapping**: PEP distribution by country
- **Family Network Analysis**: PEP relationship trees

#### Source & Quality Analytics
- **Source Reliability Scores**: Data quality metrics
- **Data Completeness Analysis**: Field availability statistics
- **Cross-Reference Validation**: Multi-source confirmation
- **Update Frequency Analysis**: Data freshness metrics

### Visualization Configuration

#### Chart Styling
- **Figure Dimensions**: 12×8 inches (default), 6×4 to 20×12 range
- **DPI Settings**: 100-300 DPI for high-resolution exports
- **Color Schemes**: Categorical, sequential, diverging palettes
- **Font Families**: Arial, Times, Helvetica, system fonts
- **Font Sizes**: 8-16 points for labels, 10-20 for titles

#### Performance Optimization
- **Node Limits**: 50-200 nodes for optimal performance
- **Edge Limits**: 100-500 edges per visualization
- **Rendering Quality**: High, medium, low quality options
- **Animation Settings**: Enable/disable transitions
- **Memory Management**: Automatic cleanup of large visualizations

---

## Export Capabilities

### Supported Export Formats

#### Excel Export (.xlsx)
- **Multi-Sheet Structure**: 
  - Summary Sheet: Overview statistics and metadata
  - Entities Sheet: Complete entity data (up to 1M rows)
  - Events Sheet: All event records
  - Relationships Sheet: Network connections
  - Analysis Sheet: Risk calculations and scores
- **Formatting Features**:
  - Conditional formatting for risk levels
  - Charts and graphs embedded
  - Frozen headers and filters
  - Cell comments for complex data
- **Advanced Features**:
  - Pivot tables for analysis
  - Data validation rules
  - Password protection options
  - Worksheet protection

#### CSV Export (.csv)
- **Character Encoding**: UTF-8 with BOM support
- **Delimiter Options**: Comma, semicolon, tab, pipe
- **Quote Handling**: RFC 4180 compliant
- **Large Dataset Support**: Streaming export for 100K+ records
- **Compression**: Optional ZIP compression
- **Multiple Files**: Separate files for different data types

#### JSON Export (.json)
- **Hierarchical Structure**: Nested objects preserving relationships
- **Metadata Inclusion**: Export timestamp, user info, criteria
- **Schema Validation**: JSON Schema compliance
- **Compression Options**: GZIP compression for large files
- **API Compatibility**: RESTful API structure
- **Unicode Support**: Full international character support

#### PDF Export (.pdf)
- **Report Layouts**: Executive summary, detailed analysis, appendices
- **Visualization Embedding**: High-resolution charts and graphs
- **Page Formatting**: Headers, footers, page numbers
- **Table of Contents**: Automated TOC generation
- **Bookmarks**: Section navigation
- **Security Features**: Password protection, printing restrictions

### Export Content Options

#### Entity Data Export
- **Core Fields**: ID, name, risk score, severity level
- **Event History**: Complete event timeline with details
- **Relationship Data**: Network connections and types
- **Geographic Information**: Addresses and locations
- **PEP Classifications**: Political exposure details
- **Source Attribution**: Data provenance and quality scores

#### Risk Analysis Export
- **Risk Calculations**: Detailed scoring methodology
- **Component Breakdown**: Event, geographic, temporal, PEP scores
- **Historical Risk**: Risk score evolution over time
- **Comparative Analysis**: Benchmarking against peer groups
- **Confidence Intervals**: Statistical uncertainty measures

#### Visualization Export
- **High-Resolution Images**: 300-600 DPI for print quality
- **Interactive Formats**: HTML with JavaScript for web embedding
- **Vector Graphics**: SVG format for scalability
- **Animation Support**: GIF export for temporal analysis
- **3D Visualizations**: WebGL exports for complex networks

### Export Security & Compliance

#### Data Protection
- **Watermarking**: Automatic watermark application
- **Access Logging**: Detailed audit trails
- **Encryption**: AES-256 encryption for sensitive exports
- **Redaction**: Automated PII removal options
- **Retention Policies**: Automated export cleanup

#### Compliance Features
- **GDPR Compliance**: Data subject rights support
- **CCPA Compliance**: California privacy law adherence
- **SOX Compliance**: Financial reporting standards
- **Audit Trails**: Complete export activity logging
- **Version Control**: Export versioning and change tracking

---

## User Interface

### Design Philosophy

#### Modern Glass Design
The application employs a contemporary glass morphism design language:
- **Backdrop Filters**: Semi-transparent panels with blur effects
- **Gradient Overlays**: Subtle color transitions
- **Depth Layering**: Visual hierarchy through shadows and elevation
- **Smooth Animations**: CSS transitions for state changes
- **Rounded Corners**: Modern border-radius styling

#### Responsive Layout System
- **Flexbox Grid**: CSS Flexbox for responsive layouts
- **Breakpoint System**: Mobile (320px), tablet (768px), desktop (1024px+)
- **Adaptive Components**: UI elements that scale appropriately
- **Touch-Friendly**: Mobile gesture support
- **Accessibility Compliance**: WCAG 2.1 AA standards

### Main Interface Components

#### Navigation Structure
- **Tab-Based Navigation**: Search, Clustering, Analysis, Settings
- **Breadcrumb Navigation**: Context-aware location tracking
- **Quick Access Toolbar**: Frequently used functions
- **Collapsible Sidebar**: Additional navigation options
- **Footer Links**: Help, documentation, version info

#### Search Interface

##### Expandable Sections
- **Core Search Fields**: Always visible primary filters
- **Advanced Filters**: Collapsible section with 30+ options
- **Date Range Controls**: Specialized temporal filtering
- **PEP & Risk Filters**: Dedicated political and risk sections
- **Relationship Filters**: Network analysis options

##### Smart Input Components
- **Auto-completion**: Real-time suggestions as user types
- **Validation**: Immediate feedback on input errors
- **Placeholders**: Contextual hints and examples
- **Multi-select Dropdowns**: Tag-based selection interface
- **Range Sliders**: Continuous value selection for scores

##### Boolean Logic Builder
- **Visual Query Builder**: Drag-and-drop condition creation
- **Logic Operators**: AND, OR, NOT with visual indicators
- **Parenthetical Grouping**: Nested condition support
- **Syntax Highlighting**: Color-coded query expressions
- **Query Validation**: Real-time syntax checking

#### Results Display

##### Data Tables
- **Sortable Columns**: Click-to-sort with direction indicators
- **Filterable Headers**: Column-specific filtering
- **Pagination Controls**: Previous/next with page jumping
- **Row Selection**: Checkbox selection for bulk operations
- **Expandable Rows**: Drill-down for detailed information
- **Column Resizing**: Draggable column boundaries
- **Column Reordering**: Drag-and-drop column arrangement

##### Entity Cards
- **Risk Level Indicators**: Color-coded severity badges
- **Expandable Sections**: Show/hide detailed information
- **Action Buttons**: Quick access to common operations
- **Thumbnail Images**: Entity photos when available
- **Progress Bars**: Data completeness indicators
- **Tags**: Quick identification labels

##### Relationship Views
- **Tree Structure**: Hierarchical relationship display
- **Network Diagrams**: Interactive node-link visualization
- **Tabular Relationships**: Structured relationship data
- **Relationship Types**: Color-coded connection types
- **Depth Indicators**: Relationship distance markers

### Interactive Features

#### Real-Time Updates
- **Live Search**: Results update as user types (debounced)
- **Progressive Loading**: Streaming results for large datasets
- **Auto-Refresh**: Configurable automatic data updates
- **Change Notifications**: Alerts for data modifications
- **Collaboration Features**: Multi-user change awareness

#### State Management
- **Session Persistence**: Maintain search state across browser sessions
- **Bookmark Support**: URL-based search state sharing
- **History Navigation**: Browser back/forward support
- **Undo/Redo**: Action reversal capabilities
- **Preference Storage**: User customization persistence

#### Performance Optimization
- **Lazy Loading**: Load content as needed
- **Virtual Scrolling**: Efficient handling of large datasets
- **Caching Strategy**: Intelligent data caching
- **Debounced Inputs**: Reduced API calls during typing
- **Compression**: Gzip compression for data transfer

### Dark Mode Support

#### Theme System
- **Automatic Detection**: System theme preference detection
- **Manual Toggle**: User-controlled theme switching
- **Persistence**: Theme preference storage
- **Smooth Transitions**: Animated theme changes
- **Component Adaptation**: All UI elements support both themes

#### Color Schemes
- **Light Mode**: High contrast, white backgrounds
- **Dark Mode**: Reduced eye strain, dark backgrounds
- **High Contrast**: Accessibility-focused variants
- **Custom Themes**: User-defined color schemes
- **Brand Compliance**: Corporate color palette support

---

## Configuration & Settings

### Database Configuration

#### Connection Settings
```python
database_config = {
    'server_hostname': 'databricks.company.com',
    'http_path': '/sql/1.0/warehouses/warehouse_id',
    'access_token': 'dapi_token',
    'catalog_name': 'prd_bronze_catalog',
    'schema_name': 'grid',
    'connection_timeout': 30,
    'query_timeout': 300,
    'max_connections': 10,
    'ssl_verify': True
}
```

#### Query Optimization
```python
query_optimization = {
    'enable_query_cache': True,
    'cache_ttl': 300,  # 5 minutes
    'enable_parallel_subqueries': True,
    'enable_index_hints': True,
    'batch_size': 1000,
    'max_parallel_queries': 4,
    'enable_partitioning': True
}
```

### Performance Settings

#### Search Performance
- **Query Timeout**: 30-600 seconds (default: 300)
- **Batch Size**: 100-1000 records (default: 1000)
- **Max Results**: 100-10000 entities (default: 1000)
- **Cache TTL**: 60-3600 seconds (default: 300)
- **Parallel Queries**: 1-10 concurrent (default: 4)

#### UI Performance
- **Result Streaming**: Enable for 100+ results
- **Virtual Scrolling**: Enable for 50+ rows
- **Progressive Loading**: Batch size 25-100
- **Auto-refresh**: 30-3600 seconds (disabled by default)
- **Debounce Delay**: 200-1000ms (default: 300ms)

### Security Configuration

#### Authentication Settings
```python
authentication_config = {
    'auth_method': 'oauth',  # local, ldap, oauth, saml
    'oauth_provider': 'azure_ad',
    'oauth_client_id': 'client_id',
    'oauth_tenant_id': 'tenant_id',
    'session_timeout': 3600,  # 1 hour
    'max_login_attempts': 5,
    'lockout_duration': 1800  # 30 minutes
}
```

#### Password Policy
```python
password_policy = {
    'min_length': 12,
    'require_uppercase': True,
    'require_lowercase': True,
    'require_numbers': True,
    'require_special_chars': True,
    'max_age_days': 90,
    'history_count': 12,
    'complexity_score': 3
}
```

#### Network Security
```python
network_security = {
    'force_https': True,
    'hsts_max_age': 31536000,  # 1 year
    'ip_whitelist': ['10.0.0.0/8', '192.168.0.0/16'],
    'rate_limiting': {
        'requests_per_minute': 100,
        'burst_limit': 20
    },
    'cors_origins': ['https://app.company.com']
}
```

### Business Logic Configuration

#### Risk Thresholds
```python
risk_thresholds = {
    'critical': 80,      # 80-100
    'valuable': 60,      # 60-79
    'investigative': 40, # 40-59
    'probative': 0       # 0-39
}
```

#### Age Thresholds
```python
age_thresholds = {
    'young_adult': 25,
    'middle_aged': 50,
    'senior': 65,
    'elderly': 80
}
```

#### Geographic Risk Factors
```python
geographic_risk_multipliers = {
    'high_risk': 2.0,    # Afghanistan, Syria, etc.
    'medium_high': 1.5,  # Russia, Venezuela, etc.
    'medium': 1.2,       # Turkey, Egypt, etc.
    'low': 0.95,         # US, Canada, UK, etc.
    'very_low': 0.9      # Switzerland, Denmark, etc.
}
```

#### Temporal Weighting
```python
temporal_weighting = {
    'enable_temporal_weighting': True,
    'decay_rate': 0.1,  # 10% decay per year
    'max_age_years': 10,
    'minimum_weight': 0.1,
    'recent_boost_months': 6,
    'recent_boost_factor': 1.2
}
```

### Data Quality Configuration

#### Completeness Scoring
```python
completeness_weights = {
    'entity_name': 0.3,
    'entity_id': 0.2,
    'events': 0.15,
    'addresses': 0.1,
    'relationships': 0.1,
    'attributes': 0.05,
    'sources': 0.05,
    'identifications': 0.05
}
```

#### Source Reliability
```python
source_reliability_scores = {
    'government_official': 0.95,
    'regulatory_filing': 0.9,
    'court_record': 0.85,
    'news_media_tier1': 0.8,
    'news_media_tier2': 0.7,
    'social_media': 0.4,
    'unverified_source': 0.2
}
```

---

## Data Processing Pipeline

### Search Execution Flow

#### 1. Input Validation & Preprocessing
```python
def validate_search_criteria(criteria):
    """Comprehensive input validation"""
    # Sanitize inputs
    # Check for SQL injection patterns
    # Validate date ranges
    # Verify numeric ranges
    # Normalize text inputs
    # Apply business rules
```

#### 2. Query Construction
```python
def build_search_query(criteria, entity_type, max_results):
    """Dynamic SQL query generation"""
    # Base entity selection
    # Dynamic JOIN construction
    # WHERE clause building
    # Aggregation with COLLECT_LIST
    # Optimization hints
    # Parameter binding
```

#### 3. Database Execution
```python
def execute_search(query, params):
    """Optimized database execution"""
    # Connection pool management
    # Query caching check
    # Parallel execution
    # Result streaming
    # Error handling
    # Performance monitoring
```

#### 4. Data Processing
```python
def process_results(raw_results):
    """Comprehensive data transformation"""
    # JSON field parsing
    # Data type conversions
    # Null value handling
    # Field validation
    # Data enrichment
    # Quality scoring
```

#### 5. Risk Calculation
```python
def calculate_comprehensive_risk(entity):
    """Multi-component risk assessment"""
    # Event risk scoring
    # Geographic risk assessment
    # Temporal risk adjustment
    # Relationship risk propagation
    # PEP risk classification
    # Final score aggregation
```

#### 6. Post-Processing
```python
def apply_post_processing(entities, criteria):
    """Final filtering and sorting"""
    # Risk score filtering
    # Additional criteria application
    # Sorting and ranking
    # Pagination preparation
    # UI data formatting
```

### Risk Calculation Pipeline

#### Component Score Calculation

##### Event Risk Scoring
```python
def calculate_event_risk(events):
    total_score = 0
    weighted_count = 0
    
    for event in events:
        # Base severity from risk code
        base_severity = risk_code_severities.get(event.category_code, 25)
        
        # Source priority multiplier
        priority_multiplier = source_priorities.get(event.source_priority, 1.0)
        
        # Temporal decay factor
        age_multiplier = calculate_event_age_multiplier(event.date)
        
        # Sub-category severity
        subcategory_multiplier = subcategory_multipliers.get(event.sub_category, 1.0)
        
        # Frequency bonus for repeat offenses
        frequency_multiplier = min(1.0 + (event_count - 1) * 0.2, 2.0)
        
        # Component score calculation
        event_score = (base_severity * priority_multiplier * 
                      age_multiplier * frequency_multiplier * 
                      subcategory_multiplier)
        
        total_score += event_score
        weighted_count += priority_multiplier
    
    return total_score / weighted_count if weighted_count > 0 else 0
```

##### Geographic Risk Assessment
```python
def calculate_geographic_risk(addresses):
    if not addresses:
        return 25.0  # Default neutral score
    
    max_risk = 0
    for address in addresses:
        country = address.get('country', '').upper()
        country_risk = geographic_risk_multipliers.get(country, 1.0)
        
        # Convert multiplier to risk score (25-75 range)
        risk_score = 25 + (country_risk - 0.5) * 50
        max_risk = max(max_risk, risk_score)
    
    return min(max_risk, 75.0)  # Cap at 75
```

##### Temporal Risk Scoring
```python
def calculate_temporal_risk(entity):
    entity_age_years = calculate_entity_age(entity.entity_date)
    recent_events = count_recent_events(entity.events, months=12)
    
    # Base temporal score
    base_score = 30.0
    
    # Age adjustment
    if entity_age_years < 1:
        age_adjustment = 10.0  # New entities get bonus
    elif entity_age_years > 10:
        age_adjustment = -5.0  # Old entities get penalty
    else:
        age_adjustment = 0.0
    
    # Recent activity bonus
    activity_bonus = min(recent_events * 2.0, 20.0)
    
    return min(base_score + age_adjustment + activity_bonus, 60.0)
```

##### Relationship Risk Propagation
```python
def calculate_relationship_risk(relationships):
    if not relationships:
        return 0
    
    total_risk = 0
    relationship_count = len(relationships)
    
    for relationship in relationships:
        # Related entity risk (if available)
        related_risk = relationship.get('related_entity_risk_score', 25)
        
        # Relationship type weight
        relationship_type = relationship.get('type', 'Unknown')
        type_weight = relationship_type_weights.get(relationship_type, 0.5)
        
        # Distance decay (1st degree = 1.0, 2nd = 0.5, 3rd = 0.25)
        distance_weight = 0.5 ** (relationship.get('degree', 1) - 1)
        
        relationship_risk = related_risk * type_weight * distance_weight
        total_risk += relationship_risk
    
    # Average with diminishing returns for many relationships
    avg_risk = total_risk / relationship_count
    network_bonus = min(relationship_count * 2.0, 20.0)
    
    return min(avg_risk + network_bonus, 50.0)
```

##### PEP Risk Classification
```python
def calculate_pep_risk(entity):
    pep_status = entity.get('pep_status', 'Unknown')
    pep_levels = entity.get('pep_levels', [])
    
    if pep_status != 'PEP':
        return 0
    
    if not pep_levels:
        return 10.0  # Default PEP score
    
    # Maximum priority from PEP levels
    max_priority = max([pep_priorities.get(level, 0) for level in pep_levels])
    
    # Convert priority (0-100) to risk score (0-40)
    pep_risk = (max_priority / 100.0) * 40.0
    
    return pep_risk
```

#### Final Score Aggregation
```python
def aggregate_final_risk_score(components):
    """Weighted aggregation of risk components"""
    
    event_score = components['event_score']
    geographic_score = components['geographic_score']
    temporal_score = components['temporal_score']
    relationship_score = components['relationship_score']
    pep_score = components['pep_score']
    
    # Weighted combination
    base_score = (
        event_score * 0.4 +           # 40% - Primary risk factor
        relationship_score * 0.2 +    # 20% - Network effects
        geographic_score * 0.15 +     # 15% - Location risk
        temporal_score * 0.15 +       # 15% - Time factors
        pep_score * 0.1               # 10% - Political exposure
    )
    
    # Apply multipliers
    final_score = (base_score * 
                  components.get('geographic_multiplier', 1.0) *
                  components.get('relationship_multiplier', 1.0) *
                  components.get('pep_multiplier', 1.0))
    
    # Cap at maximum score
    return min(round(final_score, 1), 120.0)
```

### Data Quality Processing

#### Completeness Assessment
```python
def calculate_completeness_score(entity):
    """Data completeness scoring"""
    total_weight = 0
    achieved_weight = 0
    
    for field, weight in completeness_weights.items():
        total_weight += weight
        
        field_value = entity.get(field)
        if field_value and field_value != []:
            if isinstance(field_value, list):
                # Array fields - check if non-empty
                if len(field_value) > 0:
                    achieved_weight += weight
            elif isinstance(field_value, str):
                # String fields - check if non-empty and meaningful
                if len(field_value.strip()) > 0:
                    achieved_weight += weight
            else:
                # Other types - check if truthy
                if field_value:
                    achieved_weight += weight
    
    return (achieved_weight / total_weight) * 100.0 if total_weight > 0 else 0
```

#### Source Reliability Assessment
```python
def calculate_source_reliability(sources):
    """Source reliability and confidence scoring"""
    if not sources:
        return 0.5  # Default moderate confidence
    
    total_weight = 0
    weighted_reliability = 0
    
    for source in sources:
        source_type = classify_source_type(source)
        reliability = source_reliability_scores.get(source_type, 0.5)
        
        # Source recency weight
        source_age = calculate_source_age(source.get('publish_date'))
        recency_weight = calculate_recency_weight(source_age)
        
        weighted_reliability += reliability * recency_weight
        total_weight += recency_weight
    
    return weighted_reliability / total_weight if total_weight > 0 else 0.5
```

#### Cross-Validation Processing
```python
def perform_cross_validation(entity):
    """Multi-source data validation"""
    validation_results = {
        'name_consistency': check_name_consistency(entity),
        'date_consistency': check_date_consistency(entity),
        'address_consistency': check_address_consistency(entity),
        'source_agreement': check_source_agreement(entity),
        'data_conflicts': identify_data_conflicts(entity)
    }
    
    # Overall validation confidence
    confidence_score = calculate_validation_confidence(validation_results)
    
    return {
        'validation_results': validation_results,
        'confidence_score': confidence_score,
        'validation_timestamp': datetime.now().isoformat()
    }
```

---

## Business Rules & Calculations

### Risk Assessment Rules

#### Critical Risk Determination
An entity is classified as **Critical Risk** when:
- Risk score ≥ 80 points, OR
- Any event with category codes: TER, SAN, WAR, GEN, OR
- PEP level of HOS (Head of State) with recent activity, OR
- Multiple high-severity events within 2 years, OR
- Located in very high-risk geography with criminal events

#### Valuable Risk Determination
An entity is classified as **Valuable Risk** when:
- Risk score 60-79 points, OR
- Events in categories: FRD, COR, BRB, CYB with conviction/indictment, OR
- PEP level of CAB (Cabinet) or INF (Infrastructure) with business connections, OR
- Significant relationship network (5+ connections) with other valuable entities

#### Risk Score Validation Rules
```python
def validate_risk_score(entity, calculated_score):
    """Business rule validation for risk scores"""
    
    # Minimum score rules
    if has_criminal_conviction(entity):
        calculated_score = max(calculated_score, 40.0)
    
    if has_sanctions_listing(entity):
        calculated_score = max(calculated_score, 80.0)
    
    if has_terrorism_events(entity):
        calculated_score = max(calculated_score, 90.0)
    
    # Maximum score caps
    if not has_recent_activity(entity, years=5):
        calculated_score = min(calculated_score, 60.0)
    
    if is_deceased(entity):
        calculated_score *= 0.5  # Reduce risk for deceased entities
    
    # PEP overrides
    if is_current_head_of_state(entity):
        calculated_score = max(calculated_score, 85.0)
    
    return min(calculated_score, 120.0)  # Absolute maximum
```

### PEP Classification Rules

#### Primary PEP Determination
An entity is classified as Primary PEP when:
- Holds current position: HOS, CAB, INF, MIL (senior level)
- Explicit PEP list inclusion from government sources
- Political event codes with current dates
- Government address associations with position titles

#### Family PEP Determination
An entity is classified as Family PEP when:
- Direct relationship to Primary PEP (spouse, child, parent, sibling)
- Shared addresses with known PEPs
- Business associations with PEP entities
- Name patterns indicating family relationships

#### PEP Termination Rules
```python
def check_pep_termination(entity):
    """PEP status termination logic"""
    
    # Check for explicit termination records
    for attribute in entity.get('attributes', []):
        if attribute.get('alias_code_type') == 'RMK':
            remark_text = attribute.get('alias_value', '')
            if 'Reason Of Termination' in remark_text:
                termination_date = extract_termination_date(remark_text)
                if termination_date and is_past_date(termination_date):
                    return True, termination_date
    
    # Check for position change events
    recent_events = get_recent_events(entity, months=12)
    for event in recent_events:
        if event.get('event_category_code') in ['GOV', 'POL']:
            if 'resignation' in event.get('event_description', '').lower():
                return True, event.get('event_date')
    
    return False, None
```

### Geographic Risk Rules

#### High-Risk Country Classification
Countries are classified as high-risk based on:
- Current international sanctions (OFAC, EU, UN)
- Failed state index scores > 90
- Corruption perception index < 30
- Political stability index < -1.0
- Active conflict zones
- Major money laundering concerns

#### Geographic Risk Multipliers
```python
geographic_risk_rules = {
    # Very High Risk (2.0x multiplier)
    'failed_states': ['AF', 'SY', 'YE', 'SO'],
    'sanctions_targets': ['IR', 'KP', 'RU'],
    
    # High Risk (1.5x multiplier)
    'high_corruption': ['VE', 'MM', 'BY'],
    'political_instability': ['LB', 'PK', 'NG'],
    
    # Medium Risk (1.2x multiplier)
    'emerging_markets': ['TR', 'EG', 'IN', 'BR'],
    
    # Low Risk (0.95x multiplier)
    'developed_nations': ['US', 'CA', 'UK', 'DE', 'FR'],
    
    # Very Low Risk (0.9x multiplier)
    'low_corruption': ['CH', 'DK', 'NO', 'SE']
}
```

### Temporal Risk Rules

#### Event Age Weighting
```python
def calculate_event_age_weight(event_date):
    """Temporal decay calculation for events"""
    if not event_date:
        return 0.5  # Unknown date gets moderate weight
    
    days_old = (datetime.now().date() - event_date).days
    years_old = days_old / 365.25
    
    if years_old <= 1:
        return 1.0      # Full weight for recent events
    elif years_old <= 2:
        return 0.8      # 80% weight for 1-2 year events
    elif years_old <= 3:
        return 0.6      # 60% weight for 2-3 year events
    elif years_old <= 5:
        return 0.4      # 40% weight for 3-5 year events
    elif years_old <= 10:
        return 0.2      # 20% weight for 5-10 year events
    else:
        return 0.1      # 10% minimum weight for old events
```

#### Recency Boost Rules
Events receive additional weighting when:
- Within 6 months: +20% bonus
- Multiple events within 1 year: +10% per additional event
- Escalating severity pattern: +15% bonus
- Cross-jurisdictional events: +10% bonus

### Relationship Risk Rules

#### Relationship Type Weighting
```python
relationship_weights = {
    'family': 0.8,           # High propagation for family
    'business_partner': 0.7,  # High for business relationships
    'employee': 0.5,         # Medium for employment
    'associate': 0.6,        # Medium-high for associates
    'client': 0.4,           # Medium for client relationships
    'vendor': 0.3,           # Lower for vendor relationships
    'acquaintance': 0.2,     # Low for casual connections
    'unknown': 0.3           # Default medium-low
}
```

#### Network Risk Propagation
```python
def calculate_network_risk_propagation(entity, depth=2):
    """Risk propagation through relationship networks"""
    
    total_propagated_risk = 0
    relationship_count = 0
    
    for relationship in entity.get('relationships', []):
        # Base related entity risk
        related_risk = relationship.get('related_entity_risk_score', 0)
        
        # Relationship type weight
        rel_type = relationship.get('type', 'unknown')
        type_weight = relationship_weights.get(rel_type, 0.3)
        
        # Distance decay (1st degree = full, 2nd = half, etc.)
        degree = relationship.get('degree', 1)
        distance_decay = 0.5 ** (degree - 1)
        
        # Bidirectional bonus
        bidirectional_bonus = 1.2 if relationship.get('bidirectional', False) else 1.0
        
        # Calculate propagated risk
        propagated_risk = (related_risk * type_weight * 
                          distance_decay * bidirectional_bonus)
        
        total_propagated_risk += propagated_risk
        relationship_count += 1
    
    # Network effect calculation
    if relationship_count > 0:
        avg_propagated_risk = total_propagated_risk / relationship_count
        
        # Network size bonus (diminishing returns)
        network_bonus = min(math.log(relationship_count + 1) * 5.0, 20.0)
        
        return min(avg_propagated_risk + network_bonus, 50.0)
    
    return 0
```

### Data Quality Rules

#### Minimum Data Requirements
An entity record must have:
- Valid entity_id (non-null, non-empty)
- Entity name (at least 2 characters)
- Entity type (Individual or Organization)
- At least one data source reference
- Created/modified timestamps

#### Quality Score Calculation
```python
def calculate_data_quality_score(entity):
    """Comprehensive data quality assessment"""
    
    quality_factors = {
        'completeness': calculate_completeness_score(entity),
        'accuracy': calculate_accuracy_score(entity),
        'consistency': calculate_consistency_score(entity),
        'timeliness': calculate_timeliness_score(entity),
        'validity': calculate_validity_score(entity)
    }
    
    # Weighted quality score
    weights = {
        'completeness': 0.25,
        'accuracy': 0.25,
        'consistency': 0.20,
        'timeliness': 0.15,
        'validity': 0.15
    }
    
    total_score = sum(quality_factors[factor] * weights[factor] 
                     for factor in quality_factors)
    
    return {
        'overall_score': total_score,
        'factor_scores': quality_factors,
        'quality_grade': assign_quality_grade(total_score)
    }
```

---

## Technical Implementation

### Technology Stack

#### Backend Framework
- **NiceGUI**: Modern Python web framework for rapid UI development
- **Python 3.8+**: Core programming language with async/await support
- **Databricks SQL**: Enterprise data warehouse integration
- **Pandas**: Data manipulation and analysis library
- **NetworkX**: Network analysis and graph algorithms
- **Matplotlib**: Statistical visualization and charting
- **asyncio**: Asynchronous programming for improved performance

#### Database Integration
- **Databricks SQL Connector**: Native Databricks integration
- **Connection Pooling**: Efficient database connection management
- **Query Caching**: Redis-based query result caching
- **Streaming Results**: Large dataset handling with pagination
- **Prepared Statements**: SQL injection prevention

#### Security & Authentication
- **OAuth 2.0**: Enterprise authentication integration
- **SAML 2.0**: Single sign-on support
- **JWT Tokens**: Stateless session management
- **HTTPS/TLS**: End-to-end encryption
- **RBAC**: Role-based access control

### Architecture Patterns

#### Model-View-Controller (MVC)
```python
class EntitySearchApp:
    """Main application controller"""
    
    def __init__(self):
        self.model = EntitySearchModel()      # Data layer
        self.view = EntitySearchView()        # UI layer
        self.controller = self                # Control layer
```

#### Repository Pattern
```python
class EntityRepository:
    """Data access abstraction layer"""
    
    def search_entities(self, criteria):
        """Abstract search method"""
        pass
    
    def get_entity_by_id(self, entity_id):
        """Abstract entity retrieval"""
        pass

class DatabricksEntityRepository(EntityRepository):
    """Databricks-specific implementation"""
    
    def search_entities(self, criteria):
        query, params = self._build_query(criteria)
        return self._execute_query(query, params)
```

#### Factory Pattern
```python
class VisualizationFactory:
    """Factory for creating different visualization types"""
    
    @staticmethod
    def create_visualization(viz_type, data):
        if viz_type == 'network':
            return NetworkVisualization(data)
        elif viz_type == 'risk_chart':
            return RiskChartVisualization(data)
        elif viz_type == 'pep_analysis':
            return PEPAnalysisVisualization(data)
        else:
            raise ValueError(f"Unknown visualization type: {viz_type}")
```

#### Observer Pattern
```python
class SearchResultObserver:
    """Observer for search result updates"""
    
    def update(self, search_results):
        """Called when search results change"""
        pass

class UIUpdateObserver(SearchResultObserver):
    """UI-specific result observer"""
    
    def update(self, search_results):
        self.update_result_display(search_results)
        self.update_analytics_panel(search_results)
        self.refresh_visualizations(search_results)
```

### Performance Optimizations

#### Query Optimization
```python
def optimize_search_query(query, params):
    """Query optimization strategies"""
    
    # Add index hints for large tables
    if 'individual_mapping' in query:
        query = query.replace(
            'FROM individual_mapping',
            'FROM individual_mapping /*+ USE_INDEX(idx_entity_id) */'
        )
    
    # Partition pruning for date ranges
    if has_date_filter(params):
        query = add_partition_filter(query, params)
    
    # Parallel execution hints
    if is_complex_query(query):
        query = add_parallel_hint(query)
    
    return query, params
```

#### Caching Strategy
```python
class QueryCache:
    """Multi-level caching system"""
    
    def __init__(self):
        self.l1_cache = {}  # In-memory cache (5 minutes)
        self.l2_cache = RedisCache()  # Redis cache (1 hour)
        self.l3_cache = DatabaseCache()  # Database cache (24 hours)
    
    def get(self, cache_key):
        # Check L1 cache first
        if cache_key in self.l1_cache:
            return self.l1_cache[cache_key]
        
        # Check L2 cache
        result = self.l2_cache.get(cache_key)
        if result:
            self.l1_cache[cache_key] = result
            return result
        
        # Check L3 cache
        result = self.l3_cache.get(cache_key)
        if result:
            self.l2_cache.set(cache_key, result)
            self.l1_cache[cache_key] = result
            return result
        
        return None
```

#### Async Processing
```python
async def process_search_request(criteria):
    """Asynchronous search processing"""
    
    # Parallel data retrieval
    tasks = [
        asyncio.create_task(search_entities(criteria)),
        asyncio.create_task(get_related_data(criteria)),
        asyncio.create_task(calculate_analytics(criteria))
    ]
    
    # Wait for all tasks to complete
    entities, related_data, analytics = await asyncio.gather(*tasks)
    
    # Process results
    processed_results = await process_results_async(entities, related_data)
    
    return {
        'entities': processed_results,
        'analytics': analytics,
        'metadata': generate_metadata(criteria)
    }
```

### Error Handling & Logging

#### Comprehensive Error Handling
```python
class EntitySearchException(Exception):
    """Base exception for entity search operations"""
    pass

class DatabaseConnectionError(EntitySearchException):
    """Database connectivity issues"""
    pass

class QueryExecutionError(EntitySearchException):
    """Query execution failures"""
    pass

class DataValidationError(EntitySearchException):
    """Data validation failures"""
    pass

def handle_search_error(error, context):
    """Centralized error handling"""
    
    error_info = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'context': context,
        'timestamp': datetime.now().isoformat(),
        'user_id': get_current_user_id(),
        'session_id': get_session_id()
    }
    
    # Log error
    logger.error(f"Search error: {error_info}")
    
    # User-friendly error messages
    if isinstance(error, DatabaseConnectionError):
        return "Database connection failed. Please try again later."
    elif isinstance(error, QueryExecutionError):
        return "Search query failed. Please check your criteria and try again."
    elif isinstance(error, DataValidationError):
        return f"Invalid search criteria: {error.message}"
    else:
        return "An unexpected error occurred. Please contact support."
```

#### Structured Logging
```python
import structlog

logger = structlog.get_logger()

def log_search_operation(operation, criteria, results_count, duration):
    """Structured logging for search operations"""
    
    logger.info(
        "search_operation_completed",
        operation=operation,
        criteria_count=len(criteria),
        results_count=results_count,
        duration_ms=duration * 1000,
        user_id=get_current_user_id(),
        session_id=get_session_id(),
        timestamp=datetime.now().isoformat()
    )
```

### Security Implementation

#### Input Sanitization
```python
def sanitize_search_input(criteria):
    """Comprehensive input sanitization"""
    
    sanitized = {}
    
    for key, value in criteria.items():
        if isinstance(value, str):
            # Remove potential SQL injection patterns
            value = re.sub(r'[;\'\"\\]', '', value)
            
            # Limit string length
            value = value[:1000]
            
            # HTML escape
            value = html.escape(value)
        
        elif isinstance(value, (int, float)):
            # Validate numeric ranges
            if key.endswith('_score'):
                value = max(0, min(value, 120))
            elif key.endswith('_date'):
                # Validate date ranges
                pass
        
        sanitized[key] = value
    
    return sanitized
```

#### Access Control
```python
def check_access_permissions(user, operation, resource):
    """Role-based access control"""
    
    user_roles = get_user_roles(user)
    required_permissions = get_required_permissions(operation, resource)
    
    for permission in required_permissions:
        if not has_permission(user_roles, permission):
            raise PermissionDeniedError(
                f"User {user.id} lacks permission: {permission}"
            )
    
    # Log access attempt
    logger.info(
        "access_granted",
        user_id=user.id,
        operation=operation,
        resource=resource,
        roles=user_roles
    )
```

### Monitoring & Observability

#### Performance Monitoring
```python
class PerformanceMonitor:
    """Application performance monitoring"""
    
    def __init__(self):
        self.metrics = defaultdict(list)
    
    def record_query_time(self, query_type, duration):
        self.metrics[f'query_time_{query_type}'].append(duration)
    
    def record_result_count(self, query_type, count):
        self.metrics[f'result_count_{query_type}'].append(count)
    
    def get_performance_summary(self):
        summary = {}
        
        for metric_name, values in self.metrics.items():
            summary[metric_name] = {
                'avg': statistics.mean(values),
                'median': statistics.median(values),
                'p95': numpy.percentile(values, 95),
                'p99': numpy.percentile(values, 99),
                'count': len(values)
            }
        
        return summary
```

#### Health Checks
```python
async def health_check():
    """Comprehensive system health check"""
    
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'checks': {}
    }
    
    # Database connectivity
    try:
        await check_database_connection()
        health_status['checks']['database'] = 'healthy'
    except Exception as e:
        health_status['checks']['database'] = f'unhealthy: {str(e)}'
        health_status['status'] = 'unhealthy'
    
    # Cache availability
    try:
        await check_cache_connection()
        health_status['checks']['cache'] = 'healthy'
    except Exception as e:
        health_status['checks']['cache'] = f'degraded: {str(e)}'
        if health_status['status'] == 'healthy':
            health_status['status'] = 'degraded'
    
    # External API availability
    try:
        await check_external_apis()
        health_status['checks']['external_apis'] = 'healthy'
    except Exception as e:
        health_status['checks']['external_apis'] = f'degraded: {str(e)}'
    
    return health_status
```

---

This comprehensive documentation covers all aspects of the GRID Entity Search application, from high-level business logic to detailed technical implementation. The application represents a sophisticated enterprise-grade solution for entity risk assessment and intelligence analysis, with advanced features for political exposure detection, relationship mapping, and comprehensive risk scoring.