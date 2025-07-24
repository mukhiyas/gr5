# GRID Entity Search & Analysis Platform

Complete modular entity search application with enhanced database integration and advanced features.

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/mukhiyas/advance_entity_search.git
cd advance_entity_search/advanced_entity_search
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
cp .env.template .env
# Edit .env with your database credentials
```

### 4. Run Application
```bash
python main.py
```

## 📁 Essential Files

### Core Application Files (Required)
- **`main.py`** - Main application (6,280+ lines) with full UI and functionality
- **`requirements.txt`** - Complete dependencies for all features

### Modular Integration Files (Required for Enhanced Features)
- **`main_py_integration.py`** - Primary integration controller and boolean query parser
- **`comprehensive_database_integration.py`** - Database correction engine with PEP fixes

### Supporting Modules (Recommended)
- **`database_corrections.py`** - Targeted database fixes and corrections
- **`enhanced_search.py`** - Enhanced search functionality 
- **`database_queries.py`** - Query builder and optimization

## ✨ Features

### Core Features
- ✅ Entity search across multiple criteria
- ✅ Advanced filtering and Boolean queries
- ✅ Risk scoring and severity classification
- ✅ PEP (Politically Exposed Person) detection
- ✅ Export to CSV, Excel, JSON formats
- ✅ Interactive data visualization

### Enhanced Features (Modular Integration)
- ✅ **Corrected PEP Detection**: Access to 11.9M PTY records with proper parsing
- ✅ **Advanced Boolean Search**: AST-based query parsing with complex operators
- ✅ **Enhanced Risk Calculation**: Event-based scoring with geographic multipliers
- ✅ **Relationship Analysis**: Full network visualization and analysis
- ✅ **Extended Export**: 60+ columns with corrected data
- ✅ **Date Integration**: Birth date search and age calculation
- ✅ **Performance Optimization**: Caching and parallel query execution

## 🎯 Database Integration

The modular system provides:
- **Correct PEP parsing** from `'MUN:L3'`, `'REG:L5'` format instead of simple `'L1'`, `'L2'`
- **Complete PTY access** to all 11.9M PEP records in `individual_attributes` table
- **Event integration** with 5.8M PEP events and subcategory modifiers
- **Relationship extraction** with direction and type from `relationships` table
- **Enhanced exports** with all corrected database fields

## Key Features

### 1. **Advanced Entity Search**
- **Comprehensive Search Fields**: Search across 16+ fields including:
  - Entity identifiers (ID, Risk ID, BVD ID)
  - Names and aliases
  - Addresses (street, city, province, country)
  - Source information
  - Identification documents
  - Event categories
  - Date ranges with flexible year-based filtering
- **Regex Support**: Enable regular expression patterns for complex searches
- **Logical Operators**: AND/OR operators for combining search criteria
- **Real-time Results**: Fast search with result limiting (1-1000 entities)

### 2. **Risk Assessment System**
- **4-Tier Risk Severity Classification**:
  - **Critical (80-100)**: High-risk entities (terrorism, sanctions, money laundering)
  - **Valuable (60-79)**: Significant risk (fraud, corruption, cybercrime)
  - **Investigative (40-59)**: Moderate risk (regulatory violations, environmental crimes)
  - **Probative (0-39)**: Low risk (administrative issues, minor violations)
- **50+ Risk Codes**: Comprehensive risk categorization system
- **Dynamic Risk Scoring**: Calculated based on events and source reliability
- **Customizable Thresholds**: Adjust risk severity boundaries

### 3. **PEP (Politically Exposed Persons) Filtering**
- **17 PEP Levels** including:
  - Government positions (HOS, CAB, INF, MUN)
  - Family relationships (FAM, SPO, CHI, PAR, SIB)
  - Associates (ASC, BUS, POL, LEG, FIN)
- **Multi-level Selection**: Filter by multiple PEP categories simultaneously
- **PEP Status Indicators**: Visual badges and chips for easy identification

### 4. **Clustering Analysis**
- **Risk Code Clustering**: Group entities by common risk patterns
- **PEP Level Clustering**: Analyze PEP distribution across entities
- **Geographic Clustering**: Identify entity concentrations by location
- **Source System Clustering**: Understand data source distributions
- **Automated Insights**: AI-generated insights from cluster patterns

### 5. **Network Visualization**
- **Relationship Mapping**: Visual network graphs showing entity connections
- **Bi-directional Relationships**: Both outgoing and incoming connections
- **Smart Layouts**: Automatic layout optimization based on network size
- **Interactive Graphs**: Click nodes for detailed information
- **Comprehensive Networks**: Visualize all search results or individual entities

### 6. **AI-Powered Analysis**
- **Natural Language Q&A**: Ask questions about entity data in plain English
- **Contextual Understanding**: AI comprehends entity relationships and patterns
- **Data Summarization**: Automatic insights from complex data
- **Moodys Copilot Integration**: Enterprise AI service for analysis

### 7. **Modern UI/UX Design**
- **Responsive Layout**: Adapts to different screen sizes
- **Card/Table Toggle**: Switch between card view and table view for results
- **Glass Morphism Effects**: Modern visual design with subtle animations
- **Dark Mode Support**: Eye-friendly interface options
- **Real-time Updates**: Live status indicators for database and AI connections

### 8. **Query Optimization**
- **Query Caching**: Intelligent caching with configurable TTL
- **Parallel Processing**: Execute subqueries concurrently
- **Batch Processing**: Handle large datasets efficiently
- **Index Hints**: Optimize query performance with index suggestions
- **Performance Metrics**: Monitor cache hits and query statistics

### 9. **Export Capabilities**
- **CSV Export**: Export search results for further analysis
- **Configuration Export**: Save all settings for backup/sharing
- **Risk Profiles**: Export customized risk configurations
- **Batch Downloads**: Handle large export operations

### 10. **Enterprise Security**
- **Databricks Integration**: Secure connection to data warehouse
- **Environment Variables**: Sensitive data protected
- **Access Control Ready**: Designed for enterprise authentication
- **Audit Trail Support**: Query logging capabilities

## Technical Capabilities

### Database Integration
- **Databricks SQL Warehouse**: Direct integration with enterprise data platform
- **Complex SQL Queries**: JSON aggregation, CTEs, and optimized joins
- **Real-time Data**: Live queries against production database
- **Schema Compliance**: Uses exact production table schema

### Performance Features
- **Scalable Architecture**: Handles datasets with millions of entities
- **Memory Efficient**: Batch processing for large results
- **Concurrent Operations**: Multi-threaded query execution
- **Smart Caching**: Reduces database load for repeated queries

### Data Processing
- **JSON Field Parsing**: Automatic parsing of complex nested data
- **Risk Score Calculation**: Real-time scoring based on multiple factors
- **Relationship Analysis**: Complex graph algorithms for network insights
- **Event Aggregation**: Summarize thousands of events per entity

## Use Cases

1. **Compliance Screening**: Check entities against PEP lists and sanctions
2. **Risk Assessment**: Evaluate entity risk profiles for due diligence
3. **Investigation Support**: Trace entity relationships and patterns
4. **Portfolio Monitoring**: Track risk changes across entity portfolios
5. **Regulatory Reporting**: Generate compliance reports with full data
6. **Network Analysis**: Identify hidden connections between entities
7. **Trend Analysis**: Use clustering to spot emerging risk patterns
8. **Data Quality**: Verify and validate entity information

## Benefits

- **Comprehensive Coverage**: All entity data in one platform
- **Real-time Intelligence**: Live data with no delays
- **Actionable Insights**: AI-powered analysis for decision support
- **Enterprise Ready**: Scalable, secure, and performant
- **User Friendly**: Intuitive interface requires minimal training
- **Customizable**: Adapt risk scoring to organizational needs
- **Cost Effective**: Reduces manual investigation time
- **Audit Compliant**: Full query and export tracking

## Quick Start

### 🎨 Demo Mode (UI/UX Preview)
Preview the complete modern interface without database connection:
```bash
python main.py --demo
# OR
DEMO_MODE=true python main.py
```
Perfect for design review and interface testing at `http://localhost:8080`

### 🚀 Production Mode
Full functionality with Databricks connection:
```bash
cp .env.template .env
# Configure .env with your credentials
python main.py
```