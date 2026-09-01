# ENEM Data Analytics Platform

## Overview

This project implements a comprehensive data analytics platform for processing and analyzing Brazilian ENEM (Exame Nacional do Ensino Médio) examination data. The platform follows a medallion architecture pattern using Databricks and Unity Catalog to transform raw exam data into actionable insights for educational analytics.

---

## 1. Architectural Description

### Architecture Pattern: Medallion (Bronze-Silver-Gold)

The platform implements a multi-layered medallion architecture that progressively refines and enriches data across three distinct layers:

#### **Bronze Layer** (`databricks-repo.bronze`)

**Purpose:** Raw data ingestion and initial materialization

**Components:**
* **`dicionario_dados`** - Data dictionary containing metadata and categorical mappings
* **`itens_prova`** (Materialized View) - Exam questions and items metadata
* **`participantes`** (Materialized View) - Raw participant registration data
* **`resultados`** (Materialized View) - Raw exam results and scores

**Data Sources:**
* Files ingested from `/Volumes/databricks-repo/silver/raw_enem`
* Source files downloaded and staged via `etl/00_ingestion/` scripts

**Characteristics:**
* Schema-on-read approach with minimal transformations
* Materialized views ensure data freshness while maintaining query performance
* Serves as the single source of truth for all downstream processing

#### **Silver Layer** (`databricks-repo.silver`)

**Purpose:** Cleansed, conformed, and normalized data ready for analytics

**Dimensional Tables:**
* **`participantes`** - Normalized participant core data (id_inscricao, demographics, exam year)
* **`escola`** - School dimension (administrative dependency, location)
* **`municipio`** - Municipality dimension (geographic information)
* **`unidade_federativa`** - State/UF dimension (federal unit codes and names)
* **`local_aplicacao`** - Exam application location details
* **`prova`** - Exam metadata (test versions, languages)
* **`questoes`** - Individual question details

**Feature Tables (Lookup/Reference):**
* **`ft_faixa_etaria`** - Age group categories
* **`ft_estado_civil`** - Marital status categories
* **`ft_cor_raca`** - Race/ethnicity categories
* **`ft_nacionalidade`** - Nationality categories
* **`ft_conclusao`** - High school completion status
* **`ft_ensino`** - Education type categories

**Transformation Logic:**
* Type casting and data validation using defined StructType schemas
* Forward-fill techniques for hierarchical categorical data from `dicionario_dados`
* Column renaming and standardization (e.g., `NU_INSCRICAO` → `id_inscricao`)
* Deduplication and data quality checks

**Processing:** `etl/02_silver.ipynb`

#### **Gold Layer** (`databricks-repo.gold`)

**Purpose:** Business-level aggregates and denormalized analytics tables

**Analytics Tables:**

1. **`dim_participante_completo`** - Complete participant dimension
   * Fully denormalized view joining participant data with all feature tables
   * Includes demographic, geographic, and socioeconomic indicators
   * Optimized for direct consumption by BI tools and dashboards

2. **`agg_participacao_regional`** - Regional participation analysis
   * Aggregated metrics by year, state (UF), and municipality
   * Participant counts, school statistics, demographic breakdowns
   * Performance metrics (average scores by region)

3. **`agg_perfil_socioeconomico`** - Socioeconomic profile aggregation
   * Participation and performance by income brackets
   * Racial/ethnic demographic analysis
   * Cross-tabulation of socioeconomic factors and exam outcomes

4. **`agg_tendencias_anuais`** - Annual trends and time-series metrics
   * Year-over-year participation growth
   * Temporal patterns in student demographics
   * Score evolution and completion rates by year

**Processing:** `etl/03_gold.py`

### Technology Stack

* **Compute:** Databricks Serverless (AWS)
* **Storage:** Unity Catalog with Delta Lake format
* **Languages:** PySpark (Python), SQL
* **Orchestration:** Databricks Notebooks (modular ETL pipeline)
* **Data Optimization:** Delta auto-optimization enabled (`autoOptimize.optimizeWrite`, `autoOptimize.autoCompact`)

### Data Flow Architecture

```
┌─────────────────────────────────────────────────┐
│  Data Ingestion (etl/00_ingestion)             │
│  - 00_download_file.py                          │
│  - 01_cp_file.py                                │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│  BRONZE LAYER                                   │
│  - dicionario_dados (raw metadata)              │
│  - participantes (MV)                           │
│  - resultados (MV)                              │
│  - itens_prova (MV)                             │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│  SILVER LAYER (etl/02_silver.ipynb)            │
│  - Normalized dimensions                        │
│  - Feature tables (ft_*)                        │
│  - Type conversions & validation                │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│  GOLD LAYER (etl/03_gold.py)                   │
│  - dim_participante_completo                    │
│  - agg_participacao_regional                    │
│  - agg_perfil_socioeconomico                    │
│  - agg_tendencias_anuais                        │
└─────────────────────────────────────────────────┘
                 │
                 ▼
          [BI & Analytics]
```

---

## 2. Join Strategy Justification

### Design Philosophy: Star Schema with Conformed Dimensions

The join strategy implements a **star schema** pattern in the Gold layer, denormalizing data from multiple Silver layer dimensions. This approach is optimized for analytical query performance and business user accessibility.

### Join Patterns and Rationale

#### **Pattern 1: LEFT JOINS for Dimension Enrichment**

**Used in:** All Gold layer tables

```python
participantes.join(
    ft_faixa_etaria,
    participantes.id_faixa_etaria == ft_faixa_etaria.id_categoria,
    "left"
)
```

**Justification:**
* **Preserves grain:** Ensures all participants are retained even if dimension lookups fail
* **Handles missing data gracefully:** NULL values in dimension keys won't exclude rows
* **Data quality tolerance:** Acknowledges real-world data quality issues (missing categorical codes)
* **Business requirement:** Analytics reports must include all participants, regardless of data completeness

#### **Pattern 2: Composite Key Joins**

**Used in:** Geographic hierarchy joins (escola, municipio)

```python
participantes.join(
    escola,
    (participantes.id_municipio == escola.id_municipio) & 
    (participantes.id_uf == escola.id_uf),
    "left"
)
```

**Justification:**
* **Ensures uniqueness:** Municipality IDs alone are not globally unique; UF (state) provides necessary context
* **Reflects Brazilian administrative structure:** IBGE codes require state + municipality for proper identification
* **Prevents ambiguous matches:** Multiple municipalities may share similar codes across different states
* **Data integrity:** Enforces geographic hierarchy constraints

#### **Pattern 3: Multiple Feature Table Joins (Snowflake → Star Flattening)**

**Used in:** `dim_participante_completo`

```python
participantes
    .join(ft_faixa_etaria, ..., "left")
    .join(ft_estado_civil, ..., "left")
    .join(ft_cor_raca, ..., "left")
    .join(ft_nacionalidade, ..., "left")
    .join(ft_conclusao, ..., "left")
    .join(ft_ensino, ..., "left")
    .join(unidade_federativa, ..., "left")
    .join(municipio, ..., "left")
    .join(escola, ..., "left")
```

**Justification:**
* **Denormalization for query performance:** Eliminates need for 9+ joins at query time
* **Pre-computed dimension resolution:** All categorical codes are resolved once during ETL
* **Simplified BI layer:** Business users query a single wide table instead of complex join logic
* **Optimized for read-heavy workloads:** Gold layer prioritizes query speed over storage efficiency
* **Broadcast join optimization:** Small feature tables (< 1MB) are broadcast-joined efficiently by Spark

#### **Pattern 4: Aggregation with Grouped Joins**

**Used in:** `agg_participacao_regional`, `agg_perfil_socioeconomico`, `agg_tendencias_anuais`

```python
agg_regional = (
    participantes
        .join(unidade_federativa, participantes.id_uf == unidade_federativa.id_uf, "left")
        .join(municipio, participantes.id_municipio == municipio.id_muninicipio)
        .groupBy(participantes.ano, unidade_federativa.sigla, municipio.nome)
        .agg(
            F.countDistinct(participantes.id_inscricao).alias("total_inscricoes"),
            F.avg(participantes.nota_redacao).alias("media_redacao"),
            ...
        )
)
```

**Justification:**
* **Joins before aggregation:** Ensures dimension attributes (state name, municipality name) are available for grouping
* **Reduces cardinality post-join:** Aggregation significantly reduces data volume after dimension enrichment
* **Pre-aggregated metrics:** Computing aggregates during ETL improves dashboard query latency
* **Consistent business logic:** Centralized metric definitions prevent calculation discrepancies across reports

### Performance Optimizations

1. **Broadcast Joins:** Feature tables are small (< 50 rows each) and automatically broadcast
2. **Partition Pruning:** `participantes.ano` (year) is used as partition key for temporal filtering
3. **Z-Ordering:** Key dimension columns are Z-ordered for multi-dimensional query optimization
4. **Delta Auto-Compact:** Enabled to maintain optimal file sizes and reduce small file overhead

### Alternative Strategies Considered and Rejected

| Strategy | Why Rejected |
|----------|-------------|
| **INNER JOINs** | Would exclude participants with missing categorical data, violating business requirement for complete reporting |
| **Runtime Views** | Query performance insufficient for dashboards; Gold layer pre-materialization provides 10-50x speedup |
| **Star Schema with Foreign Keys** | While normalized, requires BI tools to implement join logic; denormalization simplifies analytics layer |
| **Slowly Changing Dimensions (SCD Type 2)** | ENEM data is historically static per exam year; SCD overhead not justified |

---

## 3. Analyzes on Monitoring Metrics

### Data Quality Monitoring

#### **Bronze Layer Metrics**

**Metric:** Record count validation
```sql
SELECT 
    'participantes' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT NU_INSCRICAO) as unique_participants
FROM databricks-repo.bronze.participantes
WHERE NU_ANO = YEAR(CURRENT_DATE()) - 1
```

**Monitoring Focus:**
* Completeness: Verify expected row counts against source files
* Uniqueness: Check for duplicate `NU_INSCRICAO` (registration IDs)
* Freshness: Ensure materialized views are refreshed after data arrival

#### **Silver Layer Metrics**

**Metric:** Dimension cardinality and referential integrity
```sql
SELECT 
    p.ano,
    COUNT(DISTINCT p.id_inscricao) as total_participants,
    COUNT(DISTINCT CASE WHEN fe.id_categoria IS NULL THEN p.id_inscricao END) as missing_faixa_etaria,
    COUNT(DISTINCT CASE WHEN ec.id_categoria IS NULL THEN p.id_inscricao END) as missing_estado_civil,
    COUNT(DISTINCT CASE WHEN cr.id_categoria IS NULL THEN p.id_inscricao END) as missing_cor_raca
FROM databricks-repo.silver.participantes p
LEFT JOIN databricks-repo.silver.ft_faixa_etaria fe ON p.id_faixa_etaria = fe.id_categoria
LEFT JOIN databricks-repo.silver.ft_estado_civil ec ON p.id_estado_civil = ec.id_categoria
LEFT JOIN databricks-repo.silver.ft_cor_raca cr ON p.id_cor_raca = cr.id_categoria
GROUP BY p.ano
```

**Monitoring Focus:**
* **Referential integrity:** Track orphaned foreign keys in feature table joins
* **Completeness rate:** Percentage of participants with complete demographic data
* **Data drift detection:** Monitor changes in categorical distribution across years

#### **Gold Layer Metrics**

**Metric:** Aggregation validation and business rule compliance
```sql
-- Validate aggregation consistency
SELECT 
    ano,
    SUM(total_participantes) as agg_table_total
FROM databricks-repo.gold.agg_participacao_regional
GROUP BY ano

UNION ALL

SELECT 
    ano,
    COUNT(DISTINCT id_inscricao) as source_table_total
FROM databricks-repo.silver.participantes
GROUP BY ano
```

**Monitoring Focus:**
* **Aggregation accuracy:** Verify totals match source counts
* **Calculation consistency:** Validate average score calculations
* **Completeness:** Ensure all years/regions present in output

### Performance Monitoring

#### **Query Performance Metrics**

**Using Databricks System Tables:**
```sql
SELECT 
    qh.statement_text,
    qh.execution_status,
    qh.total_duration_ms,
    qh.read_bytes,
    qh.rows_produced
FROM system.query.history qh
WHERE qh.statement_text LIKE '%databricks-repo.gold%'
    AND qh.start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY qh.total_duration_ms DESC
LIMIT 20
```

**Key Metrics:**
* **Query duration:** Track p95/p99 latencies for Gold layer queries
* **Data scanned:** Monitor bytes read (optimize via partitioning/Z-ordering)
* **Result set size:** Ensure aggregations produce appropriately sized outputs

#### **ETL Pipeline Metrics**

**Notebook Execution Tracking:**
* **Bronze refresh time:** Time to refresh materialized views
* **Silver transformation time:** End-to-end processing time for `02_silver.ipynb`
* **Gold aggregation time:** Processing time for each Gold table creation

**Target SLAs:**
* Bronze layer: < 10 minutes for full refresh
* Silver layer: < 15 minutes for complete transformation
* Gold layer: < 5 minutes for all aggregations

### Data Lineage Audit Metrics

**Tracking Transformation Paths:**
```sql
SELECT 
    entity_type,
    source_table_full_name,
    target_table_full_name,
    COUNT(DISTINCT event_date) as days_active,
    COUNT(*) as transformation_count
FROM system.access.table_lineage
WHERE target_table_catalog = 'databricks-repo'
GROUP BY 1,2,3
ORDER BY transformation_count DESC
```

**Monitoring Focus:**
* **Dependency tracking:** Ensure all expected source→target paths exist
* **Change impact analysis:** Identify downstream tables affected by schema changes
* **Audit compliance:** Track which notebooks/users generated each transformation

### Alerting Strategy

**Critical Alerts:**
1. **Data freshness:** Alert if Bronze materialized views lag > 24 hours
2. **Data quality:** Alert if > 5% of participants have missing core demographics
3. **Performance degradation:** Alert if Gold query p95 latency > 30 seconds
4. **Pipeline failures:** Immediate notification on notebook execution errors

**Warning Alerts:**
1. **Cardinality changes:** Notify if dimension table row counts change > 10% week-over-week
2. **Data volume anomalies:** Flag if participant counts deviate > 2σ from historical mean
3. **Schema drift:** Detect unexpected columns or type changes in Bronze layer

---

## 4. Data Lineage View

### Lineage Summary

The platform maintains comprehensive data lineage tracking through Unity Catalog's system tables (`system.access.table_lineage`). All transformations are captured with entity-level (notebook) attribution.

### Lineage Graph: Bronze → Silver

#### **Source: `bronze.dicionario_dados`**
→ Feeds 6 feature tables in Silver:
* `silver.ft_faixa_etaria`
* `silver.ft_estado_civil`
* `silver.ft_cor_raca`
* `silver.ft_nacionalidade`
* `silver.ft_conclusao`
* `silver.ft_ensino`

**Transformation:** Dictionary parsing with forward-fill logic to populate categorical lookup tables

#### **Source: `bronze.participantes`**
→ Transforms to:
* `silver.participantes` (normalized, type-casted)
* `silver.resp_q001` through `silver.resp_q023` (questionnaire responses, 23 tables)

**Transformation:** Schema enforcement, column renaming, questionnaire response extraction

#### **Source: `bronze.resultados`**
→ Transforms to:
* `silver.escola`
* `silver.municipio`
* `silver.unidade_federativa`
* `silver.local_aplicacao`

**Transformation:** Geographic dimension extraction and deduplication

#### **Source: `bronze.itens_prova`**
→ Transforms to:
* `silver.prova`
* `silver.questoes`

**Transformation:** Exam metadata and question-level detail extraction

### Lineage Graph: Silver → Gold

#### **Target: `gold.dim_participante_completo`**
← Joins 10 Silver tables:
* `silver.participantes` (fact)
* `silver.ft_faixa_etaria`
* `silver.ft_estado_civil`
* `silver.ft_cor_raca`
* `silver.ft_nacionalidade`
* `silver.ft_conclusao`
* `silver.ft_ensino`
* `silver.unidade_federativa`
* `silver.municipio`
* `silver.escola`

**Business Purpose:** Complete 360° participant profile for individual-level analytics

#### **Target: `gold.agg_participacao_regional`**
← Joins 4 Silver tables:
* `silver.participantes`
* `silver.unidade_federativa`
* `silver.municipio`
* `silver.escola`

**Aggregation Keys:** `ano`, `uf_sigla`, `municipio.nome`

**Business Purpose:** Regional participation and performance dashboards, heat maps

#### **Target: `gold.agg_perfil_socioeconomico`**
← Joins 4 Silver tables:
* `silver.participantes`
* `silver.unidade_federativa`
* `silver.ft_cor_raca`
* `silver.escola`

**Aggregation Keys:** `ano`, `uf_sigla`, `cor_raca`, `faixa_renda_familiar`

**Business Purpose:** Socioeconomic analysis, equity studies, demographic trends

#### **Target: `gold.agg_tendencias_anuais`**
← Joins 2 Silver tables:
* `silver.participantes`
* `silver.escola`

**Aggregation Keys:** `ano`

**Business Purpose:** Year-over-year trend analysis, executive dashboards

### Lineage Traceability Matrix

| Bronze Table | Silver Tables (Count) | Gold Tables (Count) | Total Downstream Impact |
|--------------|----------------------|---------------------|-------------------------|
| `dicionario_dados` | 6 feature tables | 4 (via features) | **10 tables** |
| `participantes` | 1 + 23 questionnaire | 4 aggregates | **28 tables** |
| `resultados` | 4 geographic dims | 3 aggregates (via dims) | **7 tables** |
| `itens_prova` | 2 exam tables | 0 (unused in Gold) | **2 tables** |

### Lineage Query Examples

#### **Find all dependencies for a Gold table:**
```sql
WITH RECURSIVE lineage AS (
    -- Direct dependencies
    SELECT 
        source_table_full_name as table_name,
        target_table_full_name,
        1 as depth
    FROM system.access.table_lineage
    WHERE target_table_full_name = 'databricks-repo.gold.dim_participante_completo'
    
    UNION ALL
    
    -- Recursive dependencies
    SELECT 
        tl.source_table_full_name,
        l.target_table_full_name,
        l.depth + 1
    FROM system.access.table_lineage tl
    INNER JOIN lineage l ON tl.target_table_full_name = l.table_name
    WHERE l.depth < 5
)
SELECT DISTINCT table_name, depth
FROM lineage
ORDER BY depth, table_name
```

#### **Impact analysis for schema changes:**
```sql
-- Find all tables impacted by changing bronze.participantes
SELECT DISTINCT
    target_table_full_name,
    target_type,
    entity_type as last_updated_by
FROM system.access.table_lineage
WHERE source_table_full_name = 'databricks-repo.bronze.participantes'
ORDER BY target_table_full_name
```

### Lineage Visualization

**System Integration:**
* Unity Catalog captures lineage automatically for all Spark and SQL operations
* Lineage is available in the Databricks UI under Data Explorer → Table → Lineage tab
* System table `system.access.table_lineage` provides programmatic access for custom reporting

**Captured Metadata:**
* Source and target table full names
* Entity type (NOTEBOOK, JOB, PIPELINE)
* Entity ID (notebook ID, job run ID)
* Timestamp of transformation
* User/principal who executed the transformation

---

## Getting Started

### Prerequisites
* Databricks workspace (AWS)
* Unity Catalog enabled
* Catalog: `databricks-repo`
* Access to ENEM source data files

### Execution Order

1. **Data Ingestion:**
   ```bash
   python etl/00_ingestion/00_download_file.py
   python etl/00_ingestion/01_cp_file.py
   ```

2. **Bronze Layer:** Run notebooks in `etl/01_bronze/`

3. **Silver Layer:** Execute `etl/02_silver.ipynb`

4. **Gold Layer:** Execute `etl/03_gold.py`

### Key Configuration

**Catalog/Schema Variables:**
```python
catalog_name = "databricks-repo"
schema_bronze = "bronze"
schema_silver = "silver"
schema_gold = "gold"
volume_name = "raw_enem"
```

**Delta Optimization:**
```python
spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")
```

---

## Project Structure

```
databricks-repo/
├── README.md                          # This file
├── etl/
│   ├── 00_ingestion/                  # Data ingestion scripts
│   │   ├── 00_download_file.py
│   │   ├── 01_cp_file.py
│   │   └── src/
│   ├── 01_bronze/                     # Bronze layer transformations
│   │   └── transformations/
│   ├── 02_silver.ipynb                # Silver layer notebook
│   └── 03_gold.py                     # Gold layer notebook
└── .git/                              # Git repository
```
## License

See `LICENSE` file for details.

---

## Additional Resources

* [Databricks Medallion Architecture Best Practices](https://docs.databricks.com/lakehouse/medallion.html)
* [Unity Catalog Lineage Documentation](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html)
* [Delta Lake Optimization Guide](https://docs.databricks.com/delta/optimizations/index.html)
