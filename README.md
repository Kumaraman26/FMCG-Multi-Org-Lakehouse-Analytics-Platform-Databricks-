# FMCG Multi-Org Lakehouse & Analytics Platform (Databricks)

An end-to-end **Databricks Lakehouse** project that consolidates sales, product, customer, and pricing data from a parent company and a newly acquired child company ("Sports Bar") into a unified **Medallion Architecture (Bronze → Silver → Gold)**, producing a single governed analytics layer that powers an interactive BI dashboard.

![Architecture Diagram](architecture/project_architecture.png)

---

## 📌 Project Overview

A sports/fitness FMCG parent organization acquires a smaller nutrition brand ("Sports Bar"). Each company has its own raw data (customers, products, pricing, and order transactions) landing in **AWS S3**. This project builds a pipeline that:

- Ingests raw CSV data from S3 into a **Bronze** layer (raw, append-only, schema captured as-is)
- Cleans, standardizes, and de-duplicates data in a **Silver** layer (fixing typos, casing, invalid IDs, multiple date formats, negative prices, etc.)
- Conforms the child company's data to the parent's **Gold** dimensional model (star schema) and **merges** it into the shared Gold tables using Delta Lake `MERGE` (upsert)
- Aggregates the child company's transaction-level orders to the **monthly grain** to match the parent's reporting grain before merging into the shared fact table
- Supports both a **full load** (initial historical backfill) and an **incremental load** (recurring new-data ingestion via `COPY INTO` + monthly re-aggregation) for the fact table
- Exposes the final consolidated Gold layer through **Databricks SQL Dashboards** for business reporting

This mirrors a real-world data modernization scenario — onboarding an acquired company's data onto a shared, governed enterprise data platform.

---

## 🏗️ Architecture

**Flow:** `S3 (Raw/Landing)` → `Lakeflow Jobs` → `Bronze` → `Silver` (cleaning & transformation) → `Gold (Child)` → merge into `Gold (Parent, fact_orders & dimensions)` → `Databricks SQL Dashboard / Genie`

- **Storage**: AWS S3 (raw data lake, with `landing/` and `processed/` zones for the orders pipeline)
- **Processing**: Databricks (PySpark, Delta Lake)
- **Governance & Cataloging**: Unity Catalog (`fmcg` catalog with `bronze`, `silver`, `gold` schemas)
- **Modeling**: Star schema — one fact table (`fact_orders`) and four dimensions (`dim_customers`, `dim_products`, `dim_gross_price`, `dim_date`)
- **Serving**: Databricks SQL Dashboards (Genie-enabled) for self-service analytics

---

## 🗂️ Data Model (Gold Layer — Star Schema)

| Table | Type | Description |
|---|---|---|
| `fact_orders` | Fact | Monthly sold quantity per `product_code` × `customer_code` |
| `dim_customers` | Dimension | Customer master — name, market, platform (Brick & Mortar / E-Commerce), channel (Retailer / Direct / Acquisition) |
| `dim_products` | Dimension | Product master — division, category, product, variant |
| `dim_gross_price` | Dimension | Latest yearly price (INR) per product, type-2-style "current price per year" |
| `dim_date` | Dimension | Calendar/date dimension generated programmatically (month grain, year, quarter, year-quarter) |

A denormalized **Gold view** (`vw_fact_orders_enriched`) joins the fact table with all dimensions and computes `total_amount_inr = sold_quantity × price_inr`, serving as the primary source for the dashboard.

---

## 🔧 Pipeline Components

### 1. Setup (`/notebooks/1_setup`)
- **`setup_catalog`** – Creates the `fmcg` Unity Catalog, and `bronze` / `silver` / `gold` schemas for the child company
- **`utilities`** – Shared schema-name constants imported by all pipeline notebooks via `%run`
- **`dim_date_table_creation`** – Generates a monthly date dimension (2024–2025) using `sequence()` + date functions, written as a Delta table

### 2. Dimension Pipelines (`/notebooks/2_dimensions`)
Each follows the same Bronze → Silver → Gold(child) → Merge-into-parent pattern:

- **`1_customers_data_processing`**
  - Reads raw customer CSVs from `s3://sportsbar-final/customers/`
  - Deduplicates on `customer_id`, trims whitespace, fixes inconsistent city names via a typo-mapping dictionary, applies business-confirmed corrections for missing cities
  - Standardizes casing (`initcap`) and builds a composite `customer` key (`"CustomerName-City"`)
  - Tags all child-company customers with `market = India`, `platform = Sports Bar`, `channel = Acquisition`
  - Merges into the shared `fmcg.gold.dim_customers` table via Delta `MERGE` (upsert)

- **`2_products_data_processing`**
  - Reads raw product CSVs, deduplicates on `product_id`
  - Fixes category casing and spelling errors (e.g., "Protien" → "Protein")
  - Derives a business **`division`** from `category` via mapping rules (e.g., Energy/Protein Bars → "Nutrition Bars")
  - Extracts `variant` from product names using regex
  - Generates a deterministic **`product_code`** via SHA-256 hash of the product name (to align with the parent's product key format) and handles invalid `product_id`s with a fallback
  - Merges into `fmcg.gold.dim_products`

- **`3_pricing_data_processing`**
  - Parses inconsistent date formats in the `month` column using `try_to_date` with multiple format fallbacks
  - Cleans invalid/negative `gross_price` values (negatives flipped to positive, non-numeric set to 0)
  - Joins with the Silver products table to attach the correct `product_code`
  - Uses a **window function** (`ROW_NUMBER` partitioned by `product_code, year`, ordered to prefer non-zero, most-recent prices) to pick the latest valid yearly price per product
  - Merges the result into `fmcg.gold.dim_gross_price`

### 3. Fact Pipeline (`/notebooks/3_facts`)
- **`1_full_load_fact`** – One-time historical backfill:
  - Reads all order files from `s3://sportsbar-final/orders/landing/`, appends to Bronze, then moves processed files to `processed/`
  - Cleans `customer_id` (numeric-only, fallback `999999`), strips weekday prefixes from dates, parses multiple date formats, and de-duplicates
  - Joins with the Silver products table to attach `product_code`
  - Writes to Silver via `MERGE` (upsert on order/date/customer/product)
  - Builds the Gold child fact table, then **aggregates daily orders to monthly grain** (`sum(sold_quantity)` grouped by month start, product, customer)
  - Merges the monthly aggregates into the shared `fmcg.gold.fact_orders` table

- **`2_incremental_load_fact`** – Recurring incremental runs:
  - Same cleansing logic as the full load, but operates on new files landing in `orders/landing/`
  - Uses a `staging_orders` table for the incremental batch only
  - Identifies the distinct **months affected** by the new data
  - Re-pulls and **re-aggregates the entire affected month(s)** from the child Gold fact table (to keep monthly totals accurate even with late-arriving data), then merges the recalculated totals into the shared parent `fact_orders`
  - Drops the temporary staging tables at the end of the run
  - Parent-side incremental ingestion uses a `COPY INTO` statement to load new monthly data directly into `fmcg.gold.fact_orders` from a Unity Catalog volume (see `sql/incremental_data_parent_company_query.sql`)

### 4. Analytics Layer (`/sql`)
- **`denormalise_table_query_fmcg.sql`** – Creates `vw_fact_orders_enriched`, a denormalized view joining `fact_orders` with all four dimensions and computing `total_amount_inr`

---

## 📊 Dashboard

A Databricks SQL Dashboard (`fmcg_dashboard`) built on top of `vw_fact_orders_enriched`, featuring:

- KPI cards: Total Revenue, Total Quantity Sold, Unique Customers, Average Selling Price
- Top products / variants by revenue
- Revenue share by channel (Retailer / Direct / Acquisition)
- Monthly revenue trend
- Customer-level revenue ranking
- Product price vs. quantity sold (bubble chart)

Filterable by Year, Quarter, Month, Channel, and Category.

![Dashboard](dashboard/fmcg_dashboard.png)

---

## 🧰 Tech Stack

- **Platform**: Databricks (Lakeflow Jobs, SQL Dashboards, Genie)
- **Storage**: Delta Lake on AWS S3
- **Processing**: PySpark (DataFrame API, Window functions, Delta `MERGE`)
- **Governance**: Unity Catalog (catalog/schema-level data organization)
- **Languages**: Python, SQL

---

## 🔑 Key Engineering Highlights

- Implemented a **3-layer Medallion Architecture** (Bronze/Silver/Gold) with Delta Lake Change Data Feed enabled for downstream CDC
- Built **upsert (merge) logic** to incrementally onboard an acquired company's data into a shared enterprise dimensional model without duplicating or overwriting existing parent records
- Handled real-world **data quality issues**: inconsistent date formats, typos/casing inconsistencies, invalid IDs, negative/non-numeric prices, duplicate records
- Designed a **grain-alignment strategy** — reconciling daily transaction-level child data with the parent's monthly reporting grain via aggregation and re-aggregation on incremental loads
- Used **window functions** to resolve "latest valid price per product per year" from noisy historical pricing data
- Built a **denormalized analytics view** and connected it to a multi-page Databricks dashboard for business stakeholders

---

## 📁 Repository Structure

```
fmcg-databricks-lakehouse/
├── README.md
├── architecture/
│   └── project_architecture.png
├── notebooks/
│   ├── 1_setup/
│   │   ├── setup_catalog.ipynb
│   │   ├── utilities.ipynb
│   │   └── dim_date_table_creation.ipynb
│   ├── 2_dimensions/
│   │   ├── 1_customers_data_processing.ipynb
│   │   ├── 2_products_data_processing.ipynb
│   │   └── 3_pricing_data_processing.ipynb
│   └── 3_facts/
│       ├── 1_full_load_fact.ipynb
│       └── 2_incremental_load_fact.ipynb
├── sql/
│   ├── denormalise_table_query_fmcg.sql
│   └── incremental_data_parent_company_query.sql
└── dashboard/
    └── fmcg_dashboard.png
```

---

## 🚀 Future Improvements

- Orchestrate the pipeline end-to-end using Databricks Workflows / Lakeflow Jobs with scheduled triggers
- Add data quality checks (e.g., Great Expectations / Delta Live Tables expectations) at each layer
- Extend Unity Catalog governance with column-level access controls and data lineage tracking
- Add automated tests for transformation logic using `pytest` + `chispa`
