Master Data Management (MDM) Engine
The MDM Engine is a Python-based data harmonization tool designed to unify and standardize reference data (e.g., agencies, platforms, services) across multiple systems. It ensures consistency, accuracy, and reliability in data warehouses and analytics platforms like 360 Dashboards.

🚀 Key Features
  Golden Record Creation: Automatically identifies and selects the most reliable version of a record across systems.
  Data Cleansing: Normalizes Arabic/English text, removes noise, and ensures data quality.
  Matching & Grouping: Matches similar records using configurable logic (code, name, etc.) and groups them.
  Attribute Enrichment: Enhances records with additional metadata and hierarchical relationships.
  Manual Overrides: Supports manual edits, exclusions, and custom groupings via Excel input.
  Flexible Configuration: Supports MSSQL and Impala, with customizable batch sizes and matching rules.
  Airflow Integration: Fully orchestrated via Apache Airflow for scheduled execution and logging.
  
📦 Inputs
  MDM_components: Core entities (agencies, platforms, etc.)
  MDM_component_attributes: Metadata for enrichment
  priority_file.json: Defines system/attribute priority for golden record selection
  manual_file.xlsx: Manual actions (edit, exclude, match, etc.)
  config.json: Connection settings and output paths
  
📤 Outputs
  MDM_component_matches: Matched record pairs
  MDM_component_groups: Grouped entities
  MDM_group_candidate_comp: Golden records per group
  MDM_group_candidate_attr: Golden attributes per group
  
🛠️ Engine Modules
  Data Quality Module: Cleans and validates input data
  Matching Module: Applies configurable logic to find duplicates
  Grouping Module: Clusters matched records
  Golden Record Module: Selects the best representative record
  
🧩 Use Cases
  Standardizing agency names across dashboards
  Creating unified views of platforms/services
  Supporting data governance and master data management

