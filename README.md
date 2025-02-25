# dbt_odcs: Data Contracts in Action

The dbt_odcs package transforms Open Data Contract Standard (ODCS) specifications from static documentation into executable tests within dbt, a widely adopted tool in modern data stack implementations.

By leveraging dbt's familiar workflow and testing framework, organizations can now automatically validate that their data assets comply with contractual obligations across schema definitions, data quality rules, and service-level agreements. This integration specifically leverages ODCS, a Linux Foundation AI & Data project, which provides a technology-agnostic standard for data contracts that addresses modern data engineering challenges including data normalization, documentation relevance, and service-level expectations.

## Data Governance Benefits

For data governance initiatives, dbt_odcs provides immediate visibility into contract violations through detailed test results that include diagnostic SQL and actionable insights. The modular design separately evaluates schema compliance, quality metrics, and SLAs, enabling precise identification of issues before they impact downstream consumers. 

This capability is particularly valuable for:
- Regulated industries
- Organizations implementing data mesh architectures, where domain teams must deliver reliable data products with enforceable contracts that consumers can trust

ODCS's YAML-based approach ensures contracts can be easily versioned, governed, and automated—transforming static agreements into living documents that tools can actively enforce.

## Beyond Governance

dbt_odcs supports critical data initiatives including:

- **Migration projects**: Ensuring consistent data contracts between systems
- **Data acquisition activities**: Verifying external data meets quality thresholds
- **Data mesh implementations**: Enabling decentralized teams to maintain high standards while operating independently

By building on dbt's widespread adoption and ODCS's open standard framework, organizations can establish a "common language" for data communication, enabling producers and consumers to collaborate effectively while maintaining accountability throughout the data lifecycle. The result is a practical solution for operationalizing data contracts that bridges technical implementation with business requirements.

## Technical Implementation of dbt_odcs

At its core, dbt_odcs employs a modular macro architecture to transform ODCS YAML specifications into executable SQL queries. The package parses contract elements through specialized macros:

- `process_schema_tests` validates data types, nullability constraints, and pattern matching using logical/physical type definitions
- `process_quality_tests` converts quality rules into corresponding SQL validations including uniqueness checks, value set validations, and conditional assertions
- `process_sla_tests` implements time-based validations using date differential calculations

Each macro programmatically generates parameterized SQL queries that evaluate contract assertions against the actual data, creating test cases that execute within BigQuery, Snowflake, or other database engines.

The implementation uses Jinja templating extensively to transform abstract contract definitions into database-specific SQL, handling edge cases like array types, regular expression patterns, and complex logical type options.

For quality tests, the package maps ODCS operators (like `mustBeBetween` or `mustBeLessThan`) to appropriate SQL predicates, while SLA validations construct timestamp comparisons that respect different time units.

Test results are unified through the `combine_test_results` macro which aggregates all validation outcomes into a single model, capturing test failures, diagnostic queries, and execution metadata to provide comprehensive contract compliance reporting.