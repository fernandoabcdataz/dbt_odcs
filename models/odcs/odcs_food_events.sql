-- dbt_odcs/models/odcs/food_events_test_results_complex_2.sql

{{ config(materialized='table') }}

{% set contract_yaml %}
apiVersion: v3.0.1
kind: DataContract

id: 72345abc-12de-4567-89ef-0123456789ab
name: food_events_v3
version: 3.0.0
status: active
domain: health
dataProduct: adverse_events
tenant: FDA
description:
  purpose: tracks adverse events related to food and dietary products for regulatory and safety analysis.
  limitations: data may contain duplicates or missing values for historical records.
  usage: use for regulatory reporting, consumer safety analysis, and product monitoring.
tags: ['health', 'safety', 'regulation']

schema:
  - name: tbl
    logicalType: object
    physicalName: food_events
    physicalType: table
    description: provides detailed adverse event data for food and dietary products.
    authoritativeDefinitions:
      - url: https://www.fda.gov/food/adverse-event-reporting
        type: businessDefinition
    tags: ['critical', 'public']
    dataGranularityDescription: aggregated by report number and date created.
    properties:
      - name: report_number
        logicalType: string
        physicalType: string
        required: true
        primaryKey: true
        primaryKeyPosition: 1
        description: unique identifier for each adverse event report, often in formats like 'XXXX-YYYYYY' or 'YYYY-CFS-XXXXXX'.
        unique: true
        tags: ['identifier', 'critical']
        classification: public

      - name: reactions
        logicalType: string
        physicalType: string
        required: false
        description: descriptions of adverse reactions experienced, often multiple conditions in quotes and commas.
        tags: ['sensitive', 'medical']
        classification: restricted

      - name: outcomes
        logicalType: string
        physicalType: string
        required: false
        description: outcomes of the adverse events (e.g., Hospitalization, Death, Disability).
        tags: ['outcome', 'health']
        classification: public

      - name: products_brand_name
        logicalType: string
        physicalType: string
        required: false
        description: brand name of the product involved in the adverse event.
        tags: ['product']
        classification: public

      - name: products_industry_code
        logicalType: integer
        physicalType: integer
        required: false
        description: industry code of the product, typically a numeric code (e.g., 54 for vitamins/minerals).

      - name: products_role
        logicalType: string
        physicalType: string
        required: false
        description: role of the product in the event (e.g., SUSPECT, CONCOMITANT).
        tags: ['product', 'role']
        classification: public

      - name: products_industry_name
        logicalType: string
        physicalType: string
        required: false
        description: name of the product’s industry category (e.g., Vit/Min/Prot/Unconv Diet).
        tags: ['industry']
        classification: public

      - name: date_created
        logicalType: date
        physicalType: date
        required: true
        description: date the report was created, typically in YYYY-MM-DD format.
        tags: ['timestamp']
        classification: public

      - name: date_started
        logicalType: date
        physicalType: date
        required: false
        description: date the adverse event started, if known, in YYYY-MM-DD format.
        tags: ['timestamp']
        classification: public

      - name: consumer_gender
        logicalType: string
        physicalType: string
        required: false
        description: gender of the consumer (e.g., Male, Female).
        tags: ['personal']
        classification: restricted

      - name: consumer_age
        logicalType: integer
        physicalType: integer
        required: false
        description: age of the consumer, if provided.

      - name: consumer_age_unit
        logicalType: string
        physicalType: string
        required: false
        description: unit of consumer age (e.g., years), if age is provided.
        tags: ['personal']
        classification: restricted

quality:
  - type: library
    rule: uniqueCombination
    columns: ["report_number"]
    name: unique_report_numbers
    description: ensures no duplicate report numbers exist

  - type: library
    rule: valueInSet
    column: products_role
    allowedValues: ["SUSPECT", "CONCOMITANT"]
    name: valid_product_roles
    description: ensures products_role contains only valid values (SUSPECT or CONCOMITANT)

  - type: library
    rule: conditionalNotNull
    column: consumer_age_unit
    condition: consumer_age IS NOT NULL
    name: age_unit_if_age_provided
    description: ensures consumer_age_unit is provided if consumer_age is present

  - type: library
    rule: notNull
    column: report_number
    name: report_number_not_null
    description: ensures report_number is never null

  - type: library
    rule: notNull
    column: date_created
    name: date_created_not_null
    description: ensures date_created is never null

  - type: library
    rule: duplicateCount
    column: products_brand_name
    mustBeLessThan: 10
    unit: rows
    name: few_duplicate_brands
    description: ensures no more than 10 duplicate product brand names exist

  - type: library
    rule: duplicateCount
    column: reactions
    mustBeLessThan: 5
    unit: percent
    name: low_duplicate_reactions
    description: ensures duplicate reactions are less than 5% of total records

  - type: library
    rule: rowCount
    mustBeBetween: [50, 1000]
    name: row_count_range
    description: ensures row count is between 50 and 1000 records

  - type: text
    description: the report_number should be verified against FDA regulatory standards for uniqueness and format.

  - type: sql
    query: |
      SELECT COUNT(*) FROM ${object} WHERE ${property} > 0 AND ${property} < 150
    column: consumer_age
    mustBeLessThan: 900
    name: age_range_limit
    description: ensures the count of consumer ages between 0 and 150 is less than 900

  - type: custom
    engine: greatExpectations
    implementation: |
      type: expect_column_values_to_match_regex
      kwargs:
        column: report_number
        regex: "^(?:[0-9]{6}|[A-Z]{4}-[0-9]{6}|[0-9]{4}-CFS-[0-9]{6})$"
    name: report_number_format_ge
    description: ensures report_number matches the expected format using Great Expectations

  - scheduler: cron
    schedule: "0 0 * * *"
    dimension: timeliness
    severity: high
    businessImpact: "delays in data freshness impact regulatory compliance and safety reporting"

slaProperties:
  - property: frequency
    value: 1
    unit: d
    element: food_events.date_created
    description: ensures data is refreshed daily, with date_created no older than 1 day

  - property: latency
    value: 4
    unit: h
    element: food_events.date_created
    description: ensures data latency for date_created is within 4 hours

  - property: retention
    value: 7
    unit: y
    element: food_events.date_created
    description: ensures data retention for date_created is at least 7 years

support:
  - channel: fda_adverse_events
    tool: slack
    scope: interactive
    url: https://fda.slack.com/archives/C987654321
    description: interactive support channel for adverse event reporting

price:
  priceAmount: 149.95
  priceCurrency: USD
  priceUnit: record

team:
  - username: jdoe
    role: Data Engineer
    dateIn: 2023-01-01
  - username: msmith
    role: Data Scientist
    dateIn: 2023-03-15
    dateOut: 2024-01-01
    replacedByUsername: rjohnson

roles:
  - role: fda_analyst
    access: read
    firstLevelApprovers: "Safety Officer"
    secondLevelApprovers: "Regulatory Lead"
  - role: fda_admin
    access: write
    firstLevelApprovers: "IT Lead"
    secondLevelApprovers: "CIO"

servers:
  - type: bigquery
    description: Google BigQuery instance for food adverse event data
    environment: prod
    project: fda-data
    dataset: fda_food
    roles:
      - role: fda_analyst
        access: read
    customProperties:
      - property: dataRetentionPolicy
        value: "10 years"

customProperties:
  - property: dataSourceOrigin
    value: "FDA Adverse Event Reporting System"
  - property: complianceLevel
    value: "FDA 21 CFR Part 11"

contractCreatedTs: 2025-02-25T10:00:00Z
{% endset %}

{# for embedded YAML approach: #}
{{ process_data_contract('fda_food', 'food_events', contract_yaml=contract_yaml) }}