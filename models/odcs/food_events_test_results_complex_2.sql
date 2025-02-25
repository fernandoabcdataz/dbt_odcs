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
        logicalTypeOptions:
          pattern: "^(?:[0-9]{6}|[A-Z]{4}-[0-9]{6}|[0-9]{4}-CFS-[0-9]{6})$"
          minLength: 6
          maxLength: 15

      - name: reactions
        logicalType: string
        physicalType: string
        required: false
        description: descriptions of adverse reactions experienced, often multiple conditions in quotes and commas.
        tags: ['sensitive', 'medical']
        classification: restricted
        logicalTypeOptions:
          maxLength: 2000
          pattern: "^\"?[A-Za-z0-9\\s,/]+\"?(, ?\"?[A-Za-z0-9\\s,/]+\"?)*$"

      - name: outcomes
        logicalType: string
        physicalType: string
        required: false
        description: outcomes of the adverse events (e.g., Hospitalization, Death, Disability).
        tags: ['outcome', 'health']
        classification: public
        logicalTypeOptions:
          pattern: "^(Hospitalization|Death|Disability|Other Serious or Important Medical Event|Visited Emergency Room|Visited a Health Care Provider|Other Outcome|Life Threatening|Required Intervention)*$"

      - name: products_brand_name
        logicalType: string
        physicalType: string
        required: false
        description: brand name of the product involved in the adverse event.
        tags: ['product']
        classification: public
        logicalTypeOptions:
          maxLength: 100
          pattern: "^[A-Za-z0-9\\s\\(\\)-]+$"

      - name: products_industry_code
        logicalType: integer
        physicalType: integer
        required: false
        description: industry code of the product, typically a numeric code (e.g., 54 for vitamins/minerals).
        logicalTypeOptions:
          minimum: 1
          maximum: 99
          multipleOf: 1

      - name: products_role
        logicalType: string
        physicalType: string
        required: false
        description: role of the product in the event (e.g., suspect, concomitant).
        tags: ['product', 'role']
        classification: public
        logicalTypeOptions:
          pattern: "^(suspect|concomitant)$"

      - name: products_industry_name
        logicalType: string
        physicalType: string
        required: false
        description: name of the product’s industry category (e.g., Vit/Min/Prot/Unconv Diet).
        tags: ['industry']
        classification: public
        logicalTypeOptions:
          pattern: "^[A-Za-z/]+$"

      - name: date_created
        logicalType: date
        physicalType: date
        required: true
        description: date the report was created, typically in YYYY-MM-DD format.
        tags: ['timestamp']
        classification: public
        logicalTypeOptions:
          format: "yyyy-MM-dd"
          minimum: "2005-01-01"
          maximum: "2025-12-31"

      - name: date_started
        logicalType: date
        physicalType: date
        required: false
        description: date the adverse event started, if known, in YYYY-MM-DD format.
        tags: ['timestamp']
        classification: public
        logicalTypeOptions:
          format: "yyyy-MM-dd"
          minimum: "1900-01-01"
          maximum: "2025-12-31"

      - name: consumer_gender
        logicalType: string
        physicalType: string
        required: false
        description: gender of the consumer (e.g., Male, Female).
        tags: ['personal']
        classification: restricted
        logicalTypeOptions:
          pattern: "^(Male|Female)?$"

      - name: consumer_age
        logicalType: integer
        physicalType: integer
        required: false
        description: age of the consumer, if provided.
        logicalTypeOptions:
          minimum: 0
          maximum: 150
          multipleOf: 1

      - name: consumer_age_unit
        logicalType: string
        physicalType: string
        required: false
        description: unit of consumer age (e.g., years), if age is provided.
        tags: ['personal']
        classification: restricted
        logicalTypeOptions:
          pattern: "^(years|months)?$"

      - name: related_products
        logicalType: array
        physicalType: array<string>
        required: false
        description: list of related products involved in the event, if multiple products are involved.
        items:
          logicalType: string
          physicalType: string
          logicalTypeOptions:
            pattern: "^[A-Za-z0-9\\s\\(\\)-]+$"
            maxLength: 100
        logicalTypeOptions:
          maxItems: 5
          minItems: 1
          uniqueItems: true

quality:
  - type: library
    rule: uniqueCombination
    columns: ["report_number"]
    name: unique_report_numbers
    description: ensures no duplicate report numbers exist

  - type: library
    rule: valueInSet
    column: products_role
    allowedValues: ["suspect", "concomitant"]
    name: valid_product_roles
    description: ensures products_role contains only valid values (suspect or concomitant)

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

  - property: generalAvailability
    value: 2025-01-01T00:00:00Z
    description: ensures data is generally available starting January 1, 2025

  - property: endOfSupport
    value: 2030-12-31T23:59:59Z
    description: ensures support ends on December 31, 2030

  - property: endOfLife
    value: 2040-12-31T23:59:59Z
    description: ensures the data product lifecycle ends on December 31, 2040

  - property: retention
    value: 7
    unit: y
    element: food_events.date_created
    description: ensures data retention for date_created is at least 7 years

  - property: timeOfAvailability
    value: "08:00-18:00"
    element: food_events.date_created
    driver: regulatory
    description: ensures data is available between 8 AM and 6 PM daily for regulatory purposes

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