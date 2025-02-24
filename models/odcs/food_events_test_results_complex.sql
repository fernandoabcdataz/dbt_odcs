-- dbt_odcs/models/odcs/food_events_test_results_complex.sql
{{ config(materialized='table') }}

{% set contract_yaml %}
apiVersion: v3.0.1
kind: DataContract

id: 53581432-6c55-4ba2-a65f-72344a91553a
name: food_events_v2
version: 2.0.0
status: active
domain: health
dataProduct: adverse_events
tenant: FDA
description:
  purpose: Tracks adverse events related to food products for regulatory and safety analysis.
  limitations: Data may be incomplete for historical records older than 5 years.
  usage: Use for regulatory reporting, consumer safety analysis, and product monitoring.
tags: ['health', 'safety', 'regulation']

schema:
  - name: tbl
    logicalType: object
    physicalName: food_events
    physicalType: table
    description: Provides detailed adverse event data for food products.
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
        description: unique identifier for each adverse event report.
        unique: true
        tags: ['identifier', 'critical']
        classification: public
        logicalTypeOptions:
          pattern: "^[A-Z]{2}-\\d{6}$"
          minLength: 8
          maxLength: 10

      - name: reactions
        logicalType: string
        physicalType: string
        required: false
        description: descriptions of adverse reactions experienced.
        tags: ['sensitive']
        classification: restricted
        logicalTypeOptions:
          maxLength: 1000
          pattern: "^[A-Za-z0-9\\s,.-]+$"

      - name: outcomes
        logicalType: string
        physicalType: string
        required: false
        description: outcomes of the adverse events (e.g., Hospitalization, Recovery).
        tags: ['outcome', 'health']
        classification: public
        logicalTypeOptions:
          pattern: "^(Hospitalization|Recovery|Death|Other)$"

      - name: products_brand_name
        logicalType: string
        physicalType: string
        required: false
        description: brand name of the product involved.
        tags: ['product']
        classification: public

      - name: products_industry_code
        logicalType: integer
        physicalType: integer
        required: false
        description: industry code of the product.
        logicalTypeOptions:
          minimum: 100
          maximum: 9999
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
        description: Name of the products industry.
        tags: ['industry']
        classification: public

      - name: date_created
        logicalType: date
        physicalType: date
        required: true
        description: Date the report was created.
        tags: ['timestamp']
        classification: public
        logicalTypeOptions:
          format: "yyyy-MM-dd"
          maximum: "2025-12-31"

      - name: date_started
        logicalType: date
        physicalType: date
        required: false
        description: Date the adverse event started.
        tags: ['timestamp']
        classification: public
        logicalTypeOptions:
          format: "yyyy-MM-dd"
          minimum: "1900-01-01"

      - name: consumer_gender
        logicalType: string
        physicalType: string
        required: false
        description: Gender of the consumer (e.g., Male, Female, Non-binary).
        tags: ['personal']
        classification: restricted
        logicalTypeOptions:
          pattern: "^(Male|Female|Non-binary)$"

      - name: consumer_age
        logicalType: integer
        physicalType: integer
        required: false
        description: Age of the consumer.
        logicalTypeOptions:
          minimum: 0
          maximum: 150
          multipleOf: 1

      - name: consumer_age_unit
        logicalType: string
        physicalType: string
        required: false
        description: Unit of consumer age (e.g., years, months).
        tags: ['personal']
        classification: restricted
        logicalTypeOptions:
          pattern: "^(years|months)$"

      - name: related_products
        logicalType: array
        physicalType: array<string>
        required: false
        description: List of related products involved in the event.
        items:
          logicalType: string
          physicalType: string
          logicalTypeOptions:
            pattern: "^[A-Za-z0-9\\s-]+$"
            maxLength: 100
        logicalTypeOptions:
          maxItems: 10
          minItems: 1
          uniqueItems: true

quality:
  - type: library
    rule: uniqueCombination
    columns: ["report_number"]
    name: unique_report_numbers
    description: Ensures no duplicate report numbers exist

  - type: library
    rule: valueInSet
    column: products_role
    allowedValues: ["suspect", "concomitant"]
    name: valid_product_roles
    description: Ensures product_role contains only valid values

  - type: library
    rule: conditionalNotNull
    column: consumer_age_unit
    condition: consumer_age IS NOT NULL
    name: age_unit_if_age_provided
    description: Ensures consumer_age_unit is provided if consumer_age is present

  - type: library
    rule: notNull
    column: report_number
    name: report_number_not_null
    description: Ensures report_number is never null

  - type: library
    rule: notNull
    column: date_created
    name: date_created_not_null
    description: Ensures date_created is never null

  - type: library
    rule: duplicateCount
    column: reactions
    mustBeLessThan: 5
    unit: rows
    name: few_duplicate_reactions
    description: Ensures no more than 5 duplicate reactions exist

  - type: library
    rule: duplicateCount
    column: products_brand_name
    mustBeLessThan: 2
    unit: percent
    name: low_duplicate_brands
    description: Ensures duplicate brands are less than 2% of total records

  - type: library
    rule: rowCount
    mustBeBetween: [1000, 5000]
    name: row_count_range
    description: Ensures row count is between 1000 and 5000 records

  - type: text
    description: The report_number should be verified against regulatory standards.

  - type: sql
    query: |
      SELECT COUNT(*) FROM ${object} WHERE ${property} > 0
    column: consumer_age
    mustBeLessThan: 10000
    name: age_count_limit
    description: Ensures the count of consumer ages greater than 0 is less than 10000

  - type: custom
    engine: greatExpectations
    implementation: |
      type: expect_column_values_to_be_between
      kwargs:
        column: consumer_age
        min_value: 0
        max_value: 150
    name: age_range_ge
    description: Ensures consumer_age is between 0 and 150 using Great Expectations

  - scheduler: cron
    schedule: "0 0 * * *"
    dimension: timeliness
    severity: high
    businessImpact: "Delays in data freshness affect regulatory reporting."

slaProperties:
  - property: frequency
    value: 1
    unit: d
    element: food_events.date_created
    description: Ensures data is refreshed daily, with date_created no older than 1 day

  - property: latency
    value: 2
    unit: h
    element: food_events.date_created
    description: Ensures data latency for date_created is within 2 hours

  - property: generalAvailability
    value: 2025-01-01T00:00:00Z
    description: Ensures data is generally available starting January 1, 2025

  - property: endOfSupport
    value: 2030-12-31T23:59:59Z
    description: Ensures support ends on December 31, 2030

  - property: endOfLife
    value: 2040-12-31T23:59:59Z
    description: ensures the data product lifecycle ends on December 31, 2040

  - property: retention
    value: 5
    unit: y
    element: food_events.date_created
    description: ensures data retention for date_created is at least 5 years

  - property: timeOfAvailability
    value: "09:00-17:00"
    element: food_events.date_created
    driver: regulatory
    description: Ensures data is available between 9 AM and 5 PM daily for regulatory purposes

support:
  - channel: fda_food_safety
    tool: slack
    scope: interactive
    url: https://fda.slack.com/archives/C123456789
    description: Interactive support channel for food safety issues

price:
  priceAmount: 99.95
  priceCurrency: USD
  priceUnit: record

team:
  - username: jsmit
    role: Data Engineer
    dateIn: 2023-01-15
  - username: amlee
    role: Data Scientist
    dateIn: 2023-03-01
    dateOut: 2024-02-01
    replacedByUsername: rjohnson

roles:
  - role: fda_analyst
    access: read
    firstLevelApprovers: "Safety Manager"
    secondLevelApprovers: "Regulatory Director"
  - role: fda_admin
    access: write
    firstLevelApprovers: "IT Manager"
    secondLevelApprovers: "CIO"

servers:
  - type: bigquery
    description: Google BigQuery instance for food event data
    environment: prod
    project: fda-data
    dataset: fda_food
    roles:
      - role: fda_analyst
        access: read
    customProperties:
      - property: dataRetentionPolicy
        value: "7 years"

customProperties:
  - property: dataSourceOrigin
    value: "FDA Adverse Event Reporting System"
  - property: complianceLevel
    value: "HIPAA"

contractCreatedTs: 2024-02-25T12:00:00Z
{% endset %}

{# for embedded YAML approach: #}
{{ process_data_contract('fda_food', 'food_events', contract_yaml=contract_yaml) }}