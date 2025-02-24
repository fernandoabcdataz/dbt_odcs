-- dbt_odcs/models/odcs/food_events_test_results.sql
{{ config(materialized='table') }}

{% set contract_yaml %}
apiVersion: v3.0.1
kind: DataContract

schema:
  - name: tbl
    logicalType: object
    physicalName: food_events
    properties:
      - name: report_number
        logicalType: string
        physicalType: string
        required: true
        primaryKey: true
        description: unique identifier for each adverse event report
      - name: reactions
        logicalType: string
        physicalType: string
        required: false
        description: descriptions of adverse reactions experienced
      - name: outcomes
        logicalType: string
        physicalType: string
        required: false
        description: outcomes of the adverse events (e.g., Hospitalization)
      - name: products_brand_name
        logicalType: string
        physicalType: string
        required: false
        description: brand name of the product involved
      - name: products_industry_code
        logicalType: integer
        physicalType: integer
        required: false
        description: industry code of the product
      - name: products_role
        logicalType: string
        physicalType: string
        required: false
        description: role of the product in the event (e.g., suspect, concomitant)
      - name: products_industry_name
        logicalType: string
        physicalType: string
        required: false
        description: name of the products industry
      - name: date_created
        logicalType: date
        physicalType: date
        required: true
        description: date the report was created
      - name: date_started
        logicalType: date
        physicalType: date
        required: false
        description: date the adverse event started
      - name: consumer_gender
        logicalType: string
        physicalType: string
        required: false
        description: gender of the consumer (e.g., Male, Female)
      - name: consumer_age
        logicalType: integer
        physicalType: integer
        required: false
        description: age of the consumer
      - name: consumer_age_unit
        logicalType: string
        physicalType: string
        required: false
        description: unit of consumer age (e.g., years)
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
    description: ensures product_role contains only valid values
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
slaProperties:
  - property: frequency
    value: 1
    unit: d
    element: food_events.date_created
    description: ensures data is refreshed daily, with date_created no older than 1 day
{% endset %}

{# for embedded YAML approach: #}
{{ process_data_contract('fda_food', 'food_events', contract_yaml=contract_yaml) }}