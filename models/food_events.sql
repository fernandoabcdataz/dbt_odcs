{{ config(materialized='table') }}

{% set contract_yaml %}
schema:
  - name: tbl
    physicalName: food_events
    properties:
      - name: report_number
        logicalType: string
        physicalType: string
        required: true
        primaryKey: true
        description: Unique identifier for each adverse event report
      - name: reactions
        logicalType: string
        physicalType: string
        required: false
        description: Descriptions of adverse reactions experienced
      - name: outcomes
        logicalType: string
        physicalType: string
        required: false
        description: Outcomes of the adverse events (e.g., Hospitalization)
      - name: products_brand_name
        logicalType: string
        physicalType: string
        required: false
        description: Brand name of the product involved
      - name: products_industry_code
        logicalType: integer
        physicalType: integer
        required: false
        description: Industry code of the product
      - name: products_role
        logicalType: string
        physicalType: string
        required: false
        description: Role of the product in the event (e.g., suspect, concomitant)
      - name: products_industry_name
        logicalType: string
        physicalType: string
        required: false
        description: Name of the products industry
      - name: date_created
        logicalType: date
        physicalType: date
        required: true
        description: Date the report was created
      - name: date_started
        logicalType: date
        physicalType: date
        required: false
        description: Date the adverse event started
      - name: consumer_gender
        logicalType: string
        physicalType: string
        required: false
        description: Gender of the consumer (e.g., Male, Female)
      - name: consumer_age
        logicalType: integer
        physicalType: integer
        required: false
        description: Age of the consumer
      - name: consumer_age_unit
        logicalType: string
        physicalType: string
        required: false
        description: Unit of consumer age (e.g., years)
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
slaProperties:
  - property: frequency
    value: 1
    unit: d
    element: food_events.date_created
    description: Ensures data is refreshed daily, with date_created no older than 1 day
{% endset %}

{{ process_data_contract('fda_food', 'food_events', contract_yaml) }}