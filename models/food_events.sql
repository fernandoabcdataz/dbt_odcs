{{ config(materialized='table') }}

{% set contract_yaml %}
schema:
  - name: tbl
    physicalName: food_events
    properties:
      - name: report_number
        logicalType: integer
        physicalType: integer
        required: true
      - name: products_brand_name
        logicalType: string
        physicalType: varchar
        primaryKey: false
quality:
  - type: library
    rule: duplicateCount
    mustBeLessThan: 10
    name: fewer than 10 duplicate rows
    unit: rows
slaProperties:
  - property: frequency
    value: 1
    unit: d
    element: food_events.date_created
{% endset %}

{{ process_data_contract('fda_food', 'food_events', contract_yaml) }}