-- models/food_events.sql
{{ config(
    materialized='table'
) }}

{{ process_data_contract(
    contract_file=project_root ~ '/data_contracts/food_events.yml',
    source_name='fda_food',
    table_name='food_events'
) }}