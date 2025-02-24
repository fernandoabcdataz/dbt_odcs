-- dbt_odcs/models/file_based_test_results.sql
{{ config(materialized='table') }}

{# this model demonstrates the file-based approach using the vars configuration #}
{{ process_data_contract('fda_food', 'food_events', contract_path=var('contracts_path') ~ '/food_events.yml') }}