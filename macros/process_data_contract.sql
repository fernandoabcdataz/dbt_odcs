-- dbt_odcs/macros/process_data_contract.sql
{% macro process_data_contract(source_name, table_name, contract_yaml=none, contract_path=none) %}
    
    {# parse the contract from YAML string or file #}
    {% set contract = fromyaml(contract_yaml) %}
    
    {# generate tests for each category #}
    {% set schema_tests = process_schema_tests(source_name, table_name, contract) %}
    {% set quality_tests = process_quality_tests(source_name, table_name, contract) %}
    {% set sla_tests = process_sla_tests(source_name, table_name, contract) %}
    
    {# combine and return all test results #}
    {{ combine_test_results(schema_tests, quality_tests, sla_tests) }}
{% endmacro %}