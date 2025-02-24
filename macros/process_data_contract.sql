-- dbt_odcs/macros/process_data_contract.sql
{% macro process_data_contract(source_name, table_name, contract_yaml=none, contract_path=none) %}
    {# parse the contract from yaml string #}
    {% if contract_yaml %}
        {% set contract = fromyaml(contract_yaml) %}
        {# validate fundamental fields if needed #}
        {% if contract.apiVersion != 'v3.0.1' %}
            {% do log("warning: contract api version is not v3.0.1, found: " ~ contract.apiVersion, info=true) %}
        {% endif %}
        {% if contract.kind != 'DataContract' %}
            {% do log("warning: contract kind is not DataContract, found: " ~ contract.kind, info=true) %}
        {% endif %}
        {# log optional sections for documentation #}
        {% if contract.support is defined %}
            {% do log("support channels: " ~ contract.support, info=true) %}
        {% endif %}
        {% if contract.price is defined %}
            {% do log("pricing info: " ~ contract.price, info=true) %}
        {% endif %}
        {% if contract.team is defined %}
            {% do log("team info: " ~ contract.team, info=true) %}
        {% endif %}
        {% if contract.roles is defined %}
            {% do log("roles info: " ~ contract.roles, info=true) %}
        {% endif %}
        {% if contract.servers is defined %}
            {% do log("servers info: " ~ contract.servers, info=true) %}
        {% endif %}
        {% if contract.customProperties is defined %}
            {% do log("custom properties: " ~ contract.customProperties, info=true) %}
        {% endif %}
        {% if contract.contractCreatedTs is defined %}
            {% do log("contract created timestamp: " ~ contract.contractCreatedTs, info=true) %}
        {% endif %}
    {% elif contract_path %}
        {% set contract = load_contract(contract_path) %}
    {% else %}
        {% set contract = {} %}
    {% endif %}
    
    {# generate tests for each category #}
    {% set schema_tests = process_schema_tests(source_name, table_name, contract) %}
    {% set quality_tests = process_quality_tests(source_name, table_name, contract) %}
    {% set sla_tests = process_sla_tests(source_name, table_name, contract) %}
    
    {# combine and return all test results #}
    {{ combine_test_results(schema_tests, quality_tests, sla_tests) }}
{% endmacro %}