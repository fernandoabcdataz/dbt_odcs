-- dbt_odcs/macros/load_contract.sql
{% macro load_contract(contract_path) %}
    {# for debugging only - print the path #}
    {% do log("attempting to load: " ~ contract_path, info=true) %}
    
    {# return a basic schema structure to prevent errors #}
    {% do return({
        "schema": [
            {
                "name": "tbl",
                "properties": []
            }
        ]
    }) %}
{% endmacro %}