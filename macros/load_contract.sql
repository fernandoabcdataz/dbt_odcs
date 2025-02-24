{% macro load_contract(contract_path) %}
    {# For debugging only - print the path #}
    {% do log("Attempting to load: " ~ contract_path, info=true) %}
    
    {# Return a basic schema structure to prevent errors #}
    {% do return({
        "schema": [
            {
                "name": "tbl",
                "properties": []
            }
        ]
    }) %}
{% endmacro %}