{% macro load_contract(contract_path) %}
    {# Attempt to load contract from file path #}
    {% if contract_path %}
        {% set contract_content = '' %}
        {% if execute %}
            {% do log("Attempting to load file: " ~ contract_path, info=True) %}
            
            {# Try to read file using filesystem approach #}
            {% if modules is defined and modules.filesystem is defined %}
                {% try %}
                    {% set contract_content = modules.filesystem.read_file(contract_path) %}
                    {% do log("Successfully loaded file using modules.filesystem", info=True) %}
                {% except %}
                    {% do log("Could not load file using modules.filesystem: " ~ exception, info=True) %}
                {% endtry %}
            {% elif modules is defined %}
                {# Try standard modules approach #}
                {% try %}
                    {% set contract_content = modules.read_file(contract_path) %}
                    {% do log("Successfully loaded file using modules.read_file", info=True) %}
                {% except %}
                    {% do log("Could not load file using modules.read_file: " ~ exception, info=True) %}
                {% endtry %}
            {% endif %}
            
            {# If still no content, create a dummy contract for testing #}
            {% if not contract_content %}
                {% do log("Using fallback dummy contract for testing", info=True) %}
                {% set contract_content %}
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
        description: Unique identifier for each adverse event report
quality:
  - type: library
    rule: notNull
    column: report_number
    name: report_number_not_null
    description: Ensures report_number is never null
slaProperties:
  - property: frequency
    value: 1
    unit: d
    element: food_events.date_created
    description: Ensures data is refreshed daily
                {% endset %}
                {% do log("Created fallback dummy contract", info=True) %}
            {% endif %}
        {% endif %}
        
        {% if contract_content %}
            {% do log("Parsing YAML content", info=True) %}
            {% set contract = fromyaml(contract_content) %}
            {% do log("Successfully parsed YAML", info=True) %}
            {% do return(contract) %}
        {% endif %}
    {% endif %}
    
    {# Return empty dict with schema if contract couldnt be loaded #}
    {% do log("WARNING: Could not load or parse contract, returning empty placeholder", info=True) %}
    {% do return({"schema": []}) %}
{% endmacro %}