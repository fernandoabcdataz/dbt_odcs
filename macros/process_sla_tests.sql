{% macro process_sla_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# extract SLA properties from contract #}
    {% set sla_properties = contract.get('slaProperties', []) %}
    
    {# generate SLA tests #}
    {% for sla in sla_properties %}
        {# frequency test (freshness) #}
        {% if sla.property == 'frequency' %}
            {% set element_parts = sla.element.split('.') %}
            {% set column_name = element_parts[1] if element_parts|length > 1 else element_parts[0] %}
            {% set interval_value = sla.value %}
            {% set interval_unit = sla.unit | default('d') %}
            
            {# Convert unit to days if needed #}
            {% set days = interval_value %}
            {% if interval_unit == 'w' or interval_unit == 'week' or interval_unit == 'weeks' %}
                {% set days = interval_value * 7 %}
            {% elif interval_unit == 'm' or interval_unit == 'month' or interval_unit == 'months' %}
                {% set days = interval_value * 30 %}  {# Approximate #}
            {% elif interval_unit == 'y' or interval_unit == 'year' or interval_unit == 'years' %}
                {% set days = interval_value * 365 %}  {# Approximate #}
            {% endif %}
            
            {% set freshness_check = 'DATE_DIFF(CURRENT_DATE(), MAX(CAST(' ~ column_name ~ ' AS DATE)), DAY) <= ' ~ days %}
            
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'freshness',
                'description': sla.get('description', 'Ensures data is fresh with ' ~ column_name ~ ' no older than ' ~ interval_value ~ ' ' ~ interval_unit),
                'interval_value': interval_value,
                'interval_unit': interval_unit,
                'days': days,
                'sql_check': freshness_check,
                'sql': 'SELECT MAX(CAST(' ~ column_name ~ ' AS DATE)) as max_date, CURRENT_DATE() as current_date, DATE_DIFF(CURRENT_DATE(), MAX(CAST(' ~ column_name ~ ' AS DATE)), DAY) as days_old FROM ' ~ source_ref,
                'sql_count': 'SELECT CASE WHEN ' ~ freshness_check ~ ' THEN 0 ELSE 1 END FROM ' ~ source_ref
            }) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}