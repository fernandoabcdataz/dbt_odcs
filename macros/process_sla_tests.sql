-- dbt_odcs/macros/process_sla_tests.sql

{% macro process_sla_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# extract sla properties from contract #}
    {% if contract is defined and contract is mapping %}
        {% set sla_properties = contract.get('slaProperties', []) %}
    {% else %}
        {% set sla_properties = [] %}
    {% endif %}
    
    {# generate sla tests #}
    {% for sla in sla_properties %}
        {# frequency test (freshness) #}
        {% if sla.property == 'frequency' %}
            {% set element_parts = sla.element.split('.') %}
            {% set column_name = element_parts[1] if element_parts|length > 1 else element_parts[0] %}
            {% set interval_value = sla.value %}
            {% set interval_unit = sla.unit | default('d') %}
            {# convert unit to days if needed #}
            {% set days = interval_value %}
            {% if interval_unit == 'w' or interval_unit == 'week' or interval_unit == 'weeks' %}
                {% set days = interval_value * 7 %}
            {% elif interval_unit == 'm' or interval_unit == 'month' or interval_unit == 'months' %}
                {% set days = interval_value * 30 %}  {# approximate #}
            {% elif interval_unit == 'y' or interval_unit == 'year' or interval_unit == 'years' %}
                {% set days = interval_value * 365 %}  {# approximate #}
            {% endif %}
            {% set freshness_check = 'DATE_DIFF(CURRENT_DATE(), MAX(CAST(' ~ column_name ~ ' AS DATE)), DAY) <= ' ~ days %}
            {% set freshness_query = 'SELECT CASE WHEN ' ~ freshness_check ~ ' THEN 0 ELSE 1 END AS result FROM ' ~ source_ref %}
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
                'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE DATE_DIFF(CURRENT_DATE(), CAST(' ~ column_name ~ ' AS DATE), DAY) > ' ~ days
            }) %}
        
        {# COMMENTED OUT
        {% elif sla.property in ['generalAvailability', 'endOfSupport', 'endOfLife'] %}
            {% set value = sla.value | string %}
            {% set date_check = 'CASE WHEN CAST(\'' ~ value ~ '\' AS TIMESTAMP) IS NOT NULL THEN 0 ELSE 1 END' %}
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': '',
                'rule_name': sla.property,
                'description': sla.get('description', 'Ensures ' ~ sla.property ~ ' is set to ' ~ value),
                'sql_check': 'CAST(\'' ~ value ~ '\' AS TIMESTAMP) IS NOT NULL',
                'sql': 'SELECT CAST(\'' ~ value ~ '\' AS TIMESTAMP) as ' ~ sla.property,
                'sql_count': 'SELECT 0 /* Always passes for timestamp validity checks */'
            }) %} END COMMENT #}

        {% elif sla.property == 'retention' %}
            {% set element_parts = sla.element.split('.') %}
            {% set column_name = element_parts[1] if element_parts|length > 1 else element_parts[0] %}
            {% set retention_value = sla.value %}
            {% set retention_unit = sla.unit | default('d') %}    
            {# convert unit to days if needed #}
            {% set days = retention_value %}
            {% if retention_unit == 'w' or retention_unit == 'week' or retention_unit == 'weeks' %}
                {% set days = retention_value * 7 %}
            {% elif retention_unit == 'm' or retention_unit == 'month' or retention_unit == 'months' %}
                {% set days = retention_value * 30 %}
            {% elif retention_unit == 'y' or retention_unit == 'year' or retention_unit == 'years' %}
                {% set days = retention_value * 365 %}
            {% endif %}
            {# check if the date columns oldest record meets the retention period #}
            {% set retention_check = 'DATE_DIFF(CURRENT_DATE(), MIN(CAST(' ~ column_name ~ ' AS DATE)), DAY) <= ' ~ days %}            
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'retention',
                'description': sla.get('description', 'Ensures data retention for ' ~ column_name ~ ' is at least ' ~ retention_value ~ ' ' ~ retention_unit),
                'interval_value': retention_value,
                'interval_unit': retention_unit,
                'days': days,
                'sql_check': retention_check,
                'sql': 'SELECT MIN(CAST(' ~ column_name ~ ' AS DATE)) as min_date, CURRENT_DATE() as current_date, DATE_DIFF(CURRENT_DATE(), MIN(CAST(' ~ column_name ~ ' AS DATE)), DAY) as days_retained FROM ' ~ source_ref,
                'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE DATE_DIFF(CURRENT_DATE(), CAST(' ~ column_name ~ ' AS DATE), DAY) > ' ~ days
            }) %}

        {% elif sla.property == 'latency' %}
            {% set element_parts = sla.element.split('.') %}
            {% set column_name = element_parts[1] if element_parts|length > 1 else element_parts[0] %}
            {% set latency_value = sla.value %}
            {% set latency_unit = sla.unit | default('h') %}
            {# convert unit to hours if needed #}
            {% set hours = latency_value %}
            {% if latency_unit == 'm' or latency_unit == 'min' or latency_unit == 'minute' or latency_unit == 'minutes' %}
                {% set hours = latency_value / 60.0 %}
            {% elif latency_unit == 'd' or latency_unit == 'day' or latency_unit == 'days' %}
                {% set hours = latency_value * 24 %}
            {% endif %}
            {# check if the timestamp difference is within the latency requirement #}
            {% set latency_check = 'TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), HOUR) <= ' ~ hours %}            
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'latency',
                'description': sla.get('description', 'Ensures data latency for ' ~ column_name ~ ' is within ' ~ latency_value ~ ' ' ~ latency_unit),
                'interval_value': latency_value,
                'interval_unit': latency_unit,
                'hours': hours,
                'sql_check': latency_check,
                'sql': 'SELECT MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)) as max_timestamp, CURRENT_TIMESTAMP() as current_timestamp, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), HOUR) as hours_latency FROM ' ~ source_ref,
                'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), CAST(' ~ column_name ~ ' AS TIMESTAMP), HOUR) > ' ~ hours
            }) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}