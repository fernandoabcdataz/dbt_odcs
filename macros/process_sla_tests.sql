-- dbt_odcs/macros/process_sla_tests.sql
{% macro process_sla_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# extract sla properties from contract #}
    {% set sla_properties = contract.get('slaProperties', []) %}
    
    {# log default sla element if defined #}
    {% if contract.slaDefaultElement is defined %}
        {% do log("sla default element: " ~ contract.slaDefaultElement, info=true) %}
    {% endif %}
    
    {# generate sla tests #}
    {% for sla in sla_properties %}
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
            
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'freshness',
                'description': sla.get('description', 'ensures data is fresh with ' ~ column_name ~ ' no older than ' ~ interval_value ~ ' ' ~ interval_unit),
                'interval_value': interval_value,
                'interval_unit': interval_unit,
                'days': days,
                'sql_check': freshness_check,
                'sql': 'SELECT MAX(CAST(' ~ column_name ~ ' AS DATE)) as max_date, CURRENT_DATE() as current_date, DATE_DIFF(CURRENT_DATE(), MAX(CAST(' ~ column_name ~ ' AS DATE)), DAY) as days_old FROM ' ~ source_ref,
                'sql_count': '(SELECT CASE WHEN (' ~ freshness_check ~ ') THEN 0 ELSE 1 END FROM ' ~ source_ref ~ ')'
            }) %}
        {% elif sla.property == 'latency' %}
            {% set column_name = sla.element.split('.')[-1] if sla.element else '' %}
            {% set latency_check = 'DATEDIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), DAY) <= ' ~ sla.value %}
            {% if sla.unit in ['d', 'day', 'days'] %}
                {% set latency_check = latency_check %}
            {% elif sla.unit in ['w', 'week', 'weeks'] %}
                {% set latency_check = 'DATEDIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), DAY) <= ' ~ (sla.value * 7) %}
            {% elif sla.unit in ['m', 'month', 'months'] %}
                {% set latency_check = 'DATEDIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), MONTH) <= ' ~ sla.value %}
            {% elif sla.unit in ['y', 'year', 'years'] %}
                {% set latency_check = 'DATEDIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), YEAR) <= ' ~ sla.value %}
            {% endif %}
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'latency',
                'description': sla.get('description', 'ensures latency for ' ~ column_name ~ ' is within ' ~ sla.value ~ ' ' ~ sla.unit),
                'sql_check': latency_check,
                'sql': 'SELECT MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)) as last_update, CURRENT_TIMESTAMP() as now, DATEDIFF(CURRENT_TIMESTAMP(), MAX(CAST(' ~ column_name ~ ' AS TIMESTAMP)), DAY) as days_lag FROM ' ~ source_ref,
                'sql_count': '(SELECT CASE WHEN (' ~ latency_check ~ ') THEN 0 ELSE 1 END FROM ' ~ source_ref ~ ')'
            }) %}
        {% elif sla.property in ['generalAvailability', 'endOfSupport', 'endOfLife'] %}
            {% set value = sla.value | string %}
            {% set date_check = 'CAST(\'' ~ value ~ '\' AS TIMESTAMP) IS NOT NULL' %}
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': '',
                'rule_name': sla.property,
                'description': sla.get('description', 'ensures ' ~ sla.property ~ ' is set to ' ~ value),
                'sql_check': date_check,
                'sql': 'SELECT CAST(\'' ~ value ~ '\' AS TIMESTAMP) as ' ~ sla.property ~ ' FROM ' ~ source_ref ~ ' LIMIT 1',
                'sql_count': '(SELECT CASE WHEN (' ~ date_check ~ ') THEN 0 ELSE 1 END FROM ' ~ source_ref ~ ')'
            }) %}
        {% elif sla.property == 'retention' %}
            {% set column_name = sla.element.split('.')[-1] if sla.element else '' %}
            {% set retention_check = 'DATEDIFF(CURRENT_TIMESTAMP(), MIN(CAST(' ~ column_name ~ ' AS TIMESTAMP)), ' ~ sla.unit | default('DAY') ~ ') >= ' ~ sla.value %}
            {% do tests.append({
                'test_type': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'retention',
                'description': sla.get('description', 'ensures retention for ' ~ column_name ~ ' is at least ' ~ sla.value ~ ' ' ~ sla.unit),
                'sql_check': retention_check,
                'sql': 'SELECT MIN(CAST(' ~ column_name ~ ' AS TIMESTAMP)) as oldest, CURRENT_TIMESTAMP() as now, DATEDIFF(CURRENT_TIMESTAMP(), MIN(CAST(' ~ column_name ~ ' AS TIMESTAMP)), ' ~ sla.unit | default('DAY') ~ ') as retention_days FROM ' ~ source_ref,
                'sql_count': '(SELECT CASE WHEN (' ~ retention_check ~ ') THEN 0 ELSE 1 END FROM ' ~ source_ref ~ ')'
            }) %}
        {% elif sla.property == 'timeOfAvailability' %}
            {% set column_name = sla.element.split('.')[-1] if sla.element else '' %}
            {% set time_range = sla.value.split('-') %}
            {% if time_range|length == 2 %}
                {% set start_time, end_time = time_range %}
                {% set availability_check = 'EXTRACT(HOUR FROM CAST(' ~ column_name ~ ' AS TIMESTAMP)) BETWEEN EXTRACT(HOUR FROM CAST(\'' ~ start_time ~ '\' AS TIME)) AND EXTRACT(HOUR FROM CAST(\'' ~ end_time ~ '\' AS TIME))' %}
                {% do tests.append({
                    'test_type': 'Service-Level Agreement',
                    'table_name': table_name,
                    'column_name': column_name,
                    'rule_name': 'time_of_availability',
                    'description': sla.get('description', 'ensures ' ~ column_name ~ ' is available between ' ~ start_time ~ ' and ' ~ end_time),
                    'sql_check': availability_check,
                    'sql': 'SELECT ' ~ column_name ~ ', EXTRACT(HOUR FROM CAST(' ~ column_name ~ ' AS TIMESTAMP)) as hour FROM ' ~ source_ref ~ ' WHERE NOT (' ~ availability_check ~ ') LIMIT 10',
                    'sql_count': '(SELECT CASE WHEN (' ~ availability_check ~ ') THEN 0 ELSE 1 END FROM ' ~ source_ref ~ ')'
                }) %}
            {% endif %}
        {% endif %}
        
        {# log sla metadata #}
        {% if sla.driver is defined %}
            {% do log("sla driver: " ~ sla.driver, info=true) %}
        {% endif %}
        {% if sla.valueExt is defined %}
            {% do log("sla extended value: " ~ sla.valueExt, info=true) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}