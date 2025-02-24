-- dbt_odcs/macros/process_quality_tests.sql
{% macro process_quality_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# extract quality rules from contract #}
    {% set quality_rules = contract.get('quality', []) %}
    
    {# generate quality tests #}
    {% for quality in quality_rules %}
        {% if quality.type == 'library' %}
            {# unique combination test #}
            {% if quality.rule == 'uniqueCombination' %}
                {% set columns = quality.get('columns', []) %}
                {% if columns %}
                    {% set columns_str = columns | join(', ') %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': columns_str,
                        'rule_name': 'unique_combination',
                        'description': quality.get('description', 'Ensures unique combination of columns: ' ~ columns_str),
                        'sql_check': 'COUNT(*) = COUNT(DISTINCT ' ~ columns_str ~ ')',
                        'sql': 'SELECT ' ~ columns_str ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ columns_str ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM (SELECT ' ~ columns_str ~ ', COUNT(*) FROM ' ~ source_ref ~ ' GROUP BY ' ~ columns_str ~ ' HAVING COUNT(*) > 1)'
                    }) %}
                {% endif %}
            {# duplicate count test (rows or %) #}
            {% elif quality.rule == 'duplicateCount' %}
                {% set column = quality.column | default('') %}
                {% if column %}
                    {% set duplicates_query = 'SELECT COUNT(*) - COUNT(DISTINCT ' ~ column ~ ') AS duplicate_count FROM ' ~ source_ref %}
                    {% if quality.unit == 'percent' %}
                        {% set total_query = 'SELECT COUNT(*) AS total FROM ' ~ source_ref %}
                        {% set check = '(SELECT duplicate_count * 100.0 / total FROM (' ~ duplicates_query ~ ') AS dup, (' ~ total_query ~ ') AS tot) <= ' ~ quality.mustBeLessThan %}
                    {% else %}
                        {% set check = duplicates_query ~ ' <= ' ~ quality.mustBeLessThan %}
                    {% endif %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': column,
                        'rule_name': 'duplicate_count',
                        'description': quality.get('description', 'Ensures duplicates are within limit: ' ~ quality.mustBeLessThan ~ ' ' ~ quality.unit),
                        'sql_check': check,
                        'sql': 'SELECT ' ~ column ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ column ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                        'sql_count': 'SELECT CASE WHEN (' ~ check ~ ') THEN 0 ELSE 1 END'
                    }) %}
                {% endif %}
            {# row count test (object-level) #}
            {% elif quality.rule == 'rowCount' %}
                {% set total_query = 'SELECT COUNT(*) AS row_count FROM ' ~ source_ref %}
                {% if quality.mustBeBetween is defined and quality.mustBeBetween is iterable and quality.mustBeBetween|length == 2 %}
                    {% set check = total_query ~ ' BETWEEN ' ~ quality.mustBeBetween[0] ~ ' AND ' ~ quality.mustBeBetween[1] %}
                {% endif %}
                {% if check is defined %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': '',
                        'rule_name': 'row_count',
                        'description': quality.get('description', 'Ensures row count is between ' ~ quality.mustBeBetween[0] ~ ' and ' ~ quality.mustBeBetween[1]),
                        'sql_check': check,
                        'sql': total_query ~ ' LIMIT 10',
                        'sql_count': 'SELECT CASE WHEN (' ~ check ~ ') THEN 0 ELSE 1 END'
                    }) %}
                {% endif %}
            {# value in set test #}
            {% elif quality.rule == 'valueInSet' %}
                {% set column = quality.get('column', '') %}
                {% set allowed_values = quality.get('allowedValues', []) %}
                {% if column and allowed_values %}
                    {% set values_str = "'" ~ allowed_values | join("', '") ~ "'" %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': column,
                        'rule_name': 'value_in_set',
                        'description': quality.get('description', 'Ensures ' ~ column ~ ' contains only allowed values'),
                        'allowed_values': allowed_values,
                        'sql_check': column ~ ' IS NULL OR ' ~ column ~ ' IN (' ~ values_str ~ ')',
                        'sql': 'SELECT DISTINCT ' ~ column ~ ' FROM ' ~ source_ref ~ ' WHERE ' ~ column ~ ' IS NOT NULL AND ' ~ column ~ ' NOT IN (' ~ values_str ~ ') LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ column ~ ' IS NOT NULL AND ' ~ column ~ ' NOT IN (' ~ values_str ~ ')'
                    }) %}
                {% endif %}
            {# conditional not null test #}
            {% elif quality.rule == 'conditionalNotNull' %}
                {% set column = quality.get('column', '') %}
                {% set condition = quality.get('condition', '') %}
                {% if column and condition %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': column,
                        'rule_name': 'conditional_not_null',
                        'condition': condition,
                        'description': quality.get('description', 'Ensures ' ~ column ~ ' is not null when ' ~ condition),
                        'sql_check': 'NOT (' ~ condition ~ ') OR ' ~ column ~ ' IS NOT NULL',
                        'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE ' ~ condition ~ ' AND ' ~ column ~ ' IS NULL LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ condition ~ ' AND ' ~ column ~ ' IS NULL'
                    }) %}
                {% endif %}
            {# not null test #}
            {% elif quality.rule == 'notNull' %}
                {% set column = quality.get('column', '') %}
                {% if column %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': column,
                        'rule_name': 'not_null',
                        'description': quality.get('description', 'Ensures ' ~ column ~ ' is never null'),
                        'sql_check': column ~ ' IS NOT NULL',
                        'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE ' ~ column ~ ' IS NULL LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ column ~ ' IS NULL'
                    }) %}
                {% endif %}
            {% endif %}
        {# handle text type #}
        {% elif quality.type == 'text' %}
            {% do log("text quality rule: " ~ quality.description, info=true) %}
        {# handle sql type with operators #}
        {% elif quality.type == 'sql' %}
            {% set query = quality.query | replace('${object}', source_ref) | replace('${property}', quality.column | default('')) %}
            {% if quality.mustBe is defined %}
                {% set check = query ~ ' = ' ~ quality.mustBe %}
            {% elif quality.mustNotBe is defined %}
                {% set check = query ~ ' != ' ~ quality.mustNotBe %}
            {% elif quality.mustBeGreaterThan is defined %}
                {% set check = query ~ ' > ' ~ quality.mustBeGreaterThan %}
            {% elif quality.mustBeGreaterOrEqualTo is defined %}
                {% set check = query ~ ' >= ' ~ quality.mustBeGreaterOrEqualTo %}
            {% elif quality.mustBeLessThan is defined %}
                {% set check = query ~ ' < ' ~ quality.mustBeLessThan %}
            {% elif quality.mustBeLessOrEqualTo is defined %}
                {% set check = query ~ ' <= ' ~ quality.mustBeLessOrEqualTo %}
            {% elif quality.mustBeBetween is defined and quality.mustBeBetween is iterable and quality.mustBeBetween|length == 2 %}
                {% set check = query ~ ' BETWEEN ' ~ quality.mustBeBetween[0] ~ ' AND ' ~ quality.mustBeBetween[1] %}
            {% elif quality.mustNotBeBetween is defined and quality.mustNotBeBetween is iterable and quality.mustNotBeBetween|length == 2 %}
                {% set check = 'NOT (' ~ query ~ ' BETWEEN ' ~ quality.mustNotBeBetween[0] ~ ' AND ' ~ quality.mustNotBeBetween[1] ~ ')' %}
            {% endif %}
            {% if check is defined %}
                {% do tests.append({
                    'test_type': 'Data Quality',
                    'table_name': table_name,
                    'column_name': quality.column | default(''),
                    'rule_name': 'custom_sql_' ~ (quality.name | default('sql_check')),
                    'description': quality.get('description', 'custom sql quality check'),
                    'sql_check': check,
                    'sql': query ~ ' LIMIT 10',
                    'sql_count': 'SELECT CASE WHEN (' ~ check ~ ') THEN 0 ELSE 1 END'
                }) %}
            {% endif %}
        {# handle custom type #}
        {% elif quality.type == 'custom' %}
            {% do log("custom quality rule (vendor-specific): " ~ quality.engine ~ ' - ' ~ quality.implementation, info=true) %}
        {% endif %}
        
        {# handle scheduling info #}
        {% if quality.scheduler is defined or quality.schedule is defined %}
            {% do log("scheduling info: scheduler=" ~ quality.scheduler | default('none') ~ ", schedule=" ~ quality.schedule | default('none'), info=true) %}
        {% endif %}
        
        {# log dimensions and severity #}
        {% if quality.dimension is defined %}
            {% do log("quality dimension: " ~ quality.dimension, info=true) %}
        {% endif %}
        {% if quality.severity is defined %}
            {% do log("quality severity: " ~ quality.severity, info=true) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}