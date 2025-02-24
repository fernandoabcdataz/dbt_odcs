{% macro process_quality_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# extract quality rules from contract #}
    {% set quality_rules = contract.get('quality', []) %}
    
    {# generate quality tests #}
    {% for quality in quality_rules %}
        {% if quality.type == 'library' %}
            {# Unique Combination test #}
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
            {% endif %}
            
            {# Value In Set test #}
            {% if quality.rule == 'valueInSet' %}
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
            {% endif %}
            
            {# Conditional Not Null test #}
            {% if quality.rule == 'conditionalNotNull' %}
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
            {% endif %}
            
            {# Not Null test (could be handled in schema tests, but included here for completeness) #}
            {% if quality.rule == 'notNull' %}
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
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}