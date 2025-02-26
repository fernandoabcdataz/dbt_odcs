-- dbt_odcs/macros/process_quality_tests.sql

{# helper function to determine if a column might be a date type #}
{% macro is_date_column(column_name) %}
    {% set date_indicators = ['date', 'created', 'modified', 'updated', 'timestamp', 'time', 'start', 'end', 'birth', 'death'] %}
    {% for indicator in date_indicators %}
        {% if column_name | lower is string and indicator in column_name | lower %}
            {% do return(True) %}
        {% endif %}
    {% endfor %}
    {% do return(False) %}
{% endmacro %}

{% macro process_quality_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# Extract quality rules from contract #}
    {% if contract is defined and contract is mapping %}
        {% set quality_rules = contract.get('quality', []) %}
    {% else %}
        {% set quality_rules = [] %}
        {% do log("WARNING: Contract is not a valid mapping or is undefined", info=true) %}
    {% endif %}
    
    {# Generate quality tests #}
    {% for quality in quality_rules %}
        {% if quality.type == 'library' %}
            {# Unique Combination test #}
            {% if quality.rule == 'uniqueCombination' %}
                {% set columns = quality.get('columns', []) %}
                {% if columns %}
                    {% set columns_str = columns | join(', ') %}
                    {% do tests.append({
                        'check_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': columns_str,
                        'rule_name': 'unique_combination',
                        'description': quality.get('description', 'Ensures unique combination of columns: ' ~ columns_str),
                        'sql_check': 'COUNT(*) = COUNT(DISTINCT ' ~ columns_str ~ ')',
                        'sql': 'SELECT ' ~ columns_str ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ columns_str ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM (SELECT ' ~ columns_str ~ ', COUNT(*) FROM ' ~ source_ref ~ ' GROUP BY ' ~ columns_str ~ ' HAVING COUNT(*) > 1)'
                    }) %}
                {% endif %}
            {# Duplicate count test (rows or %) #}
            {% elif quality.rule == 'duplicateCount' %}
                {% set column = quality.column | default('') %}
                {% if column %}
                    {% set duplicates_query = 'SELECT COUNT(*) - COUNT(DISTINCT ' ~ column ~ ') as duplicate_count FROM ' ~ source_ref %}
                    {% if quality.unit == 'percent' %}
                        {% set total_query = 'SELECT COUNT(*) as total FROM ' ~ source_ref %}
                        {% set check = '(SELECT duplicate_count * 100.0 / total FROM (' ~ duplicates_query ~ ') as dup, (' ~ total_query ~ ') as tot) <= ' ~ quality.mustBeLessThan %}
                    {% else %}
                        {% set check = '(SELECT duplicate_count FROM (' ~ duplicates_query ~ ')) <= ' ~ quality.mustBeLessThan %}
                    {% endif %}
                    {% do tests.append({
                        'check_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': column,
                        'rule_name': 'duplicate_count',
                        'description': quality.get('description', 'Ensures duplicates are within limit: ' ~ quality.mustBeLessThan ~ ' ' ~ quality.unit),
                        'sql_check': check,
                        'sql': 'SELECT ' ~ column ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ column ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                        'sql_count': 'SELECT (CASE WHEN ' ~ check ~ ' THEN 0 ELSE 1 END) as failed_records'
                    }) %}
                {% endif %}
            {# Row count test (object-level) #}
            {% elif quality.rule == 'rowCount' %}
                {% set total_query = 'SELECT COUNT(*) as row_count FROM ' ~ source_ref %}
                {% if quality.mustBeBetween is defined and quality.mustBeBetween is iterable and quality.mustBeBetween|length == 2 %}
                    {% set min_value, max_value = quality.mustBeBetween %}
                    {% set check = '(SELECT row_count FROM (' ~ total_query ~ ')) BETWEEN ' ~ min_value ~ ' AND ' ~ max_value %}
                {% endif %}
                {% if check is defined %}
                    {% do tests.append({
                        'check_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': '',
                        'rule_name': 'row_count',
                        'description': quality.get('description', 'Ensures row count is between ' ~ quality.mustBeBetween[0] ~ ' and ' ~ quality.mustBeBetween[1]),
                        'sql_check': check,
                        'sql': total_query ~ ' LIMIT 10',
                        'sql_count': 'SELECT (CASE WHEN ' ~ check ~ ' THEN 0 ELSE 1 END) as failed_records'
                    }) %}
                {% endif %}
            {# Value in set test #}
            {% elif quality.rule == 'valueInSet' %}
                {% set column = quality.get('column', '') %}
                {% set allowed_values = quality.get('allowedValues', []) %}
                {% if column and allowed_values %}
                    {% set values_str = "'" ~ allowed_values | join("', '") ~ "'" %}
                    {% do tests.append({
                        'check_type': 'Data Quality',
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
            {# Conditional Not Null test #}
            {% elif quality.rule == 'conditionalNotNull' %}
                {% set column = quality.get('column', '') %}
                {% set condition = quality.get('condition', '') %}
                {% if column and condition %}
                    {% do tests.append({
                        'check_type': 'Data Quality',
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
            {# Not Null test #}
            {% elif quality.rule == 'notNull' %}
                {% set column = quality.get('column', '') %}
                {% if column %}
                    {% do tests.append({
                        'check_type': 'Data Quality',
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
        {# Handle text type #}
        {% elif quality.type == 'text' %}
            {% do log("Text quality rule: " ~ quality.description, info=true) %}
            {% do tests.append({
                'check_type': 'Data Quality',
                'table_name': table_name,
                'column_name': '',
                'rule_name': 'text_description',
                'description': quality.get('description', 'Text quality description'),
                'sql_check': 'TRUE',
                'sql': "SELECT 'text quality check - no sql needed' as description LIMIT 1",
                'sql_count': 'SELECT 0 as failed_records'
            }) %}
        {# Handle SQL type with operators #}
        {% elif quality.type == 'sql' %}
            {% set query = quality.query | replace('${object}', source_ref) | replace('${property}', quality.column | default('')) %}
            {% set query_single_line = query | replace('\n', ' ') %}
            
            {# Fix for duplicate SELECT keywords #}
            {% if query_single_line.startswith('SELECT ') %}
                {% set fixed_query = query_single_line %}
            {% else %}
                {% set fixed_query = 'SELECT ' ~ query_single_line %}
            {% endif %}
            
            {% set count_query = fixed_query %}
            {% set count_result = '(' ~ count_query ~ ' LIMIT 1)' %}
            
            {# Apply proper type casting for numeric and date comparisons #}
            {% set column_expr = count_result %}
            {% set is_likely_numeric = False %}
            {% set is_likely_date = False %}
            
            {% if quality.column is defined and quality.column %}
                {% set column = quality.column %}
                {# Check if column is likely numeric based on the query or column name #}
                {% if column.endswith('_count') or column.endswith('_code') or column.endswith('_id') 
                   or column.endswith('_num') or column.endswith('_age') or column.startswith('count') %}
                    {% set is_likely_numeric = True %}
                {% endif %}
                
                {% if is_date_column(column) %}
                    {% set is_likely_date = True %}
                {% endif %}
                
                {% if is_likely_numeric %}
                    {# If the column appears to be numeric, use SAFE_CAST for comparisons #}
                    {% set column_expr = 'SAFE_CAST(' ~ count_result ~ ' AS FLOAT64)' %}
                {% elif is_likely_date %}
                    {# If the column appears to be a date, make sure to handle date literals correctly #}
                    {% set column_expr = 'CAST(' ~ count_result ~ ' AS DATE)' %}
                {% endif %}
            {% endif %}
            
            {# Process comparison operators with appropriate value formatting #}
            {% if quality.mustBe is defined %}
                {% set value = quality.mustBe %}
                {% if is_likely_date %}
                    {% set value = "DATE '" ~ value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' = ' ~ value %}
            {% elif quality.mustNotBe is defined %}
                {% set value = quality.mustNotBe %}
                {% if is_likely_date %}
                    {% set value = "DATE '" ~ value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' != ' ~ value %}
            {% elif quality.mustBeGreaterThan is defined %}
                {% set value = quality.mustBeGreaterThan %}
                {% if is_likely_date %}
                    {% set value = "DATE '" ~ value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' > ' ~ value %}
            {% elif quality.mustBeGreaterOrEqualTo is defined %}
                {% set value = quality.mustBeGreaterOrEqualTo %}
                {% if is_likely_date %}
                    {% set value = "DATE '" ~ value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' >= ' ~ value %}
            {% elif quality.mustBeLessThan is defined %}
                {% set value = quality.mustBeLessThan %}
                {% if is_likely_date %}
                    {% set value = "DATE '" ~ value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' < ' ~ value %}
            {% elif quality.mustBeLessOrEqualTo is defined %}
                {% set value = quality.mustBeLessOrEqualTo %}
                {% if is_likely_date %}
                    {% set value = "DATE '" ~ value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' <= ' ~ value %}
            {% elif quality.mustBeBetween is defined and quality.mustBeBetween is iterable and quality.mustBeBetween|length == 2 %}
                {% set min_value, max_value = quality.mustBeBetween %}
                {% if is_likely_date %}
                    {% set min_value = "DATE '" ~ min_value ~ "'" %}
                    {% set max_value = "DATE '" ~ max_value ~ "'" %}
                {% endif %}
                {% set check = column_expr ~ ' BETWEEN ' ~ min_value ~ ' AND ' ~ max_value %}
            {% elif quality.mustNotBeBetween is defined and quality.mustNotBeBetween is iterable and quality.mustNotBeBetween|length == 2 %}
                {% set min_value, max_value = quality.mustNotBeBetween %}
                {% if is_likely_date %}
                    {% set min_value = "DATE '" ~ min_value ~ "'" %}
                    {% set max_value = "DATE '" ~ max_value ~ "'" %}
                {% endif %}
                {% set check = 'NOT (' ~ column_expr ~ ' BETWEEN ' ~ min_value ~ ' AND ' ~ max_value ~ ')' %}
            {% endif %}
            
            {% if check is defined %}
                {% do tests.append({
                    'check_type': 'Data Quality',
                    'table_name': table_name,
                    'column_name': quality.column | default(''),
                    'rule_name': 'custom_sql_' ~ (quality.name | default('sql_check')),
                    'description': quality.get('description', 'Custom SQL quality check'),
                    'sql_check': check,
                    'sql': count_query ~ ' LIMIT 10',
                    'sql_count': 'SELECT (CASE WHEN ' ~ check ~ ' THEN 0 ELSE 1 END) as failed_records'
                }) %}
            {% endif %}
        {# Handle custom type #}
        {% elif quality.type == 'custom' %}
            {% do log("Custom quality rule (vendor-specific): " ~ quality.engine ~ ' - ' ~ quality.implementation, info=true) %}
            {% do tests.append({
                'check_type': 'Data Quality',
                'table_name': table_name,
                'column_name': quality.column | default(''),
                'rule_name': 'custom_' ~ (quality.engine | default('vendor')) ~ '_' ~ (quality.name | default('check')),
                'description': quality.get('description', 'Vendor-specific quality check: ' ~ quality.engine),
                'sql_check': 'TRUE',
                'sql': "SELECT 'custom quality check - no sql needed' as description LIMIT 1",
                'sql_count': 'SELECT 0 as failed_records'
            }) %}
        {% endif %}
        
        {# Handle scheduling info #}
        {% if quality.scheduler is defined or quality.schedule is defined %}
            {% do log("Scheduling info: scheduler=" ~ quality.scheduler | default('none') ~ ", schedule=" ~ quality.schedule | default('none'), info=true) %}
        {% endif %}
        
        {# Log dimensions and severity #}
        {% if quality.dimension is defined %}
            {% do log("Quality dimension: " ~ quality.dimension, info=true) %}
        {% endif %}
        {% if quality.severity is defined %}
            {% do log("Quality severity: " ~ quality.severity, info=true) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}