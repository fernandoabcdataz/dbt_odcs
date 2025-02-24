-- dbt_odcs/macros/process_quality_tests.sql
{% macro process_quality_tests(source_name, table_name, contract, api_version, contract_id, contract_name, contract_version, contract_status) %}
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
                        'description': quality.get('description', 'ensures unique combination of columns: ' ~ columns_str),
                        'api_version': api_version,
                        'contract_id': contract_id,
                        'contract_name': contract_name,
                        'contract_version': contract_version,
                        'contract_status': contract_status,
                        'sql_check': 'count(*) = count(distinct ' ~ columns_str ~ ')',
                        'sql': 'select ' ~ columns_str ~ ', count(*) as count from ' ~ source_ref ~ ' group by ' ~ columns_str ~ ' having count(*) > 1 limit 10',
                        'sql_count': 'select count(*) from (select ' ~ columns_str ~ ', count(*) from ' ~ source_ref ~ ' group by ' ~ columns_str ~ ' having count(*) > 1)'
                    }) %}
                {% endif %}
            {# duplicate count test (rows or %) #}
            {% elif quality.rule == 'duplicateCount' %}
                {% set column = quality.column | default('') %}
                {% if column %}
                    {% set duplicates_query = 'select count(*) - count(distinct ' ~ column ~ ') as duplicate_count from ' ~ source_ref %}
                    {% if quality.unit == 'percent' %}
                        {% set total_query = 'select count(*) as total from ' ~ source_ref %}
                        {% set check = '(select duplicate_count * 100.0 / total from (' ~ duplicates_query ~ ') as dup, (' ~ total_query ~ ') as tot) <= ' ~ quality.mustBeLessThan %}
                    {% else %}
                        {% set check = '(select duplicate_count from (' ~ duplicates_query ~ ')) <= ' ~ quality.mustBeLessThan %}
                    {% endif %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': column,
                        'rule_name': 'duplicate_count',
                        'description': quality.get('description', 'ensures duplicates are within limit: ' ~ quality.mustBeLessThan ~ ' ' ~ quality.unit),
                        'api_version': api_version,
                        'contract_id': contract_id,
                        'contract_name': contract_name,
                        'contract_version': contract_version,
                        'contract_status': contract_status,
                        'sql_check': check,
                        'sql': 'select ' ~ column ~ ', count(*) as count from ' ~ source_ref ~ ' group by ' ~ column ~ ' having count(*) > 1 limit 10',
                        'sql_count': 'select case when ' ~ check ~ ' then 0 else 1 end'
                    }) %}
                {% endif %}
            {# row count test (object-level) #}
            {% elif quality.rule == 'rowCount' %}
                {% set total_query = 'select count(*) as row_count from ' ~ source_ref %}
                {% if quality.mustBeBetween is defined and quality.mustBeBetween is iterable and quality.mustBeBetween|length == 2 %}
                    {% set check = total_query ~ ' between ' ~ quality.mustBeBetween[0] ~ ' and ' ~ quality.mustBeBetween[1] %}
                {% endif %}
                {% if check is defined %}
                    {% do tests.append({
                        'test_type': 'Data Quality',
                        'table_name': table_name,
                        'column_name': '',
                        'rule_name': 'row_count',
                        'description': quality.get('description', 'ensures row count is between ' ~ quality.mustBeBetween[0] ~ ' and ' ~ quality.mustBeBetween[1]),
                        'api_version': api_version,
                        'contract_id': contract_id,
                        'contract_name': contract_name,
                        'contract_version': contract_version,
                        'contract_status': contract_status,
                        'sql_check': check,
                        'sql': total_query ~ ' limit 10',
                        'sql_count': 'select case when ' ~ check ~ ' then 0 else 1 end'
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
                        'description': quality.get('description', 'ensures ' ~ column ~ ' contains only allowed values'),
                        'api_version': api_version,
                        'contract_id': contract_id,
                        'contract_name': contract_name,
                        'contract_version': contract_version,
                        'contract_status': contract_status,
                        'allowed_values': allowed_values,
                        'sql_check': column ~ ' is null or ' ~ column ~ ' in (' ~ values_str ~ ')',
                        'sql': 'select distinct ' ~ column ~ ' from ' ~ source_ref ~ ' where ' ~ column ~ ' is not null and ' ~ column ~ ' not in (' ~ values_str ~ ') limit 10',
                        'sql_count': 'select count(*) from ' ~ source_ref ~ ' where ' ~ column ~ ' is not null and ' ~ column ~ ' not in (' ~ values_str ~ ')'
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
                        'description': quality.get('description', 'ensures ' ~ column ~ ' is not null when ' ~ condition),
                        'api_version': api_version,
                        'contract_id': contract_id,
                        'contract_name': contract_name,
                        'contract_version': contract_version,
                        'contract_status': contract_status,
                        'sql_check': 'not (' ~ condition ~ ') or ' ~ column ~ ' is not null',
                        'sql': 'select * from ' ~ source_ref ~ ' where ' ~ condition ~ ' and ' ~ column ~ ' is null limit 10',
                        'sql_count': 'select count(*) from ' ~ source_ref ~ ' where ' ~ condition ~ ' and ' ~ column ~ ' is null'
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
                        'description': quality.get('description', 'ensures ' ~ column ~ ' is never null'),
                        'api_version': api_version,
                        'contract_id': contract_id,
                        'contract_name': contract_name,
                        'contract_version': contract_version,
                        'contract_status': contract_status,
                        'sql_check': column ~ ' is not null',
                        'sql': 'select * from ' ~ source_ref ~ ' where ' ~ column ~ ' is null limit 10',
                        'sql_count': 'select count(*) from ' ~ source_ref ~ ' where ' ~ column ~ ' is null'
                    }) %}
                {% endif %}
            {% endif %}
        {# handle text type #}
        {% elif quality.type == 'text' %}
            {% do log("text quality rule: " ~ quality.description, info=true) %}
            {% do tests.append({
                'test_type': 'Data Quality',
                'table_name': table_name,
                'column_name': '',
                'rule_name': 'text_description',
                'description': quality.get('description', 'text quality description'),
                'api_version': api_version,
                'contract_id': contract_id,
                'contract_name': contract_name,
                'contract_version': contract_version,
                'contract_status': contract_status,
                'sql_check': 'true',
                'sql': 'select \'text quality check - no sql needed\' as description limit 1',
                'sql_count': 'select 0 as failed_records'
            }) %}
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
                {% set check = query ~ ' between ' ~ quality.mustBeBetween[0] ~ ' and ' ~ quality.mustBeBetween[1] %}
            {% elif quality.mustNotBeBetween is defined and quality.mustNotBeBetween is iterable and quality.mustNotBeBetween|length == 2 %}
                {% set check = 'not (' ~ query ~ ' between ' ~ quality.mustNotBeBetween[0] ~ ' and ' ~ quality.mustNotBeBetween[1] ~ ')' %}
            {% endif %}
            {% if check is defined %}
                {% do tests.append({
                    'test_type': 'Data Quality',
                    'table_name': table_name,
                    'column_name': quality.column | default(''),
                    'rule_name': 'custom_sql_' ~ (quality.name | default('sql_check')),
                    'description': quality.get('description', 'custom sql quality check'),
                    'api_version': api_version,
                    'contract_id': contract_id,
                    'contract_name': contract_name,
                    'contract_version': contract_version,
                    'contract_status': contract_status,
                    'sql_check': check,
                    'sql': query ~ ' limit 10',
                    'sql_count': 'select case when (' ~ check ~ ') then 0 else 1 end'
                }) %}
            {% endif %}
        {# handle custom type #}
        {% elif quality.type == 'custom' %}
            {% do log("custom quality rule (vendor-specific): " ~ quality.engine ~ ' - ' ~ quality.implementation, info=true) %}
            {% do tests.append({
                'test_type': 'Data Quality',
                'table_name': table_name,
                'column_name': quality.column | default(''),
                'rule_name': 'custom_' ~ (quality.engine | default('vendor')) ~ '_' ~ (quality.name | default('check')),
                'description': quality.get('description', 'vendor-specific quality check: ' ~ quality.engine),
                'api_version': api_version,
                'contract_id': contract_id,
                'contract_name': contract_name,
                'contract_version': contract_version,
                'contract_status': contract_status,
                'sql_check': 'true',
                'sql': 'select \'custom quality check - no sql needed\' as description limit 1',
                'sql_count': 'select 0 as failed_records'
            }) %}
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