-- dbt_odcs/macros/process_schema_tests.sql
{% macro process_schema_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# extract schema properties from contract #}
    {% set schema_obj = contract.schema[0] %}
    {% set properties = schema_obj.properties %}
    
    {# generate schema tests #}
    {% for prop in properties %}
        {# handle arrays #}
        {% if prop.logicalType | lower == 'array' and prop.items is defined %}
            {% set items_type = prop.items.logicalType | lower %}
            {% set array_check = 'ARRAY_LENGTH(' ~ prop.name ~ ') IS NOT NULL' %}
            {% if items_type in ['string', 'integer', 'date'] %}
                {% set item_check = {
                    'string': '(' ~ prop.name ~ ' IS NULL OR EVERY(x -> LENGTH(TRIM(CAST(x AS STRING))) > 0, UNNEST(' ~ prop.name ~ ')))',
                    'integer': 'EVERY(x -> SAFE_CAST(x AS INT64) IS NOT NULL, UNNEST(' ~ prop.name ~ '))',
                    'date': 'EVERY(x -> SAFE_CAST(x AS DATE) IS NOT NULL, UNNEST(' ~ prop.name ~ '))'
                }.get(items_type, 'TRUE') %}
                {% set array_check = array_check ~ ' AND ' ~ item_check %}
            {% endif %}
            {% do tests.append({
                'test_type': 'Schema',
                'table_name': table_name,
                'column_name': prop.name,
                'rule_name': 'array_structure',
                'description': 'checks if column ' ~ prop.name ~ ' is a valid array with ' ~ items_type ~ ' items',
                'sql_check': array_check,
                'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE NOT (' ~ array_check ~ ') LIMIT 10',
                'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ array_check ~ ')'
            }) %}
        {% else %}
            {# data type test #}
            {% set type_check = {
                'integer': 'SAFE_CAST(' ~ prop.name ~ ' AS INT64) IS NOT NULL OR ' ~ prop.name ~ ' IS NULL',
                'date': 'SAFE_CAST(' ~ prop.name ~ ' AS DATE) IS NOT NULL OR ' ~ prop.name ~ ' IS NULL',
                'string': '(' ~ prop.name ~ ' IS NULL OR LENGTH(TRIM(CAST(' ~ prop.name ~ ' AS STRING))) > 0)',
                'number': 'SAFE_CAST(' ~ prop.name ~ ' AS FLOAT64) IS NOT NULL OR ' ~ prop.name ~ ' IS NULL',
                'boolean': 'CAST(' ~ prop.name ~ ' AS BOOLEAN) IS NOT NULL OR ' ~ prop.name ~ ' IS NULL'
            }.get(prop.logicalType | lower, 'TRUE') %}
            
            {% do tests.append({
                'test_type': 'Schema',
                'table_name': table_name,
                'column_name': prop.name,
                'rule_name': 'data_type',
                'description': 'checks if column ' ~ prop.name ~ ' has the correct data type (' ~ prop.logicalType ~ ')',
                'expected_type': prop.logicalType,
                'physical_type': prop.physicalType,
                'sql_check': type_check,
                'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE NOT (' ~ type_check ~ ') LIMIT 10',
                'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ type_check ~ ')'
            }) %}
            
            {# required/not null test #}
            {% if prop.get('required', false) %}
                {% do tests.append({
                    'test_type': 'Schema',
                    'table_name': table_name,
                    'column_name': prop.name,
                    'rule_name': 'not_null',
                    'description': 'checks if required column ' ~ prop.name ~ ' contains null values',
                    'sql_check': prop.name ~ ' IS NOT NULL',
                    'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL LIMIT 10',
                    'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL'
                }) %}
            {% endif %}
            
            {# unique test #}
            {% if prop.get('unique', false) %}
                {% do tests.append({
                    'test_type': 'Schema',
                    'table_name': table_name,
                    'column_name': prop.name,
                    'rule_name': 'unique',
                    'description': 'checks if column ' ~ prop.name ~ ' contains unique values',
                    'sql_check': 'COUNT(DISTINCT ' ~ prop.name ~ ') = COUNT(' ~ prop.name ~ ')',
                    'sql': 'SELECT ' ~ prop.name ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                    'sql_count': 'SELECT COUNT(*) FROM (SELECT ' ~ prop.name ~ ', COUNT(*) FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1)'
                }) %}
            {% endif %}
            
            {# primary key test #}
            {% if prop.get('primaryKey', false) %}
                {% do tests.append({
                    'test_type': 'Schema',
                    'table_name': table_name,
                    'column_name': prop.name,
                    'rule_name': 'unique',
                    'description': 'checks if primary key column ' ~ prop.name ~ ' contains duplicate values',
                    'sql_check': 'COUNT(DISTINCT ' ~ prop.name ~ ') = COUNT(' ~ prop.name ~ ')',
                    'sql': 'SELECT ' ~ prop.name ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                    'sql_count': 'SELECT COUNT(*) FROM (SELECT ' ~ prop.name ~ ', COUNT(*) FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1)'
                }) %}
            {% endif %}
            
            {# logical type options #}
            {% if prop.logicalTypeOptions is defined %}
                {% if prop.logicalTypeOptions.maximum is defined %}
                    {% set max_check = prop.name ~ ' <= ' ~ prop.logicalTypeOptions.maximum %}
                    {% do tests.append({
                        'test_type': 'Schema',
                        'table_name': table_name,
                        'column_name': prop.name,
                        'rule_name': 'maximum_value',
                        'description': 'checks if column ' ~ prop.name ~ ' is less than or equal to ' ~ prop.logicalTypeOptions.maximum,
                        'sql_check': max_check,
                        'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE NOT (' ~ max_check ~ ') LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ max_check ~ ')'
                    }) %}
                {% endif %}
                {% if prop.logicalTypeOptions.minimum is defined %}
                    {% set min_check = prop.name ~ ' >= ' ~ prop.logicalTypeOptions.minimum %}
                    {% do tests.append({
                        'test_type': 'Schema',
                        'table_name': table_name,
                        'column_name': prop.name,
                        'rule_name': 'minimum_value',
                        'description': 'checks if column ' ~ prop.name ~ ' is greater than or equal to ' ~ prop.logicalTypeOptions.minimum,
                        'sql_check': min_check,
                        'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE NOT (' ~ min_check ~ ') LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ min_check ~ ')'
                    }) %}
                {% endif %}
                {% if prop.logicalTypeOptions.pattern is defined %}
                    {% set pattern_check = 'REGEXP_CONTAINS(' ~ prop.name ~ ', "' ~ prop.logicalTypeOptions.pattern ~ '")' %}
                    {% do tests.append({
                        'test_type': 'Schema',
                        'table_name': table_name,
                        'column_name': prop.name,
                        'rule_name': 'pattern_match',
                        'description': 'checks if column ' ~ prop.name ~ ' matches pattern ' ~ prop.logicalTypeOptions.pattern,
                        'sql_check': prop.name ~ ' IS NULL OR ' ~ pattern_check,
                        'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NOT NULL AND NOT (' ~ pattern_check ~ ') LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NOT NULL AND NOT (' ~ pattern_check ~ ')'
                    }) %}
                {% endif %}
                {% if prop.logicalTypeOptions.format is defined %}
                    {% set format_check = 'TRUE' %}  {# Placeholder; implement specific format checks as needed #}
                    {% do tests.append({
                        'test_type': 'Schema',
                        'table_name': table_name,
                        'column_name': prop.name,
                        'rule_name': 'format_check',
                        'description': 'checks if column ' ~ prop.name ~ ' matches format ' ~ prop.logicalTypeOptions.format,
                        'sql_check': format_check,
                        'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE NOT (' ~ format_check ~ ') LIMIT 10',
                        'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ format_check ~ ')'
                    }) %}
                {% endif %}
            {% endif %}
        {% endif %}
        
        {# log or skip unused schema fields #}
        {% if prop.physicalName is defined %}
            {% do log("schema physical name: " ~ prop.physicalName, info=true) %}
        {% endif %}
        {% if prop.description is defined %}
            {% do log("schema description: " ~ prop.description, info=true) %}
        {% endif %}
        {% if prop.businessName is defined %}
            {% do log("schema business name: " ~ prop.businessName, info=true) %}
        {% endif %}
        {% if prop.tags is defined %}
            {% do log("schema tags: " ~ prop.tags, info=true) %}
        {% endif %}
        {% if prop.classification is defined %}
            {% do log("schema classification: " ~ prop.classification, info=true) %}
        {% endif %}
        {% if prop.transformLogic is defined %}
            {% do log("schema transform logic: " ~ prop.transformLogic, info=true) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}