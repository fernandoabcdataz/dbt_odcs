
{% macro process_schema_tests(source_name, table_name, contract) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}
    
    {# Extract schema properties from contract #}
    {% set schema_obj = contract.schema[0] %}
    {% set properties = schema_obj.properties %}
    
    {# Generate schema tests #}
    {% for prop in properties %}
        {# Data type test #}
        {% set type_check = {
            'integer': 'SAFE_CAST(' ~ prop.name ~ ' AS INT64) IS NOT NULL OR ' ~ prop.name ~ ' IS NULL',
            'date': 'SAFE_CAST(' ~ prop.name ~ ' AS DATE) IS NOT NULL OR ' ~ prop.name ~ ' IS NULL',
            'string': '(' ~ prop.name ~ ' IS NULL OR LENGTH(TRIM(CAST(' ~ prop.name ~ ' AS STRING))) > 0)'
        }.get(prop.logicalType | lower, 'TRUE') %}
        
        {% do tests.append({
            'test_type': 'Schema',
            'table_name': table_name,
            'column_name': prop.name,
            'rule_name': 'data_type',
            'description': 'Checks if column ' ~ prop.name ~ ' has the correct data type (' ~ prop.logicalType ~ ')',
            'expected_type': prop.logicalType,
            'physical_type': prop.physicalType,
            'sql_check': type_check,
            'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE NOT (' ~ type_check ~ ') LIMIT 10',
            'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ type_check ~ ')'
        }) %}
        
        {# Required/Not Null test #}
        {% if prop.get('required', false) %}
            {% do tests.append({
                'test_type': 'Schema',
                'table_name': table_name,
                'column_name': prop.name,
                'rule_name': 'not_null',
                'description': 'Checks if required column ' ~ prop.name ~ ' contains null values',
                'sql_check': prop.name ~ ' IS NOT NULL',
                'sql': 'SELECT * FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL LIMIT 10',
                'sql_count': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL'
            }) %}
        {% endif %}
        
        {# Primary Key test #}
        {% if prop.get('primaryKey', false) %}
            {% do tests.append({
                'test_type': 'Schema',
                'table_name': table_name,
                'column_name': prop.name,
                'rule_name': 'unique',
                'description': 'Checks if primary key column ' ~ prop.name ~ ' contains duplicate values',
                'sql_check': 'COUNT(DISTINCT ' ~ prop.name ~ ') = COUNT(' ~ prop.name ~ ')',
                'sql': 'SELECT ' ~ prop.name ~ ', COUNT(*) as count FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1 LIMIT 10',
                'sql_count': 'SELECT COUNT(*) FROM (SELECT ' ~ prop.name ~ ', COUNT(*) FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1)'
            }) %}
        {% endif %}
    {% endfor %}
    
    {% do return(tests) %}
{% endmacro %}