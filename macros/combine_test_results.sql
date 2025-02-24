{% macro combine_test_results(schema_tests, quality_tests, sla_tests) %}
    {% set all_tests = schema_tests + quality_tests + sla_tests %}
    
    {% if all_tests|length == 0 %}
        SELECT 
            NULL as test_id,
            NULL as test_type,
            NULL as table_name,
            NULL as column_name,
            NULL as rule_name,
            NULL as description,
            NULL as sql_check,
            NULL as sql,
            NULL as sql_count,
            NULL as failed_records,
            NULL as test_status,
            NULL as execution_time
        WHERE 1=0
    {% else %}
        WITH test_results AS (
            {% for test in all_tests %}
                SELECT
                    {{ test.test_type ~ '_' ~ test.rule_name ~ '_' ~ test.column_name | replace("'", "") }} AS test_id,
                    {{ test.test_type }} AS test_type,
                    {{ test.table_name }} AS table_name,
                    {{ test.column_name }} AS column_name,
                    {{ test.rule_name }} AS rule_name,
                    {{ test.description | replace("'", "''") }} AS description,
                    {{ test.sql_check | replace("'", "''") }} AS sql_check,
                    {{ test.sql | replace("'", "''") }} AS sql,
                    {{ test.sql_count | replace("'", "''") }} AS sql_count,
                    ({{ test.sql_count }}) AS failed_records
                    {% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
        )
        SELECT
            test_id,
            test_type,
            table_name,
            column_name,
            rule_name,
            description,
            sql_check,
            sql,
            sql_count,
            failed_records,
            CASE WHEN failed_records > 0 THEN 'FAILED' ELSE 'PASSED' END AS test_status,
            CURRENT_TIMESTAMP() AS execution_time
        FROM test_results
        ORDER BY test_type, table_name, column_name, rule_name
    {% endif %}
{% endmacro %}