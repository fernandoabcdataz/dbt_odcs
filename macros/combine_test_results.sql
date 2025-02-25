-- dbt_odcs/macros/combine_test_results.sql

{% macro combine_test_results(schema_tests, quality_tests, sla_tests) %}
    {% set all_tests = schema_tests + quality_tests + sla_tests %}
    
    {% if all_tests|length == 0 %}
        SELECT 
            'no_tests' as test_id,
            'No Tests' as test_type,
            'none' as table_name,
            'none' as column_name,
            'none' as rule_name,
            'No tests were generated' as description,
            'none' as sql_check,
            'none' as sql,
            'none' as sql_count,
            0 as failed_records,
            'SKIPPED' as test_status,
            CURRENT_TIMESTAMP() as execution_time
    {% else %}
        WITH test_results AS (
            {% for test in all_tests %}
                SELECT
                    CONCAT('{{ test.test_type }}', ': ', '{{ test.rule_name }}', '_', '{{ test.column_name }}') AS test_id,
                    '{{ test.test_type }}' AS test_type,
                    '{{ test.table_name }}' AS table_name,
                    '{{ test.column_name }}' AS column_name,
                    '{{ test.rule_name }}' AS rule_name,
                    {% if test.description is defined %}
                    """{{ test.description }}""" AS description,
                    {% else %}
                    'No description' AS description,
                    {% endif %}
                    {% if test.sql_check is defined %}
                    """{{ test.sql_check }}""" AS sql_check,
                    {% else %}
                    'No SQL check' AS sql_check,
                    {% endif %}
                    {% if test.sql is defined %}
                    """{{ test.sql }}""" AS sql,
                    {% else %}
                    'No SQL' AS sql,
                    {% endif %}
                    {% if test.sql_count is defined %}
                    """{{ test.sql_count }}""" AS sql_count,
                    ({{ test.sql_count }}) AS failed_records
                    {% else %}
                    'No SQL count' AS sql_count,
                    0 AS failed_records
                    {% endif %}
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