-- macros/process_data_contract.sql
{% macro process_data_contract(source_name, table_name, contract_yaml) %}
    {% set contract = fromyaml(contract_yaml) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}

    {# Schema Tests #}
    {% for prop in contract.schema[0].properties %}
        {% set type_check = {
            'integer': 'SAFE_CAST(' ~ prop.name ~ ' AS INT64) IS NOT NULL',
            'date': 'SAFE_CAST(' ~ prop.name ~ ' AS DATE) IS NOT NULL',
            'string': 'LENGTH(TRIM(' ~ prop.name ~ ')) > 0 AND ' ~ prop.name ~ ' IS NOT NULL'
        }.get(prop.physicalType | lower, 'FALSE') %}
        {% do tests.append({'category': 'Schema', 'table_name': table_name, 'column_name': prop.name, 'rule_name': 'data_type', 'expected_type': prop.physicalType, 'sql': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE NOT (' ~ type_check ~ ')'}) %}
        {% if prop.get('required', false) %}
            {% do tests.append({'category': 'Schema', 'table_name': table_name, 'column_name': prop.name, 'rule_name': 'not_null', 'sql': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL'}) %}
        {% endif %}
        {% if prop.get('primaryKey', false) %}
            {% do tests.append({'category': 'Schema', 'table_name': table_name, 'column_name': prop.name, 'rule_name': 'unique', 'sql': 'SELECT COUNT(*) FROM (SELECT ' ~ prop.name ~ ' FROM ' ~ source_ref ~ ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1)'}) %}
        {% endif %}
    {% endfor %}

    {# Quality Tests #}
    {% for quality in contract.get('quality', []) %}
        {% if quality.rule == 'duplicateCount' and quality.get('mustBeLessThan') %}
            {% set unique_columns = quality.get('unique_columns', ['report_number']) %}
            {% do tests.append({'category': 'Data Quality', 'table_name': table_name, 'rule_name': 'duplicate_count', 'threshold': quality.mustBeLessThan, 'sql': 'SELECT COUNT(*) FROM (SELECT COUNT(*) AS cnt FROM ' ~ source_ref ~ ' GROUP BY ' ~ unique_columns | join(', ') ~ ' HAVING cnt > ' ~ quality.mustBeLessThan ~ ')'}) %}
        {% endif %}
    {% endfor %}

    {# SLA Tests #}
    {% for sla in contract.get('slaProperties', []) %}
        {% if sla.property == 'frequency' %}
            {% set column_name = sla.element.split('.')[1] %}
            {% set date_column_type = sla.get('column_type', 'DATE') %}
            {% do tests.append({'category': 'Service-Level Agreement', 'table_name': table_name, 'column_name': column_name, 'rule_name': 'freshness', 'interval': sla.value, 'sql': 'SELECT CASE WHEN MAX(CAST(' ~ column_name ~ ' AS ' ~ date_column_type ~ ')) < CURRENT_DATE - ' ~ sla.value ~ ' THEN 1 ELSE 0 END FROM ' ~ source_ref}) %}
        {% endif %}
    {% endfor %}

    {# Execute Tests #}
    WITH test_results AS (
        {% for test in tests %}
            SELECT
                '{{ test.category }}' AS test_type,
                '{{ test.table_name }}' AS table_name,
                {% if test.column_name is defined %}'{{ test.column_name }}'{% else %}NULL{% endif %} AS column_name,
                '{{ test.rule_name }}' AS rule_name,
                ({{ test.sql }}) AS failed_records,
                {% if test.expected_type is defined %}'{{ test.expected_type }}'{% else %}NULL{% endif %} AS expected_type,
                {% if test.threshold is defined %}{{ test.threshold }}{% else %}NULL{% endif %} AS threshold,
                {% if test.interval is defined %}{{ test.interval }}{% else %}NULL{% endif %} AS freshness_days
            {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
    )
    SELECT
        table_name,
        column_name,
        test_type,
        rule_name,
        failed_records,
        expected_type,
        threshold,
        freshness_days,
        CASE WHEN failed_records > 0 THEN 'FAILED' ELSE 'PASSED' END AS test_status
    FROM test_results
    ORDER BY column_name
{% endmacro %}