{% macro process_data_contract(source_name, table_name, contract_yaml) %}
    {% set contract = fromyaml(contract_yaml) %}
    {% set source_ref = source(source_name, table_name) %}
    {% set tests = [] %}

    -- Schema Tests
    {% for prop in contract.schema[0].properties %}
        {% if prop.get('required', false) %}
            {% do tests.append({
                'category': 'Schema',
                'table_name': table_name,
                'column_name': prop.name,
                'rule_name': 'not_null',
                'sql': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL'
            }) %}
        {% endif %}
        {% if prop.get('primaryKey', false) %}
            {% do tests.append({
                'category': 'Schema',
                'table_name': table_name,
                'column_name': prop.name,
                'rule_name': 'unique',
                'sql': 'SELECT COUNT(*) FROM (SELECT ' ~ prop.name ~ ' FROM ' ~ source_ref ~ 
                       ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1)'
            }) %}
        {% endif %}
    {% endfor %}

    -- Quality Tests
    {% for quality in contract.get('quality', []) %}
        {% if quality.rule == 'duplicateCount' and quality.get('mustBeLessThan') %}
            {% do tests.append({
                'category': 'Data Quality',
                'table_name': table_name,
                'rule_name': 'duplicate_count',
                'threshold': quality.mustBeLessThan,
                'sql': 'SELECT COUNT(*) FROM (SELECT COUNT(*) AS cnt FROM ' ~ source_ref ~ ' AS t GROUP BY TO_JSON_STRING(t) HAVING cnt > ' ~ quality.mustBeLessThan ~ ')'
            }) %}
        {% endif %}
    {% endfor %}

    -- SLA Tests
    {% for sla in contract.get('slaProperties', []) %}
        {% if sla.property == 'frequency' %}
            {% set column_name = sla.element.split('.')[1] %}
            {% do tests.append({
                'category': 'Service-Level Agreement',
                'table_name': table_name,
                'column_name': column_name,
                'rule_name': 'freshness',
                'interval': sla.value,
                'sql': 'SELECT CASE WHEN MAX(' ~ column_name ~ ') < CURRENT_DATE - ' ~ sla.value ~ ' THEN 1 ELSE 0 END FROM ' ~ source_ref
            }) %}
        {% endif %}
    {% endfor %}

    -- Execute Tests
    WITH test_results AS (
        {% for test in tests %}
            SELECT
                '{{ test.category }}' AS test_type,
                '{{ test.table_name }}' AS table_name,
                {% if test.column_name is defined %}'{{ test.column_name }}'{% else %}NULL{% endif %} AS column_name,
                '{{ test.rule_name }}' AS rule_name,
                ({{ test.sql }}) AS failed_records,
                {% if test.threshold is defined %}{{ test.threshold }}{% else %}NULL{% endif %} AS threshold,
                {% if test.interval is defined %}{{ test.interval }}{% else %}NULL{% endif %} AS freshness_days
            {% if not loop.last %} UNION ALL {% endif %}
        {% endfor %}
    )
    SELECT
        ROW_NUMBER() OVER () AS test_id,
        test_type,
        table_name,
        column_name,
        rule_name,
        failed_records,
        threshold,
        freshness_days,
        CASE WHEN failed_records > 0 THEN 'FAILED' ELSE 'PASSED' END AS test_status
    FROM test_results
{% endmacro %}