{% macro process_data_contract(contract_file, source_name=none, table_name=none) %}
    {% if execute %}
        {{ log("Attempting to read contract file: " ~ contract_file, info=True) }}
        
        {% set f = open(contract_file) %}
        {% set contract_content = f.read() %}
        {% do f.close() %}
        
        {{ log("Contract content loaded: " ~ contract_content, info=True) }}
        {% set contract = fromyaml(contract_content) %}
        {{ log("Contract parsed: " ~ contract, info=True) }}
        
        {% set tests = [] %}
        {% set source_ref = source(source_name, table_name) %}

        -- Process Schema Tests
        {% for prop in contract.schema[0].properties %}
            {% if prop.get('required', false) %}
                {% do tests.append({
                    'table_ref': source_ref,
                    'column_name': prop.name,
                    'test_type': 'not_null',
                    'sql': 'SELECT COUNT(*) FROM ' ~ source_ref ~ ' WHERE ' ~ prop.name ~ ' IS NULL'
                }) %}
            {% endif %}
            
            {% if prop.get('primaryKey', false) %}
                {% do tests.append({
                    'table_ref': source_ref,
                    'column_name': prop.name,
                    'test_type': 'unique',
                    'sql': 'SELECT COUNT(*) FROM (SELECT ' ~ prop.name ~ ' FROM ' ~ source_ref ~ 
                           ' GROUP BY ' ~ prop.name ~ ' HAVING COUNT(*) > 1)'
                }) %}
            {% endif %}
        {% endfor %}

        -- Process Quality Tests
        {% for quality in contract.get('quality', []) %}
            {% if quality.rule == 'duplicateCount' and quality.get('mustBeLessThan') %}
                {% do tests.append({
                    'table_ref': source_ref,
                    'test_type': 'duplicate_count',
                    'threshold': quality.mustBeLessThan,
                    'sql': 'SELECT COUNT(*) FROM (SELECT COUNT(*) as cnt FROM ' ~ source_ref ~ 
                           ' GROUP BY TO_JSON_STRING(t) HAVING cnt > ' ~ quality.mustBeLessThan ~ ')'
                }) %}
            {% endif %}
        {% endfor %}

        -- Process SLA Tests
        {% for sla in contract.get('slaProperties', []) %}
            {% if sla.property == 'frequency' %}
                {% set column_name = sla.element.split('.')[1] %}
                {% do tests.append({
                    'table_ref': source_ref,
                    'column_name': column_name,
                    'test_type': 'freshness',
                    'freshness_value': sla.value,
                    'sql': 'SELECT CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(' ~ column_name ~ '), DAY) > ' ~ sla.value ~ 
                           ' THEN 1 ELSE 0 END FROM ' ~ source_ref
                }) %}
            {% endif %}
        {% endfor %}

        -- Generate Results
        {% if tests|length > 0 %}
            with test_results as (
                {% for test in tests %}
                    select
                        '{{ test.table_ref }}' as table_name,
                        {% if test.get('column_name') %}
                        '{{ test.column_name }}' as column_name,
                        {% else %}
                        null as column_name,
                        {% endif %}
                        '{{ test.test_type }}' as test_type,
                        {{ test.sql }} as failed_records
                        {% if test.get('threshold') %}
                        , {{ test.threshold }} as threshold
                        {% endif %}
                        {% if test.get('freshness_value') %}
                        , {{ test.freshness_value }} as freshness_days
                        {% endif %}
                    {% if not loop.last %}
                    union all
                    {% endif %}
                {% endfor %}
            )

            select 
                table_name,
                column_name,
                test_type,
                failed_records,
                case when test_type = 'duplicate_count' then threshold else null end as threshold,
                case when test_type = 'freshness' then freshness_days else null end as freshness_days,
                case when failed_records > 0 then 'FAILED' else 'PASSED' end as test_status
            from test_results
            order by test_status desc, test_type, column_name
        {% else %}
            -- Return empty result set if no tests
            select 
                cast(null as string) as table_name,
                cast(null as string) as column_name,
                cast(null as string) as test_type,
                cast(null as int64) as failed_records,
                cast(null as int64) as threshold,
                cast(null as int64) as freshness_days,
                cast(null as string) as test_status
            where false
        {% endif %}
    {% endif %}
{% endmacro %}