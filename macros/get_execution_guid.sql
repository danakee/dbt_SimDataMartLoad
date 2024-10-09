-- macros/get_execution_guid.sql
{% macro get_execution_guid() %}
    {% if execute %}
        {% set timestamp_str = run_started_at.isoformat() %}
        {% set timestamp_str = timestamp_str.replace("'", "''") %}
        {% set sql %}
            SELECT CAST(
                HASHBYTES('SHA2_256', '{{ timestamp_str }}') AS UNIQUEIDENTIFIER
            ) AS ExecutionGUID
        {% endset %}
        {% set results = run_query(sql) %}
        {% if results and results.rows | length > 0 %}
            {% set execution_guid = results.columns[0].values()[0] %}
            {{ return(execution_guid) }}
        {% else %}
            {{ exceptions.raise_compiler_error("Failed to generate ExecutionGUID.") }}
        {% endif %}
    {% else %}
        {{ return('00000000-0000-0000-0000-000000000000') }}
    {% endif %}
{% endmacro %}
