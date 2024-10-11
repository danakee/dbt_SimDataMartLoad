{% macro get_stage_last_update(table_name) %}
    {% if flags.FULL_REFRESH %}
        {{ return('1900-01-01T00:00:00.000+00:00') }}
    {% else %}
        {% set query %}
            SELECT 
                CONVERT(VARCHAR(27), LastUpdated, 126)
            FROM 
                [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate]
            WHERE 
                TableName = '{{ table_name }}'
        {% endset %}

        {% set results = run_query(query) %}

        {% if execute %}
            {% set last_updated = results.columns[0].values()[0] %}
            {% if last_updated is none or last_updated == '' %}
                {% set last_updated = '1900-01-01T00:00:00.000+00:00' %}
            {% endif %}
            {{ return(last_updated) }}
        {% else %}
            {{ return('1900-01-01T00:00:00.000+00:00') }}
        {% endif %}
    {% endif %}
{% endmacro %}