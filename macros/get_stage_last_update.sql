{% macro get_stage_last_update(table_name) %}
    {% set query %}
        -- Check for missing row and insert if necessary
        IF NOT EXISTS (SELECT 1 FROM [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate] WHERE TableName = '{{ table_name }}')
        BEGIN
            INSERT INTO [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate] (TableName, LastUpdated)
            VALUES ('{{ table_name }}', '1900-01-01T00:00:00.000+00:00');
        END

        -- Retrieve LastUpdated value, considering full refresh flag
        DECLARE @LastUpdated datetimeoffset(3);
        
        {% if flags.FULL_REFRESH %}
            SET @LastUpdated = '1900-01-01T00:00:00.000+00:00';
        {% else %}
            SELECT @LastUpdated = LastUpdated
            FROM [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate]
            WHERE TableName = '{{ table_name }}';
        {% endif %}

        SELECT CONVERT(VARCHAR(27), @LastUpdated, 126) AS LastUpdated;
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% else %}
        {{ return('1900-01-01T00:00:00.000+00:00') }}
    {% endif %}
{% endmacro %}