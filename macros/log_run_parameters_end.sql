{% macro log_run_parameters_end() %}
    {% if execute %}
        {% call statement('get_current_datetime', fetch_result=True) %}
            SELECT CONVERT(VARCHAR(50), SYSDATETIMEOFFSET(), 127) AS current_datetime;
        {% endcall %}
        
        {% set run_completed_at = load_result('get_current_datetime')['data'][0][0] %}
        
        {{ log("Debug - Run Completed At: " ~ run_completed_at, info=True) }}
        
        {% call statement('insert_run_end_parameters') %}
            DECLARE @run_completed_at DATETIME2(3) = '{{ run_completed_at }}';
            DECLARE @timezone_offset VARCHAR(6);

            SET @timezone_offset = FORMAT(SYSDATETIMEOFFSET(), 'zzz');

            INSERT INTO [SimulationsAnalyticsLogging].[dbo].[DBTExecutionParameterLog] (
                InvocationGUID, 
                ParameterName, 
                ParameterValue, 
                LogDateTime
            )
            VALUES 
            (
                '{{ invocation_id }}',
                'RUN_COMPLETED_AT',
                --'{{ run_completed_at }}',
                CONCAT(FORMAT(@run_completed_at, 'yyyy-MM-dd HH:mm:ss.fff'), ' ', @timezone_offset),
                SYSDATETIMEOFFSET()
            );
        {% endcall %}
    {% endif %}
{% endmacro %}