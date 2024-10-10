{% macro log_run_parameters() %}
    {% set run_started_at_str = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}
    {% set command_line = env_var('DBT_COMMAND_LINE', '') %}

    {% if command_line == '' %}
        {{ exceptions.raise_compiler_error("DBT_COMMAND_LINE environment variable is not set. Please set this variable before running dbt.") }}
    {% endif %}

    {{ log("Debug - Command Line from env: " ~ command_line, info=True) }}
    
    {% call statement('insert_run_parameters') %}
        INSERT INTO [SimulationsAnalyticsLogging].[dbo].[DBTExecutionParameterLog] (
            InvocationGUID, 
            ParameterName, 
            ParameterValue, 
            StartDateTime
        )
        VALUES 
        (
            '{{ invocation_id }}',
            'COMMAND_LINE',
            '{{ command_line | replace("''", "''''") }}',
            '{{ run_started_at_str }}'
        );
    {% endcall %}
{% endmacro %}