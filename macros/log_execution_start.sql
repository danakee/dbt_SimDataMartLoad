-- macros/log_execution_start.sql
-- This macro is used to insert a new row into the DBTExecutionLog table to log the start of a DBT execution.

{% macro log_execution_start() %}
    {% set insert_statement %}
        INSERT INTO {{ source('logging', 'DBTExecutionLog') }} (
            InvocationGUID,
            ExecutionGUID,
            Command,
            StartDateTime,
            UserName,
            TargetName,
            DBTVersion,
            PythonVersion,
            CompletionStatus
        )
        VALUES (
            TRY_CAST('{{ invocation_id }}' AS UNIQUEIDENTIFIER),
            NULL,
            '{{ flags.WHICH }}',
            SYSDATETIMEOFFSET(),
            SUSER_NAME(),
            '{{ target.name }}',
            '{{ dbt_version }}',
            '{{ var("python_version") }}',
            'Incomplete'
        );
    {% endset %}

    {% do log('Executing SQL: ' ~ insert_statement, info=True) %}

    {% set results = run_query(insert_statement) %}

    {% if execute %}
        {% do log('Start log entry inserted.', info=True) %}
    {% endif %}
{% endmacro %}