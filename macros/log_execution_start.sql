-- macros/log_execution_start.sql
-- This macro is used to insert a new row into the DBTExecutionLog table to log the start of a DBT execution.

{% macro log_execution_start() %}

    -- Create the INSERT statement to log the start of the execution
    {% set insert_statement %}
        INSERT INTO {{ source('logging', 'DBTExecutionLog') }} (
            InvocationGUID,
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
            '{{ flags.WHICH }}',
            SYSDATETIMEOFFSET(),
            SUSER_NAME(),
            '{{ target.name }}',
            '{{ dbt_version }}',
            '{{ var("python_version") }}',
            'Incomplete'
        );
    {% endset %}

    -- Log the sql statement to the dbt logs and the console
    {% do log('Executing SQL: ' ~ insert_statement, info=True) %}

    -- Execute the insert statement and return the results
    {% set results = run_query(insert_statement) %}

    {% if execute %}
        {% do log('Start log entry inserted.', info=True) %}
    {% endif %}

{% endmacro %}