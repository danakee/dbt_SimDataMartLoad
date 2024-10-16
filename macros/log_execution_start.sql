-- macros/log_execution_start.sql
-- This macro is used to insert a new row into the DBTExecutionLog table to log the start of a DBT execution.

{% macro log_execution_start() %}

    -- Fetch SQL Server specific information
    {% set sql_server_info %}
        SELECT 
            @@SERVERNAME AS TargetSQLServer,
            @@VERSION AS SQLServerVersion,
            @@MICROSOFTVERSION AS MicrosoftVersion
    {% endset %}

    {% set sql_server_results = run_query(sql_server_info) %}
    {% if execute %}
        {% set sql_server_data = sql_server_results.rows[0] %}
    {% else %}
        {% set sql_server_data = ['Unknown', 'Unknown', 'Unknown'] %}
    {% endif %}

    -- Create the INSERT statement to log the start of the execution
    {% set insert_statement %}
        INSERT INTO {{ source('logging', 'DBTExecutionLog') }} (
            InvocationGUID,
            Command,
            StartDateTime,
            UserName,
            TargetName,
            TargetSQLServer,
            SQLServerVersion,
            MicrosoftVersion,
            ClientComputerName,
            DBTVersion,
            DBTSQLServerVersion,
            PythonVersion,
            CompletionStatus
        )
        VALUES (
            TRY_CAST('{{ invocation_id }}' AS UNIQUEIDENTIFIER),
            '{{ flags.WHICH }}',
            SYSDATETIMEOFFSET(),
            SUSER_NAME(),
            '{{ target.name }}',
            '{{ sql_server_data[0] }}',
            '{{ sql_server_data[1] }}',
            '{{ sql_server_data[2] }}',
            '{{ env_var("COMPUTERNAME", "Unknown") }}',
            '{{ dbt_version }}',
            '{{ env_var("DBT_SQLSERVER_VERSION", "Unknown") }}',            
            '{{ env_var("DBT_PYTHON_VERSION", "Unknown") }}',
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