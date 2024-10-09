-- This macro is used to insert a new row into the DBTExecutionLog table to log the start of a DBT execution.
-- The macro returns the ExecutionGUID of the new row.

{% macro log_execution_start() %}
    {% set dbt_execution_guid = get_execution_guid() %}
    {% do log('Generated execution_guid for INSERT: ' ~ dbt_execution_guid, info=True) %}

    {% set insert_statement %}
        DECLARE @ExecutionGUID UNIQUEIDENTIFIER = TRY_CAST('{{ dbt_execution_guid }}' AS UNIQUEIDENTIFIER);
        
        IF @ExecutionGUID IS NULL
        BEGIN
            SET @ExecutionGUID = NEWID();
            PRINT 'Invalid GUID format. Generated new GUID: ' + CAST(@ExecutionGUID AS NVARCHAR(36));
        END

        INSERT INTO {{ source('logging', 'DBTExecutionLog') }} (
            ExecutionGUID,
            Command,
            CommandLineParams,
            StartDateTime,
            UserName,
            TargetName,
            DBTVersion,
            PythonVersion,
            CompletionStatus
        )
        OUTPUT
            INSERTED.ExecutionGUID
        VALUES (
            @ExecutionGUID,
            '{{ flags.WHICH }}',
            NULL,
            SYSDATETIMEOFFSET(),
            SUSER_NAME(),
            '{{ target.name }}',
            '{{ dbt_version }}',
            '{{ var("python_version") }}',
            'Incomplete'
        );
    {% endset %}

    {% set result = run_query(insert_statement) %}

    {% if result and result.rows | length > 0 %}
        {% set inserted_guid = result.rows[0][0] %}
        {% do log('Inserted start log entry. ExecutionGUID: ' ~ inserted_guid, info=True) %}
        {{ return(inserted_guid) }}
    {% else %}
        {% do log('Failed to insert start log entry.', info=True) %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}