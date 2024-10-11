{% macro load_stage_table(model_name, source_tables, unique_key) %}

    {%- set target_relation = this -%}
    {%- set target_table = target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.name -%}
    {%- set process_name = "'" ~ model_name ~ "'" -%}
    {%- set source_table_list = "'" ~ source_tables|join(", ") ~ "'" -%}

    -- Declare variables
    DECLARE @process_guid UNIQUEIDENTIFIER = NEWID();
    DECLARE @affected_rows INT;

    -- Log process start
    INSERT INTO {{ source('logging', 'DBTProcessExecutionLog') }} (
        ProcessGUID, ProcessName, SourceTable, TargetTable, ExecutionStatus, ProcessStartTime)
    VALUES (
        @process_guid, {{ process_name }}, {{ source_table_list }}, '{{ target_table }}', 'Started', SYSDATETIMEOFFSET()
    );

    -- Truncate the target table
    TRUNCATE TABLE {{ target_relation }};

    -- Insert the data
    {{ sql }};

    -- Get the number of affected rows
    SET @affected_rows = @@ROWCOUNT;

    -- Update last update time
    UPDATE 
        {{ source('logging', 'StageTableLastUpdate') }}
    SET 
        LastUpdated = SYSDATETIMEOFFSET()
    WHERE 
        TableName = '{{ model_name }}';

    -- Log process end and row count
    UPDATE 
        {{ source('logging', 'DBTProcessExecutionLog') }}
    SET 
        ExecutionStatus = 'Completed',
        ProcessEndTime = SYSDATETIMEOFFSET(),
        RowsAffected = @affected_rows
    WHERE 
        ProcessGUID = @process_guid;

{% endmacro %}
