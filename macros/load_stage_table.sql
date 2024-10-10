-- macros/load_stage_table.sql

{% macro load_stage_table(model_name, source_tables, unique_key) %}
    {%- set target_relation = this -%}
    {%- set target_table = target_relation.database ~ '.' ~ target_relation.schema ~ '.' ~ target_relation.name -%}
    {%- set process_guid = "'" ~ uuid.uuid4() ~ "'" -%}
    {%- set process_name = "'" ~ model_name ~ "'" -%}
    {%- set source_table_list = "'" ~ source_tables|join(", ") ~ "'" -%}

    -- Log process start
    INSERT INTO {{ source('logging', 'StageProcessExecutionLog') }}
    (ProcessGUID, ProcessName, SourceTable, TargetTable, ExecutionStatus, ProcessStartTime)
    VALUES ({{ process_guid }}, {{ process_name }}, {{ source_table_list }}, '{{ target_table }}', 'Started', SYSDATETIMEOFFSET());

    -- Perform the merge operation
    MERGE INTO {{ target_relation }} AS target
    USING (
        {{ sql }}
    ) AS source
    ON target.{{ unique_key }} = source.{{ unique_key }}
    WHEN MATCHED THEN
        UPDATE SET
            {% for column in source.columns %}
                {% if column != unique_key %}
                    target.{{ column }} = source.{{ column }}{% if not loop.last %},{% endif %}
                {% endif %}
            {% endfor %}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            {% for column in source.columns %}
                {{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        )
        VALUES (
            {% for column in source.columns %}
                source.{{ column }}{% if not loop.last %},{% endif %}
            {% endfor %}
        );

    -- Get the number of affected rows
    {%- set affected_rows = run_query('SELECT @@ROWCOUNT AS affected_rows').columns[0].values()[0] -%}

    -- Log row count
    {%- set final_count_query -%}
    SELECT COUNT(*) AS row_count FROM {{ target_relation }}
    {%- endset -%}
    {%- set results = run_query(final_count_query) -%}
    {%- set row_count = results.columns['row_count'].values()[0] -%}

    INSERT INTO {{ source('logging', 'StageTableRowCounts') }} 
    (TableName, RowCount, LoggedAt) 
    VALUES ('{{ model_name }}', {{ row_count }}, SYSDATETIMEOFFSET());

    -- Update last update time
    UPDATE {{ source('logging', 'StageTableLastUpdate') }}
    SET LastUpdated = SYSDATETIMEOFFSET()
    WHERE TableName = '{{ model_name }}';

    -- Log process end
    UPDATE {{ source('logging', 'StageProcessExecutionLog') }}
    SET 
        ExecutionStatus = 'Completed',
        ProcessEndTime = SYSDATETIMEOFFSET(),
        RowsAffected = {{ affected_rows }}
    WHERE ProcessGUID = {{ process_guid }};

{% endmacro %}