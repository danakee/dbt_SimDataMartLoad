{% macro load_stage_table(target_table, source_query, source_tables, unique_key) %}
    /*
    This macro is used to truncate and load data from a source query into a target table.
    It also logs the process in the DBTProcessExecutionLog table.
    */
    
    -- Get the last update timestamp
    {% set last_updated = get_stage_last_update(target_table.name) %}

    -- Setup logging variables
    DECLARE @BracketedTargetTable nvarchar(1024) = 
        QUOTENAME(PARSENAME('{{ target_table }}', 3)) + '.' + 
        QUOTENAME(PARSENAME('{{ target_table }}', 2)) + '.' + 
        QUOTENAME(PARSENAME('{{ target_table }}', 1));

    DECLARE @ProcessGUID uniqueidentifier = NEWID();
    DECLARE @ProcessName nvarchar(1024) = '{{ this.name }}';    
    DECLARE @ExecutionStatus nvarchar(20) = 'Incomplete';
    DECLARE @ExecutionMessage nvarchar(4000) = 'Process not complete';
    DECLARE @RowsInserted int = 0;
    DECLARE @ProcessStartTime datetimeoffset = sysdatetimeoffset();
    DECLARE @InitialRowCount int;
    DECLARE @FinalRowCount int;
    DECLARE @ErrorNumber int = NULL;
    DECLARE @ErrorSeverity int = NULL;
    DECLARE @ErrorState int = NULL;
    DECLARE @LoggingErrorMessage nvarchar(4000);
    DECLARE @SourceTables nvarchar(4000) = '{{ source_tables | join(", ") }}';

    BEGIN TRY
        -- Initial Logging Entry
        SELECT @InitialRowCount = COUNT(*) FROM {{ target_table }};

        INSERT INTO {{ source('logging', 'DBTProcessExecutionLog') }}        
        (
            InvocationGUID,
            ProcessGUID,
            ProcessName,
            SourceTable,
            TargetTable,
            IsFullRefresh,
            ExecutionStatus,
            ExecutionMessage,
            InitialRowCount,
            ProcessStartTime
        )
        VALUES
        (
            '{{ invocation_id }}',
            @ProcessGUID,
            @ProcessName,
            @SourceTables,
            @BracketedTargetTable,
            CAST({% if flags.FULL_REFRESH %}1{% else %}0{% endif %} AS bit),
            @ExecutionStatus,
            @ExecutionMessage,
            @InitialRowCount,
            @ProcessStartTime
        );

        -- Truncate the target table
        TRUNCATE TABLE {{ target_table }};

        -- Execute the source query (which now includes the INSERT INTO)
        {{ source_query }}

        -- Get number of affected rows
        SET @RowsInserted = @@ROWCOUNT;

        -- Update final row count
        SELECT @FinalRowCount = COUNT(*) FROM {{ target_table }};

        SET @ExecutionStatus = 'Success';
        SET @ExecutionMessage = 'Process completed successfully';

    END TRY
    BEGIN CATCH
        SET @ExecutionStatus = 'Failed';
        SET @ExecutionMessage = ERROR_MESSAGE();
        SET @ErrorNumber = ERROR_NUMBER();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState = ERROR_STATE();
    END CATCH

    -- Finalize Logging Update
    UPDATE {{ source('logging', 'DBTProcessExecutionLog') }}
    SET
        ExecutionStatus = @ExecutionStatus,
        ExecutionMessage = @ExecutionMessage,
        RowsInserted = @RowsInserted,
        RowsUpdated = 0,
        RowsDeleted = 0,
        FinalRowCount = @FinalRowCount,
        ProcessEndTime = sysdatetimeoffset(),
        ErrorNumber = @ErrorNumber,
        ErrorSeverity = @ErrorSeverity,
        ErrorState = @ErrorState
    WHERE
        ProcessGUID = @ProcessGUID;

    -- If the process failed, raise an error
    IF @ExecutionStatus = 'Failed'
    BEGIN
        DECLARE @ErrorMsg nvarchar(4000) = 'Failed to load stage table: ' + @ExecutionMessage;
        RAISERROR(@ErrorMsg, 16, 1);
    END

    -- Always ensure a row exists in StageTableLastUpdate, but only update LastUpdated on full refresh
    IF @ExecutionStatus = 'Success'
    BEGIN
        -- First, try to insert a new row if it doesn't exist. We will assume a full load in this case.
        IF NOT EXISTS (SELECT 1 FROM [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate] WHERE TableName = '{{ target_table.name }}')
        BEGIN
            INSERT INTO [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate] (
                TableName, 
                LastUpdated
            )
            VALUES (
                '{{ target_table.name }}', 
                CAST('1900-01-01 00:00:00.000 -00:00' AS datetimeoffset(3))
            );
        END

        -- Then, update LastUpdated only if it's a full refresh
        {% if flags.FULL_REFRESH %}
        UPDATE 
            [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate]
        SET 
            LastUpdated = CAST('1900-01-01 00:00:00.000 -00:00' AS datetimeoffset(3))
        WHERE 
            TableName = '{{ target_table.name }}';
        {% endif %}
    END

{% endmacro %}