{% macro load_stage_table(target_table, source_query, source_tables, unique_key) %}
    /*
    This macro is used to truncate and load data from a source query into a target table.
    It also logs the process in the DBTProcessExecutionLog and StageTableLastUpdate tables.
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
    DECLARE @RowsDeleted int = 0;
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
        -- Set RowsDeleted to InitialRowCount before truncation to account for truncation in our row counts
        SET @RowsDeleted = @InitialRowCount;  

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

        -- Get number of inserted rows
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
    UPDATE
        {{ source('logging', 'DBTProcessExecutionLog') }}
    SET
        ExecutionStatus = @ExecutionStatus,
        ExecutionMessage = @ExecutionMessage,
        RowsInserted = @RowsInserted,
        RowsUpdated = 0, -- We never perform updates in a stage table
        RowsDeleted = @RowsDeleted,        
        FinalRowCount = @FinalRowCount,
        ProcessEndTime = sysdatetimeoffset(),
        ErrorNumber = @ErrorNumber,
        ErrorSeverity = @ErrorSeverity,
        ErrorState = @ErrorState
    WHERE
        ProcessGUID = @ProcessGUID;

    -- Update StageTableLastUpdate with current datetime if process was successful
    IF @ExecutionStatus = 'Success'
    BEGIN
        UPDATE 
            [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate]
        SET 
            LastUpdated = CAST(SYSDATETIMEOFFSET() AS datetimeoffset(3))
        WHERE 
            TableName = '{{ target_table.name }}';
    END

    -- If the process failed, raise an error
    IF @ExecutionStatus = 'Failed'
    BEGIN
        DECLARE @ErrorMsg nvarchar(4000) = 'Failed to load stage table: ' + @ExecutionMessage;
        RAISERROR(@ErrorMsg, 16, 1);
    END

{% endmacro %}