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
            CAST(0 AS bit),
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

    -- Update the StageTableLastUpdate table
    IF @ExecutionStatus = 'Success'
    BEGIN
        MERGE INTO {{ source('logging', 'StageTableLastUpdate') }} AS target
        USING (SELECT '{{ target_table.name }}' AS TableName, sysdatetimeoffset() AS LastUpdated) AS source
        ON target.TableName = source.TableName
        WHEN MATCHED THEN
            UPDATE SET LastUpdated = source.LastUpdated
        WHEN NOT MATCHED THEN
            INSERT (TableName, LastUpdated)
            VALUES (source.TableName, source.LastUpdated);
    END

{% endmacro %}