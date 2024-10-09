{% macro merge_dimension(target_table, source_table, unique_key, columns_to_update, columns_to_insert) %}
    /*
    This macro is used to merge data from a source table into a target table.
    The merge is done using the unique key provided and the columns to update and insert.
    The macro also logs the process in the ProcessExecutionLog table in the SimulationsAnalyticsLogging database.
    */
    
    -- Get the execution GUID for the entire dbt process
    {% set dbt_execution_guid = get_execution_guid() %}

    -- Setup logging variables - These are used downstream 
    DECLARE @BracketedSoureTable nvarchar(1024) = 
        QUOTENAME(PARSENAME('{{ source_table }}', 3)) +  '.' + 
        QUOTENAME(PARSENAME('{{ source_table }}', 2)) + '.' + 
        QUOTENAME(PARSENAME('{{ source_table }}', 1));
    DECLARE @BracketedTargetTable nvarchar(1024) = 
        QUOTENAME(PARSENAME('{{ target_table }}', 3)) + '.' + 
        QUOTENAME(PARSENAME('{{ target_table }}', 2)) + '.' + 
        QUOTENAME(PARSENAME('{{ target_table }}', 1));

    {% set is_full_refresh = flags.FULL_REFRESH %}
    DECLARE @ExecutionGUID uniqueidentifier = '{{ dbt_execution_guid }}';
    DECLARE @ProcessGUID uniqueidentifier = NEWID();
    DECLARE @ProcessName nvarchar(1024) = '{{ this.name }}';    
    DECLARE @ExecutionStatus nvarchar(20) = 'Incomplete';
    DECLARE @ExecutionMessage nvarchar(4000) = 'Process not complete';
    DECLARE @RowsInserted int = 0;
    DECLARE @RowsUpdated int = 0;
    DECLARE @RowsDeleted int = 0;
    DECLARE @ProcessStartTime datetimeoffset = sysdatetimeoffset();
    DECLARE @InitialRowCount int;
    DECLARE @FinalRowCount int;
    DECLARE @ErrorNumber int = NULL;
    DECLARE @ErrorSeverity int = NULL;
    DECLARE @ErrorState int = NULL;
    DECLARE @LoggingErrorMessage nvarchar(4000);

    DECLARE @MergeResults TABLE (
        MergeAction nvarchar(10),
        AffectedRows int
    );

    -- Initial Logging Entry
    SELECT @InitialRowCount = COUNT(*) FROM {{ target_table }};

    BEGIN TRY
        INSERT INTO {{ source('logging', 'DBTProcessExecutionLog') }}        
        (
            ExecutionGUID,
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
            @ExecutionGUID,
            @ProcessGUID,
            @ProcessName,
            @BracketedSoureTable,
            @BracketedTargetTable,
            {% if is_full_refresh %}1{% else %}0{% endif %},
            @ExecutionStatus,
            @ExecutionMessage,
            @InitialRowCount,
            @ProcessStartTime
        );
    END TRY
    BEGIN CATCH
        SET @LoggingErrorMessage  = ERROR_MESSAGE();
        RAISERROR('Initial logging failed: %s', 16, 1, @LoggingErrorMessage);
        RETURN;
    END CATCH

    -- Type one SCD MERGE to dimension table
    BEGIN TRY
        BEGIN TRANSACTION;
        MERGE INTO {{ target_table }} AS tgt
        USING (
            SELECT 
                *
                ,sysdatetimeoffset() AS EDWCreatedDatetime
                ,sysdatetimeoffset() AS EDWLastUpdatedDatetime
            FROM 
                {{ source_table }}
            {% if is_incremental() %}
            WHERE 
                HvrChangeTime > (SELECT ISNULL(MAX(HvrChangeTime), '1900-01-01') FROM {{ target_table }})
            {% endif %}
            ) AS src
            ON tgt.{{ unique_key }} = src.{{ unique_key }}

        WHEN MATCHED AND NOT (
            {% for column in columns_to_update %}
            {% if column != 'EDWLastUpdatedDatetime' %}
            (
                tgt.{{ column }} = src.{{ column }}
                OR (tgt.{{ column }} IS NULL AND src.{{ column }} IS NULL)
            )
            AND
            {% endif %}
            {% endfor %}
            1=1
        )
        THEN
            UPDATE SET
                {% for column in columns_to_update %}
                tgt.{{ column }} = src.{{ column }}{% if not loop.last %}, {% endif %}
                {% endfor %}

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                {% for column in columns_to_insert %}
                {{ column }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
            VALUES (
                {% for column in columns_to_insert %}
                src.{{ column }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        OUTPUT 
            $action AS MergeAction,
            1 AS AffectedRows
        INTO @MergeResults (MergeAction, AffectedRows);

        SELECT 
            @RowsInserted = ISNULL(SUM(CASE WHEN MergeAction = 'INSERT' THEN AffectedRows END), 0),
            @RowsUpdated = ISNULL(SUM(CASE WHEN MergeAction = 'UPDATE' THEN AffectedRows END), 0),
            @RowsDeleted = ISNULL(SUM(CASE WHEN MergeAction = 'DELETE' THEN AffectedRows END), 0)
        FROM @MergeResults;

        SELECT @FinalRowCount = COUNT(*) FROM {{ target_table }};
        
        COMMIT TRANSACTION;

        SET @ExecutionStatus = 'Success';
        SET @ExecutionMessage = 'Process completed successfully';

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @ExecutionStatus = 'Failed';
        SET @ExecutionMessage = ERROR_MESSAGE();
        SET @ErrorNumber = ERROR_NUMBER();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState = ERROR_STATE();
    END CATCH

    -- Finalize Logging Update
    BEGIN TRY
        UPDATE 
            {{ source('logging', 'DBTProcessExecutionLog') }}
        SET
            ExecutionStatus   = @ExecutionStatus,
            ExecutionMessage  = @ExecutionMessage,
            RowsInserted      = @RowsInserted,
            RowsUpdated       = @RowsUpdated,
            RowsDeleted       = @RowsDeleted,
            FinalRowCount     = @FinalRowCount,
            ProcessEndTime    = sysdatetimeoffset(),
            ErrorNumber       = @ErrorNumber,
            ErrorSeverity     = @ErrorSeverity,
            ErrorState        = @ErrorState
        WHERE
            ProcessGUID = @ProcessGUID;
    END TRY
    BEGIN CATCH
        SET @LoggingErrorMessage = ERROR_MESSAGE();
        RAISERROR('Final logging update failed: %s', 16, 1, @LoggingErrorMessage);
    END CATCH

{% endmacro %}