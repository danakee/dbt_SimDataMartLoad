{% macro dim_simulator() %}
    {{ return(dim_simulator_implementation()) }}
{% endmacro %}

{% materialization dim_simulator, adapter='sqlserver' %}
    {%- set existing_relation = load_relation(this) -%}
    {%- set target_relation = this -%}

    -- Setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    -- Create or update the target table
    {% call statement('main') -%}
        IF OBJECT_ID('{{ target_relation }}') IS NULL
        BEGIN
            -- Create the target table
            CREATE TABLE {{ target_relation }} (
                [SimulatorKey] [int] IDENTITY(1,1) CONSTRAINT PK_DimSimulator PRIMARY KEY,
                [SimulatorPKey] [int] NOT NULL,
                [SimulatorId] [int] NULL,
                [SimulatorVersionNumber] [int] NULL,
                [IsLatest] [bit] NULL,
                [SimulatorName] [varchar](50) NULL,
                [SourceEffectiveDate] [datetime ]NULL,
                [SourceExpiryDate] [datetime] NULL,
                [SourceIsCurrent] [bit] NULL,
                [FSIAssetNumber] [nvarchar](50) NULL,
                [LocationId] [int] NULL,
                [LeadLocationId] [int] NULL,
                [DestinationId] [int] NULL,
                [IsILS] [bit] NULL,
                [ReadyForTrainingTimestamp] [datetime] NULL,
                [SimulatorTypeKey] [int] NULL,
                [SimulatorTypeName] [varchar](50) NULL,
                [AircraftModelKey] [int] NULL,
                [AircraftModelCode] [varchar](50) NULL,
                [AircraftModelName] [varchar](100) NULL,
                [AircraftModelTypevalue] [varchar](50) NULL,
                [AircraftTypeName] [varchar](50) NULL,
                [AircraftTypeDescription] [varchar](256) NULL,
                [StatusKey] [int] NULL,
                [TrackLostTime] [bit] NULL,
                [SimulatorGroupKey] [int] NULL,
                [SimulatorGroupName] [varchar](50) NULL,
                [SimulatorGroupDescription] [varchar](256) NULL,
                [ShipDate] [datetime] NULL,
                [CreateDate] [datetime] NULL,
                [CreateEmployeeId] [int] NULL,
                [LastModifiedDate] [datetime] NULL,
                [LastModifiedEmployeeId] [int] NULL,
                [OwnerId] [int] NULL,
                [OwnerCustomerId] [varchar](50) NULL,
                [OwnerDescription] [varchar](256) NULL,
                [ManufacturerId] [varchar](15) NULL,
                [ManufacturerName] [varchar](50) NULL,
                [IsCurrent] [bit] NULL,
                [HvrChangeTime] [datetimeoffset](3) NOT NULL,
                [EDWCreatedDatetime] [datetimeoffset](3)     NOT NULL CONSTRAINT [DF_DimSimulator_EDWCreatedDatetime]     DEFAULT (sysdatetimeoffset()),
                [EDWLastUpdatedDatetime] [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimSimulator_EDWLastUpdatedDatetime] DEFAULT (sysdatetimeoffset())
            );

            -- Insert initial data
            INSERT INTO {{ target_relation }} (
                 [SimulatorPKey]
                ,[SimulatorId]
                ,[SimulatorVersionNumber]
                ,[IsLatest]
                ,[SimulatorName]
                ,[SourceEffectiveDate]
                ,[SourceExpiryDate]
                ,[SourceIsCurrent]
                ,[FSIAssetNumber]
                ,[LocationId]
                ,[LeadLocationId]
                ,[DestinationId]
                ,[IsILS]
                ,[ReadyForTrainingTimestamp]
                ,[SimulatorTypeKey]
                ,[SimulatorTypeName]
                ,[AircraftModelKey]
                ,[AircraftModelCode]
                ,[AircraftModelName]
                ,[AircraftModelTypevalue]
                ,[AircraftTypeName]
                ,[AircraftTypeDescription]
                ,[StatusKey]
                ,[TrackLostTime]
                ,[SimulatorGroupKey]
                ,[SimulatorGroupName]
                ,[SimulatorGroupDescription]
                ,[ShipDate]
                ,[CreateDate]
                ,[CreateEmployeeId]
                ,[LastModifiedDate]
                ,[LastModifiedEmployeeId]
                ,[OwnerId]
                ,[OwnerCustomerId]
                ,[OwnerDescription]
                ,[ManufacturerId]
                ,[ManufacturerName]
                ,[IsCurrent]
                ,[HvrChangeTime]
            )
            {{ sql }};
        END
        ELSE
        BEGIN
            -- Perform incremental update
MERGE INTO {{ target_relation }} AS [tgt]
        USING ({{ sql }}) AS [src]
            ON [tgt].[SimulatorPKey] = [src].[SimulatorPKey]
        WHEN MATCHED THEN
            UPDATE SET
                [tgt].[SimulatorId] = [src].[SimulatorId],
                [tgt].[SimulatorVersionNumber] = [src].[SimulatorVersionNumber],
                [tgt].[IsLatest] = [src].[IsLatest],
                [tgt].[SimulatorName] = [src].[SimulatorName],
                [tgt].[SourceEffectiveDate] = [src].[SourceEffectiveDate],
                [tgt].[SourceExpiryDate] = [src].[SourceExpiryDate],
                [tgt].[SourceIsCurrent] = [src].[SourceIsCurrent],
                [tgt].[FSIAssetNumber] = [src].[FSIAssetNumber],
                [tgt].[LocationId] = [src].[LocationId],
                [tgt].[LeadLocationId] = [src].[LeadLocationId],
                [tgt].[DestinationId] = [src].[DestinationId],
                [tgt].[IsILS] = [src].[IsILS],
                [tgt].[ReadyForTrainingTimestamp] = [src].[ReadyForTrainingTimestamp],
                [tgt].[SimulatorTypeKey] = [src].[SimulatorTypeKey],
                [tgt].[SimulatorTypeName] = [src].[SimulatorTypeName],
                [tgt].[AircraftModelKey] = [src].[AircraftModelKey],
                [tgt].[AircraftModelCode] = [src].[AircraftModelCode],
                [tgt].[AircraftModelName] = [src].[AircraftModelName],
                [tgt].[AircraftModelTypevalue] = [src].[AircraftModelTypevalue],
                [tgt].[AircraftTypeName] = [src].[AircraftTypeName],
                [tgt].[AircraftTypeDescription] = [src].[AircraftTypeDescription],
                [tgt].[StatusKey] = [src].[StatusKey],
                [tgt].[TrackLostTime] = [src].[TrackLostTime],
                [tgt].[SimulatorGroupKey] = [src].[SimulatorGroupKey],
                [tgt].[SimulatorGroupName] = [src].[SimulatorGroupName],
                [tgt].[SimulatorGroupDescription] = [src].[SimulatorGroupDescription],
                [tgt].[ShipDate] = [src].[ShipDate],
                [tgt].[CreateDate] = [src].[CreateDate],
                [tgt].[CreateEmployeeId] = [src].[CreateEmployeeId],
                [tgt].[LastModifiedDate] = [src].[LastModifiedDate],
                [tgt].[LastModifiedEmployeeId] = [src].[LastModifiedEmployeeId],
                [tgt].[OwnerId] = [src].[OwnerId],
                [tgt].[OwnerCustomerId] = [src].[OwnerCustomerId],
                [tgt].[OwnerDescription] = [src].[OwnerDescription],
                [tgt].[ManufacturerId] = [src].[ManufacturerId],
                [tgt].[ManufacturerName] = [src].[ManufacturerName],
                [tgt].[IsCurrent] = [src].[IsCurrent],
                [tgt].[HvrChangeTime] = [src].[HvrChangeTime],
                [tgt].[EDWLastUpdatedDatetime] = sysdatetimeoffset()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                [SimulatorPKey], 
                [SimulatorId], 
                [SimulatorVersionNumber], 
                [IsLatest],
                [SimulatorName], 
                [SourceEffectiveDate], 
                [SourceExpiryDate], 
                [SourceIsCurrent],
                [FSIAssetNumber], 
                [LocationId], 
                [LeadLocationId], 
                [DestinationId],
                [IsILS], 
                [ReadyForTrainingTimestamp], 
                [SimulatorTypeKey], 
                [SimulatorTypeName],
                [AircraftModelKey], 
                [AircraftModelCode], 
                [AircraftModelName],
                [AircraftModelTypevalue], 
                [AircraftTypeName], 
                [AircraftTypeDescription],
                [StatusKey], 
                [TrackLostTime], 
                [SimulatorGroupKey], 
                [SimulatorGroupName],
                [SimulatorGroupDescription], 
                [ShipDate], 
                [CreateDate], 
                [CreateEmployeeId],
                [LastModifiedDate], 
                [LastModifiedEmployeeId], 
                [OwnerId], 
                [OwnerCustomerId],
                [OwnerDescription], 
                [ManufacturerId], 
                [ManufacturerName], 
                [IsCurrent],
                [HvrChangeTime], 
                [EDWCreatedDatetime],
                [EDWLastUpdatedDatetime]
            )
            VALUES (
                [src].[SimulatorPKey], 
                [src].[SimulatorId], 
                [src].[SimulatorVersionNumber], 
                [src].[IsLatest],
                [src].[SimulatorName], 
                [src].[SourceEffectiveDate], 
                [src].[SourceExpiryDate], 
                [src].[SourceIsCurrent],
                [src].[FSIAssetNumber], 
                [src].[LocationId], 
                [src].[LeadLocationId], 
                [src].[DestinationId],
                [src].[IsILS], 
                [src].[ReadyForTrainingTimestamp], 
                [src].[SimulatorTypeKey], 
                [src].[SimulatorTypeName],
                [src].[AircraftModelKey], 
                [src].[AircraftModelCode], 
                [src].[AircraftModelName],
                [src].[AircraftModelTypevalue], 
                [src].[AircraftTypeName], 
                [src].[AircraftTypeDescription],
                [src].[StatusKey], 
                [src].[TrackLostTime], 
                [src].[SimulatorGroupKey], 
                [src].[SimulatorGroupName],
                [src].[SimulatorGroupDescription], 
                [src].[ShipDate], 
                [src].[CreateDate], 
                [src].[CreateEmployeeId],
                [src].[LastModifiedDate], 
                [src].[LastModifiedEmployeeId], 
                [src].[OwnerId], 
                [src].[OwnerCustomerId],
                [src].[OwnerDescription], 
                [src].[ManufacturerId], 
                [src].[ManufacturerName], 
                [src].[IsCurrent],
                [src].[HvrChangeTime], 
                sysdatetimeoffset(), 
                sysdatetimeoffset()
            );
        END
    {%- endcall %}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
