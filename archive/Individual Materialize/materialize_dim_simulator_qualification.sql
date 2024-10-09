{% macro dim_simulator_qualification() %}
    {{ return(dim_simulator_qualification_implementation()) }}
{% endmacro %}

{% materialization dim_simulator_qualification, adapter='sqlserver' %}
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
                [SimulatorQualificationKey] [int] IDENTITY(1,1) CONSTRAINT [PK_DimSimulatorQualification] PRIMARY KEY,
                [SimulatorQualificationPKey] [int] NOT NULL,
                [SimulatorConfigurationPKey] [int] NOT NULL,
                [SimulatorPKey] [int] NOT NULL,
                [QualificationAgencyId] [int] NULL,
                [QualificationLevelId] [int] NULL,
                [SimulatorQualificationEffectiveDate] [date] NULL,
                [SimulatorQualificationExpiryDate] [date] NULL,
                [SimulatorQualificationDescription] [varchar](512) NULL,
                [SimulatorQualificationProcessStartDate] [date] NULL,
                [ScheduledRequalificationDate] [date] NULL,
                [InitialSimulatorQualificationDate] [date] NULL,
                [IsSimulatorQualificationDeleted] [bit] NULL,
                [OriginalSimulatorQualificationProjectName] [varchar](25) NULL,
                [IsHiddenInReport] [bit] NULL,
                [SimulatorQualificationLevelName] [varchar](50) NULL,
                [SimulatorQualificationLevelDescription] [varchar](256) NULL,
                [IsQualificationLevelApproved] [bit] NULL,
                [ACCircularName] [varchar](50) NULL,
                [IsACCircularApproved] [bit] NULL,
                [SimulatorQualificationAgencyName] [varchar](20) NULL,
                [SimulatorQualificationAgencyDescription] [varchar](100) NULL,
                [AgencySimulatorId] [varchar](50) NULL,
                [IsAgencyDomestic] [bit] NULL,
                [AgencySponsorCountryCode] [int] NULL,
                [IsAgencyDeleted] [bit] NULL,
                [SimulatorConfigurationKey] [int] NULL, -- Snowflake FK to DimSimulatorConfiguration
                [OriginalSimulatorQualificationProjectKey] [int] NULL, -- Snowflake FK to DimProject
                [HvrChangeTime] [datetimeoffset](3) NOT NULL,
                [EDWCreatedDatetime]     [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimSimulatorQualification_EDWCreatedDatetime]     DEFAULT (sysdatetimeoffset()),
                [EDWLastUpdatedDatetime] [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimSimulatorQualification_EDWLastUpdatedDatetime] DEFAULT (sysdatetimeoffset())
            );

            -- Insert initial data
            INSERT INTO {{ target_relation }} (
                 [SimulatorQualificationPKey]
                ,[SimulatorConfigurationPKey]
                ,[SimulatorPKey]
                ,[QualificationAgencyId]
                ,[QualificationLevelId]
                ,[SimulatorQualificationEffectiveDate]
                ,[SimulatorQualificationExpiryDate]
                ,[SimulatorQualificationDescription]
                ,[SimulatorQualificationProcessStartDate]
                ,[ScheduledRequalificationDate]
                ,[InitialSimulatorQualificationDate]
                ,[IsSimulatorQualificationDeleted]
                ,[OriginalSimulatorQualificationProjectName]
                ,[IsHiddenInReport]
                ,[SimulatorQualificationLevelName]
                ,[SimulatorQualificationLevelDescription]
                ,[IsQualificationLevelApproved]
                ,[ACCircularName]
                ,[IsACCircularApproved]
                ,[SimulatorQualificationAgencyName]
                ,[SimulatorQualificationAgencyDescription]
                ,[AgencySimulatorId]
                ,[IsAgencyDomestic]
                ,[AgencySponsorCountryCode]
                ,[IsAgencyDeleted]
                ,[SimulatorConfigurationKey] -- Snowflake FK to DimSimulatorConfiguration
                ,[OriginalSimulatorQualificationProjectKey] -- Snowflake FK to DimProject
                ,[HvrChangeTime]
            )
            {{ sql }};
        END
        ELSE
        BEGIN
            -- Perform incremental update
            MERGE INTO {{ target_relation }} AS [tgt]
            USING ({{ sql }}) AS [src]
                ON [tgt].[SimulatorQualificationPKey] = [src].[SimulatorQualificationPKey]
            WHEN MATCHED THEN
                UPDATE SET
                     [tgt].[SimulatorConfigurationPKey]                 = [src].[SimulatorConfigurationPKey]    
                    ,[tgt].[SimulatorPKey]                              = [src].[SimulatorPKey]
                    ,[tgt].[QualificationAgencyId]                      = [src].[QualificationAgencyId]
                    ,[tgt].[QualificationLevelId]                       = [src].[QualificationLevelId]
                    ,[tgt].[SimulatorQualificationEffectiveDate]        = [src].[SimulatorQualificationEffectiveDate]
                    ,[tgt].[SimulatorQualificationExpiryDate]           = [src].[SimulatorQualificationExpiryDate]
                    ,[tgt].[SimulatorQualificationDescription]          = [src].[SimulatorQualificationDescription]
                    ,[tgt].[SimulatorQualificationProcessStartDate]     = [src].[SimulatorQualificationProcessStartDate]
                    ,[tgt].[ScheduledRequalificationDate]               = [src].[ScheduledRequalificationDate]
                    ,[tgt].[InitialSimulatorQualificationDate]          = [src].[InitialSimulatorQualificationDate]
                    ,[tgt].[IsSimulatorQualificationDeleted]            = [src].[IsSimulatorQualificationDeleted]
                    ,[tgt].[OriginalSimulatorQualificationProjectName]  = [src].[OriginalSimulatorQualificationProjectName]
                    ,[tgt].[IsHiddenInReport]                           = [src].[IsHiddenInReport]
                    ,[tgt].[SimulatorQualificationLevelName]            = [src].[SimulatorQualificationLevelName]
                    ,[tgt].[SimulatorQualificationLevelDescription]     = [src].[SimulatorQualificationLevelDescription]
                    ,[tgt].[IsQualificationLevelApproved]               = [src].[IsQualificationLevelApproved]
                    ,[tgt].[ACCircularName]                             = [src].[ACCircularName]
                    ,[tgt].[IsACCircularApproved]                       = [src].[IsACCircularApproved]
                    ,[tgt].[SimulatorQualificationAgencyName]           = [src].[SimulatorQualificationAgencyName]
                    ,[tgt].[SimulatorQualificationAgencyDescription]    = [src].[SimulatorQualificationAgencyDescription]
                    ,[tgt].[AgencySimulatorId]                          = [src].[AgencySimulatorId]
                    ,[tgt].[IsAgencyDomestic]                           = [src].[IsAgencyDomestic]
                    ,[tgt].[AgencySponsorCountryCode]                   = [src].[AgencySponsorCountryCode]
                    ,[tgt].[IsAgencyDeleted]                            = [src].[IsAgencyDeleted]
                    ,[tgt].[SimulatorConfigurationKey]                  = [src].[SimulatorConfigurationKey] -- Snowflake FK to DimSimulatorConfiguration    
                    ,[tgt].[OriginalSimulatorQualificationProjectKey]   = [src].[OriginalSimulatorQualificationProjectKey] -- Snowflake FK to DimProject
                    ,[tgt].[HvrChangeTime]                              = [src].[HvrChangeTime]
                    ,[tgt].[EDWLastUpdatedDatetime]                     = sysdatetimeoffset()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                     [SimulatorQualificationPKey]
                    ,[SimulatorConfigurationPKey]    
                    ,[SimulatorPKey]
                    ,[QualificationAgencyId]
                    ,[QualificationLevelId]
                    ,[SimulatorQualificationEffectiveDate]
                    ,[SimulatorQualificationExpiryDate]
                    ,[SimulatorQualificationDescription]
                    ,[SimulatorQualificationProcessStartDate]
                    ,[ScheduledRequalificationDate]
                    ,[InitialSimulatorQualificationDate]
                    ,[IsSimulatorQualificationDeleted]
                    ,[OriginalSimulatorQualificationProjectName]
                    ,[IsHiddenInReport]
                    ,[SimulatorQualificationLevelName]
                    ,[SimulatorQualificationLevelDescription]
                    ,[IsQualificationLevelApproved]
                    ,[ACCircularName]
                    ,[IsACCircularApproved]
                    ,[SimulatorQualificationAgencyName]
                    ,[SimulatorQualificationAgencyDescription]
                    ,[AgencySimulatorId]
                    ,[IsAgencyDomestic]
                    ,[AgencySponsorCountryCode]
                    ,[IsAgencyDeleted]
                    ,[SimulatorConfigurationKey] -- Snowflake FK to DimSimulatorConfiguration
                    ,[OriginalSimulatorQualificationProjectKey] -- Snowflake FK to DimProject
                    ,[HvrChangeTime]
                    ,[EDWCreatedDatetime]
                    ,[EDWLastUpdatedDatetime]
                )
                VALUES (
                     [SimulatorQualificationPKey]
                    ,[SimulatorConfigurationPKey]    
                    ,[SimulatorPKey]
                    ,[QualificationAgencyId]
                    ,[QualificationLevelId]
                    ,[SimulatorQualificationEffectiveDate]
                    ,[SimulatorQualificationExpiryDate]
                    ,[SimulatorQualificationDescription]
                    ,[SimulatorQualificationProcessStartDate]
                    ,[ScheduledRequalificationDate]
                    ,[InitialSimulatorQualificationDate]
                    ,[IsSimulatorQualificationDeleted]
                    ,[OriginalSimulatorQualificationProjectName]
                    ,[IsHiddenInReport]
                    ,[SimulatorQualificationLevelName]
                    ,[SimulatorQualificationLevelDescription]
                    ,[IsQualificationLevelApproved]
                    ,[ACCircularName]
                    ,[IsACCircularApproved]
                    ,[SimulatorQualificationAgencyName]
                    ,[SimulatorQualificationAgencyDescription]
                    ,[AgencySimulatorId]
                    ,[IsAgencyDomestic]
                    ,[AgencySponsorCountryCode]
                    ,[IsAgencyDeleted]
                    ,[SimulatorConfigurationKey] -- Snowflake FK to DimSimulatorConfiguration
                    ,[OriginalSimulatorQualificationProjectKey] -- Snowflake FK to DimProject
                    ,[HvrChangeTime]
                    ,sysdatetimeoffset()
                    ,sysdatetimeoffset()
                );
        END
    {%- endcall %}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
