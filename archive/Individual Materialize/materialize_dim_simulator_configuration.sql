{% macro dim_simulator_configuration() %}
    {{ return(dim_simulator_configuration_implementation()) }}
{% endmacro %}

{% materialization dim_simulator_configuration, adapter='sqlserver' %}
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
                [SimulatorConfigurationKey] [int] IDENTITY(1,1) CONSTRAINT [PK_DimSimulatorConfiguration] PRIMARY KEY,
                [SimulatorConfigurationPKey] [int] NULL,
                [SimulatorPKey] [int] NULL,
                [ConfigurationName] [varchar](50) NULL,
                [LocationName] [varchar](50) NULL,
                [IsDefault] [bit] NULL,
                [SourceCreatedDatetime] [datetime] NULL,
                [SourceLastUpdatedDatetime] [datetime] NULL,
                [SimulatorKey] [int] NULL, -- Snowflake to DimSimulator
                [HvrChangeTime] [datetimeoffset](3) NOT NULL,
                [EDWCreatedDatetime]     [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimSimulatorConfiguration_EDWCreatedDatetime]     DEFAULT (sysdatetimeoffset()),
                [EDWLastUpdatedDatetime] [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimSimulatorConfiguration_EDWLastUpdatedDatetime] DEFAULT (sysdatetimeoffset())
            );

            -- Insert initial data
            INSERT INTO {{ target_relation }} (
                [SimulatorConfigurationPKey],
                [SimulatorPKey],
                [ConfigurationName],
                [LocationName],
                [IsDefault],
                [SourceCreatedDatetime],
                [SourceLastUpdatedDatetime],
                [SimulatorKey], -- Snowflake to DimSimulator
                [HvrChangeTime]
            )
            {{ sql }};
        END
        ELSE
        BEGIN
            -- Perform incremental update
            MERGE INTO {{ target_relation }} AS [tgt]
            USING ({{ sql }}) AS [src]
                ON [tgt].[SimulatorConfigurationPKey] = [src].[SimulatorConfigurationPKey]
            WHEN MATCHED THEN
                UPDATE SET
                    [tgt].[SimulatorPKey] = [src].[SimulatorPKey],
                    [tgt].[ConfigurationName] = [src].[ConfigurationName],
                    [tgt].[LocationName] = [src].[LocationName],
                    [tgt].[IsDefault] = [src].[IsDefault],
                    [tgt].[SourceCreatedDatetime] = [src].[SourceCreatedDatetime],
                    [tgt].[SourceLastUpdatedDatetime] = [src].[SourceLastUpdatedDatetime],
                    [tgt].[SimulatorKey] = [src].[SimulatorKey], -- Snowflake to DimSimulator
                    [tgt].[HvrChangeTime] = [src].[HvrChangeTime],
                    [tgt].[EDWLastUpdatedDatetime] = sysdatetimeoffset()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    [SimulatorConfigurationPKey],
                    [SimulatorPKey],
                    [ConfigurationName],
                    [LocationName],
                    [IsDefault],
                    [SourceCreatedDatetime],
                    [SourceLastUpdatedDatetime],
                    [SimulatorKey], -- Snowflake to DimSimulator
                    [HvrChangeTime],
                    [EDWCreatedDatetime],
                    [EDWLastUpdatedDatetime]
                )
                VALUES (
                    [src].[SimulatorConfigurationPKey],
                    [src].[SimulatorPKey],
                    [src].[ConfigurationName],
                    [src].[LocationName],
                    [src].[IsDefault],
                    [src].[SourceCreatedDatetime],
                    [src].[SourceLastUpdatedDatetime],
                    [src].[SimulatorKey], -- Snowflake to DimSimulator
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
