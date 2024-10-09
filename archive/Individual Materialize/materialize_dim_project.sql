{% macro dim_project() %}
    {{ return(dim_project_implementation()) }}
{% endmacro %}

{% materialization dim_project, adapter='sqlserver' %}
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
                [ProjectKey] [int] IDENTITY(1,1) CONSTRAINT [PK_DimProject] PRIMARY KEY,
                [ProjectPKey] [int],
                [ProjectName] [varchar](25),
                [ProjectDescription] [varchar](255),
                [ProjectStatusCode] [varchar](50),
                [ProjectStatusDescription] [varchar](256),
                [ProjectTypeCode] [varchar](50),
                [ProjectTypeDescription] [varchar](256),
                [ReportTypeDescription] [varchar](256),
                [IsEAC] [bit],
                [IsStopDR] [bit],
                [ProjectManagerFirstName] [varchar](50),
                [ProjectManagerLastName] [varchar](50),
                [EffectiveDate] [datetime],
                [IsLatest] [bit],
                [HvrChangeTime] [datetimeoffset](3),
                [EDWCreatedDatetime]     [datetimeoffset](3) CONSTRAINT [DF_DimProject_EDWCreatedDatetime]     DEFAULT (sysdatetimeoffset()),
                [EDWLastUpdatedDatetime] [datetimeoffset](3) CONSTRAINT [DF_DimProject_EDWLastUpdatedDatetime] DEFAULT (sysdatetimeoffset())
            );
    
            -- Insert initial data
            INSERT INTO {{ target_relation }} (
                [ProjectPKey], 
                [ProjectName], 
                [ProjectDescription], 
                [ProjectStatusCode],
                [ProjectStatusDescription], 
                [ProjectTypeCode], 
                [ProjectTypeDescription],
                [ReportTypeDescription], 
                [IsEAC], 
                [IsStopDR], 
                [ProjectManagerFirstName],
                [ProjectManagerLastName], 
                [EffectiveDate], 
                [IsLatest], 
                [HvrChangeTime]
            )
            {{ sql }};
        END
        ELSE
        BEGIN
            -- Perform incremental update
            MERGE INTO {{ target_relation }} AS [tgt]
            USING ({{ sql }}) AS [src]
                ON [tgt].[ProjectPKey] = [src].[ProjectPKey]
            WHEN MATCHED THEN
                UPDATE SET
                    [tgt].[ProjectName] = [src].[ProjectName],
                    [tgt].[ProjectDescription] = [src].[ProjectDescription],
                    [tgt].[ProjectStatusCode] = [src].[ProjectStatusCode],
                    [tgt].[ProjectStatusDescription] = [src].[ProjectStatusDescription],
                    [tgt].[ProjectTypeCode] = [src].[ProjectTypeCode],
                    [tgt].[ProjectTypeDescription] = [src].[ProjectTypeDescription],
                    [tgt].[ReportTypeDescription] = [src].[ReportTypeDescription],
                    [tgt].[IsEAC] = [src].[IsEAC],
                    [tgt].[IsStopDR] = [src].[IsStopDR],
                    [tgt].[ProjectManagerFirstName] = [src].[ProjectManagerFirstName],
                    [tgt].[ProjectManagerLastName] = [src].[ProjectManagerLastName],
                    [tgt].[EffectiveDate] = [src].[EffectiveDate],
                    [tgt].[IsLatest] = [src].[IsLatest],
                    [tgt].[HvrChangeTime] = [src].[HvrChangeTime],
                    [tgt].[EDWLastUpdatedDateTime] = sysdatetimeoffset()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    [ProjectPKey], 
                    [ProjectName], 
                    [ProjectDescription], 
                    [ProjectStatusCode],
                    [ProjectStatusDescription], 
                    [ProjectTypeCode], 
                    [ProjectTypeDescription],
                    [ReportTypeDescription], 
                    [IsEAC], 
                    [IsStopDR], 
                    [ProjectManagerFirstName],
                    [ProjectManagerLastName], 
                    [EffectiveDate], 
                    [IsLatest], 
                    [HvrChangeTime],
                    [EDWCreatedDateTime], 
                    [EDWLastUpdatedDateTime]
                )
                VALUES (
                    [src].[ProjectPKey], 
                    [src].[ProjectName], 
                    [src].[ProjectDescription],
                    [src].[ProjectStatusCode], 
                    [src].[ProjectStatusDescription],
                    [src].[ProjectTypeCode], 
                    [src].[ProjectTypeDescription],
                    [src].[ReportTypeDescription], 
                    [src].[IsEAC], 
                    [src].[IsStopDR],
                    [src].[ProjectManagerFirstName], 
                    [src].[ProjectManagerLastName],
                    [src].[EffectiveDate], 
                    [src].[IsLatest], 
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
