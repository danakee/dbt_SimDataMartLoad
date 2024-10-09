{% macro dim_project() %}
    {{ return(dim_project_implementation()) }}
{% endmacro %}

{% materialization dim_project, adapter='sqlserver' %}
    {%- set existing_relation = load_relation(this) -%}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
    {%- set temp_relation = make_temp_relation(target_relation) -%}

    -- Setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    -- Create the temporary table
    {% call statement('create_temp_table') -%}
        CREATE TABLE {{ temp_relation }} (
            ProjectPKey int,
            ProjectName varchar(25),
            ProjectDescription varchar(255),
            ProjectStatusCode varchar(50),
            ProjectStatusDescription varchar(256),
            ProjectTypeCode varchar(50),
            ProjectTypeDescription varchar(256),
            ReportTypeDescription varchar(256),
            IsEAC bit,
            IsStopDR bit,
            ProjectManagerFirstName varchar(50),
            ProjectManagerLastName varchar(50),
            EffectiveDate datetime,
            IsLatest bit,
            HvrChangeTime datetimeoffset(3)
        );

        INSERT INTO {{ temp_relation }} (
            ProjectPKey, ProjectName, ProjectDescription, ProjectStatusCode,
            ProjectStatusDescription, ProjectTypeCode, ProjectTypeDescription,
            ReportTypeDescription, IsEAC, IsStopDR, ProjectManagerFirstName,
            ProjectManagerLastName, EffectiveDate, IsLatest, HvrChangeTime
        )
        {{ sql }};
    {%- endcall %}

    {% if existing_relation is none %}
        -- Initial creation of the table
        {% call statement('main') -%}
            -- Create the target table with all necessary columns and constraints
            CREATE TABLE {{ target_relation }} (
                ProjectKey int IDENTITY(1,1) CONSTRAINT PK_DimProject PRIMARY KEY,
                ProjectPKey int,
                ProjectName varchar(25),
                ProjectDescription varchar(255),
                ProjectStatusCode varchar(50),
                ProjectStatusDescription varchar(256),
                ProjectTypeCode varchar(50),
                ProjectTypeDescription varchar(256),
                ReportTypeDescription varchar(256),
                IsEAC bit,
                IsStopDR bit,
                ProjectManagerFirstName varchar(50),
                ProjectManagerLastName varchar(50),
                EffectiveDate datetime,
                IsLatest bit,
                HvrChangeTime datetimeoffset(3),
                EDWCreatedDatetime datetimeoffset(3) CONSTRAINT DF_DimProject_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) CONSTRAINT DF_DimProject_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            );

            -- Insert data from the temporary table into the target table
            INSERT INTO {{ target_relation }} (
                ProjectPKey, ProjectName, ProjectDescription, ProjectStatusCode,
                ProjectStatusDescription, ProjectTypeCode, ProjectTypeDescription,
                ReportTypeDescription, IsEAC, IsStopDR, ProjectManagerFirstName,
                ProjectManagerLastName, EffectiveDate, IsLatest, HvrChangeTime
            )
            SELECT 
                ProjectPKey, ProjectName, ProjectDescription, ProjectStatusCode,
                ProjectStatusDescription, ProjectTypeCode, ProjectTypeDescription,
                ReportTypeDescription, IsEAC, IsStopDR, ProjectManagerFirstName,
                ProjectManagerLastName, EffectiveDate, IsLatest, HvrChangeTime
            FROM {{ temp_relation }};
        {%- endcall %}
    {% else %}
        -- Incremental update
        {% call statement('main') -%}
            MERGE INTO {{ target_relation }} AS tgt
            USING {{ temp_relation }} AS src
                ON tgt.ProjectPKey = src.ProjectPKey
            WHEN MATCHED THEN
                UPDATE SET
                    ProjectName = src.ProjectName,
                    ProjectDescription = src.ProjectDescription,
                    ProjectStatusCode = src.ProjectStatusCode,
                    ProjectStatusDescription = src.ProjectStatusDescription,
                    ProjectTypeCode = src.ProjectTypeCode,
                    ProjectTypeDescription = src.ProjectTypeDescription,
                    ReportTypeDescription = src.ReportTypeDescription,
                    IsEAC = src.IsEAC,
                    IsStopDR = src.IsStopDR,
                    ProjectManagerFirstName = src.ProjectManagerFirstName,
                    ProjectManagerLastName = src.ProjectManagerLastName,
                    EffectiveDate = src.EffectiveDate,
                    IsLatest = src.IsLatest,
                    HvrChangeTime = src.HvrChangeTime,
                    EDWLastUpdatedDateTime = sysdatetimeoffset()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    ProjectPKey, ProjectName, ProjectDescription, ProjectStatusCode,
                    ProjectStatusDescription, ProjectTypeCode, ProjectTypeDescription,
                    ReportTypeDescription, IsEAC, IsStopDR, ProjectManagerFirstName,
                    ProjectManagerLastName, EffectiveDate, IsLatest, HvrChangeTime, 
                    EDWCreatedDateTime, EDWLastUpdatedDateTime
                )
                VALUES (
                    src.ProjectPKey, src.ProjectName, src.ProjectDescription,
                    src.ProjectStatusCode, src.ProjectStatusDescription,
                    src.ProjectTypeCode, src.ProjectTypeDescription,
                    src.ReportTypeDescription, src.IsEAC, src.IsStopDR,
                    src.ProjectManagerFirstName, src.ProjectManagerLastName,
                    src.EffectiveDate, src.IsLatest, src.HvrChangeTime,
                    sysdatetimeoffset(), sysdatetimeoffset()
                );
        {%- endcall %}
    {% endif %}

    -- Cleanup
    {% call statement('cleanup') -%}
        DROP TABLE {{ temp_relation }};
    {%- endcall %}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}