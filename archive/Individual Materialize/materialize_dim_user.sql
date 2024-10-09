{% macro dim_user() %}
    {{ return(dim_user_implementation()) }}
{% endmacro %}

{% materialization dim_user, adapter='sqlserver' %}
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
                [UserKey] [int] IDENTITY(1,1) CONSTRAINT [PK_DimUser] PRIMARY KEY,
                [EmployeePKey] [int] NOT NULL,
                [EmployeeNumber] [int] NULL,
                [UserFirstName] [varchar](50) NULL,
                [UserLastName] [varchar](50) NULL,                
                [HvrChangeTime] [datetimeoffset](3) NOT NULL,
                [EDWCreatedDatetime]     [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimUser_EDWCreatedDatetime]     DEFAULT (sysdatetimeoffset()),
                [EDWLastUpdatedDatetime] [datetimeoffset](3) NOT NULL CONSTRAINT [DF_DimUser_EDWLastUpdatedDatetime] DEFAULT (sysdatetimeoffset())
            );
    
            -- Insert initial data
            INSERT INTO {{ target_relation }} (
                [EmployeePKey], 
                [EmployeeNumber], 
                [UserFirstName], 
                [UserLastName],
                [HvrChangeTime]
            )
            {{ sql }};

        END
        ELSE
        BEGIN
            -- Perform incremental update
            MERGE INTO {{ target_relation }} AS [tgt]
            USING ({{ sql }}) AS [src]
                ON [tgt].[EmployeePKey] = [src].[EmployeePKey]
            WHEN MATCHED THEN
                UPDATE SET
                    [tgt].[EmployeeNumber] = [src].[EmployeeNumber],
                    [tgt].[UserFirstName] = [src].[UserFirstName],
                    [tgt].[UserLastName] = [src].[UserLastName],
                    [tgt].[HvrChangeTime] = [src].[HvrChangeTime],
                    [tgt].[EDWLastUpdatedDateTime] = sysdatetimeoffset()
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    [EmployeePKey], 
                    [EmployeeNumber], 
                    [UserFirstName], 
                    [UserLastName],
                    [HvrChangeTime], 
                    [EDWCreatedDateTime], 
                    [EDWLastUpdatedDateTime]
                )
                VALUES (
                    [src].[EmployeePKey], 
                    [src].[EmployeeNumber], 
                    [src].[UserFirstName],
                    [src].[UserLastName], 
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
