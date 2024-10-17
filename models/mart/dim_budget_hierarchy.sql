{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimBudgetHierarchy',
        should_log=true,
        tags=['mart', 'dim', 'budget']
    )
}}

{% set columns_to_update = [
    'BudgetItemName',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

-- Important: columns_to_insert should always include all columns from columns_to_update, the primary key columns, and the EDWCreatedDatetime column
{% set columns_to_insert = columns_to_update + ['BudgetCategoryTypeCode', 'BudgetCategoryName', 'BudgetItemCode', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                BudgetHierarchyKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                BudgetCategoryTypeCode varchar(10) NOT NULL,
                BudgetCategoryName varchar(60) NOT NULL,
                BudgetItemCode varchar(50) NOT NULL,
                BudgetItemName varchar(100) NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset()),
                CONSTRAINT UX_{{ this.identifier }}_Composite UNIQUE (BudgetCategoryTypeCode, BudgetCategoryName, BudgetItemCode)
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_budget_hierarchy'),
    unique_key=['BudgetCategoryTypeCode', 'BudgetCategoryName', 'BudgetItemCode'],
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}