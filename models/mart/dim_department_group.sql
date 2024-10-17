{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimDepartmentGroup',
        should_log=true,
        tags=['mart', 'dim', 'department_group']
    )
}}

{% set columns_to_update = [
    'DepartmentId',
    'DepartmentGroupName',
    'DepartmentGroupDescription',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['DepartmentPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                DepartmentGroupKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                DepartmentPKey int NOT NULL,
                DepartmentId varchar(50) NULL,
                DepartmentGroupName varchar(50) NULL,
                DepartmentGroupDescription varchar(256) NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_department_group'),
    unique_key='DepartmentPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}