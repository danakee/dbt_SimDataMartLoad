{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimDepartment',
        should_log=true,
        tags=['mart', 'dim', 'department']
    )
}}

{% set columns_to_update = [
    'DepartmentName',
    'DepartmentDescription',
    'DepartmentStatusCode',
    'DepartmentStatusDescription',
    'DepartmentTypeCode',
    'DepartmentTypeDescription',
    'ManagerFirstName',
    'ManagerLastName',
    'EffectiveDate',
    'IsLatest',
    'DepartmentCreateDate',
    'DepartmentUpdateDate',
    'DepartmentVersion',
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
                DepartmentKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                DepartmentPKey int NOT NULL,           
                DepartmentName varchar(50) NULL,
                DepartmentDescription varchar(255) NULL,
                DepartmentStatusCode char(50) NULL,
                DepartmentStatusDescription char(256) NULL,
                DepartmentTypeCode varchar(50) NULL,
                DepartmentTypeDescription varchar(255) NULL,
                ManagerFirstName varchar(50) NULL,
                ManagerLastName varchar(50) NULL,
                EffectiveDate datetime NULL,
                IsLatest bit NULL,
                DepartmentCreateDate datetime NULL,
                DepartmentUpdateDate datetime NULL,
                DepartmentVersion int NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_department'),
    unique_key='DepartmentPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}