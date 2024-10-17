{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimProject',
        should_log=true,
        tags=['mart', 'dim', 'project']
    )
}}

{% set columns_to_update = [
    'ProjectName',
    'ProjectDescription',
    'ProjectStatusCode',
    'ProjectStatusDescription',
    'ProjectTypeCode',
    'ProjectTypeDescription',
    'ReportTypeDescription',
    'IsEAC',
    'IsStopDR',
    'ProjectManagerFirstName',
    'ProjectManagerLastName',
    'EffectiveDate',
    'IsLatest',
    'ProjectCreateDate',
    'ProjectUpdateDate',
    'ProjectVersion',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['ProjectPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                ProjectKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                ProjectPKey int NOT NULL,           
                ProjectName varchar(25) NULL,
                ProjectDescription varchar(255) NULL,
                ProjectStatusCode char(50) NULL,
                ProjectStatusDescription char(256) NULL,
                ProjectTypeCode varchar(50) NULL,
                ProjectTypeDescription varchar(255) NULL,
                ReportTypeDescription varchar(256) NULL,
                IsEAC bit NULL,
                IsStopDR bit NULL,
                ProjectManagerFirstName varchar(50) NULL,
                ProjectManagerLastName varchar(50) NULL,
                EffectiveDate datetime NULL,
                IsLatest bit NULL,
                ProjectCreateDate datetime NULL,
                ProjectUpdateDate datetime NULL,
                ProjectVersion int NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_project'),
    unique_key='ProjectPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}