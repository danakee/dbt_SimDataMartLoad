{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimSystem',
        should_log=true,
        tags=['mart', 'dim', 'system']
    )
}}

{% set columns_to_update = [
    'SystemName',
    'SystemDescription',
    'IsAffectedSystem',
    'IsSystemActive',
    'SystemGroupName',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['SystemPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                SystemKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                SystemPKey int NOT NULL,           
                SystemName varchar(50) NULL,
                SystemDescription varchar(256) NULL,
                IsAffectedSystem bit NULL,
                IsSystemActive bit NULL,
                SystemGroupName varchar(50) NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_system'),
    unique_key='SystemPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}