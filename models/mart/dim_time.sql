{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimTime',
        should_log=true,
        tags=['mart', 'dim', 'time']
    )
}}

{% set columns_to_update = [
    'Time',
    'Hour',
    'Minute',
    'Second',
    'AMPM',
    'Hour24',
    'Hour12',
    'MinuteOfDay',
    'SecondOfDay',
    'PeriodOfDay',
    'TimeInWords',
    'HVRChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['TimeKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                TimeKey bigint NOT NULL CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                Time time(7) NULL,
                Hour int NULL,
                Minute int NULL,
                Second int NULL,
                AMPM varchar(2) NOT NULL,
                Hour24 int NULL,
                Hour12 int NULL,
                MinuteOfDay int NULL,
                SecondOfDay int NULL,
                PeriodOfDay varchar(9) NOT NULL,
                TimeInWords nvarchar(4000) NULL,
                HVRChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_time'),
    unique_key='TimeKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}