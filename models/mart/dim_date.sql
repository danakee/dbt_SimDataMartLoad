{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimDate',
        should_log=true,
        tags=['mart', 'dim', 'date']
    )
}}

{% set columns_to_update = [
    'CalendarDate',
    'AccountingPeriodKey',
    'AccountingPeriodName',
    'AccountingPeriodStartDate',
    'AccountingPeriodEndDate',
    'Year',
    'Quarter',
    'Month',
    'MonthName',
    'MonthAbbreviation',
    'Day',
    'DayOfWeek',
    'DayName',
    'IsWeekend',
    'WeekOfYear',
    'HVRChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['DateKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                DateKey int NOT NULL CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                CalendarDate datetime NULL,
                AccountingPeriodKey int NULL,
                AccountingPeriodName varchar(50) NULL,
                AccountingPeriodStartDate date NULL,
                AccountingPeriodEndDate date NULL,
                Year int NULL,
                Quarter int NULL,
                Month int NULL,
                MonthName nvarchar(30) NULL,
                MonthAbbreviation nvarchar(3) NULL,
                Day int NULL,
                DayOfWeek int NULL,
                DayName nvarchar(30) NULL,
                IsWeekend int NOT NULL,
                WeekOfYear int NULL,
                HVRChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_date'),
    unique_key='DateKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}