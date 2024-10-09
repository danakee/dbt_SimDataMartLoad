{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimSimulatorConfiguration',
        should_log=true,
        tags=['mart', 'dim', 'simulatorconfiguration']
    )
}}

-- Add the below columns for future snowflake surrogate key columns
-- 'SimulatorKey',

{% set columns_to_update = [
    'SimulatorPKey',
    'ConfigurationName',
    'LocationName',
    'IsDefault',
    'SourceCreatedDatetime',
    'SourceLastUpdatedDatetime',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['SimulatorConfigurationPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                SimulatorConfigurationKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
	            SimulatorConfigurationPKey int NOT NULL,
	            SimulatorPKey int NOT NULL,
	            ConfigurationName varchar(50) NULL,
	            LocationName varchar(50) NULL,
	            IsDefault bit NULL,
	            SourceCreatedDatetime datetime NULL,
	            SourceLastUpdatedDatetime datetime NULL,
	            --SimulatorKey int NULL,                           -- Snowflake surrogate key to DimSimulator
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_simulator_configuration'),
    unique_key='SimulatorConfigurationPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) 
}}
