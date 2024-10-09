{{
    config(
        database='SimulationsAnalytics',
        alias='DimSimulator',
        materialized='custom_incremental_materialization',
        should_log=true,
        tags=['mart', 'dim', 'simulator']
    )
}}

{% set columns_to_update = [
        'SimulatorId',
        'SimulatorName',
        'SourceCreatedDate',
        'SourceLastUpdatedDate',
        'FSIAssetNumber',
        'LocationId',
        'LeadLocationId',
        'DestinationId',
        'IsILS',
        'DateReadyForTraining',
        'SimulatorTypeKey',
        'SimulatorTypeName',
        'AircraftModelKey',
        'AircraftModelCode',
        'AircraftModelName',
        'AircraftModelTypeValue',
        'AircraftTypeName',
        'AircraftTypeDescription',
        'StatusKey',
        'StatusName',
        'TrackLostTime',
        'SimulatorGroupKey',
        'SimulatorGroupName',
        'SimulatorGroupDescription',
        'ShipDate',
        'OwnerId',
        'OwnerCustomerId',
        'OwnerDescription',
        'ManufacturerId',
        'ManufacturerName',
        'HvrChangeTime',
        'EDWLastUpdatedDatetime'
    ] 
%}

{% set columns_to_insert = columns_to_update + ['SimulatorPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                SimulatorKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
                SimulatorPKey int NOT NULL,
                SimulatorId int NULL,
                SimulatorName varchar(50) NULL,
                SourceCreatedDate datetime NULL,
                SourceLastUpdatedDate datetime NULL,
                FSIAssetNumber nvarchar(50) NULL,
                LocationId int NULL,
                LeadLocationId int NULL,
                DestinationId int NULL,
                IsILS int NULL,
                DateReadyForTraining datetime NULL,
                SimulatorTypeKey int NULL,
                SimulatorTypeName varchar(50) NULL,
                AircraftModelKey int NULL,
                AircraftModelCode varchar(50) NULL,
                AircraftModelName varchar(100) NULL,
                AircraftModelTypeValue int NULL,
                AircraftTypeName varchar(50) NULL,
                AircraftTypeDescription varchar(256) NULL,
                StatusKey int NULL,
                StatusName varchar(50) NULL,
                TrackLostTime int NULL,
                SimulatorGroupKey int NULL,
                SimulatorGroupName varchar(50) NULL,
                SimulatorGroupDescription varchar(256) NULL,
                ShipDate datetime NULL,
                OwnerId int NULL,
                OwnerCustomerId varchar(50) NULL,
                OwnerDescription varchar(256) NULL,
                ManufacturerId varchar(15) NULL,
                ManufacturerName varchar(50) NULL,
                HvrChangeTime datetimeoffset(3) NOT NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_simulator'),
    unique_key='SimulatorPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
    ) 
}}