{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SimulatorPKey',
        compare_columns=[
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
            'HvrChangeTime'
        ],
        merge_update_columns=[
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
        ],
        database='SimulationsAnalytics',
        alias='DimSimulator',
        tags=['mart', 'simulator']
    )
}}

SELECT
     [SimulatorPKey]
    ,[SimulatorId]
    ,[SimulatorName]
    ,[SourceCreatedDate]
    ,[SourceLastUpdatedDate]
    ,[FSIAssetNumber]
    ,[LocationId]
    ,[LeadLocationId]
    ,[DestinationId]
    ,[IsILS]
    ,[DateReadyForTraining]
    ,[SimulatorTypeKey]
    ,[SimulatorTypeName]
    ,[AircraftModelKey]
    ,[AircraftModelCode]
    ,[AircraftModelName]
    ,[AircraftModelTypeValue]
    ,[AircraftTypeName]
    ,[AircraftTypeDescription]
    ,[StatusKey]
    ,[StatusName]
    ,[TrackLostTime]
    ,[SimulatorGroupKey]
    ,[SimulatorGroupName]
    ,[SimulatorGroupDescription]
    ,[ShipDate]
    ,[OwnerId]
    ,[OwnerCustomerId]
    ,[OwnerDescription]
    ,[ManufacturerId]
    ,[ManufacturerName]
    ,[HvrChangeTime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWCreatedDatetime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWLastUpdatedDatetime]    
FROM 
    {{ ref('stg_simulator') }}
WHERE
    1=1
    {% if is_incremental() %}
    AND [HvrChangeTime] > (SELECT ISNULL(MAX([HvrChangeTime]), '1900-01-01') FROM {{ this }})
    {% endif %}
