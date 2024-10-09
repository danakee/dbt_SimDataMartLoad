{{
    config(
        materialized='dim_simulator',
        database='SimulationsAnalytics',
        alias='DimSimulator',
        tags=['mart', 'simulator']
    )
}}

SELECT
    SimulatorPKey,
    SimulatorId,
    CAST(NULL AS int) AS SimulatorVersionNumber,  -- Added as a placeholder
    CAST(1 AS bit) AS IsLatest,  -- Added as a default value
    SimulatorName,
    CAST(NULL AS datetime) AS SourceEffectiveDate,  -- Added as a placeholder
    CAST(NULL AS datetime) AS SourceExpiryDate,  -- Added as a placeholder
    CAST(1 AS bit) AS SourceIsCurrent,  -- Added as a default value
    FSIAssetNumber,
    LocationId,
    LeadLocationId,
    DestinationId,
    IsILS,
    DateReadyForTraining AS ReadyForTrainingTimestamp,
    SimulatorTypeKey,
    SimulatorTypeName,
    AircraftModelKey,
    AircraftModelCode,
    AircraftModelName,
    AircraftModelTypeValue AS AircraftModelTypevalue,
    AircraftTypeName,
    AircraftTypeDescription,
    StatusKey,
    CAST(0 AS bit) AS TrackLostTime,  -- Added as a default value
    SimulatorGroupKey,
    SimulatorGroupName,
    SimulatorGroupDescription,
    ShipDate,
    CAST(NULL AS datetime) AS CreateDate,  -- Added as a placeholder
    CAST(NULL AS int) AS CreateEmployeeId,  -- Added as a placeholder
    CAST(NULL AS datetime) AS LastModifiedDate,  -- Added as a placeholder
    CAST(NULL AS int) AS LastModifiedEmployeeId,  -- Added as a placeholder
    OwnerId,
    OwnerCustomerId,
    OwnerDescription,
    ManufacturerId,
    ManufacturerName,
    CAST(1 AS bit) AS IsCurrent,  -- Added as a default value
    HvrChangeTime
FROM 
    {{ ref('stg_simulator') }}