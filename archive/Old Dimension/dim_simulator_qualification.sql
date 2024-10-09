{{
    config(
        materialized='dim_simulator_qualification',
        database='SimulationsAnalytics',
        alias='DimSimulatorQualification',
        tags=['mart', 'simulatorqualification']
    )
}}

SELECT 
     [SimulatorQualificationPKey]
    ,[SimulatorConfigurationPKey]
    ,[SimulatorPKey]
    ,[QualificationAgencyId]
    ,[QualificationLevelId]
    ,[SimulatorQualificationEffectiveDate]
    ,[SimulatorQualificationExpiryDate]
    ,[SimulatorQualificationDescription]
    ,[SimulatorQualificationProcessStartDate]
    ,[ScheduledRequalificationDate]
    ,[InitialSimulatorQualificationDate]
    ,[IsSimulatorQualificationDeleted]
    ,[OriginalSimulatorQualificationProjectName]
    ,[IsHiddenInReport]
    ,[SimulatorQualificationLevelName]
    ,[SimulatorQualificationLevelDescription]
    ,[IsQualificationLevelApproved]
    ,[ACCircularName]
    ,[IsACCircularApproved]
    ,[SimulatorQualificationAgencyName]
    ,[SimulatorQualificationAgencyDescription]
    ,[AgencySimulatorId]
    ,[IsAgencyDomestic]
    ,[AgencySponsorCountryCode]
    ,[IsAgencyDeleted]
    ,CAST(NULL AS int) AS [SimulatorConfigurationKey] -- (Placeholder) Snowflake to DimSimulatorConfiguration
    ,CAST(NULL AS int) AS [OriginalSimulatorQualificationProjectKey] -- (Placeholder) Snowflake to DimProject
    ,[HvrChangeTime]
FROM 
    {{ ref('stg_simulator_qualification') }}
