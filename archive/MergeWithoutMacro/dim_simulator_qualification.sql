{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SimulatorQualificationPKey',
        compare_columns=[
            'SimulatorConfigurationPKey',
            'SimulatorPKey',
            'QualificationAgencyId',
            'QualificationLevelId',
            'SimulatorQualificationEffectiveDate',
            'SimulatorQualificationExpiryDate',
            'SimulatorQualificationDescription',
            'SimulatorQualificationProcessStartDate',
            'ScheduledRequalificationDate',
            'InitialSimulatorQualificationDate',
            'IsSimulatorQualificationDeleted',
            'OriginalSimulatorQualificationProjectName',
            'IsHiddenInReport',
            'SimulatorQualificationLevelName',
            'SimulatorQualificationLevelDescription',
            'IsQualificationLevelApproved',
            'ACCircularName',
            'IsACCircularApproved',
            'SimulatorQualificationAgencyName',
            'SimulatorQualificationAgencyDescription',
            'AgencySimulatorId',
            'IsAgencyDomestic',
            'AgencySponsorCountryCode',
            'IsAgencyDeleted',
            'SimulatorConfigurationKey',
            'OriginalSimulatorQualificationProjectKey',
            'HvrChangeTime'
        ],
        merge_update_columns=[
            'SimulatorConfigurationPKey',
            'SimulatorPKey',
            'QualificationAgencyId',
            'QualificationLevelId',
            'SimulatorQualificationEffectiveDate',
            'SimulatorQualificationExpiryDate',
            'SimulatorQualificationDescription',
            'SimulatorQualificationProcessStartDate',
            'ScheduledRequalificationDate',
            'InitialSimulatorQualificationDate',
            'IsSimulatorQualificationDeleted',
            'OriginalSimulatorQualificationProjectName',
            'IsHiddenInReport',
            'SimulatorQualificationLevelName',
            'SimulatorQualificationLevelDescription',
            'IsQualificationLevelApproved',
            'ACCircularName',
            'IsACCircularApproved',
            'SimulatorQualificationAgencyName',
            'SimulatorQualificationAgencyDescription',
            'AgencySimulatorId',
            'IsAgencyDomestic',
            'AgencySponsorCountryCode',
            'IsAgencyDeleted',
            'SimulatorConfigurationKey',
            'OriginalSimulatorQualificationProjectKey',
            'HvrChangeTime',
            'EDWLastUpdatedDatetime'
        ],
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
    ,CAST(NULL AS int)                              AS [SimulatorConfigurationKey] -- (Placeholder) Snowflake to DimSimulatorConfiguration
    ,CAST(NULL AS int)                              AS [OriginalSimulatorQualificationProjectKey] -- (Placeholder) Snowflake to DimProject
    ,[HvrChangeTime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWCreatedDatetime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWLastUpdatedDatetime]   
FROM 
    {{ ref('stg_simulator_qualification') }}
WHERE
    1=1
    {% if is_incremental() %}
    AND [HvrChangeTime] > (SELECT ISNULL(MAX([HvrChangeTime]), '1900-01-01') FROM {{ this }})
    {% endif %}
