{{ 
    config(
        materialized='table',
        database='SimulationsAnalytics',
        alias='StageSimulatorQualification',
        tags=['staging']
    )
}}

WITH [SimulatorQualification] AS (
SELECT 
    --,[s].[sim_id]               AS [SimulatorId]
    --,[c].[cfgid]                AS [ConfigurationId]
    -- COUNT(1) OVER (PARTITION BY [s].[PKey], [c].[PKey], [q].[PKey]) AS [QualificationMultiCount]
     [q].[PKey]                             AS [SimulatorQualificationPKey]
    ,[c].[PKey]                             AS [SimulatorConfigurationPKey]
    ,[s].[PKey]                             AS [SimulatorPKey]
    ,[a].[agency_id]                        AS [QualificationAgencyId] -- Do these still provide value?
    ,[cl].[PKey]                            AS [QualificationLevelId]  -- Do these still provide value?
    ,CAST([q].[effstartdate] AS date)       AS [SimulatorQualificationEffectiveDate]
    ,CAST([q].[effenddate] AS date)         AS [SimulatorQualificationExpiryDate]
    ,[q].[description]                      AS [SimulatorQualificationDescription]
    ,CAST([q].[processstartdate] AS date)   AS [SimulatorQualificationProcessStartDate]
    ,CAST([q].[scheduledrecertdt] AS date)  AS [ScheduledRequalificationDate]
    ,CAST([q].[initialdate] AS date)        AS [InitialSimulatorQualificationDate]
    ,[q].[deleted]                          AS [IsSimulatorQualificationDeleted]
    ,[q].[orig_project]                     AS [OriginalSimulatorQualificationProjectName]
    ,[q].[HideInReports]                    AS [IsHiddenInReport]
    ,[cl].[name]                            AS [SimulatorQualificationLevelName]
    ,[cl].[description]                     AS [SimulatorQualificationLevelDescription]
    ,[cl].[approved]                        AS [IsQualificationLevelApproved]
    ,[acc].[name]                           AS [ACCircularName]                             
    ,[acc].[approved]                       AS [IsACCircularApproved]
    ,[a].[Agency_name]                      AS [SimulatorQualificationAgencyName]
    ,[a].[Description]                      AS [SimulatorQualificationAgencyDescription]
    ,[q].[agencysimid]                      AS [AgencySimulatorId]
    ,[a].[Domestic]                         AS [IsAgencyDomestic]
    ,[a].[Sponsor_Country]                  AS [AgencySponsorCountryCode]
    ,[a].[deleted]                          AS [IsAgencyDeleted]
    --,[s].[name]                           AS [SimulatorName]                -- Do we want to keep these in the this table?
    --,[c].[name]                           AS [ConfigurationName]            -- Do we want to keep these in the this table?
    ,(SELECT MAX([ct]) FROM (VALUES 
            ([s].[hvr_change_time]),
            ([so].[hvr_change_time]),
            ([scc].[hvr_change_time]),
            ([c].[hvr_change_time]),
            ([co].[hvr_change_time]),
            ([ccc].[hvr_change_time]),
            ([q].[hvr_change_time]),
            ([a].[hvr_change_time]),
            ([cl].[hvr_change_time]),
            ([acc].[hvr_change_time])
    ) AS ChangeTime([ct])) AS [HvrChangeTime]
FROM
    {{ source('sim2', 'tblSim') }} AS [s]
    INNER JOIN {{ source('sim2', 'tblObjectHier') }} AS [so]
        ON [s].[PKey] = [so].[fk_head]
    INNER JOIN {{ source('sim2', 'tblClassLink') }} AS [scc]
        ON  [so].[fk_classlink] = [scc].[PKey]
        AND [scc].[name] = 'sim_to_cfg'
    INNER JOIN {{ source('sim2', 'tblConfig') }} AS [c]
        ON [so].[fk_tail] = [c].[PKey]
    INNER JOIN {{ source('sim2', 'tblObjectHier') }} AS [co]
        ON [c].[PKey] = [co].[fk_head]
    INNER JOIN {{ source('sim2', 'tblClassLink') }} AS [ccc]
        ON  [co].[fk_classlink] = [ccc].[PKey]
        AND [ccc].[name] = 'cfg_to_cert'
    INNER JOIN {{ source('sim2', 'tblCertification') }} AS [q]
        ON  [co].[fk_tail] = [q].[PKey]
    INNER JOIN {{ source('sim2', 'tblCertAgency') }} AS [a]
        ON [q].[fk_certagency] = [a].[PKey]
    INNER JOIN {{ source('sim2', 'tblCertLevel') }} AS [cl]
        ON [q].[fk_certlevel] = [cl].[PKey]
    LEFT OUTER JOIN {{ source('sim2', 'tblACCircular') }} AS [acc]
        ON [q].[fk_accircular] = [acc].[PKey]
WHERE   
    1=1
    --AND [s].[latest] = 1 -- Latest simulator
    --AND [c].[latest] = 1 -- Latest configuration
    AND [q].[latest] = 1 -- Latest SimulatorQualification
)

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
    ,[HvrChangeTime]
FROM 
    [SimulatorQualification];
