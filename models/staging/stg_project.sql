{{ config(
        database='SimulationsAnalyticsStage',
        alias='StageProject',
        materialized='incremental',
        unique_key='ProjectPKey',
        pre_hook="{% if is_incremental() %}TRUNCATE TABLE {{ this }}{% endif %}",
        post_hook='{% do run_query("UPDATE [SimulationsAnalyticsLogging].[dbo].[StageTableLastUpdate] SET [LastUpdated] = SYSDATETIMEOFFSET() WHERE [TableName] = \'StageProject\'") %}',
        tags=['staging', 'project']
) }}

{% set last_update_time = get_stage_last_update('StageProject') %}

WITH ProjectManager AS (
    SELECT 
         CAST(pmx.fk_Project AS int)    AS ProjectPKey
        ,m.PKey                         AS ProjectManagerPKey
        ,m.Employee_No                  AS ProjectManagerEmployeeNumber
        ,m.First_Name                   AS ProjectManagerFirstName
        ,m.Last_Name                    AS ProjectManagerLastName
        ,COUNT(1) OVER (
            PARTITION BY 
                pmx.fk_Project)         AS MultiMgrCount
        ,ROW_NUMBER() OVER (
            PARTITION BY 
                pmx.fk_Project 
            ORDER BY 
                m.PKey DESC)            AS SeqNumber
        ,(SELECT MAX(ct) FROM (VALUES 
            (pmx.hvr_change_time), 
            (m.hvr_change_time)
        ) AS ChangeTime(ct))            AS hvr_change_time
    FROM 
        {{ source('project2', 'tblProjPMXref') }} AS pmx
        LEFT JOIN {{ source('employee2', 'tblEmployee') }} AS m
            ON pmx.fk_ProjMgr = m.PKey
    WHERE 
        pmx.CurrentPM = 1
),

ProjectData AS (
    SELECT 
         p.PKey                                     AS ProjectPKey
        ,p.ProjName                                 AS ProjectName
        ,p.ProjDesc                                 AS ProjectDescription
        ,s.status                                   AS ProjectStatusCode
        ,s.Description                              AS ProjectStatusDescription
        ,t.Name                                     AS ProjectTypeCode
        ,t.Description                              AS ProjectTypeDescription
        ,a.description                              AS ReportTypeDescription
        ,p.eac                                      AS IsEAC
        ,p.stopdr                                   AS IsStopDR
        ,pm.ProjectManagerFirstName                 AS ProjectManagerFirstName     
        ,pm.ProjectManagerLastName                  AS ProjectManagerLastName     
        ,p.EffStartDt                               AS EffectiveDate
        ,CAST(NULL AS bit)                          AS IsLatest
        ,p.CreateDt                                 AS ProjectCreateDate
        ,p.lastmoddt                                AS ProjectUpdateDate
        ,p.ver                                      AS ProjectVersion
        ,(SELECT MAX(ct) FROM (VALUES 
                (p.hvr_change_time), 
                (s.hvr_change_time),
                (t.hvr_change_time),
                (a.hvr_change_time),
                (pm.hvr_change_time)
        ) AS ChangeTime(ct))                        AS HvrChangeTime
        ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
        ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageLastUpdatedDatetime   
    FROM 
        {{ source('project2', 'tblProject') }} AS p
        JOIN {{ source('project2', 'tblStatus') }} AS s 
            ON p.fk_Status = s.PKey
        LEFT JOIN {{ source('project2', 'tblProjType') }} AS t 
            ON p.fk_type = t.PKey
        LEFT JOIN {{ source('project2', 'tblAccountingType') }} AS a 
            ON p.fk_reporttype = a.PKey
        LEFT JOIN ProjectManager AS pm
            ON p.PKey = pm.ProjectPKey
            AND pm.SeqNumber = 1
)

SELECT 
     ProjectPKey
    ,ProjectName
    ,ProjectDescription
    ,ProjectStatusCode
    ,ProjectStatusDescription
    ,ProjectTypeCode
    ,ProjectTypeDescription
    ,ReportTypeDescription
    ,IsEAC
    ,IsStopDR
    ,ProjectManagerFirstName
    ,ProjectManagerLastName
    ,EffectiveDate
    ,IsLatest
    ,ProjectCreateDate
    ,ProjectUpdateDate
    ,ProjectVersion
    ,HvrChangeTime
    ,StageCreatedDatetime
    ,StageLastUpdatedDatetime    
FROM 
    ProjectData
WHERE
    1=1
    {% if is_incremental() %}
    AND HvrChangeTime > '{{ last_update_time }}'
    {% endif %}
