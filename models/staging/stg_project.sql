{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageProject',
        tags=['staging', 'project']
    )
}}

{% set source_query %}
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

    INSERT INTO {{ this }} (
        ProjectPKey, ProjectName, ProjectDescription, ProjectStatusCode, ProjectStatusDescription,
        ProjectTypeCode, ProjectTypeDescription, ReportTypeDescription, IsEAC, IsStopDR,
        ProjectManagerFirstName, ProjectManagerLastName, EffectiveDate, IsLatest,
        ProjectCreateDate, ProjectUpdateDate, ProjectVersion, HvrChangeTime,
        StageCreatedDatetime, StageLastUpdatedDatetime
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
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "project2.tblProject", 
        "project2.tblStatus", 
        "project2.tblProjType", 
        "project2.tblAccountingType", 
        "employee2.tblEmployee", 
        "project2.tblProjPMXref"
    ],
    unique_key="ProjectPKey"
) }}