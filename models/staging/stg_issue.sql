{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageIssue',
        tags=['staging', 'issue']
    )
}}

{% set source_query %}
    WITH IssueSimulatorConfiguration AS (
        SELECT 
             x.fk_issue_pkey    AS IssueKey
            ,s.pkey             AS SimulatorKey
            ,s.name             AS SimulatorName
            ,cfg.pkey           AS ConfigurationKey
            ,cfg.name           AS ConfigurationName
            ,(SELECT MAX(ct) FROM (VALUES 
                    (x.hvr_change_time),
                    (cfg.hvr_change_time),
                    (oh.hvr_change_time),
                    (s.hvr_change_time)
            ) AS ChangeTime(ct)) AS hvr_change_time
        FROM 
            {{ source('fsi_issues2', 'tblIssueObjectXref') }} AS x
            INNER JOIN {{ source('sim2', 'tblConfig') }} AS cfg
                ON x.fk_object_pkey = cfg.pkey
                AND x.fk_classlink IN (44, 45, 46)
                AND x.fk_issue_pkey IS NOT NULL
            INNER JOIN {{ source('sim2', 'tblObjectHier') }} AS oh 
                ON x.fk_object_pkey = oh.fk_tail
            INNER JOIN {{ source('sim2', 'tblSim') }} AS s 
                ON  s.pkey = oh.fk_head 
                AND oh.fk_parent IS NULL 
                AND oh.fk_classlink = 2
    ),
    IssueWithConfigurationList AS (
        SELECT 
             IssueKey
            ,STRING_AGG(ConfigurationName, '|') AS ConfigurationNameList
            ,STRING_AGG(CAST(ConfigurationKey AS VARCHAR(20)), '|') AS ConfigurationKeyList
            ,COUNT(*) AS ConfigurationCount
            ,MAX(hvr_change_time) AS hvr_change_time
        FROM 
            IssueSimulatorConfiguration
        GROUP BY 
            IssueKey
    ),
    IssueProjectSim AS (
        SELECT
             COUNT(*) OVER (PARTITION BY iox.fk_issue_pkey) AS IssueMultiCount
            ,iox.pkey           AS iox_pkey
            ,iox.fk_issue_pkey  AS IssuePKey
            ,p.Pkey             AS ProjectPKey
            ,s.pkey             AS SimulatorPKey
            ,p.ProjName         AS ProjectName
            ,s.name             AS SimulatorName
            ,iox.fk_object_pkey AS Iox_obj_pkey
            ,p.Deleted          AS IsProjectDeleted
            ,(SELECT MAX(ct) FROM (VALUES 
                    (iox.hvr_change_time),
                    (psx.hvr_change_time),
                    (s.hvr_change_time),
                    (p.hvr_change_time),
                    (cl.hvr_change_time)
            ) AS ChangeTime(ct)) AS hvr_change_time
        FROM
            {{ source('fsi_issues2', 'tblIssueObjectXref') }} AS iox
            INNER JOIN {{ source('sim2', 'tblProjSimXref') }} AS psx
                ON iox.fk_object_pkey = psx.pkey
            INNER JOIN {{ source('sim2', 'tblSim') }} AS s
                ON psx.fk_sim = s.pkey
            INNER JOIN {{ source('project2', 'tblProject') }} AS p
                ON psx.fk_project = p.Pkey
            INNER JOIN {{ source('fsi_issues2', 'tblClassLink') }} AS cl
                ON iox.fk_classlink = cl.pkey
        WHERE
            cl.fk_linkdb = 1
            AND p.Deleted <> 1
    ),
    Issue AS (
        SELECT
             i.pkey                     AS IssuePKey
            ,i.issueid                  AS IssueId
            ,CAST(i.issue_dt AS date)   AS IssueDate
            ,CAST(i.issue_dt AS time)   AS IssueTime
            ,s.pkey                     AS SimulatorPKey
            ,s.sim_id                   AS SimulatorId
            ,sy.affectedsystem          AS IsAffectedSystem    
            ,iwcl.ConfigurationNameList AS AffectedConfigurationList
            ,CAST(NULL AS int)          AS CenterKey
            ,i.description              AS IssueDescription
            ,CAST(NULL AS varchar)      AS IssueTypeDescription     
            ,pr.name                    AS IssuePriorityNumber      
            ,pr.description             AS IssuePriority   
            ,iss.desc_short             AS IssueStatusName
            ,ic.category                AS IssueCategoryName
            ,icl.Classification         AS IssueClassificationName
            ,i.date_closed              AS IssueCloseTimestamp      
            ,i.training_impact          AS TrainingRestrictionText
            ,i.training_critical        AS IsCriticalTrainingIssue
            ,trc.name                   AS TrainingCategoryName
            ,p.ProjName                 AS JobNumber
            ,CAST(NULL AS varchar)      AS IssueType
            ,CAST(i.fk_fstd_lost_time_qty AS int)   AS FSTDLostTimeMinutes
            ,CAST(NULL AS varchar)      AS ActivityLog
            ,i.mmi                      AS IsMMI
            ,i.issuedocno               AS DocumentNumber
            ,i.chargecode               AS ChargeCode
            ,CAST(i.duedt AS date)      AS Duedate    
            ,i.writtenby                AS WrittenByName    
            ,i.writtenby_contact        AS WrittenByContactInfoTxt
            ,ra.Agency_name             AS RegulatoryAgencyName
            ,ltt.description            AS LostTimeTypeDescription
            ,CAST(NULL AS int)          AS LostTimeMinutesNumber
            ,CAST(NULL AS int)          AS FSTDLostTimeMinutesNumber
            ,cs.type                    AS RootCauseTypeName
            ,cs.description             AS RootCauseTypeDescription
            ,rct.RCType                 AS FSTDTypeName
            ,(SELECT MAX(ct) FROM (VALUES 
                    (ips.hvr_change_time),
                    (i.hvr_change_time),
                    (s.hvr_change_time),
                    (p.hvr_change_time),
                    (iss.hvr_change_time),
                    (icl.hvr_change_time),
                    (ltt.hvr_change_time),
                    (l.hvr_change_time),
                    (pr.hvr_change_time),
                    (ic.hvr_change_time),
                    (ra.hvr_change_time),
                    (trc.hvr_change_time),
                    (sy.hvr_change_time),
                    (cs.hvr_change_time),
                    (rct.hvr_change_time),
                    (iwcl.hvr_change_time)
            ) AS ChangeTime(ct)) AS HvrChangeTime
        FROM 
            IssueProjectSim AS ips
            INNER JOIN {{ source('fsi_issues2', 'tblIssue') }} AS i
                ON ips.IssuePKey = i.pkey
            INNER JOIN {{ source('sim2', 'tblSim') }} AS s
                ON ips.SimulatorPKey = s.pkey
            INNER JOIN {{ source('project2', 'tblProject') }} AS p
                ON ips.ProjectPKey = p.pkey
            INNER JOIN {{ source('fsi_issues2', 'tblStatus') }} AS iss
                ON i.fk_statuspk = iss.pkey
            INNER JOIN {{ source('fsi_issues2', 'tblClassification') }} AS icl
                ON i.fk_classificationpk = icl.PKey
            LEFT OUTER JOIN {{ source('fsi_issues2', 'tblLostTimeType') }} AS ltt
                ON i.fk_lost_time_typepk = ltt.pkey
            LEFT OUTER JOIN {{ source('employee2', 'tblLocation') }} AS l
                ON s.loc_id = l.PKey
            LEFT OUTER JOIN {{ source('fsi_issues2', 'tblPriority') }} AS pr
                ON i.fk_prioritypk = pr.pkey
            LEFT OUTER JOIN {{ source('fsi_issues2', 'tblIssueCategory') }} AS ic
                ON i.fk_issue_category = ic.pkey
            LEFT OUTER JOIN {{ source('sim2', 'tblCertAgency') }} AS ra
                ON i.fk_agencypk = ra.pkey
            LEFT OUTER JOIN {{ source('fsi_issues2', 'tblIssue_Trng_Category') }} AS trc
                ON i.fk_issue_category = trc.pkey
            LEFT OUTER JOIN {{ source('sim2', 'tblSystems') }} AS sy
                ON i.fk_systemspk = sy.pkey
            LEFT OUTER JOIN {{ source('fsi_issues2', 'tblCause') }} AS cs
                ON i.fk_rootcausepk = cs.pkey
            LEFT OUTER JOIN {{ source('fsi_issues2', 'tblRootCauseType') }} AS rct
                ON i.RCType = rct.pkey
            LEFT OUTER JOIN IssueWithConfigurationList AS iwcl
                ON i.pkey = iwcl.IssueKey
    )

    INSERT INTO {{ this }} (
         IssuePKey
        ,IssueID
        ,IssueDate
        ,IssueTime
        ,SimulatorPKey
        ,SimulatorId
        ,IsAffectedSystem
        ,AffectedConfigurationList
        ,CenterKey
        ,IssueDescription
        ,IssueTypeDescription
        ,IssuePriorityNumber
        ,IssueStatusName
        ,IssueCategoryName
        ,IssueClassificationName
        ,IssueCloseTimestamp
        ,TrainingRestrictionText
        ,IsCriticalTrainingIssue
        ,TrainingCategoryName
        ,JobNumber
        ,IsMMI
        ,DocumentNumber
        ,ChargeCode
        ,DueDate
        ,WrittenByName
        ,WrittenByContactInfoTxt
        ,RegulatoryAgencyName
        ,LostTimeTypeDescription
        ,LostTimeMinutesNumber
        ,FSTDLostTimeMinutesNumber
        ,FSTDTypeName
        ,RootCauseTypeName
        ,RootCauseTypeDescription
        ,HvrChangeTime
        ,StageCreatedDatetime
    )
    SELECT
         IssuePKey
        ,IssueID
        ,IssueDate
        ,IssueTime
        ,SimulatorPKey
        ,SimulatorId
        ,IsAffectedSystem
        ,AffectedConfigurationList
        ,CenterKey
        ,IssueDescription
        ,IssueTypeDescription
        ,IssuePriorityNumber
        ,IssueStatusName
        ,IssueCategoryName
        ,IssueClassificationName
        ,IssueCloseTimestamp
        ,TrainingRestrictionText
        ,IsCriticalTrainingIssue
        ,TrainingCategoryName
        ,JobNumber
        ,IsMMI
        ,DocumentNumber
        ,ChargeCode
        ,DueDate
        ,WrittenByName
        ,WrittenByContactInfoTxt
        ,RegulatoryAgencyName
        ,LostTimeTypeDescription
        ,LostTimeMinutesNumber
        ,FSTDLostTimeMinutesNumber
        ,FSTDTypeName
        ,RootCauseTypeName
        ,RootCauseTypeDescription
        ,HvrChangeTime
        ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
    FROM 
        Issue
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "fsi_issues2.tblIssueObjectXref",
        "sim2.tblConfig",
        "sim2.tblObjectHier",
        "sim2.tblSim",
        "sim2.tblProjSimXref",
        "project2.tblProject",
        "fsi_issues2.tblClassLink",
        "fsi_issues2.tblIssue",
        "fsi_issues2.tblStatus",
        "fsi_issues2.tblClassification",
        "fsi_issues2.tblLostTimeType",
        "employee2.tblLocation",
        "fsi_issues2.tblPriority",
        "fsi_issues2.tblIssueCategory",
        "sim2.tblCertAgency",
        "fsi_issues2.tblIssue_Trng_Category",
        "sim2.tblSystems",
        "fsi_issues2.tblCause",
        "fsi_issues2.tblRootCauseType"
    ],
    unique_key="IssuePKey"
) }}