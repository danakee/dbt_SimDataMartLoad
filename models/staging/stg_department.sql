{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageDepartment',
        tags=['staging', 'department']
    )
}}

{% set source_query %}
    WITH DepartmentData AS (
        SELECT 
             d.PKey                                     AS DepartmentPKey
            ,d.Id                                       AS DepartmentId
            ,l.Id                                       AS LocationId
            ,TRIM(l.Name)                               AS DepartmentLocationName
            ,TRIM(d.Name)                               AS DepartmentName
            ,ed.Name                                    AS EACDepartmentName
            ,d.Eac_Group_Id                             AS EACGroupId
            ,eg.eac_group_desc                          AS EACGroupName
            ,dv.id                                      AS DivisionIdCode
            ,dv.name                                    AS DivisionName
            ,pdv.id                                     AS ParentDivisionIdCode
            ,pdv.name                                   AS ParentDivisionName
            ,lt.name                                    AS LocationTypeCode
            ,lt.description                             AS LocationTypeDescription
            ,d.FK_EmpId                                 AS DepartmentHeadId
            ,ed.DeptPoolId                              AS DepartmentPoolId
            ,dp.Name                                    AS DepartmentPoolName
            ,CASE
                WHEN ISNULL(TRIM(e.First_Name), '') = '' AND ISNULL(TRIM(e.Last_Name), '') = '' THEN ''
                WHEN ISNULL(TRIM(e.Last_Name), '') = '' THEN NULLIF(TRIM(e.First_Name), '')
                WHEN ISNULL(TRIM(e.First_Name), '') = '' THEN NULLIF(TRIM(e.Last_Name), '')
                ELSE ISNULL(TRIM(e.Last_Name), '') + ', ' + NULLIF(TRIM(e.First_Name), '')
             END                                        AS DepartmentHeadName
            ,jt.Title                                   AS DepartmentHeadTitle
            ,d.Active                                   AS IsActive
            ,(SELECT MAX(ct) FROM (VALUES 
                (d.hvr_change_time), 
                (l.hvr_change_time),
                (lt.hvr_change_time),
                (dv.hvr_change_time),
                (pdv.hvr_change_time),
                (e.hvr_change_time),
                (jt.hvr_change_time),
                (eg.hvr_change_time),
                (ed.hvr_change_time),
                (dp.hvr_change_time)
            ) AS ChangeTime(ct))                        AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
        FROM 
            {{ source('employee2', 'tblDepartment') }} AS d
            LEFT JOIN {{ source('employee2', 'tblLocation') }} AS l
                ON d.FK_Location = l.PKey
            LEFT JOIN {{ source('employee2', 'tblLocationType') }} AS lt 
                ON l.FK_LocType = lt.PKey
            LEFT JOIN {{ source('employee2', 'tblDivision') }} AS dv
                ON l.FK_Division = dv.PKey
            LEFT JOIN {{ source('employee2', 'tblDivision') }} AS pdv
                ON dv.fk_parentdiv = pdv.PKey
            LEFT JOIN {{ source('employee2', 'tblEmployee') }} AS e
                ON d.FK_EmpId = e.PKey
            LEFT JOIN {{ source('employee2', 'tblEmpJobTitle') }} AS jt
                ON e.FK_JobTitle = jt.PKey
            LEFT JOIN {{ source('eac_report', 'EAC_Groups') }} AS eg
                ON d.Eac_Group_Id = eg.eac_group_PKey
            LEFT JOIN {{ source('eac_report', 'EACDept') }} AS ed
                ON d.Id = CAST(ed.Dept AS varchar(50)) 
            LEFT JOIN {{ source('eac_report', 'DeptPool') }} AS dp
                ON ed.DeptPoolId = dp.PKey
        WHERE 
            d.Active = 1
            AND d.Name IS NOT NULL
            AND (TRIM(d.Id) <> '' OR d.Id IS NULL)
    )

    INSERT INTO {{ this }} (
         DepartmentPKey
        ,DepartmentId
        ,LocationId
        ,DepartmentLocationName
        ,DepartmentName
        ,EACDepartmentName
        ,EACGroupId
        ,EACGroupName
        ,DivisionIdCode
        ,DivisionName
        ,ParentDivisionIdCode
        ,ParentDivisionName
        ,LocationTypeCode
        ,LocationTypeDescription
        ,DepartmentHeadId
        ,DepartmentPoolId
        ,DepartmentPoolName
        ,DepartmentHeadName
        ,DepartmentHeadTitle
        ,IsActive
        ,HvrChangeTime
        ,StageCreatedDatetime
    )
    SELECT 
         DepartmentPKey
        ,DepartmentId
        ,LocationId
        ,DepartmentLocationName
        ,DepartmentName
        ,EACDepartmentName
        ,EACGroupId
        ,EACGroupName
        ,DivisionIdCode
        ,DivisionName
        ,ParentDivisionIdCode
        ,ParentDivisionName
        ,LocationTypeCode
        ,LocationTypeDescription
        ,DepartmentHeadId
        ,DepartmentPoolId
        ,DepartmentPoolName
        ,DepartmentHeadName
        ,DepartmentHeadTitle
        ,IsActive
        ,HvrChangeTime
        ,StageCreatedDatetime
    FROM 
        DepartmentData
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "employee2.tblDepartment", 
        "employee2.tblLocation", 
        "employee2.tblLocationType", 
        "employee2.tblDivision", 
        "employee2.tblEmployee", 
        "employee2.tblEmpJobTitle", 
        "eac_report.EAC_Groups", 
        "eac_report.EACDept", 
        "eac_report.DeptPool"
    ],
    unique_key="DepartmentPKey"
) }}