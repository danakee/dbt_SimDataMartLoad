{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageDepartmentGroup',
        tags=['staging', 'department', 'group']
    )
}}

{% set source_query %}
    WITH DepartmentGroup AS (
        SELECT 
             d.PKey         AS DepartmentPKey
            ,d.Id           AS DepartmentId
            ,dg.Name        AS DepartmentGroupName
            ,dg.Description AS DepartmentGroupDescription
            ,(SELECT MAX(ct) FROM (VALUES 
                    (dg.hvr_change_time),
                    (d.hvr_change_time)
            ) AS ChangeTime(ct)) AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
        FROM 
            {{ source('employee2', 'tblDeptGroup') }} AS dg
            INNER JOIN {{ source('employee2', 'tblDepartment') }} AS d
                ON dg.FK_DeptId = d.PKey
    )

    INSERT INTO {{ this }} (
         DepartmentPKey
        ,DepartmentId
        ,DepartmentGroupName
        ,DepartmentGroupDescription
        ,HvrChangeTime
        ,StageCreatedDatetime
    )
    SELECT 
         DepartmentPKey
        ,DepartmentId
        ,DepartmentGroupName
        ,DepartmentGroupDescription
        ,HvrChangeTime
        ,StageCreatedDatetime
    FROM 
        DepartmentGroup
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "employee2.tblDeptGroup", 
        "employee2.tblDepartment"
    ],
    unique_key="DepartmentPKey"
) }}