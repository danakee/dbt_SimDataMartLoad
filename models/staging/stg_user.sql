{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageUser',
        tags=['staging', 'user']
    )
}}

{% set source_query %}
    WITH UserData AS (
        SELECT 
             CAST(e.pkey AS int)                        AS EmployeePKey
            ,e.employee_no                              AS EmployeeNumber
            ,TRIM(e.First_Name)                         AS UserFirstName
            ,TRIM(e.Last_Name)                          AS UserLastName
            ,e.hvr_change_time                          AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageLastUpdatedDatetime
        FROM
            {{ source('employee2', 'tblEmployee') }} AS e
        WHERE 
            e.employee_no != 0
            AND NOT (
                LOWER(TRIM(e.Last_Name)) LIKE 'user%' 
                AND LOWER(TRIM(e.First_Name)) = 'test'
            )
    )

    INSERT INTO {{ this }} (
        EmployeePKey,
        EmployeeNumber,
        UserFirstName,
        UserLastName,
        HvrChangeTime,
        StageCreatedDatetime,
        StageLastUpdatedDatetime
    )
    SELECT 
         EmployeePKey
        ,EmployeeNumber
        ,UserFirstName
        ,UserLastName
        ,HvrChangeTime
        ,StageCreatedDatetime
        ,StageLastUpdatedDatetime
    FROM 
        UserData
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "employee2.tblEmployee"
    ],
    unique_key="EmployeePKey"
) }}