{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='EmployeePKey',
        compare_columns=[
            'EmployeeNumber', 
            'UserFirstName', 
            'UserLastName', 
            'HvrChangeTime'
        ],
        merge_update_columns=[
            'EmployeeNumber',
            'UserFirstName',
            'UserLastName',
            'HvrChangeTime',
            'StageLastUpdatedDatetime'
        ],
        database='SimulationsAnalyticsStage',
        alias='StageUser',
        tags=['staging', 'user']
    )
}}

WITH [UserData] AS (
SELECT 
     CAST([e].[pkey] AS int)                        AS [EmployeePKey]
    ,[e].[employee_no]                              AS [EmployeeNumber]
    ,TRIM([e].[First_Name])                         AS [UserFirstName]
    ,TRIM([e].[Last_Name])                          AS [UserLastName]
    ,[e].[hvr_change_time]                          AS [HvrChangeTime]
    ,LOWER(TRIM([e].[First_Name]))                  AS [UserFirstNameLower]
    ,LOWER(TRIM([e].[Last_Name]))                   AS [UserLastNameLower]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [StageCreatedDatetime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [StageLastUpdatedDatetime]
FROM
    {{ source('employee2', 'tblEmployee') }} AS [e]
WHERE 
    1=1
)

SELECT 
     [EmployeePKey]
    ,[EmployeeNumber]
    ,[UserFirstName]
    ,[UserLastName]
    ,[HvrChangeTime]
    ,[StageCreatedDatetime]
    ,[StageLastUpdatedDatetime]
FROM 
    [UserData]
WHERE
    1=1
    AND [EmployeeNumber] != 0
    AND NOT (
        [UserLastNameLower] LIKE 'user%' 
        AND [UserFirstNameLower] = 'test')
    {% if is_incremental() %}
    AND [HvrChangeTime] > (SELECT ISNULL(MAX([HvrChangeTime]), '1900-01-01') FROM {{ this }})
    {% endif %};
