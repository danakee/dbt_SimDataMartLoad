{{ 
    config(
        materialized='incremental',
        unique_key='EmployeePKey',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        database='SimulationsAnalytics',
        alias='StageUser',
        tags=['staging'],
    )
}}

WITH [UserInitialTransform] AS (
    SELECT 
         [e].[pkey]             AS [EmployeePKey]
        ,[e].[employee_no]      AS [EmployeeNumber]
        ,TRIM([e].[First_Name]) AS [UserFirstName]
        ,TRIM([e].[Last_Name])  AS [UserLastName]
        ,[e].[hvr_change_time]  AS [HvrChangeTime]
    FROM
        {{ source('employee2', 'tblEmployee') }} AS [e]
    WHERE 
        1=1
        AND NOT ([e].[Last_Name] LIKE 'User%' AND [e].[First_Name] = 'Test')
        AND [e].[employee_no] != 0
        {% if is_incremental() %}
        AND [e].[hvr_change_time] > (SELECT MAX([HvrChangeTime]) FROM {{ this }})
        {% endif %}
)
SELECT 
     [EmployeePKey]
    ,[EmployeeNumber]
    ,[UserFirstName]
    ,[UserLastName]
    ,[HvrChangeTime]
FROM
    [UserInitialTransform] AS [u]