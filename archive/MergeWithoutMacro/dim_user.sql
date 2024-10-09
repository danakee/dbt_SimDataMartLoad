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
            'EDWLastUpdatedDatetime'
        ],
        database='SimulationsAnalytics',
        alias='DimUser',
        tags=['mart', 'user'],
    )
}}

SELECT 
     [EmployeePKey]
    ,[EmployeeNumber]
    ,[UserFirstName]
    ,[UserLastName]
    ,[HvrChangeTime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWCreatedDatetime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWLastUpdatedDatetime]
FROM 
    {{ ref('stg_user') }}
WHERE
    1=1
    {% if is_incremental() %}
    AND [HvrChangeTime] > (SELECT ISNULL(MAX([HvrChangeTime]), '1900-01-01') FROM {{ this }})
    {% endif %}