{{
    config(
        materialized='dim_user',
        database='SimulationsAnalytics',
        alias='DimUser',
        tags=['mart']
    )
}}

SELECT
     [EmployeePKey]
    ,[EmployeeNumber]
    ,[UserFirstName]
    ,[UserLastName]
    ,[HvrChangeTime]
FROM 
    {{ ref('stg_user') }}
