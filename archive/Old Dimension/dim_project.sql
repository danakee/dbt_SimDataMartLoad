{{
    config(
        materialized='dim_project',
        database='SimulationsAnalytics',
        alias='DimProject',
        tags=['mart', 'project']
    )
}}

SELECT
     [ProjectPKey]
    ,[ProjectName]
    ,[ProjectDescription]
    ,[ProjectStatusCode]
    ,[ProjectStatusDescription]
    ,[ProjectTypeCode]
    ,[ProjectTypeDescription]
    ,[ReportTypeDescription]
    ,[IsEAC]
    ,[IsStopDR]
    ,[ProjectManagerFirstName]
    ,[ProjectManagerLastName]
    ,[EffectiveDate]
    ,[IsLatest]
    ,[HvrChangeTime]
FROM 
    {{ ref('stg_project') }}