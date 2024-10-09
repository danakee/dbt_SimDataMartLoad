{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='ProjectPKey',
        compare_columns=[
            'ProjectName',
            'ProjectDescription',
            'ProjectStatusCode',
            'ProjectStatusDescription',
            'ProjectTypeCode',
            'ProjectTypeDescription',
            'ReportTypeDescription',
            'IsEAC',
            'IsStopDR',
            'ProjectManagerFirstName',
            'ProjectManagerLastName',
            'EffectiveDate',
            'IsLatest',
            'ProjectCreateDate',
            'ProjectUpdateDate',
            'ProjectVersion',
            'HvrChangeTime'
        ],
        merge_update_columns=[
            'ProjectName',
            'ProjectDescription',
            'ProjectStatusCode',
            'ProjectStatusDescription',
            'ProjectTypeCode',
            'ProjectTypeDescription',
            'ReportTypeDescription',
            'IsEAC',
            'IsStopDR',
            'ProjectManagerFirstName',
            'ProjectManagerLastName',
            'EffectiveDate',
            'IsLatest',
            'ProjectCreateDate',
            'ProjectUpdateDate',
            'ProjectVersion',
            'HvrChangeTime',
            'EDWLastUpdatedDatetime'
        ],
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
    ,[ProjectCreateDate]
    ,[ProjectUpdateDate]
    ,[ProjectVersion]
    ,[HvrChangeTime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWCreatedDatetime]
    ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS [EDWLastUpdatedDatetime]
FROM 
    {{ ref('stg_project') }}
WHERE
    1=1
    {% if is_incremental() %}
    AND [HvrChangeTime] > (SELECT ISNULL(MAX([HvrChangeTime]), '1900-01-01') FROM {{ this }})
    {% endif %}
