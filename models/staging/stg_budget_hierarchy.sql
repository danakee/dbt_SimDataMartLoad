{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageBudgetHierarchy',
        tags=['staging', 'budget']
    )
}}

{% set source_query %}
    WITH SourceBudgetData AS (
        SELECT
             'Non-Labor'        AS BudgetCategoryTypeCode
            ,big.Name           AS BudgetCategoryName
            ,bi.Name            AS BudgetItemCode
            ,bi.Description     AS BudgetItemName
            ,(SELECT MAX(ct) FROM (VALUES 
                (big.hvr_change_time), 
                (bi.hvr_change_time)
            ) AS ChangeTime(ct))    AS HvrChangeTime
        FROM 
            {{ source('eac_report', 'MaterialBudgetItem') }} AS bi
            LEFT OUTER JOIN {{ source('eac_report', 'MaterialBudgetItemGroup') }} AS big
                ON bi.MaterialBudgetItemGroupId = big.PKey
        WHERE 
            big.PKey BETWEEN 1 AND 7

        UNION ALL

        SELECT 
             'Labor'                        AS BudgetCategoryTypeCode
            ,p.Name + ' Labor'              AS BudgetCategoryName
            ,d.Name                         AS BudgetItemCode
            ,d.Name                         AS BudgetItemName
            ,(SELECT MAX(ct) FROM (VALUES 
                (d.hvr_change_time), 
                (p.hvr_change_time)
            ) AS ChangeTime(ct))            AS HvrChangeTime
        FROM 
            {{ source('eac_report', 'EACDept') }} AS d
            LEFT OUTER JOIN {{ source('eac_report', 'DeptPool') }} AS p
                ON d.DeptPoolId = p.PKey
    ),

    BudgetCategoryAll AS (
        SELECT DISTINCT 
             BudgetCategoryTypeCode
            ,'ALL'                          AS BudgetCategoryName
            ,'ALL'                          AS BudgetItemCode
            ,'Roll-up for category type'    AS BudgetItemName
            ,MAX(HvrChangeTime)             AS HvrChangeTime
        FROM 
            SourceBudgetData AS sbd
        WHERE NOT EXISTS (
            SELECT 
                1 
            FROM 
                SourceBudgetData AS d
            WHERE 
                d.BudgetCategoryTypeCode = sbd.BudgetCategoryTypeCode
                AND d.BudgetCategoryName = 'ALL'
                AND d.BudgetItemCode = 'ALL')
        GROUP BY
            BudgetCategoryTypeCode
    ),

    BudgetItemAll AS (
        SELECT DISTINCT 
             BudgetCategoryTypeCode
            ,BudgetCategoryName
            ,'ALL'                          AS BudgetItemCode
            ,'Roll-up for category level'   AS BudgetItemName
            ,MAX(HvrChangeTime)             AS HvrChangeTime
        FROM 
            SourceBudgetData AS sbd
        WHERE NOT EXISTS (
            SELECT 
                1 
            FROM 
                SourceBudgetData AS d
            WHERE 
                d.BudgetCategoryTypeCode = sbd.BudgetCategoryTypeCode  
                AND d.BudgetCategoryName = sbd.BudgetCategoryName
                AND d.BudgetItemCode = 'ALL')
        GROUP BY
            BudgetCategoryTypeCode,
            BudgetCategoryName
    ),

    BudgetHierarchy AS (
        SELECT * FROM SourceBudgetData
        UNION
        SELECT * FROM BudgetCategoryAll
        UNION
        SELECT * FROM BudgetItemAll
    )

    INSERT INTO {{ this }} (
         BudgetCategoryTypeCode
        ,BudgetCategoryName
        ,BudgetItemCode
        ,BudgetItemName
        ,HvrChangeTime
        ,StageCreatedDatetime
    )
    SELECT 
         BudgetCategoryTypeCode
        ,BudgetCategoryName
        ,BudgetItemCode
        ,BudgetItemName
        ,HvrChangeTime
        ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
    FROM 
        BudgetHierarchy
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "eac_report.MaterialBudgetItem",
        "eac_report.MaterialBudgetItemGroup",
        "eac_report.EACDept",
        "eac_report.DeptPool"
    ],
    unique_key=["BudgetCategoryTypeCode", "BudgetCategoryName", "BudgetItemCode"]
) }}