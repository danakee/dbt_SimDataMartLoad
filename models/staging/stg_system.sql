{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageSystem',
        tags=['staging', 'system']
    )
}}

{% set source_query %}
    WITH SystemData AS (
        SELECT 
             s.PKey                AS SystemPKey
            ,s.sysname             AS SystemName
            ,s.sysdesc             AS SystemDescription
            ,s.affectedsystem      AS IsAffectedSystem
            ,s.Active              AS IsSystemActive
            ,sg.grpname            AS SystemGroupName
            ,(SELECT MAX(ct) FROM (VALUES 
                (s.hvr_change_time), 
                (sg.hvr_change_time)
            ) AS ChangeTime(ct))   AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
        FROM 
            {{ source('sim2', 'tblSystems') }} AS s
            LEFT OUTER JOIN {{ source('sim2', 'tblSysGroup') }} AS sg
                ON s.fk_sysgrouppk = sg.PKey
    )

    INSERT INTO {{ this }} (
         SystemPKey
        ,SystemName
        ,SystemDescription
        ,IsAffectedSystem
        ,IsSystemActive
        ,SystemGroupName
        ,HVRChangeTime
        ,StageCreatedDatetime
    )
    SELECT 
         SystemPKey
        ,SystemName
        ,SystemDescription
        ,IsAffectedSystem
        ,IsSystemActive
        ,SystemGroupName
        ,HvrChangeTime
        ,StageCreatedDatetime
    FROM 
        SystemData
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "sim2.tblSystems", 
        "sim2.tblSysGroup"
    ],
    unique_key="SystemPKey"
) }}