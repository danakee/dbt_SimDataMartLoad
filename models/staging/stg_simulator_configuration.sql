{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageSimulatorConfiguration',
        tags=['staging', 'simulatorconfiguration']
    )
}}

{% set source_query %}
    WITH SimulationConfiguration AS (
        SELECT 
             CAST(c.PKey AS int)                        AS SimulatorConfigurationPKey
            ,s.PKey                                     AS SimulatorPKey
            ,c.name                                     AS ConfigurationName
            ,l.Name                                     AS LocationName
            ,c.[default]                                AS IsDefault
            ,c.createdt                                 AS SourceCreatedDatetime
            ,c.lastmoddt                                AS SourceLastUpdatedDatetime
            ,(SELECT MAX(ct) FROM (VALUES 
                    (s.hvr_change_time),
                    (so.hvr_change_time),
                    (scc.hvr_change_time),
                    (c.hvr_change_time),
                    (l.hvr_change_time)
            ) AS ChangeTime(ct))                        AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageLastUpdatedDatetime
        FROM
            {{ source('sim2', 'tblSim') }} AS s
            JOIN {{ source('sim2', 'tblObjectHier') }} AS so
                ON s.PKey = so.fk_head
            JOIN {{ source('sim2', 'tblClassLink') }} AS scc
                ON  so.fk_classlink = scc.PKey
                AND scc.name = 'sim_to_cfg'
            JOIN {{ source('sim2', 'tblConfig') }} AS c
                ON so.fk_tail = c.PKey
            LEFT JOIN {{ source('employee2', 'tblLocation') }} AS l
                ON s.loc_id = l.PKey
        WHERE   
            1=1
    )

    INSERT INTO {{ this }} (
        SimulatorConfigurationPKey,
        SimulatorPKey,
        ConfigurationName,
        LocationName,
        IsDefault,
        SourceCreatedDatetime,
        SourceLastUpdatedDatetime,
        HvrChangeTime,
        StageCreatedDatetime,
        StageLastUpdatedDatetime
    )
    SELECT 
         SimulatorConfigurationPKey
        ,SimulatorPKey
        ,ConfigurationName
        ,LocationName
        ,IsDefault
        ,SourceCreatedDatetime
        ,SourceLastUpdatedDatetime
        ,HvrChangeTime
        ,StageCreatedDatetime
        ,StageLastUpdatedDatetime
    FROM 
        SimulationConfiguration
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "sim2.tblSim",
        "sim2.tblObjectHier",
        "sim2.tblClassLink",
        "sim2.tblConfig",
        "employee2.tblLocation"
    ],
    unique_key="SimulatorConfigurationPKey"
) }}