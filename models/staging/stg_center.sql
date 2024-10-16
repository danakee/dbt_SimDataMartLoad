{{
    config(
        materialized='custom_truncate_and_load',
        database='SimulationsAnalyticsStage',
        alias='StageCenter',
        tags=['staging', 'center']
    )
}}

{% set source_query %}
    WITH CenterInfo AS (
        SELECT 
             l.PKey                 AS CenterPKey
            ,l.Id                   AS CenterId
            ,l.Name                 AS CenterName
            ,l.address1             AS CenterAddress1
            ,l.address2             AS CenterAddress2
            ,l.city                 AS CenterCityName
            ,l.state                AS CenterStateCode
            ,l.zip                  AS CenterPostalCode
            ,l.country              AS CenterCountryName
            ,l.airport              AS CenterAirportName
            ,l.Email                AS CenterEmailAddress
            ,l.phone                AS CenterPhoneNumber
            ,l.fax                  AS CenterFaxNumber
            ,l.speedial             AS CenterSpeedDialCode
            ,l.phone_night          AS CenterNightPhoneNumber
            ,l.shop_number          AS CenterShopPhoneNumber
            ,l.cell_phone           AS CenterCellPhoneNumber
            ,l.maint_phone          AS CenterMaintenancePhoneNumber
            ,l.shipping_address1    AS CenterShippingAddress1
            ,l.shipping_address2    AS CenterShippingAddress2
            ,l.shipping_city        AS CenterShippingCityName
            ,l.shipping_state       AS CenterShippingStateCode
            ,l.shipping_zip         AS CenterShippingPostalCode
            ,l.shipping_country     AS CenterShippingCountryName
            ,l.Active               AS IsActive
            ,TRIM(lt.name)          AS CenterLocationTypeCode
            ,TRIM(lt.description)   AS CenterLocationTypeDescription
            ,TRIM(r.name)           AS CenterRegion
            ,TRIM(r.description)    AS CenterRegionDescription
            ,TRIM(mgr.First_Name)   AS CenterManagerFirstName
            ,TRIM(mgr.Last_Name)    AS CenterManagerLastName
            ,TRIM(mgr.Email)        AS CenterManagerEmail
            ,(SELECT MAX(ct) FROM (VALUES 
                (l.hvr_change_time),
                (lt.hvr_change_time),
                (mgr.hvr_change_time),
                (r.hvr_change_time)
            ) AS ChangeTime(ct))    AS HvrChangeTime
            ,CAST(sysdatetimeoffset() AS datetimeoffset(3)) AS StageCreatedDatetime
        FROM 
            {{ source('employee2', 'tblLocation') }} AS l
            LEFT JOIN {{ source('employee2', 'tblLocationType') }} AS lt
                ON l.FK_LocType = lt.PKey
            LEFT JOIN {{ source('employee2', 'tblEmployee') }} AS mgr
                ON l.FK_Mgr = mgr.PKey
            LEFT JOIN {{ source('employee2', 'tblRegion') }} AS r
                ON l.FK_Region = r.PKey
        WHERE 
            l.FK_LocType = 1
    )

    INSERT INTO {{ this }} (
         CenterPKey
        ,CenterId
        ,CenterName
        ,CenterAddress1
        ,CenterAddress2
        ,CenterCityName
        ,CenterStateCode
        ,CenterPostalCode
        ,CenterCountryName
        ,CenterAirportName
        ,CenterEmailAddress
        ,CenterPhoneNumber
        ,CenterFaxNumber
        ,CenterSpeedDialCode
        ,CenterNightPhoneNumber
        ,CenterShopPhoneNumber
        ,CenterCellPhoneNumber
        ,CenterMaintenancePhoneNumber
        ,CenterShippingAddress1
        ,CenterShippingAddress2
        ,CenterShippingCityName
        ,CenterShippingStateCode
        ,CenterShippingPostalCode
        ,CenterShippingCountryName
        ,IsActive
        ,CenterLocationTypeCode
        ,CenterLocationTypeDescription
        ,CenterRegion
        ,CenterRegionDescription
        ,CenterManagerFirstName
        ,CenterManagerLastName
        ,CenterManagerEmail
        ,HvrChangeTime
        ,StageCreatedDatetime
    )
    SELECT 
         CenterPKey
        ,CenterId
        ,CenterName
        ,CenterAddress1
        ,CenterAddress2
        ,CenterCityName
        ,CenterStateCode
        ,CenterPostalCode
        ,CenterCountryName
        ,CenterAirportName
        ,CenterEmailAddress
        ,CenterPhoneNumber
        ,CenterFaxNumber
        ,CenterSpeedDialCode
        ,CenterNightPhoneNumber
        ,CenterShopPhoneNumber
        ,CenterCellPhoneNumber
        ,CenterMaintenancePhoneNumber
        ,CenterShippingAddress1
        ,CenterShippingAddress2
        ,CenterShippingCityName
        ,CenterShippingStateCode
        ,CenterShippingPostalCode
        ,CenterShippingCountryName
        ,IsActive
        ,CenterLocationTypeCode
        ,CenterLocationTypeDescription
        ,CenterRegion
        ,CenterRegionDescription
        ,CenterManagerFirstName
        ,CenterManagerLastName
        ,CenterManagerEmail
        ,HvrChangeTime
        ,StageCreatedDatetime
    FROM 
        CenterInfo
    WHERE 
        HvrChangeTime > '{{ get_stage_last_update(this.name) }}';
{% endset %}

{{ load_stage_table(
    target_table=this,
    source_query=source_query,
    source_tables=[
        "employee2.tblLocation", 
        "employee2.tblLocationType", 
        "employee2.tblEmployee", 
        "employee2.tblRegion"
    ],
    unique_key="CenterPKey"
) }}