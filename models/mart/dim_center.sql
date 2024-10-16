{{
    config(
        materialized='custom_incremental_materialization',
        database='SimulationsAnalytics',
        alias='DimCenter',
        should_log=true,
        tags=['mart', 'dim', 'center']
    )
}}

{% set columns_to_update = [
    'CenterId',
    'CenterName',
    'CenterAddress1',
    'CenterAddress2',
    'CenterCityName',
    'CenterStateCode',
    'CenterPostalCode',
    'CenterCountryName',
    'CenterAirportName',
    'CenterEmailAddress',
    'CenterPhoneNumber',
    'CenterFaxNumber',
    'CenterSpeedDialCode',
    'CenterNightPhoneNumber',
    'CenterShopPhoneNumber',
    'CenterCellPhoneNumber',
    'CenterMaintenancePhoneNumber',
    'CenterShippingAddress1',
    'CenterShippingAddress2',
    'CenterShippingCityName',
    'CenterShippingStateCode',
    'CenterShippingPostalCode',
    'CenterShippingCountryName',
    'IsActive',
    'CenterLocationTypeCode',
    'CenterLocationTypeDescription',
    'CenterRegion',
    'CenterRegionDescription',
    'CenterManagerFirstName',
    'CenterManagerLastName',
    'CenterManagerEmail',
    'HvrChangeTime',
    'EDWLastUpdatedDatetime'
] %}

{% set columns_to_insert = columns_to_update + ['CenterPKey', 'EDWCreatedDatetime'] %}

{% if execute %}
    {% set table_exists = adapter.get_relation(this.database, this.schema, this.identifier) %}
    {% if table_exists is none %}
        {{ log("Creating table " ~ this, info=True) }}
        {% call statement('create_table', fetch_result=False) -%}
            CREATE TABLE {{ this }} (
                CenterKey int IDENTITY(1, 1) CONSTRAINT PK_{{ this.identifier }} PRIMARY KEY,
	            CenterPKey int NOT NULL,
	            CenterID varchar(15) NULL,
	            CenterName varchar(50) NULL,
	            CenterAddress1 varchar(100) NULL,
	            CenterAddress2 varchar(100) NULL,
	            CenterCityName varchar(50) NULL,
	            CenterStateCode varchar(4) NULL,
	            CenterPostalCode varchar(12) NULL,
	            CenterCountryName varchar(30) NULL,
	            CenterAirportName varchar(50) NULL,
	            CenterEmailAddress varchar(50) NULL,
	            CenterPhoneNumber varchar(25) NULL,
	            CenterFaxNumber varchar(25) NULL,
	            CenterSpeedDialCode varchar(6) NULL,
	            CenterNightPhoneNumber varchar(25) NULL,
	            CenterShopPhoneNumber varchar(25) NULL,
	            CenterCellPhoneNumber varchar(25) NULL,
	            CenterMaintenancePhoneNumber varchar(25) NULL,
	            CenterShippingAddress1 varchar(100) NULL,
	            CenterShippingAddress2 varchar(100) NULL,
	            CenterShippingCityName varchar(50) NULL,
	            CenterShippingStateCode varchar(4) NULL,
	            CenterShippingPostalCode varchar(12) NULL,
	            CenterShippingCountryName varchar(30) NULL,
	            IsActive bit NULL,
	            CenterLocationTypeCode varchar(50) NULL,
	            CenterLocationTypeDescription varchar(256) NULL,
	            CenterManagerFirstName varchar(50) NULL,
	            CenterManagerLastName varchar(50) NULL,
	            CenterManagerEmail varchar(255) NULL,
                HvrChangeTime datetimeoffset(3) NULL,
                EDWCreatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWCreatedDatetime DEFAULT (sysdatetimeoffset()),
                EDWLastUpdatedDatetime datetimeoffset(3) NOT NULL CONSTRAINT DF_{{ this.identifier }}_EDWLastUpdatedDatetime DEFAULT (sysdatetimeoffset())
            ) ON [PRIMARY];
        {%- endcall %}
    {% endif %}
{% endif %}

{{ merge_dimension(
    target_table=this,
    source_table=ref('stg_center'),
    unique_key='CenterPKey',
    columns_to_update=columns_to_update,
    columns_to_insert=columns_to_insert
) }}