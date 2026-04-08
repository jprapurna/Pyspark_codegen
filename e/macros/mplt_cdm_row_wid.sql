{% macro mplt_cdm_row_wid(tgt_table_name, in_table_name) %}
-- Derive ROW_WID using lookup and conditional logic
-- source: mapplet mplt_CDM_ROW_WID
-- do not print or log anything here

with
    /* 1) Normalize inputs as CTE to enforce stable names and data types */
    input_data as (
        select 
            {{ tgt_table_name }} as TGT_TABLE_NAME,
            {{ in_table_name }} as IN_TABLE_NAME
    ),

    /* 2) Lookup transformation to retrieve maximum ROW_WID */
    lkp_max_row_wid as (
        select
            nvl(max(row_wid), 0) as ROW_WID
        from {{ source('snowflake_cloud_data_warehouse', '$$SCHEMA_CDM.$$TGT_TABLE_NAME') }}
        where table_name = (select IN_TABLE_NAME from input_data)
    ),

    /* 3) Expression transformation to calculate ROW_WID */
    exp_row_wid as (
        select
            case 
                when v2 = 0 then (select ROW_WID from lkp_max_row_wid)
                else v2
            end as V1,
            V1 + 1 as V2,
            V2 as ROW_WID
        from input_data
    )

select
    ROW_WID
from exp_row_wid
{% endmacro %}