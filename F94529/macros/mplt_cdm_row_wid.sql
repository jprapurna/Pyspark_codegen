{% macro mplt_cdm_row_wid(in_table_name) %}
-- Derives ROW_WID using lookup and conditional logic
-- source: mapplet mplt_CDM_ROW_WID
-- do not print or log anything here

with
    /* 1) Normalize inputs as CTE to enforce stable names and data types */
    input_data as (
        select 
            {{ in_table_name }} as IN_TABLE_NAME
    ),

    /* 2) Lookup transformation to retrieve maximum ROW_WID and TABLE_NAME */
    lkp_max_row_wid as (
        select
            nvl(max(ROW_WID), 0) as ROW_WID,
            'TABLE_NAME' as TABLE_NAME
        from {{ source('snowflake_cloud_data_warehouse_v2', 'custom_table') }}
        where TABLE_NAME = (select IN_TABLE_NAME from input_data)
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
        from (
            select 
                0 as v2 -- Initialize v2 for conditional logic
            )
    )

select
    ROW_WID
from exp_row_wid
{% endmacro %}