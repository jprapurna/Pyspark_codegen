{% macro mplt_cdm_row_wid(in_table_name) %}
-- Derives ROW_WID based on the maximum ROW_WID from a lookup table and increments it.
-- source: mapplet mplt_CDM_ROW_WID
-- do not print or log anything here

with
    /* 1) Normalize inputs as CTE to enforce stable names and data types */
    input_data as (
        select 
            {{ in_table_name }} as IN_TABLE_NAME
    ),

    /* 2) Lookup transformation to retrieve the maximum ROW_WID */
    lkp_max_row_wid as (
        select
            coalesce(max(ROW_WID), 0) as ROW_WID,
            'TABLE_NAME' as TABLE_NAME
        from {{ source(var('schema_cdm'), var('tgt_table_name')) }}
        where TABLE_NAME = (select IN_TABLE_NAME from input_data)
    ),

    /* 3) Expression transformation to calculate ROW_WID */
    exp_row_wid as (
        select
            case 
                when 0 = 0 then lkp_max_row_wid.ROW_WID -- Replace with actual logic if needed
                else 0 -- Placeholder for V2 logic
            end as V1,
            V1 + 1 as V2,
            V2 as ROW_WID
        from lkp_max_row_wid
    )

select
    ROW_WID
from exp_row_wid
{% endmacro %}