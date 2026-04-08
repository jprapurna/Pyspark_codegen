{% macro mplt_cdm_batch_id(source_name) %}
-- Derives the maximum batch ID for the given source name, handling null values.
-- source: mapplet mplt_CDM_BATCH_ID
-- do not print or log anything here

with
    /* 1) Normalize inputs as CTE to enforce stable names and data types */
    input_data as (
        select 
            {{ source_name }} as SOURCE_NAME
    ),

    /* 2) Perform lookup on CDM_BATCH_CTRLID table */
    lkp_cdm_batch_ctrlid as (
        select
            SOURCE_NAME,
            max(BATCH_ID) as LKP_BATCH_ID
        from {{ source('Snowflake_Cloud_Data_Warehouse', '$LKP_W_CLAIM_CD_BUR_SCD3') }}
        where SOURCE_NAME = (select SOURCE_NAME from input_data)
        group by SOURCE_NAME
    ),

    /* 3) Check for null values in batch ID and derive output */
    exp_null_check as (
        select
            iif(isnull(LKP_BATCH_ID), -999, LKP_BATCH_ID) as o_BATCH_ID,
            SOURCE_NAME
        from lkp_cdm_batch_ctrlid
    )

select
    o_BATCH_ID
from exp_null_check
{% endmacro %}