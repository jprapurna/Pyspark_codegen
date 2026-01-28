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
            max(batch_id) as lkp_batch_id,
            ltrim(rtrim(source_name)) as source_name
        from {{ source('snowflake_cloud_data_warehouse_v2', 'cdm_batch_ctrlid') }}
        where status = {{ var('status_running') }}
        group by source_name
    ),
    
    /* 3) Check for null values in batch ID and apply default value */
    exp_null_check as (
        select
            iif(isnull(lkp_batch_id), -999, lkp_batch_id) as o_batch_id,
            source_name
        from input_data
        left join lkp_cdm_batch_ctrlid
        on input_data.source_name = lkp_cdm_batch_ctrlid.source_name
    )

select
    o_batch_id,
    source_name
from exp_null_check
{% endmacro %}