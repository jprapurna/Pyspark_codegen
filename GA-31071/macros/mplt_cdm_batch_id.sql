{% macro mplt_cdm_batch_id(in_source_name) %}
-- Derives the maximum batch ID for the given source name, handling null values.
-- source: mapplet mplt_CDM_BATCH_ID
-- do not print or log anything here

with
    /* 1) Normalize inputs as CTE to enforce stable names and data types */
    input_data as (
        select 
            {{ in_source_name }} as SOURCE_NAME
    ),

    /* 2) Lookup maximum batch ID and trimmed source name from CDM_BATCH_CTRLID */
    lkp_cdm_batch_ctrlid as (
        select
            max(BATCH_ID) as LKP_BATCH_ID,
            ltrim(rtrim(SOURCE_NAME)) as SOURCE_NAME
        from {{ source('CDM', 'CDM_BATCH_CTRLID') }}
        where STATUS = {{ var('status_running') }}
        group by SOURCE_NAME
    ),

    /* 3) Check for null values in batch ID and derive output */
    exp_null_check as (
        select
            iif(isnull(lkp_cdm_batch_ctrlid.LKP_BATCH_ID), -999, lkp_cdm_batch_ctrlid.LKP_BATCH_ID) as o_BATCH_ID,
            input_data.SOURCE_NAME
        from input_data
        left join lkp_cdm_batch_ctrlid
        on input_data.SOURCE_NAME = lkp_cdm_batch_ctrlid.SOURCE_NAME
    )

select
    o_BATCH_ID
from exp_null_check
{% endmacro %}