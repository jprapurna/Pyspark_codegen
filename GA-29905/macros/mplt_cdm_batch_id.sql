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

    /* 2) Lookup maximum batch ID from CDM_BATCH_CTRLID table */
    lkp_cdm_batch_ctrlid as (
        select
            SOURCE_NAME,
            max(BATCH_ID) as LKP_BATCH_ID
        from {{ source('CDM', 'lkp_CDM_BATCH_CTRLID') }}
        where SOURCE_NAME = (select SOURCE_NAME from input_data)
        group by SOURCE_NAME
    ),

    /* 3) Check for null values in batch ID and apply default value */
    exp_null_check as (
        select
            SOURCE_NAME,
            iif(isnull(LKP_BATCH_ID), -999, LKP_BATCH_ID) as o_BATCH_ID
        from lkp_cdm_batch_ctrlid
    )

select
    o_BATCH_ID
from exp_null_check
{% endmacro %}