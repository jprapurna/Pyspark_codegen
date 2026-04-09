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

    /* 2) Lookup transformation to retrieve maximum batch ID */
    lkp_cdm_batch_ctrlid as (
        select
            SOURCE_NAME,
            max(BATCH_ID) as LKP_BATCH_ID
        from {{ source('CDM', 'lkp_CDM_BATCH_CTRLID') }}
        where SOURCE_NAME = (select SOURCE_NAME from input_data)
        group by SOURCE_NAME
    ),

    /* 3) Expression transformation to handle null values */
    exp_null_check as (
        select
            SOURCE_NAME,
            case 
                when LKP_BATCH_ID is null then -999
                else LKP_BATCH_ID
            end as o_BATCH_ID
        from lkp_cdm_batch_ctrlid
    )

select
    o_BATCH_ID
from exp_null_check
{% endmacro %}