-- Source node: SQ_CDH_GW_BUR
WITH SQ_CDH_GW_BUR AS (
    SELECT 
        CDH_GW_BUR.POLICY_STATE AS POLICY_STATE, -- string
        CDH_GW_BUR.BUR AS BUR, -- string
        'GWCDH' AS SOURCE_NAME -- string
    FROM {{ source('snowflake_cloud_data_warehouse', 'CDH_GW_BUR') }}
)


-- Lookup node: LKP_W_CLAIM_CD_BUR_SCD3
, LKP_W_CLAIM_CD_BUR_SCD3 AS (
    SELECT 
        INTEGRATION_ID AS LKP_INTEGRATION_ID,
        NEW_BUR AS LKP_NEW_BUR,
        SOURCE_NAME,
        o_BATCH_ID,
        INTEGRATION_ID,
        BUR
    FROM {{ source('snowflake_cloud_data_warehouse', 'CDM.LKP_W_CLAIM_CD_BUR_SCD3') }}
    WHERE INTEGRATION_ID IS NOT NULL
)


-- Transformation node: EXP_BUR
, EXP_BUR AS (
    SELECT 
        POLICY_STATE AS INTEGRATION_ID, -- Maps POLICY_STATE to INTEGRATION_ID
        BUR, -- Passes BUR without transformation
        SOURCE_NAME, -- Passes SOURCE_NAME without transformation
        LKP_ROW_WID, -- Derived or passed from previous node
        LKP_INTEGRATION_ID, -- Derived or passed from previous node
        o_BATCH_ID, -- Derived or passed from previous node
        LKP_NEW_BUR, -- Derived or passed from previous node
        ROW_ID -- Derived or passed from previous node
    FROM 6 -- References the previous node by its ID
)


-- Lookup transformation node: LKP_W_CLAIM_CD_BUR_SCD3
, LKP_W_CLAIM_CD_BUR_SCD3 AS (
    SELECT 
        lkp.LKP_INTEGRATION_ID,
        lkp.LKP_NEW_BUR,
        lkp.SOURCE_NAME,
        lkp.o_BATCH_ID,
        src.INTEGRATION_ID,
        src.BUR
    FROM {{ source('snowflake_cloud_data_warehouse', '$LKP_W_CLAIM_CD_BUR_SCD3') }} lkp
    LEFT JOIN <PREVIOUS_NODE_NAME> src
        ON lkp.LKP_INTEGRATION_ID = src.INTEGRATION_ID
)


-- Transformation node: EXP_Flag
, EXP_Flag AS (
    SELECT 
        -- Derived fields with transformation expressions
        CASE 
            WHEN LKP_ROW_WID IS NULL THEN 'I'
            WHEN MD5(BUR) = MD5(LKP_NEW_BUR) THEN 'NC'
            ELSE 'U'
        END AS o_Flag,
        SYSDATE AS CDM_INSERT_DT,
        SYSDATE AS CDM_UPDATE_DT,
        'W_CLAIM_CD_BUR_SCD3' AS TGT_TABLE_NAME,

        -- Passthrough fields
        LKP_INTEGRATION_ID,
        INTEGRATION_ID AS in_INTEGRATION_ID,
        o_BATCH_ID AS BATCH_ID
    FROM EXP_BUR
    LEFT JOIN LKP_W_CLAIM_CD_BUR_SCD3 ON EXP_BUR.NK_OFFC_ID = LKP_W_CLAIM_CD_BUR_SCD3.NK_OFFC_ID
)


-- Transformation node: rtr_CLM_INSERT_UPD
, rtr_CLM_INSERT_UPD AS (
    SELECT 
        *,
        CASE 
            WHEN o_Flag = 'I' THEN CURRENT_TIMESTAMP
            ELSE NULL
        END AS CDM_INSERT_DT,
        CASE 
            WHEN o_Flag = 'U' THEN CURRENT_TIMESTAMP
            ELSE NULL
        END AS CDM_UPDATE_DT,
        CASE 
            WHEN o_Flag = 'I' OR o_Flag = 'U' THEN TGT_TABLE_NAME
            ELSE NULL
        END AS TGT_TABLE_NAME,
        LKP_INTEGRATION_ID,
        in_INTEGRATION_ID,
        BATCH_ID
    FROM rtr_CLM_INSERT_UPD
    WHERE o_Flag = 'I' OR o_Flag = 'U'
)


-- Transformation node: UPD_BUR
, UPD_BUR AS (
    SELECT 
        'DD_UPDATE' AS Update_Strategy_Expression_78066
    FROM 33 -- Reference to the previous node
)


{{ config(
    materialized='incremental',
    alias='W_CLAIM_CD_BUR_SCD3_U',
    unique_key='ROW_WID',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    merge_update_columns=['ROW_WID']
) }}

final AS (
    SELECT
        ROW_WID
    FROM 18
)

SELECT * FROM final


{{ config(
    materialized='incremental',
    alias='W_CLAIM_CD_BUR_SCD3_I',
    unique_key='o_Flag',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    merge_update_columns=['o_Flag']
) }}

final AS (
    SELECT
        *
    FROM W_CLAIM_CD_BUR_SCD3_I
)

SELECT * FROM final


{{ config(
    materialized='incremental',
    alias='W_CLAIM_CD_BUR_SCD3_U',
    unique_key='ROW_WID',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    merge_update_columns=[]
) }}

final AS (
    SELECT
        *
    FROM UPD_BUR
)

SELECT * FROM final