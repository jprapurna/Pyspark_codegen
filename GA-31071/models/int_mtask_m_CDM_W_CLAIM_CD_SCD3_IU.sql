-- Source node: SQ_CDH_GW_BUR
WITH SQ_CDH_GW_BUR AS (
    SELECT 
        POLICY_STATE AS POLICY_STATE, -- string
        BUR AS BUR,                   -- string
        'GWCDH' AS SOURCE_NAME        -- string
    FROM 
        {{ source(var('SCHEMA_CDH_GWODS'), 'CDH_GW_BUR') }}
)


-- Lookup source node: LKP_W_CLAIM_CD_BUR_SCD3
, LKP_W_CLAIM_CD_BUR_SCD3 AS (
    SELECT 
        W_CLAIM_CD_BUR_SCD3.ROW_WID AS LKP_ROW_WID, -- decimal
        W_CLAIM_CD_BUR_SCD3.INTEGRATION_ID AS LKP_INTEGRATION_ID, -- string
        W_CLAIM_CD_BUR_SCD3.NEW_BUR AS LKP_NEW_BUR -- string
    FROM 
        CDM.W_CLAIM_CD_BUR_SCD3
)


-- Transformation node: EXP_BUR
, EXP_BUR AS (
    SELECT 
        CASE 
            WHEN POLICY_STATE IS NOT NULL THEN POLICY_STATE
            WHEN BUR IS NOT NULL THEN BUR
            WHEN SOURCE_NAME IS NOT NULL THEN SOURCE_NAME
            ELSE NULL
        END AS INTEGRATION_ID
    FROM 6
)


-- Lookup transformation node: LKP_W_CLAIM_CD_BUR_SCD3
, LKP_W_CLAIM_CD_BUR_SCD3 AS (
    SELECT 
        LKP_INTEGRATION_ID,
        LKP_ROW_WID,
        LKP_NEW_BUR
    FROM {{ source('CDM', 'W_CLAIM_CD_BUR_SCD3') }}
    WHERE LKP_INTEGRATION_ID = in_INTEGRATION_ID
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
        'W_CLAIM_CD_BUR_SCD3' AS TGT_TABLE_NAME
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
        END AS TGT_TABLE_NAME
    FROM 14 -- Reference to the previous node
    WHERE o_Flag = 'I' OR o_Flag = 'U'
)


-- Transformation node: UPD_BUR
, UPD_BUR AS (
    SELECT 
        'DD_UPDATE' AS Update_Strategy_Expression_78066
    FROM 33
)


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