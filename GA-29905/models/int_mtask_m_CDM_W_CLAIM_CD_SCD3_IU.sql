-- Source node: SQ_CDH_GW_BUR
WITH SQ_CDH_GW_BUR AS (
    SELECT 
        POLICY_STATE AS POLICY_STATE, -- string
        BUR AS BUR,                   -- string
        'GWCDH' AS SOURCE_NAME        -- string
    FROM {{ source('SCHEMA_CDH_GWODS', 'CDH_GW_BUR') }}
)


-- Lookup transformation node: LKP_W_CLAIM_CD_BUR_SCD3
, LKP_W_CLAIM_CD_BUR_SCD3 AS (
    SELECT 
        src.INTEGRATION_ID AS LKP_INTEGRATION_ID,
        src.BUR AS LKP_NEW_BUR,
        'CDM.LKP_W_CLAIM_CD_BUR_SCD3' AS SOURCE_NAME,
        {{ var('BATCH_ID') }} AS o_BATCH_ID,
        src.INTEGRATION_ID,
        src.BUR
    FROM {{ source('CDM', 'W_CLAIM_CD_BUR_SCD3') }} src
    WHERE src.INTEGRATION_ID IS NOT NULL
)


-- Transformation node: EXP_BUR
, EXP_BUR AS (
    SELECT 
        POLICY_STATE AS INTEGRATION_ID, -- Mapping POLICY_STATE to INTEGRATION_ID
        BUR, -- Passing BUR without transformation
        SOURCE_NAME, -- Passing SOURCE_NAME without transformation
        LKP_ROW_WID, -- Derived or passed from previous node
        LKP_INTEGRATION_ID, -- Derived or passed from previous node
        o_BATCH_ID, -- Derived or passed from previous node
        LKP_NEW_BUR, -- Derived or passed from previous node
        ROW_ID -- Derived or passed from previous node
    FROM 6 -- Reference to the previous node
)


-- Lookup transformation node: LKP_W_CLAIM_CD_BUR_SCD3
, LKP_W_CLAIM_CD_BUR_SCD3 AS (
    SELECT 
        previous_node.INTEGRATION_ID,
        previous_node.BUR,
        previous_node.o_BATCH_ID,
        previous_node.SOURCE_NAME,
        lookup_table.LKP_ROW_WID,
        lookup_table.LKP_INTEGRATION_ID,
        lookup_table.LKP_NEW_BUR
    FROM previous_node
    LEFT JOIN {{ source('CDM', 'W_CLAIM_CD_BUR_SCD3') }} AS lookup_table
        ON lookup_table.LKP_INTEGRATION_ID = previous_node.INTEGRATION_ID
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
        
        -- Renamed fields
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
    WHERE o_Flag IN ('I', 'U')
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
    merge_update_columns=['ROW_WID']
) }}

final AS (
    SELECT
        *
    FROM 18
)

SELECT * FROM final


{{ config(
    materialized='incremental',
    alias='W_CLAIM_CD_BUR_SCD3',   -- Target table name
    unique_key='o_Flag',           -- Unique key field
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    merge_update_columns=['o_Flag'] -- All target fields
) }}

final AS (
, previous_node_23 AS (
        SELECT *
        FROM 23 -- Replace with actual CTE name for node 23
    ),
    previous_node_33 AS (
        SELECT *
        FROM 33 -- Replace with actual CTE name for node 33
    )
    SELECT
        *
    FROM previous_node_23
    UNION ALL
    SELECT
        *
    FROM previous_node_33
)

SELECT * FROM final