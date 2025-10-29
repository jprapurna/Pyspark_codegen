# Import necessary libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

# Set up logger
logger = logging.getLogger(\"m_4201_dt_chn_interactiongroupstatus\")
logger.setLevel(logging.INFO)

# Configuration
CATALOG = \"CATALOG\"
WORK_SCHEMA = \"TEST\"
FND_SCHEMA = \"TEST\"
PARAMS = {
    \"$$interaction_fnd_db\": f\"{CATALOG}.{FND_SCHEMA}\",
    \"$$load_event_id\": \"12345\"  # Replace with actual load event ID
}
BIG_END = F.lit('3500-01-01')
DEC_BIG_END = F.lit(350036535003659999)

# Define the mapping function
def run_m_4201_dt_chn_interactiongroupstatus():
    try:
        # T1 — Load InteractionGroupStatus1 source
        logger.info(\"Loading source: InteractionGroupStatus1\")
        df_interaction_group_status = spark.table(f\"{CATALOG}.{FND_SCHEMA}.InteractionGroupStatus\")
        df_interaction_group_status = df_interaction_group_status.select(
            F.col(\"InteractionGroupStatus_Id\").cast(\"decimal(18,0)\"),
            F.col(\"InteractionGroup_Id\").cast(\"decimal(18,0)\"),
            F.col(\"BusinessStatus_Cd\").cast(\"string\"),
            F.col(\"BusinessStatus_Tp\").cast(\"smallint\"),
            F.col(\"Status_Cd\").cast(\"string\"),
            F.col(\"Status_Tp\").cast(\"smallint\"),
            F.col(\"StatusReason_Cd\").cast(\"string\"),
            F.col(\"StatusReason_Tp\").cast(\"smallint\"),
            F.col(\"Effective_Dt\").cast(\"date\"),
            F.col(\"Expiration_Dt\").cast(\"date\"),
            F.col(\"Transaction_Ts\").cast(\"timestamp\"),
            F.col(\"Revision_Ts\").cast(\"timestamp\"),
            F.col(\"SequenceStart_It\").cast(\"decimal(18,0)\"),
            F.col(\"SequenceEnd_It\").cast(\"decimal(18,0)\"),
            F.col(\"Source_Cd\").cast(\"string\"),
            F.col(\"LoadEvent_Id\").cast(\"decimal(18,0)\")
        )
        logger.info(f\"Source InteractionGroupStatus1 loaded with {df_interaction_group_status.count()} rows.\")
        df_interaction_group_status.createOrReplaceTempView(\"InteractionGroupStatus1\")

        # T2 — SQL Override in SQ_DateChain
        logger.info(\"Executing SQL override in SQ_DateChain\")
        sql_query = f\"\"\"
        SELECT
            O.InteractionGroupStatus_Id,
            O.InteractionGroup_Id,
            O.NewExpiration_Dt,
            O.NewRevision_Ts,
            O.NewSequenceEnd_It,
            O.Source_Cd
        FROM (
            SELECT 
                InteractionGroupStatus_Id,
                InteractionGroup_Id,
                Source_Cd,
                COALESCE(
                    MIN(Effective_Dt) OVER (
                        PARTITION BY InteractionGroup_Id
                        ORDER BY Effective_Dt ASC, SequenceStart_It ASC
                        ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                    ),
                    DATE '{BIG_END}'
                ) AS NewExpiration_Dt,
                COALESCE(
                    MIN(Transaction_Ts) OVER (
                        PARTITION BY InteractionGroup_Id
                        ORDER BY Transaction_Ts ASC, SequenceStart_It ASC
                        ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                    ),
                    TIMESTAMP '{BIG_END} 00:00:00.000000'
                ) AS NewRevision_Ts,
                COALESCE(
                    MIN(SequenceStart_It) OVER (
                        PARTITION BY InteractionGroup_Id
                        ORDER BY SequenceStart_It ASC
                        ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
                    ),
                    {DEC_BIG_END}
                ) AS NewSequenceEnd_It
            FROM {PARAMS['$$interaction_fnd_db']}.InteractionGroupStatus tgt
            WHERE EXISTS (
                SELECT 1
                FROM {PARAMS['$$interaction_fnd_db']}.InteractionGroupStatus tgt1
                WHERE tgt.InteractionGroup_Id = tgt1.InteractionGroup_Id
                  AND tgt.Source_Cd = tgt1.Source_Cd
                  AND tgt1.LoadEvent_Id >= {PARAMS['$$load_event_id']}
            )
            QUALIFY NOT (
                Expiration_Dt = NewExpiration_Dt
                AND Revision_Ts = NewRevision_Ts
                AND SequenceEnd_It = NewSequenceEnd_It
            )
        ) O
        \"\"\"
        df_sq_date_chain = spark.sql(sql_query)
        logger.info(f\"SQL override executed in SQ_DateChain with {df_sq_date_chain.count()} rows.\")
        df_sq_date_chain.createOrReplaceTempView(\"SQ_DateChain\")

        # T3 — Write to Target InteractionGroupStatus
        logger.info(\"Writing to target: InteractionGroupStatus\")
        target_fqn = f\"{CATALOG}.TEST.InteractionGroupStatus\"
        df_sq_date_chain.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_fqn)
        logger.info(f\"Data written to target table {target_fqn} successfully.\")

    except Exception as e:
        logger.exception(\"Error occurred during mapping execution.\")
        raise

# Run the mapping
run_m_4201_dt_chn_interactiongroupstatus()


# PySpark script for mapping: m_4201_fnd_interactiongroupstatus

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

# Set up logger
logger = logging.getLogger(\"m_4201_fnd_interactiongroupstatus\")
logger.setLevel(logging.INFO)

# Configurations
CATALOG = \"CATALOG\"
WORK_SCHEMA = \"TEST\"
FND_SCHEMA = \"TEST\"
PARAMS = {
    \"$$interaction_work_db\": f\"{CATALOG}.{WORK_SCHEMA}\",
    \"$$interaction_fnd_db\": f\"{CATALOG}.{FND_SCHEMA}\",
    \"$$excluded_sources\": \"'source1', 'source2'\",
    \"$$IGS_excluded_source\": \"'source3', 'source4'\",
    \"$$job_cd\": \"'job_code'\"
}
BIG_END = F.lit('3500-01-01')
DEC_BIG_END = F.lit(999999999999999999)

# Helper function for parameter substitution
def substitute_params(sql_query, params):
    for key, value in params.items():
        sql_query = sql_query.replace(key, value)
    return sql_query

# Main mapping function
def run_m_4201_fnd_interactiongroupstatus():
    try:
        # T1 — SQ_InteractionEvent
        logger.info(\"Starting T1 — SQ_InteractionEvent\")
        sql_query = \"\"\"
            SELECT ROW_NUMBER() OVER (
                ORDER BY P.InteractionGroup_Id, P.Sequence_It
            ) AS NUMBER_SEQ,
            P.InteractionGroup_Id,
            P.BusinessStatus_Cd,
            P.Status_Cd,
            P.StatusReason_Cd,
            P.TransactionEffective_Dt,
            P.Transaction_Ts,
            P.Sequence_It,
            P.Source_Cd,
            P.LoadEvent_Id
            FROM (
                SELECT VIR_TBL.InteractionGroup_Id AS InteractionGroup_Id,
                    VIR_TBL.BusinessStatus_Cd AS BusinessStatus_Cd,
                    VIR_TBL.Status_Cd AS Status_Cd,
                    VIR_TBL.StatusReason_Cd AS StatusReason_Cd,
                    VIR_TBL.Action_Tp AS Action_Tp,
                    VIR_TBL.TransactionEffective_Dt AS TransactionEffective_Dt,
                    VIR_TBL.Transaction_Ts AS Transaction_Ts,
                    VIR_TBL.Sequence_It AS Sequence_It,
                    VIR_TBL.Source_Cd AS Source_Cd,
                    VIR_TBL.LoadEvent_Id AS LoadEvent_Id,
                    ROW_NUMBER() OVER (
                        PARTITION BY InteractionGroup_Id ORDER BY InteractionGroup_Id, Sequence_It
                    ) AS Row_Num,
                    TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd AS Curr_Row,
                    COALESCE(MIN(TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd) OVER (
                        PARTITION BY InteractionGroup_Id 
                        ORDER BY InteractionGroup_Id, Sequence_It 
                        ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
                    ), 0) AS Prev_Row
                FROM (
                    SELECT CASE 
                        WHEN IE.source_cd IN ($$IGS_excluded_source) AND IE.TransactionType_Tp = 9 THEN 'PHONE'
                        WHEN IE.source_cd IN ($$excluded_sources) THEN 'Intct'
                        WHEN (IE.source_cd NOT IN ($$excluded_sources)) AND (IE.source_cd NOT IN ($$IGS_excluded_source)) THEN CASE
                            WHEN IE.TransactionType_Tp = 10 THEN 'EMAIL'
                            WHEN IE.TransactionType_Tp = 11 THEN 'MOBILE'
                            WHEN IE.TransactionType_Tp = 3 THEN CASE 
                                WHEN IE.Action_Tp IN (9, 20, 22) OR IE.ActionResultReason_Tp IN (62, 63, 64, 65) THEN 'Trgt'
                                WHEN IE.Action_Tp IN (5, 21) THEN 'Dstrb'
                                WHEN IE.ActionType_Tp IN (4, 8, 9, 10, 11, 12) THEN 'Intct'
                                WHEN IE.ActionType_Tp = 5 AND IE.Action_Tp IN (26, 27, 28, 29) THEN 'Ident'
                                ELSE 'Notdef'
                            END
                        END
                    END AS BusinessStatus_Cd,
                    IE.InteractionGroup_Id,
                    IE.ActionType_Tp,
                    IE.Action_Tp,
                    IE.ActionResult_Tp,
                    IE.ActionResultDetail_Tp,
                    IE.ActionResultReason_Tp,
                    IE.TransactionEffective_Dt,
                    IE.Transaction_Ts,
                    IE.Sequence_It,
                    IE.Source_Cd,
                    IE.LoadEvent_Id
                    FROM $$interaction_work_db.INTERACTIONEVENT IE
                    INNER JOIN $$interaction_work_db.LOADEVENT_ID_XREF XREF
                    ON TRIM(IE.SOURCE_CD) = TRIM(XREF.Source_Cd)
                    AND IE.LOADEVENT_ID = XREF.LOADEVENT_ID
                    AND XREF.JOB_CD = $$job_cd 
                    WHERE (
                        XREF.SOURCE_CD NOT IN ($$excluded_sources) AND XREF.SOURCE_CD NOT IN ($$IGS_excluded_source)
                        AND NOT EXISTS (
                            SELECT ''
                            FROM $$interaction_work_db.INTERACTIONEVENT IE2
                            WHERE IE.InteractionGroup_Id = IE2.InteractionGroup_Id
                            AND XREF.Source_cd = IE2.Source_Cd
                            AND IE2.LoadEvent_Id = XREF.LoadEvent_Id
                            AND IE.Sequence_It > IE2.Sequence_it
                            AND IE.Action_Tp = 22
                            AND IE2.Transaction_Tp = 14
                            AND (
                                IE2.Action_Tp = 6
                                OR IE2.ActionType_Tp = 9
                                OR IE2.ActionResultDetail_Tp IN (3, 6, 10)
                            )
                        )
                    )
                ) VIR_TBL
            ) P
            WHERE P.CURR_ROW <> P.PREV_ROW
        \"\"\"
        sql_query = substitute_params(sql_query, PARAMS)
        df_sq_interactionevent = spark.sql(sql_query)
        df_sq_interactionevent.createOrReplaceTempView(\"SQ_InteractionEvent\")
        logger.info(f\"T1 — SQ_InteractionEvent completed. Row count: {df_sq_interactionevent.count()}\")

        # T2 — exp_get_tp_values
        logger.info(\"Starting T2 — exp_get_tp_values\")
        df_exp_get_tp_values = df_sq_interactionevent.select(
            F.col(\"LoadEvent_Id\"),
            F.col(\"NUMBER_SEQ\"),
            F.col(\"BusinessStatus_Cd\"),
            F.col(\"Status_Cd\"),
            F.col(\"StatusReason_Cd\"),
            F.lit(-2).alias(\"o_BusinessStatus_Tp\"),
            F.lit(-2).alias(\"o_Status_Tp\"),
            F.lit(-2).alias(\"o_StatusReason_Tp\"),
            F.concat_ws(\"\", F.col(\"LoadEvent_Id\").cast(\"string\"), F.lit(\"000000000000000000\")).alias(\"o_InteractionGroupStatus_Id\")
        )
        df_exp_get_tp_values.createOrReplaceTempView(\"exp_get_tp_values\")
        logger.info(f\"T2 — exp_get_tp_values completed. Row count: {df_exp_get_tp_values.count()}\")

        # T3 — exp_set_default_Tp_values
        logger.info(\"Starting T3 — exp_set_default_Tp_values\")
        df_exp_set_default_tp_values = df_exp_get_tp_values.select(
            F.col(\"BusinessStatus_Tp\"),
            F.col(\"Status_Tp\"),
            F.col(\"StatusReason_Tp\"),
            F.when(F.col(\"BusinessStatus_Tp\").isNotNull(), F.col(\"BusinessStatus_Tp\")).otherwise(F.lit(-2)).alias(\"o_BusinessStatus_Tp\"),
            F.when(F.col(\"Status_Tp\").isNotNull(), F.col(\"Status_Tp\")).otherwise(F.lit(-2)).alias(\"o_Status_Tp\"),
            F.when(F.col(\"StatusReason_Tp\").isNotNull(), F.col(\"StatusReason_Tp\")).otherwise(F.lit(-2)).alias(\"o_StatusReason_Tp\")
        )
        df_exp_set_default_tp_values.createOrReplaceTempView(\"exp_set_default_Tp_values\")
        logger.info(f\"T3 — exp_set_default_Tp_values completed. Row count: {df_exp_set_default_tp_values.count()}\")

        # Final Target Write
        logger.info(\"Writing final output to InteractionGroupStatus\")
        target_fqn = f\"{CATALOG}.{FND_SCHEMA}.InteractionGroupStatus\"
        df_exp_set_default_tp_values.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_fqn)
        logger.info(\"Final output written successfully.\")

    except Exception as e:
        logger.exception(\"Error occurred during mapping execution.\")
        raise

# Execute the mapping
run_m_4201_fnd_interactiongroupstatus()