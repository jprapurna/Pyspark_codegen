import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SNOWFLAKE_SECRET_NAME = "REPLACE_WITH_SNOWFLAKE_SECRET_NAME"
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)

from pyspark.sql.functions import col, trim, upper, broadcast, substring, length, when, concat, lit

# Top-of-script placeholders for mapping parameters, connections and outputs
INTERACTION_WORK_DB = "REPLACE_WITH_INTERACTION_WORK_DB"
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB"
JOB_CD = "REPLACE_WITH_JOB_CD"
EXCLUDED_SOURCES = "REPLACE_WITH_EXCLUDED_SOURCES"
IGS_EXCLUDED_SOURCE = "REPLACE_WITH_IGS_EXCLUDED_SOURCE"
REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_URL = "REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_URL"
REPLACE_WITH_RL_EDW_EINTERACTION_USER = "REPLACE_WITH_RL_EDW_EINTERACTION_USER"
REPLACE_WITH_RL_EDW_EINTERACTION_PASSWORD = "REPLACE_WITH_RL_EDW_EINTERACTION_PASSWORD"

# Lookup source connection placeholders (read the whole small lookup table and broadcast-join)
LKP_XREF_XT_JDBC_URL = "REPLACE_WITH_LKP_XREF_XT_JDBC_URL"
LKP_XREF_XT_USER = "REPLACE_WITH_LKP_XREF_XT_USER"
LKP_XREF_XT_PASSWORD = "REPLACE_WITH_LKP_XREF_XT_PASSWORD"
LKP_XREF_XT_TABLE = "REPLACE_WITH_LKP_XREF_XT_TABLE"

S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

# -------------------------------------------------------------------------
# InteractionEvent (Source) - bypassed (downstream SQ has SQL override that reads directly from the external source)
# The Source node is represented here for lineage only; no direct S3/catalog read is emitted because the SQ override will read the external system via JDBC.
# -------------------------------------------------------------------------

# Source: Shortcut_to InteractionEvent (bypassed — SQL Override below reads it directly)

# SQ_InteractionEvent: read via JDBC using the node's SQL Override
sql_query = f"""
SELECT ROW_NUMBER() OVER (
		ORDER BY P.InteractionGroup_Id
			,P.Sequence_It
		) NUMBER_SEQ
	,P.InteractionGroup_Id
	,P.BusinessStatus_Cd
	,P.Status_Cd
	,P.StatusReason_Cd
	,P.TransactionEffective_Dt
	,P.Transaction_Ts
	,P.Sequence_It
	,P.Source_Cd
	,P.LoadEvent_Id
FROM (
	SELECT VIR_TBL.InteractionGroup_Id AS InteractionGroup_Id
		,VIR_TBL.BusinessStatus_Cd AS BusinessStatus_Cd
		,VIR_TBL.Status_Cd AS Status_Cd
		,VIR_TBL.StatusReason_Cd AS StatusReason_Cd
		,VIR_TBL.Action_Tp AS Action_Tp
		,VIR_TBL.TransactionEffective_Dt AS TransactionEffective_Dt
		,VIR_TBL.Transaction_Ts AS Transaction_Ts
		,VIR_TBL.Sequence_It AS Sequence_It
		,VIR_TBL.Source_Cd AS Source_Cd
		,VIR_TBL.LoadEvent_Id AS LoadEvent_Id
		,ROW_NUMBER() OVER (
			PARTITION BY InteractionGroup_Id ORDER BY InteractionGroup_Id
				,Sequence_It
			) AS Row_Num
		,TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd AS Curr_Row
		,COALESCE(MIN(TO_CHAR(VIR_TBL.InteractionGroup_Id) || VIR_TBL.BusinessStatus_Cd || VIR_TBL.Status_Cd || VIR_TBL.StatusReason_Cd) OVER (
			PARTITION BY InteractionGroup_Id 
			ORDER BY InteractionGroup_Id
				     ,Sequence_It 
			ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 0) AS Prev_Row
	FROM (
		SELECT CASE 
		        WHEN IE.source_cd IN ('{IGS_EXCLUDED_SOURCE}')
                     AND  IE.TransactionType_Tp = 9 
                     THEN 'PHONE'
					WHEN IE.source_cd IN ('{EXCLUDED_SOURCES}')
						THEN 'Intct'
					WHEN (IE.source_cd NOT IN ( '{EXCLUDED_SOURCES}' ))AND(IE.source_cd NOT IN ('{IGS_EXCLUDED_SOURCE}'))
					    THEN CASE
						WHEN IE.TransactionType_Tp = 10
                        THEN 'EMAIL'
						WHEN IE.TransactionType_Tp = 11 
						THEN 'MOBILE'

                        WHEN IE.TransactionType_Tp = 3
							THEN CASE 
							WHEN IE.Action_Tp IN (
									9
									,20
									,22
								)
								OR IE.ActionResultReason_Tp IN (
									62
									,63
									,64
									,65
								)
								THEN 'Trgt'
									WHEN IE.Action_Tp IN (
										5
										,21
										)
									THEN 'Dstrb'
									WHEN IE.ActionType_Tp IN (
										4
										,8
										,9
										,10
										,11
										,12
									)
								THEN 'Intct'
									WHEN IE.ActionType_Tp = 5
									AND IE.Action_Tp IN (
										26
										,27
										,28
										,29
									)
									THEN 'Ident'
										ELSE 'Notdef'
										END
							END AS BusinessStatus_Cd
									
								,CASE 
								WHEN  IE.source_cd IN ('{IGS_EXCLUDED_SOURCE}')  AND  IE.TransactionType_Tp = 9 
                    THEN 'A' || TRIM(IE.Action_Tp)
										WHEN IE.Source_Cd IN  ('{EXCLUDED_SOURCES}')
										THEN CASE 
											WHEN IE.ActionResultDetail_Tp IN (
												6
												,10
												,50
												,51
												,52
												,58
											)
											THEN 'Closed'
											WHEN IE.ActionResultDetail_Tp IN (
												7
												,8
												,35
												,41
												,53
												,54
												,55
												,56
												,57
												,59
												,60
												,61
												,62
											)
											THEN 'WIP'
											ELSE 'Notdef'
											END
								WHEN (IE.source_cd NOT IN ('{EXCLUDED_SOURCES}') )AND (IE.source_cd NOT IN ('{IGS_EXCLUDED_SOURCE}'))
								THEN CASE
								WHEN IE.TransactionType_Tp = 10 
								THEN 'A' || TRIM(IE.Action_Tp)
								WHEN IE.TransactionType_Tp = 11 
								THEN 'AR' || TRIM(IE.ActionResult_Tp)
								
								WHEN IE.TransactionType_Tp = 3
								THEN CASE 
									WHEN IE.Action_Tp = 5
										THEN 'Recvd'
									WHEN IE.ActionResult_Tp IN (
											16
											,17
										)
										OR IE.ActionResultReason_Tp = 62
									THEN 'Crtd'
									WHEN IE.Action_Tp IN (
										20
										,22
										)
										OR IE.ActionResultReason_Tp IN (
											63
											,64
											,65
											)
									THEN 'Asgd'
									WHEN IE.Action_Tp = 21
									THEN 'Sent'
									WHEN IE.Action_Tp = 6
									OR IE.ActionType_Tp = 9
									THEN 'Syscld'
									WHEN IE.ActionResultDetail_Tp IN (
										3
										,6
										,10
										,33
										)
									THEN 'Closed'
                                            WHEN IE.ActionResultDetail_Tp IN (32,50)
                                                         AND IE.Transaction_Tp IN (40,41)
                                                         THEN 'Closed'
									WHEN IE.Action_Tp IN (
										3
										,4
										,14
										)
									OR IE.ActionType_Tp IN (
										10
										,11
										)
									THEN 'WIP'
									WHEN IE.ActionType_Tp = 12
									THEN 'View'
									WHEN IE.ActionType_Tp = 5
									AND IE.Action_Tp = 26
									THEN 'Qcrtd'
									WHEN IE.ActionType_Tp = 5
									AND IE.Action_Tp IN (
										27
										,28
										,29
										)
									THEN 'Qabdn'
									ELSE 'Notdef'
									END
							END AS Status_Cd
										
								,CASE 
								WHEN IE.Source_Cd IN ('{IGS_EXCLUDED_SOURCE}')
								     AND IE.TransactionType_Tp = 9 
                    THEN 'AR' || TRIM( IE.ActionResult_Tp)
										WHEN IE.Source_Cd IN ('{EXCLUDED_SOURCES}')
										THEN TRIM(XREF.source_cd) || CAST(CAST(ActionResultDetail_Tp AS INTEGER FORMAT '9(3)') AS CHAR(3))
										WHEN (XREF.source_cd NOT IN ('{EXCLUDED_SOURCES}' ))AND (XREF.source_cd NOT IN ('{IGS_EXCLUDED_SOURCE}'))
									    THEN CASE
										WHEN IE.TransactionType_Tp = 10 
										THEN '@' 
										WHEN IE.TransactionType_Tp = 11 
										THEN '@' 
										WHEN IE.TransactionType_Tp = 3
										THEN CASE 
											WHEN IE.ActionResult_Tp IN (
												16
												,17
												)
											OR IE.ActionResultReason_Tp = 62
											THEN 'Crtd'
											WHEN IE.ActionResultReason_Tp = 5
											OR IE.ActionResult_Tp = 28
											THEN 'CCNW'
											WHEN IE.ActionResultDetail_Tp = 3
											THEN 'AgntRm'
											WHEN IE.ActionResultDetail_Tp = 6
											THEN 'Intrsd'
											WHEN IE.ActionResultDetail_Tp = 10
											THEN 'NotInt'
                                            WHEN IE.ActionResultDetail_Tp = 50
                                                    AND IE.ActionType_Tp = 8
                                                    THEN 'Complt'
											WHEN IE.ActionResultDetail_Tp = 12
											THEN 'CallBk'
											WHEN IE.ActionResult_Tp = 6
											THEN 'Qlfd'
											WHEN IE.ActionResult_Tp = 7
											THEN 'Err'
											WHEN IE.ActionResult_Tp = 10
											OR IE.ActionResultReason_Tp = 54
											THEN 'Expire'
											WHEN IE.ActionResult_Tp = 12
											THEN 'MnRmvd'
											WHEN IE.ActionResult_Tp = 13
											THEN 'Suprs'
											WHEN IE.ActionResult_Tp = 11
											THEN 'KO'
											WHEN IE.ActionResult_Tp IN (
												8
												,9
												)
											OR IE.ActionType_Tp = 9
											THEN 'Gnrl'
											WHEN IE.Action_Tp IN (
												20
												,22
												)
											OR IE.ActionResultReason_Tp IN (
												63
												,64
												,65
												)
											THEN CASE 
											WHEN IE.TransactionReason_Tp = 3
												THEN 'Ctrl'
												ELSE 'Trtbl'
											END
										WHEN IE.Action_Tp = 21
											THEN 'Sent'
										WHEN IE.ActionType_Tp IN (
											4
											,10
											,11
											)
											AND NOT IE.ActionResultDetail_Tp IN (
												3
												,6
												,10
												)
											THEN 'Active'
											WHEN IE.ActionResultDetail_Tp IN (
												32
												,33
												)
											THEN 'AgntMl'
											WHEN IE.ActionResultDetail_Tp = 37
												THEN 'MltSt'
											WHEN IE.ActionResultDetail_Tp = 34
												THEN 'DelPx'
											WHEN IE.ActionResultDetail_Tp = 38
												THEN 'PDRmd'
											WHEN IE.ActionResultDetail_Tp = 40
												THEN 'Sysrmv'
											WHEN IE.ActionResultReason_Tp IN (
													49
													,50
													,55
													,56
													,57
													,58
													,60
													,69
													,70
													)
											THEN 'NoQlf'
											WHEN IE.ActionResult_Tp = 24
												THEN 'Delvd'
												WHEN IE.ActionResult_Tp = 25
												THEN 'Undelv'
												WHEN IE.ActionType_Tp = 12
												THEN 'View'
											WHEN IE.ActionType_Tp = 5
												AND IE.Action_Tp = 26
												THEN 'Qcrtd'
												WHEN IE.ActionType_Tp = 5
												AND IE.Action_Tp IN (
													27
													,28
													,29
													)
												THEN 'Qabdn'
												ELSE 'Notdef'
											END AS StatusReason_Cd
												
								,IE.InteractionGroup_Id
								,IE.ActionType_Tp
								,IE.Action_Tp
								,IE.ActionResult_Tp
								,IE.ActionResultDetail_Tp
								,IE.ActionResultReason_Tp
								,IE.TransactionEffective_Dt
								,IE.Transaction_Ts
								,IE.Sequence_It
								,IE.Source_Cd
								,IE.LoadEvent_Id
								,CASE
WHEN IE.Transaction_Tp = 40 
THEN 'Include'
when (IE.Action_Tp in ( 15, 57, 58, 59, 60, 61, 62, 63 ) AND (IE.TransactionType_Tp = 10 ))
then 'Include'
when IE.TransactionType_Tp = 11 
then 'Include'

WHEN IE.Action_Tp IN (7, 8, 11, 56)
THEN 'Exclude'
WHEN IE.ActionType_Tp = 3
THEN 'Exclude'
WHEN (IE.Action_Tp = 12 
AND IE.ActionResult_Tp IN (33, 34, 35, 36))
THEN 'Exclude'
WHEN (IE.Action_Tp = 19 
AND IE.ActionResult_Tp IN (33, 34, 35, 36))
THEN 'Exclude'
WHEN IE.ActionResultDetail_Tp = 35 
THEN 'Exclude'
WHEN IE.Action_Tp IN (10, 13) 
THEN 'Exclude'
WHEN ( IE.ActionType_Tp = 5
AND IE.Action_Tp = 10
AND IE.ActionResult_Tp = 7) 
THEN 'Exclude'
WHEN ( IE.ActionType_Tp = 6 
AND IE.Action_Tp = 29) 
THEN 'Exclude'
WHEN ( IE.ActionType_Tp = 6 
AND IE.Action_Tp = 21) 
THEN 'Exclude'
WHEN (ActionResultDetail_Tp = 0 
AND Transaction_Tp = 41 ) 
THEN 'Exclude'
ELSE 'Include'
end AS StatusIncludeExclude_Cd
								FROM {INTERACTION_WORK_DB}.INTERACTIONEVENT IE
								INNER JOIN {INTERACTION_WORK_DB}.LOADEVENT_ID_XREF XREF
								ON TRIM(IE.SOURCE_CD) = TRIM(XREF.Source_Cd)
								AND IE.LOADEVENT_ID = XREF.LOADEVENT_ID
								AND XREF.JOB_CD = {JOB_CD} 
								WHERE (
									XREF.SOURCE_CD NOT IN ('{EXCLUDED_SOURCES}') AND XREF.SOURCE_CD NOT IN ('{IGS_EXCLUDED_SOURCE}')
								     AND StatusIncludeExclude_Cd = 'Include'
								
									AND NOT EXISTS (
										SELECT ''
										FROM {INTERACTION_WORK_DB}.INTERACTIONEVENT IE2
										WHERE IE.InteractionGroup_Id = IE2.InteractionGroup_Id
											AND XREF.Source_cd = IE2.Source_Cd
											AND IE2.LoadEvent_Id = XREF.LoadEvent_Id
											AND IE.Sequence_It > IE2.Sequence_it
											AND IE.Action_Tp = 22
											AND IE2.Transaction_Tp = 14
											AND (
												IE2.Action_Tp = 6
												OR IE2.ActionType_Tp = 9
												OR IE2.ActionResultDetail_Tp IN (
													3
													,6
													,10
													)
											)
										)
								)
								OR( XREF.SOURCE_CD IN( '{EXCLUDED_SOURCES}' , '{IGS_EXCLUDED_SOURCE}' )	
								AND SystemResult_Cd <> 'Planned Event')          	
								) VIR_TBL
		
	) P
WHERE P.CURR_ROW <> P.PREV_ROW
	AND NOT EXISTS (
		SELECT ''
		FROM {INTERACTION_FND_DB}.INTERACTIONGROUPSTATUS G
		WHERE (
				G.EXPIRATION_DT = '3500-01-01'
				AND P.Row_Num = 1
				AND G.InteractionGroup_Id = P.InteractionGroup_Id
				AND G.BusinessStatus_Cd = P.BusinessStatus_Cd
				AND G.Status_Cd = P.Status_Cd
				AND G.StatusReason_Cd = P.StatusReason_Cd
				)
			OR (
				G.InteractionGroup_Id = P.InteractionGroup_Id
				AND G.BusinessStatus_Cd = P.BusinessStatus_Cd
				AND G.Status_Cd = P.Status_Cd
				AND G.StatusReason_Cd = P.StatusReason_Cd
				AND G.SequenceStart_It = P.Sequence_It
			)
			OR (
				P.InteractionGroup_Id = G.InteractionGroup_Id
				AND P.Source_cd = G.Source_cd
				AND P.Source_cd NOT IN ('{EXCLUDED_SOURCES}') AND P.Source_cd NOT IN ('{IGS_EXCLUDED_SOURCE}')
				AND P.Sequence_It >= G.SequenceStart_It
				AND P.Sequence_It < G.SequenceEnd_It
				AND P.Action_Tp = 22
				AND G.Status_Tp IN (
					4
					,9
				)
			)
		)
"""

try:
    logger.info("Reading SQ_InteractionEvent_calm_kant from external system via JDBC override query")
    df_SQ_InteractionEvent_calm_kant = (
        spark.read.format("jdbc")
        .option("url", REPLACE_WITH_RL_EDW_EINTERACTION_JDBC_URL)
        .option("user", REPLACE_WITH_RL_EDW_EINTERACTION_USER)
        .option("password", REPLACE_WITH_RL_EDW_EINTERACTION_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_InteractionEvent_calm_kant from external system: {e}", exc_info=True)
    raise

# Represent Source node for lineage only - assign df_InteractionEvent_loving_hawking to SQ dataframe for provenance
# (the SQ override bypasses the physical Source read; this assignment keeps the original Source df_name available for lineage)
df_InteractionEvent_loving_hawking = df_SQ_InteractionEvent_calm_kant

# -------------------------------------------------------------------------
# exp_get_tp_values: join to small lookup reference(s) to derive numeric type codes and compute o_InteractionGroupStatus_Id
# Input ports used: LoadEvent_Id, NUMBER_SEQ, BusinessStatus_Cd, Status_Cd, StatusReason_Cd
# -------------------------------------------------------------------------
try:
    logger.info("Transform: exp_get_tp_values_zen_maxwell - preparing base projection and loading lookup reference")
    df_base = df_SQ_InteractionEvent_calm_kant.select(
        col("LoadEvent_Id"),
        col("NUMBER_SEQ"),
        col("BusinessStatus_Cd"),
        col("Status_Cd"),
        col("StatusReason_Cd")
    )

    # ensure placeholder in case of accidental external references to df_in elsewhere
    df_in = df_base

    # read the small lookup reference table (expected to be small -> will be broadcast)
    lookup_df = (
        spark.read.format("jdbc")
        .option("url", LKP_XREF_XT_JDBC_URL)
        .option("user", LKP_XREF_XT_USER)
        .option("password", LKP_XREF_XT_PASSWORD)
        .option("dbtable", LKP_XREF_XT_TABLE)
        .load()
    )

    # normalize lookup code columns - assume lookup table contains columns: lookup_code, lookup_cd, lookup_tp
    lookup_norm = lookup_df.withColumn("lookup_cd_norm", upper(trim(col("lookup_cd")))).select(
        col("lookup_code"), col("lookup_cd_norm"), col("lookup_tp")
    )

    # helper: join for a specific lookup_code and source column
    def attach_lookup(df_in, src_col_name, lookup_code, out_col_name):
        filtered = lookup_norm.filter(col("lookup_code") == lookup_code).select(
            col("lookup_cd_norm"), col("lookup_tp")
        ).withColumnRenamed("lookup_tp", out_col_name)
        # join on normalized upper(trim(src_col)) == lookup_cd_norm
        joined = df_in.join(
            broadcast(filtered),
            on=(upper(trim(col(src_col_name))) == col("lookup_cd_norm")),
            how="left"
        ).drop("lookup_cd_norm")
        return joined

    # attach each lookup result
    df_lkp1 = attach_lookup(df_base, "BusinessStatus_Cd", "5021", "o_BusinessStatus_Tp")
    df_lkp2 = attach_lookup(df_lkp1, "Status_Cd", "5022", "o_Status_Tp")
    df_lkp3 = attach_lookup(df_lkp2, "StatusReason_Cd", "5023", "o_StatusReason_Tp")

    # compute o_InteractionGroupStatus_Id per source logic:
    # TO_decimal(TO_CHAR(LoadEvent_Id)||SUBSTR('000000000000000000',0,18 - LENGTH(to_char(LoadEvent_Id)))) + NUMBER_SEQ
    zeros = lit("000000000000000000")
    loadid_str = col("LoadEvent_Id").cast("string")
    n_zeros = (lit(18) - length(loadid_str)).cast("int")
    suffix_zeros = when(n_zeros > 0, substring(zeros, 1, n_zeros)).otherwise(lit(""))
    left_part = concat(loadid_str, suffix_zeros)
    # cast left_part to decimal and add NUMBER_SEQ; use large decimal precision during compute then cast down
    df_with_id = df_lkp3.withColumn(
        "o_InteractionGroupStatus_Id",
        (left_part.cast("decimal(38,0)") + col("NUMBER_SEQ").cast("decimal(38,0)")).cast("decimal(18,0)")
    )

    df_exp_get_tp_values_zen_maxwell = df_with_id

except Exception as e:
    logger.error(f"Failed processing exp_get_tp_values_zen_maxwell: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# exp_set_default_Tp_values: substitute -2 where lookup-derived types are NULL
# Input ports: o_BusinessStatus_Tp, o_Status_Tp, o_StatusReason_Tp (from previous expr)
# -------------------------------------------------------------------------
try:
    logger.info("Transform: exp_set_default_Tp_values_heroic_faraday - substituting default -2 for null TP codes")
    df = df_exp_get_tp_values_zen_maxwell

    df_exp_set_default_Tp_values_heroic_faraday = (
        df.withColumn(
            "o_BusinessStatus_Tp",
            when(col("o_BusinessStatus_Tp").isNotNull(), col("o_BusinessStatus_Tp")).otherwise(lit(-2))
        ).withColumn(
            "o_Status_Tp",
            when(col("o_Status_Tp").isNotNull(), col("o_Status_Tp")).otherwise(lit(-2))
        ).withColumn(
            "o_StatusReason_Tp",
            when(col("o_StatusReason_Tp").isNotNull(), col("o_StatusReason_Tp")).otherwise(lit(-2))
        )
    )

except Exception as e:
    logger.error(f"Failed processing exp_set_default_Tp_values_heroic_faraday: {e}", exc_info=True)
    raise

# -------------------------------------------------------------------------
# InteractionGroupStatus (Output): build final target rows and write as parquet to S3
# Join SQ to the expression outputs on deterministic keys LoadEvent_Id and NUMBER_SEQ
# -------------------------------------------------------------------------
try:
    logger.info("Building final InteractionGroupStatus payload and writing to S3 as parquet (overwrite)")

    # join the SQ to the final expression output (exp_set_default contains the finalized TP columns and the generated ID)
    df_joined = df_SQ_InteractionEvent_calm_kant.join(
        df_exp_set_default_Tp_values_heroic_faraday.select(
            "LoadEvent_Id",
            "NUMBER_SEQ",
            "o_InteractionGroupStatus_Id",
            "o_BusinessStatus_Tp",
            "o_Status_Tp",
            "o_StatusReason_Tp",
        ),
        on=["LoadEvent_Id", "NUMBER_SEQ"],
        how="left"
    )

    # project the Output node's ports into the target schema
    df_InteractionGroupStatus_heroic_shannon = df_joined.select(
        col("o_InteractionGroupStatus_Id").alias("InteractionGroupStatus_Id"),
        col("InteractionGroup_Id"),
        col("BusinessStatus_Cd"),
        col("o_BusinessStatus_Tp").alias("BusinessStatus_Tp"),
        col("Status_Cd"),
        col("o_Status_Tp").alias("Status_Tp"),
        col("StatusReason_Cd"),
        col("o_StatusReason_Tp").alias("StatusReason_Tp"),
        col("TransactionEffective_Dt").alias("Effective_Dt"),
        lit('3500-01-01').cast('timestamp').alias("Expiration_Dt"),
        col("Transaction_Ts"),
        lit(None).cast('timestamp').alias("Revision_Ts"),
        col("Sequence_It").alias("SequenceStart_It"),
        lit(None).cast('long').alias("SequenceEnd_It"),
        col("Source_Cd"),
        col("LoadEvent_Id")
    )

    # write as parquet to S3 using the target table name (strip Shortcut_/FDR_LIB_ prefixes for the external path if present - here the target_table is InteractionGroupStatus)
    df_InteractionGroupStatus_heroic_shannon.write.mode("overwrite").parquet(
        f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"
    )

except Exception as e:
    logger.error(f"Failed writing InteractionGroupStatus to S3: {e}", exc_info=True)
    raise


job.commit()
