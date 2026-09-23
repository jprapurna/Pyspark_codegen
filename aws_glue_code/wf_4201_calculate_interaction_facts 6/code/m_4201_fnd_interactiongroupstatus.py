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


# Top-of-script placeholders for mapping parameters, JDBC connection, lookup/catalog DBs, and output bucket
INTERACTION_WORK_DB = "REPLACE_WITH_INTERACTION_WORK_DB"
JOB_CD = "REPLACE_WITH_JOB_CD_VALUE"
EXCLUDED_SOURCES = "REPLACE_WITH_EXCLUDED_SOURCES_VALUE"  # comma-separated quoted list as used in SQL, e.g. '''SRC1','SRC2'''
IGS_EXCLUDED_SOURCE = "REPLACE_WITH_IGS_EXCLUDED_SOURCE_VALUE"
INTERACTION_FND_DB = "REPLACE_WITH_INTERACTION_FND_DB"
TERADATA_JDBC_URL = "REPLACE_WITH_TERADATA_JDBC_URL"
TERADATA_USER = "REPLACE_WITH_TERADATA_USER"
TERADATA_PASSWORD = "REPLACE_WITH_TERADATA_PASSWORD"
LOOKUP_DB = "REPLACE_WITH_LOOKUP_DB"
S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"

# Additional imports used by transformations below
from pyspark.sql.functions import col, upper, trim, length, substring, concat, lit, when
from pyspark.sql.functions import broadcast

# -----------------------------------------------------------------------------
# Source: InteractionEvent (bypassed  SQL Override below reads it directly)
# -----------------------------------------------------------------------------
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
		        WHEN IE.source_cd IN ({IGS_EXCLUDED_SOURCE})
                     AND  IE.TransactionType_Tp = 9 
                     THEN 'PHONE'
					WHEN IE.source_cd IN ({EXCLUDED_SOURCES})
						THEN 'Intct'
					WHEN (IE.source_cd NOT IN ( {EXCLUDED_SOURCES} ))AND(IE.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
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
								END
							END AS BusinessStatus_Cd
							,CASE 
							WHEN  IE.source_cd IN ({IGS_EXCLUDED_SOURCE})  AND  IE.TransactionType_Tp = 9 
                    THEN 'A' || TRIM(IE.Action_Tp)
							WHEN IE.Source_Cd IN  ({EXCLUDED_SOURCES})
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
								WHEN (IE.source_cd NOT IN ({EXCLUDED_SOURCES}) )AND (IE.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
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
									WHEN IE.Source_Cd IN ({IGS_EXCLUDED_SOURCE})
									     AND IE.TransactionType_Tp = 9 
                    THEN 'AR' || TRIM( IE.ActionResult_Tp)
									WHEN IE.Source_Cd IN ({EXCLUDED_SOURCES})
										THEN TRIM(XREF.source_cd) || CAST(CAST(ActionResultDetail_Tp AS INTEGER FORMAT '9(3)') AS CHAR(3))
									WHEN (XREF.source_cd NOT IN ({EXCLUDED_SOURCES} ))AND (XREF.source_cd NOT IN ({IGS_EXCLUDED_SOURCE}))
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
											END
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
					XREF.SOURCE_CD NOT IN ({EXCLUDED_SOURCES}) AND XREF.SOURCE_CD NOT IN ({IGS_EXCLUDED_SOURCE})
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
				AND P.Source_cd NOT IN ({EXCLUDED_SOURCES}) AND P.Source_cd NOT IN ({IGS_EXCLUDED_SOURCE})
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

# read the SQL-override result from Teradata via JDBC
try:
    logger.info("Reading SQ_InteractionEvent (sql override) from Teradata via JDBC")
    df_SQ_InteractionEvent_mystifying_socrates = (
        spark.read.format("jdbc")
        .option("url", TERADATA_JDBC_URL)
        .option("user", TERADATA_USER)
        .option("password", TERADATA_PASSWORD)
        .option("query", sql_query)
        .load()
    )
except Exception as e:
    logger.error(f"Failed reading SQ_InteractionEvent from Teradata: {e}", exc_info=True)
    raise

# assign the upstream Source node variable so subsequent nodes that expect df_InteractionEvent_mighty_franklin can find it
try:
    logger.info("Assigning Source InteractionEvent dataframe variable for lineage (df_InteractionEvent_mighty_franklin)")
    df_InteractionEvent_mighty_franklin = df_SQ_InteractionEvent_mystifying_socrates
except Exception as e:
    logger.error(f"Failed assigning df_InteractionEvent_mighty_franklin: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# exp_get_tp_values: derive o_InteractionGroupStatus_Id and lookup-based code types
# -----------------------------------------------------------------------------
try:
    logger.info("Transform: exp_get_tp_values (derive surrogate and lookup codes)")
    # Read lookup reference tables from Glue Catalog (placeholder names; replace in deployment)
    try:
        lkp_5021 = glueContext.create_dynamic_frame.from_catalog(database=LOOKUP_DB, table_name="lkp_xref_xt_5021").toDF()
        lkp_5022 = glueContext.create_dynamic_frame.from_catalog(database=LOOKUP_DB, table_name="lkp_xref_xt_5022").toDF()
        lkp_5023 = glueContext.create_dynamic_frame.from_catalog(database=LOOKUP_DB, table_name="lkp_xref_xt_5023").toDF()
        logger.info("Lookup tables lkp_xref_xt_5021/5022/5023 read from Glue Catalog (LOOKUP_DB)")
    except Exception as e_lookup:
        logger.error(f"Failed reading lookup tables from Glue Catalog: {e_lookup}", exc_info=True)
        raise

    # Prepare key columns for join: uppercase(trim(code)) on both sides
    df_exp_base = df_SQ_InteractionEvent_mystifying_socrates

    # compute padded-right LoadEvent_Id string then cast to decimal and add NUMBER_SEQ
    # padded_right = TO_CHAR(LoadEvent_Id) || SUBSTR('000...'(18),1,18-LENGTH(TO_CHAR(LoadEvent_Id)))
    # note: substring in Spark is 1-indexed
    df_with_surrogate = df_exp_base.withColumn("_le_str", col("LoadEvent_Id").cast("string"))
    df_with_surrogate = df_with_surrogate.withColumn(
        "_zeros_needed",
        (lit(18) - length(col("_le_str"))).cast("int")
    )
    df_with_surrogate = df_with_surrogate.withColumn(
        "_zeros_part",
        when(col("_zeros_needed") > 0, substring(lit('000000000000000000'), 1, col("_zeros_needed"))).otherwise(lit(''))
    )
    df_with_surrogate = df_with_surrogate.withColumn(
        "o_InteractionGroupStatus_Id",
        (concat(col("_le_str"), col("_zeros_part")).cast("decimal(38,0)") + col("NUMBER_SEQ")).cast("decimal(18,0)")
    )

    # Prepare lookup join keys from the source columns (upper/trim)
    df_with_keys = df_with_surrogate.withColumn("_bizstat_key", upper(trim(col("BusinessStatus_Cd"))))
    df_with_keys = df_with_keys.withColumn("_status_key", upper(trim(col("Status_Cd"))))
    df_with_keys = df_with_keys.withColumn("_statusreason_key", upper(trim(col("StatusReason_Cd"))))

    # Prepare lookup tables' key naming assumptions: assume each lkp has 'code' and 'tp' columns
    # Use broadcast joins for small reference tables
    lkp_5021_prepped = lkp_5021.withColumn("_lk_code", upper(trim(col("code")))).withColumnRenamed("tp", "lk_5021_tp")
    lkp_5022_prepped = lkp_5022.withColumn("_lk_code", upper(trim(col("code")))).withColumnRenamed("tp", "lk_5022_tp")
    lkp_5023_prepped = lkp_5023.withColumn("_lk_code", upper(trim(col("code")))).withColumnRenamed("tp", "lk_5023_tp")

    # Join to derive the numeric type codes from lookup; left join so missing lookups remain null (handled downstream)
    df_joined = df_with_keys.join(broadcast(lkp_5021_prepped), df_with_keys["_bizstat_key"] == lkp_5021_prepped["_lk_code"], how="left")
    df_joined = df_joined.join(broadcast(lkp_5022_prepped), df_joined["_status_key"] == lkp_5022_prepped["_lk_code"], how="left")
    df_joined = df_joined.join(broadcast(lkp_5023_prepped), df_joined["_statusreason_key"] == lkp_5023_prepped["_lk_code"], how="left")

    # Map lookup outputs into port names expected by downstream: produce both 'o_' and non-'o' versions conservatively
    df_exp_get_tp_values_keen_franklin = df_joined.withColumn("o_BusinessStatus_Tp", col("lk_5021_tp")).withColumn("BusinessStatus_Tp", col("lk_5021_tp"))
    df_exp_get_tp_values_keen_franklin = df_exp_get_tp_values_keen_franklin.withColumn("o_Status_Tp", col("lk_5022_tp")).withColumn("Status_Tp", col("lk_5022_tp"))
    df_exp_get_tp_values_keen_franklin = df_exp_get_tp_values_keen_franklin.withColumn("o_StatusReason_Tp", col("lk_5023_tp")).withColumn("StatusReason_Tp", col("lk_5023_tp"))

    # Keep the computed surrogate already present
    # Drop helper columns we don't want to propagate
    drop_cols = ["_le_str", "_zeros_needed", "_zeros_part", "_bizstat_key", "_status_key", "_statusreason_key", "_lk_code"]
    for c in drop_cols:
        if c in df_exp_get_tp_values_keen_franklin.columns:
            df_exp_get_tp_values_keen_franklin = df_exp_get_tp_values_keen_franklin.drop(c)

except Exception as e:
    logger.error(f"Failed in exp_get_tp_values transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# exp_set_default_Tp_values: default NULL smallint codes to -2
# -----------------------------------------------------------------------------
try:
    logger.info("Transform: exp_set_default_Tp_values (normalize NULLs to -2)")
    df = df_exp_get_tp_values_keen_franklin

    df_exp_set_default_Tp_values_jovial_shannon = (
        df.withColumn("o_BusinessStatus_Tp", when(col("BusinessStatus_Tp").isNotNull(), col("BusinessStatus_Tp")).otherwise(lit(-2)))
          .withColumn("o_Status_Tp", when(col("Status_Tp").isNotNull(), col("Status_Tp")).otherwise(lit(-2)))
          .withColumn("o_StatusReason_Tp", when(col("StatusReason_Tp").isNotNull(), col("StatusReason_Tp")).otherwise(lit(-2)))
    )

except Exception as e:
    logger.error(f"Failed in exp_set_default_Tp_values transformation: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# InteractionGroupStatus (target): assemble columns and write Parquet to S3
# -----------------------------------------------------------------------------
try:
    logger.info("Assemble InteractionGroupStatus target and write to S3 as parquet (overwrite)")

    # Join the exp_set_default outputs back to the SQ to ensure all passthrough columns are present
    # Join keys: InteractionGroup_Id, LoadEvent_Id, NUMBER_SEQ
    df_left = df_SQ_InteractionEvent_mystifying_socrates.alias("sq")
    df_right = df_exp_set_default_Tp_values_jovial_shannon.alias("exp")

    join_cond = [
        col("sq.InteractionGroup_Id") == col("exp.InteractionGroup_Id"),
        col("sq.LoadEvent_Id") == col("exp.LoadEvent_Id"),
        col("sq.NUMBER_SEQ") == col("exp.NUMBER_SEQ")
    ]

    df_target = df_left.join(df_right, on=join_cond, how="left")

    # Select and alias the exact target columns. For any target fields not present upstream, emit NULLs.
    # Target fields per mapping: InteractionGroupStatus_Id, InteractionGroup_Id, BusinessStatus_Cd, BusinessStatus_Tp,
    # Status_Cd, Status_Tp, StatusReason_Cd, StatusReason_Tp, Effective_Dt, Expiration_Dt, Transaction_Ts,
    # Revision_Ts, SequenceStart_It, SequenceEnd_It, Source_Cd, LoadEvent_Id

    df_InteractionGroupStatus_relaxed_einstein = df_target.select(
        col("exp.o_InteractionGroupStatus_Id").alias("InteractionGroupStatus_Id"),
        col("sq.InteractionGroup_Id").alias("InteractionGroup_Id"),
        col("sq.BusinessStatus_Cd").alias("BusinessStatus_Cd"),
        col("exp.o_BusinessStatus_Tp").alias("BusinessStatus_Tp"),
        col("sq.Status_Cd").alias("Status_Cd"),
        col("exp.o_Status_Tp").alias("Status_Tp"),
        col("sq.StatusReason_Cd").alias("StatusReason_Cd"),
        col("exp.o_StatusReason_Tp").alias("StatusReason_Tp"),
        # The following fields are not derived in this mapping's nodes; emit NULLs so schema is present
        lit(None).cast("date").alias("Effective_Dt"),
        lit(None).cast("date").alias("Expiration_Dt"),
        col("sq.Transaction_Ts").alias("Transaction_Ts"),
        lit(None).cast("timestamp").alias("Revision_Ts"),
        lit(None).alias("SequenceStart_It"),
        lit(None).alias("SequenceEnd_It"),
        col("sq.Source_Cd").alias("Source_Cd"),
        col("sq.LoadEvent_Id").alias("LoadEvent_Id")
    )

    # write intermediate/final target as parquet to S3 (overwrite)
    try:
        logger.info("Writing InteractionGroupStatus to S3 as parquet (overwrite)")
        df_InteractionGroupStatus_relaxed_einstein.write.mode("overwrite").parquet(
            f"s3://{S3_OUTPUT_BUCKET}/InteractionGroupStatus/"
        )
    except Exception as e_write:
        logger.error(f"Failed writing InteractionGroupStatus to S3: {e_write}", exc_info=True)
        raise

except Exception as e:
    logger.error(f"Failed assembling/writing InteractionGroupStatus target: {e}", exc_info=True)
    raise



job.commit()
