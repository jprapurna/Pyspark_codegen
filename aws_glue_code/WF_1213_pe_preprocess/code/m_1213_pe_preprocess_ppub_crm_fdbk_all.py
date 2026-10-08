import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils import *

args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "GLUE_DATABASE", "S3_OUTPUT_BUCKET", "SNOWFLAKE_SECRET_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# Resolved once here so every mapping script stops inventing its own differently-spelled
# placeholder for the same handful of values (six spellings of "the Glue database" alone,
# across scripts, before this). Node-level code must reference these directly, never
# redeclare them.
ENV = args["ENV"]
GLUE_DATABASE = args["GLUE_DATABASE"]
S3_OUTPUT_BUCKET = args["S3_OUTPUT_BUCKET"]

SNOWFLAKE_SECRET_NAME = args["SNOWFLAKE_SECRET_NAME"]
SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD = get_snowflake_connection(SNOWFLAKE_SECRET_NAME)

from pyspark.sql import functions as F
from pyspark.sql.functions import expr, trim, col, when, row_number, substring_index
from pyspark.sql.window import Window
import os
import glob
import shutil

# Placeholder for Informatica target directory mapping-parameter
PM_TARGET_FILE_DIR = "REPLACE_WITH_PM_TARGET_FILE_DIR"

# ---------- Source: File_PPUB_CRM_FDBK_ALL ----------
try:
    logger.info("Reading source File_PPUB_CRM_FDBK_ALL from Glue Data Catalog")
    dyf_File_PPUB_CRM_FDBK_ALL = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE, table_name="File_PPUB_CRM_FDBK_ALL"
    )
    df_File_PPUB_CRM_FDBK_ALL_nice_rutherford = (
        dyf_File_PPUB_CRM_FDBK_ALL.toDF()
        .select(
            "SEGMENT_ID",
            "TARGET_KEY",
            "TGT_LVL_CD",
            "OPERATIONALIZED_KEY",
            "STAT_DT",
            "STAT_TS",
            "STAT_CD",
            "WEIGHT",
            "DIRECTION",
            "OWNING_RES_AGCY_NO",
            "OWNING_RES_PDCR_NO",
            "UPDATEDBY_RES_AGCY_NO",
            "UPDATEDBY_RES_PDCR_NO",
            "APPLICATION_ID",
            "LAST_UPDT_ID",
            "LAST_UPDT_DOM",
            "ORDER_ID",
            "LINE_ITEM_ID",
            "OPP_ID",
            "OPP_DTL_ID",
            "OPP_LIB_ID",
            "EXP_DT",
            "PCN_STG_DT",
            "PRIORITY",
            "PRIORITY_STAR",
            "AGENT_ADDED_IN",
            "ARM_TOUCHPOINT",
            "ARM_LEAD_KEY_ID",
            "ARM_LEAD_INSTANCE_ID",
            "ARM_LOGICAL_SEGMENT_ID",
            "LOAD_DT",
            "CurrentlyProcessedFileName"
        )
    )
except Exception as e:
    logger.error(f"Failed reading File_PPUB_CRM_FDBK_ALL from Glue Catalog: {e}", exc_info=True)
    raise

# ---------- Source: File_PPUB_MLDB_OPP_DTL ----------
try:
    logger.info("Reading source File_PPUB_MLDB_OPP_DTL from Glue Data Catalog")
    dyf_File_PPUB_MLDB_OPP_DTL = glueContext.create_dynamic_frame_from_catalog(
        database=GLUE_DATABASE, table_name="File_PPUB_MLDB_OPP_DTL"
    )
    df_File_PPUB_MLDB_OPP_DTL_humble_shannon = (
        dyf_File_PPUB_MLDB_OPP_DTL.toDF()
        .select(
            "Opp_Dtl_Id",
            "Opp_Lib_Id",
            "Source_Cd",
            "Source_Dtl_Key",
            "Exp_Dt",
            "Create_Dt",
            "DM_URL",
            "Long_Ds"
        )
    )
except Exception as e:
    logger.error(f"Failed reading File_PPUB_MLDB_OPP_DTL from Glue Catalog: {e}", exc_info=True)
    raise

# ---------- SQ_File_PPUB_CRM_FDBK_ALL (passthrough projection) ----------
try:
    logger.info("Projecting SQ_File_PPUB_CRM_FDBK_ALL (passthrough from File_PPUB_CRM_FDBK_ALL)")
    df_SQ_File_PPUB_CRM_FDBK_ALL_silly_noether = (
        df_File_PPUB_CRM_FDBK_ALL_nice_rutherford.select(
            "LOAD_DT",
            "SEGMENT_ID",
            "TARGET_KEY",
            "TGT_LVL_CD",
            "OPERATIONALIZED_KEY",
            "STAT_DT",
            "STAT_TS",
            "STAT_CD",
            "WEIGHT",
            "DIRECTION",
            "OWNING_RES_AGCY_NO",
            "OWNING_RES_PDCR_NO",
            "UPDATEDBY_RES_AGCY_NUM",
            "UPDATEDBY_RES_PDCR_NUM",
            "APPLICATION_ID",
            "LAST_UPDT_ID",
            "LAST_UPDT_DOM",
            "ORDER_ID",
            "LINE_ITEM_ID",
            "OPP_ID",
            "OPP_DTL_ID",
            "OPP_LIB_ID",
            "EXP_DT",
            "PCN_STG_DT",
            "PRIORITY",
            "PRIORITY_STAR",
            "AGENT_ADDED_IN",
            "ARM_TOUCHPOINT",
            "ARM_LEAD_KEY_ID",
            "ARM_LOGICAL_SEGMENT_ID",
            "ARM_LEAD_INSTANCE_ID",
            "CurrentlyProcessedFileName"
        )
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_File_PPUB_CRM_FDBK_ALL: {e}", exc_info=True)
    raise

# ---------- SQ_File_PPUB_MLDB_OPP_DTL (passthrough projection) ----------
try:
    logger.info("Projecting SQ_File_PPUB_MLDB_OPP_DTL (passthrough from File_PPUB_MLDB_OPP_DTL)")
    df_SQ_File_PPUB_MLDB_OPP_DTL_eager_planck = (
        df_File_PPUB_MLDB_OPP_DTL_humble_shannon.select(
            "Opp_Dtl_Id",
            "Opp_Lib_Id",
            "Source_Cd",
            "Source_Dtl_Key",
            "Exp_Dt",
            "Create_Dt",
            "DM_URL",
            "Long_Ds"
        )
    )
except Exception as e:
    logger.error(f"Failed projecting SQ_File_PPUB_MLDB_OPP_DTL: {e}", exc_info=True)
    raise

# ---------- Joiner: Join_Fdbk_All_with_MLDB_Dtl (Master Outer -> right join) ----------
try:
    logger.info("Joining CRM feedback with MLDB detail (Join_Fdbk_All_with_MLDB_Dtl) as Master Outer -> right join")
    l = df_SQ_File_PPUB_CRM_FDBK_ALL_silly_noether.alias("l")
    r = df_SQ_File_PPUB_MLDB_OPP_DTL_eager_planck.alias("r")
    join_cond = col("l.FDBK_ALL_OPP_DTL_ID") == col("r.Opp_Dtl_Id")
    # perform right join so master (MLDB) is preserved
    df_joined_temp = l.join(r, join_cond, how="right")
    # project the expected Joiner output ports - prefer master (r) columns where marked MASTER,
    # otherwise coalesce(master, detail) so we retain whichever side is present after the right join
    df_Join_Fdbk_All_with_MLDB_Dtl_nice_archimedes = df_joined_temp.select(
        # Master-side MLDB fields (use r.x)
        col("r.Opp_Dtl_Id").alias("MLDB_Opp_Dtl_Id"),
        col("r.Opp_Lib_Id").alias("MLDB_Opp_Lib_Id"),
        col("r.DM_URL").alias("MLDB_DM_URL"),
        col("r.Source_Cd").alias("MLDB_Source_Cd"),
        # Detail-side FDBK fields (prefer l., but coalesce to r if missing)
        F.coalesce(col("l.FDBK_ALL_OPP_DTL_ID"), col("r.Opp_Dtl_Id")).alias("FDBK_ALL_OPP_DTL_ID"),
        col("l.FDBK_ALL_OPP_LIB_ID").alias("FDBK_ALL_OPP_LIB_ID"),
        # common/pass-through fields - use coalesce(r, l) so right-joined master values prevail when present
        F.coalesce(col("r.LOAD_DT"), col("l.LOAD_DT")).alias("LOAD_DT"),
        F.coalesce(col("r.SEGMENT_ID"), col("l.SEGMENT_ID")).alias("SEGMENT_ID"),
        F.coalesce(col("r.TARGET_KEY"), col("l.TARGET_KEY")).alias("TARGET_KEY"),
        F.coalesce(col("r.TGT_LVL_CD"), col("l.TGT_LVL_CD")).alias("TGT_LVL_CD"),
        F.coalesce(col("r.OPERATIONALIZED_KEY"), col("l.OPERATIONALIZED_KEY")).alias("OPERATIONALIZED_KEY"),
        F.coalesce(col("r.STAT_DT"), col("l.STAT_DT")).alias("STAT_DT"),
        F.coalesce(col("r.STAT_TS"), col("l.STAT_TS")).alias("STAT_TS"),
        F.coalesce(col("r.STAT_CD"), col("l.STAT_CD")).alias("STAT_CD"),
        F.coalesce(col("r.WEIGHT"), col("l.WEIGHT")).alias("WEIGHT"),
        F.coalesce(col("r.DIRECTION"), col("l.DIRECTION")).alias("DIRECTION"),
        F.coalesce(col("r.OWNING_RES_AGCY_NO"), col("l.OWNING_RES_AGCY_NO")).alias("OWNING_RES_AGCY_NO"),
        F.coalesce(col("r.OWNING_RES_PDCR_NO"), col("l.OWNING_RES_PDCR_NO")).alias("OWNING_RES_PDCR_NO"),
        F.coalesce(col("r.UPDATEDBY_RES_AGCY_NUM"), col("l.UPDATEDBY_RES_AGCY_NUM")).alias("UPDATEDBY_RES_AGCY_NUM"),
        F.coalesce(col("r.UPDATEDBY_RES_PDCR_NUM"), col("l.UPDATEDBY_RES_PDCR_NUM")).alias("UPDATEDBY_RES_PDCR_NUM"),
        F.coalesce(col("r.APPLICATION_ID"), col("l.APPLICATION_ID")).alias("APPLICATION_ID"),
        F.coalesce(col("r.LAST_UPDT_ID"), col("l.LAST_UPDT_ID")).alias("LAST_UPDT_ID"),
        F.coalesce(col("r.LAST_UPDT_DOM"), col("l.LAST_UPDT_DOM")).alias("LAST_UPDT_DOM"),
        F.coalesce(col("r.ORDER_ID"), col("l.ORDER_ID")).alias("ORDER_ID"),
        F.coalesce(col("r.LINE_ITEM_ID"), col("l.LINE_ITEM_ID")).alias("LINE_ITEM_ID"),
        F.coalesce(col("r.OPP_ID"), col("l.OPP_ID")).alias("OPP_ID"),
        F.coalesce(col("r.OPP_DTL_ID"), col("l.OPP_DTL_ID")).alias("OPP_DTL_ID"),
        F.coalesce(col("r.OPP_LIB_ID"), col("l.OPP_LIB_ID")).alias("OPP_LIB_ID"),
        F.coalesce(col("r.EXP_DT"), col("l.EXP_DT")).alias("EXP_DT"),
        F.coalesce(col("r.PCN_STG_DT"), col("l.PCN_STG_DT")).alias("PCN_STG_DT"),
        F.coalesce(col("r.PRIORITY"), col("l.PRIORITY")).alias("PRIORITY"),
        F.coalesce(col("r.PRIORITY_STAR"), col("l.PRIORITY_STAR")).alias("PRIORITY_STAR"),
        F.coalesce(col("r.AGENT_ADDED_IN"), col("l.AGENT_ADDED_IN")).alias("AGENT_ADDED_IN"),
        F.coalesce(col("r.ARM_TOUCHPOINT"), col("l.ARM_TOUCHPOINT")).alias("ARM_TOUCHPOINT"),
        F.coalesce(col("r.ARM_LEAD_KEY_ID"), col("l.ARM_LEAD_KEY_ID")).alias("ARM_LEAD_KEY_ID"),
        F.coalesce(col("r.ARM_LOGICAL_SEGMENT_ID"), col("l.ARM_LOGICAL_SEGMENT_ID")).alias("ARM_LOGICAL_SEGMENT_ID"),
        F.coalesce(col("r.ARM_LEAD_INSTANCE_ID"), col("l.ARM_LEAD_INSTANCE_ID")).alias("ARM_LEAD_INSTANCE_ID"),
        F.coalesce(col("r.SRC_FILE_NM_CRM_FDBK_ALL"), col("l.CurrentlyProcessedFileName")).alias("SRC_FILE_NM_CRM_FDBK_ALL")
    )
except Exception as e:
    logger.error(f"Failed during Join_Fdbk_All_with_MLDB_Dtl: {e}", exc_info=True)
    raise

# ---------- Expression: Exp_Generate_Source_Order_And_Interaction_Groupkeycd ----------
try:
    logger.info("Computing Source_Order and o_ARM_LEAD_INSTANCE_ID (Exp_Generate_Source_Order_And_Interaction_Groupkeycd)")
    # build Source_Order via CASE/CAST equivalent to TO_INTEGER(DECODE(TRUE,...))
    source_order_expr = (
        "CAST(CASE "
        "WHEN STAT_CD = 'U' THEN 1 "
        "WHEN STAT_CD = 'F' THEN 2 "
        "WHEN STAT_CD IN ('C','D','I','M','N','Z') THEN 3 "
        "WHEN STAT_CD IN ('H','K','L','O','P','R','V','W','E') THEN 4 "
        "ELSE 5 END AS INT)"
    )

    # o_ARM_LEAD_INSTANCE_ID: IIF(ISNULL(LTRIM(RTRIM(ARM_LEAD_INSTANCE_ID))),'@',ARM_LEAD_INSTANCE_ID)
    df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan = (
        df_Join_Fdbk_All_with_MLDB_Dtl_nice_archimedes
        .withColumn("Source_Order", expr(source_order_expr))
        .withColumn(
            "o_ARM_LEAD_INSTANCE_ID",
            when(
                (trim(col("ARM_LEAD_INSTANCE_ID")).isNull()) | (trim(col("ARM_LEAD_INSTANCE_ID")) == ""),
                F.lit("@")
            ).otherwise(col("ARM_LEAD_INSTANCE_ID"))
        )
    )
except Exception as e:
    logger.error(f"Failed computing Source_Order / o_ARM_LEAD_INSTANCE_ID: {e}", exc_info=True)
    raise

# ---------- Sorter: Srt_to_Generate_PE_Line_Nbr ----------
try:
    logger.info("Sorting records for PE line number generation (Srt_to_Generate_PE_Line_Nbr)")
    df_Srt_to_Generate_PE_Line_Nbr_mystifying_darwin = (
        df_Exp_Generate_Source_Order_And_Interaction_Groupkeycd_nifty_ramanujan
        .orderBy(
            col("OPP_ID").asc(),
            col("STAT_TS").asc(),
            col("Source_Order").asc(),
            col("ARM_LEAD_KEY_ID").asc(),
            col("ARM_LEAD_INSTANCE_ID").asc(),
            col("ARM_LOGICAL_SEGMENT_ID").asc(),
            col("DM_URL").asc(),
            col("PCN_STG_DT").asc(),
            col("OPERATIONALIZED_KEY").asc()
        )
    )
except Exception as e:
    logger.error(f"Failed during sorting for PE line numbers: {e}", exc_info=True)
    raise

# ---------- Expression: Exp_Generate_PE_Line_Nbr ----------
try:
    logger.info("Generating PE_Line_Nbr and o_SRC_File_Nm (Exp_Generate_PE_Line_Nbr)")
    w = Window.partitionBy("OPP_ID").orderBy(
        col("STAT_TS").asc(),
        col("Source_Order").asc(),
        col("ARM_LEAD_KEY_ID").asc(),
        col("ARM_LEAD_INSTANCE_ID").asc(),
        col("ARM_LOGICAL_SEGMENT_ID").asc(),
        col("DM_URL").asc(),
        col("PCN_STG_DT").asc(),
        col("OPERATIONALIZED_KEY").asc()
    )
    df_Exp_Generate_PE_Line_Nbr_careful_leibniz = (
        df_Srt_to_Generate_PE_Line_Nbr_mystifying_darwin
        .withColumn("PE_Line_Nbr", row_number().over(w))
        .withColumn("o_SRC_File_Nm", substring_index(col("SRC_FILE_NM_CRM_FDBK_ALL"), "/", -1))
    )
except Exception as e:
    logger.error(f"Failed generating PE_Line_Nbr / o_SRC_File_Nm: {e}", exc_info=True)
    raise

# ---------- Output: File_PPUB_CRM_FDBK_ALL_Processed (flat file target) ----------
try:
    logger.info("Assigning mapping output dataframe name for File_PPUB_CRM_FDBK_ALL_Processed and writing final flat file target via single-part staging")
    # assign the node's expected df_name
    df_File_PPUB_CRM_FDBK_ALL_Processed_cool_archimedes = df_Exp_Generate_PE_Line_Nbr_careful_leibniz

    tmp_dir = os.path.join(PM_TARGET_FILE_DIR, "_tmp_File_PPUB_CRM_FDBK_ALL_Processed")
    # ensure tmp_dir uses forward slashes regardless of incoming trailing/backslashes in placeholder
    tmp_dir = tmp_dir.replace('\\\\', '/')
    # write as single-part CSV (delimited by ASCII 4), no header
    df_File_PPUB_CRM_FDBK_ALL_Processed_cool_archimedes.coalesce(1).write.mode("overwrite").option("header", False).option("sep", "\x04").csv(tmp_dir)

    # find the single part file
    part_file = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_file:
        raise FileNotFoundError(f"No part file found in staging dir {tmp_dir}")
    part_file = part_file[0]
    target_path = os.path.join(PM_TARGET_FILE_DIR, "PE_Preprocess_Outupt.out")
    # move/rename the part file to the exact target path
    shutil.move(part_file, target_path)
    # cleanup staging directory
    shutil.rmtree(tmp_dir, ignore_errors=True)
    logger.info(f"Successfully wrote flat file target to {target_path}")
except Exception as e:
    logger.error(f"Failed writing flat file target File_PPUB_CRM_FDBK_ALL_Processed: {e}", exc_info=True)
    raise



job.commit()
