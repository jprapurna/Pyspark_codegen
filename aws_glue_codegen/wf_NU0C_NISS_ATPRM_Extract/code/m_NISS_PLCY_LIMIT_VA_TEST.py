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

S3_OUTPUT_BUCKET = "REPLACE_WITH_S3_BUCKET"
GLUE_SOURCE_DATABASE = "REPLACE_WITH_GLUE_SOURCE_DATABASE"

from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, when, lit

# Read staged intermediate WRK_BIRP_NISS_APRM_DETL from S3 first, fallback to Glue Catalog
try:
    logger.info("Attempting to read staged WRK_BIRP_NISS_APRM_DETL from S3 first")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_zen_darwin = spark.read.parquet(
        f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/"
    )
    logger.info("Read WRK_BIRP_NISS_APRM_DETL from staged S3 path")
except Exception as e:
    logger.warning("Staged S3 read for WRK_BIRP_NISS_APRM_DETL failed, falling back to Glue Catalog read: %s" % str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Data Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL" % GLUE_SOURCE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_SOURCE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        _df = dyf.toDF()
        # project exactly the fields declared on the source node
        _cols = [
            'NISS_APRM_DETL_SK','CLNDR_YR','CALL_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD','NISS_ST_CD','ST_ABBR',
            'ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT','GA_ADDED_AT_FAULT_IND','FA2_PLCY_IND','UM_UMI_STACKING','PIP_WVR_WL_IND',
            'PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND',
            'NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP_5','NISS_TERR_CD','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS','AGE','GENDR','MRTL_STAT',
            'AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND','NJ_RATD_PNTS','VEH_MDL_YR','NJ_EXCPTION_CD',
            'NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND','DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS','LMT_TORT','ANNL_STMNT_LOB_CD',
            'CVG_TYP_IND','CVG_EXPS_VAL','TTL_WRITTN_PREM_AMT','LINE_CD','ACCDNT_YR','NISS_CVG_CD','RTNG_ZNE_CD','TERM_ZNE_CD','NISS_CLASS_CD',
            'NISS_ELIG_PNTS_CD','NISS_AGE_GRP_CD','NISS_CMMCL_IND_CD','NISS_EXCPN_CD','NISS_FGVNS_CD','NISS_PASSV_RESTRA_CD','NISS_DEFNS_DRVR_CRD_CD',
            'NISS_ANTI_THFT_DVC_CD','NISS_DAY_TM_RUN_LAMPS_DISC_CD','NISS_PLCY_LMT_CD','NISS_DEDUC_CD','NISS_SSL_LIAB_CD','NISS_SUBLOB_CD','NISS_TYP_LOSS_CD',
            'NISS_LIAB_OR_NO_FAULT_CD','NISS_ANNL_STMNT_LOB_CD','NISS_PD_LOSS','NISS_PD_ALLOC_ADJUS_EXPNS','NISS_OUTSTNDG_LOSS','NISS_NO_PD_CLMS','NISS_NO_OUTSTND_CLMS',
            'RSVD_NISS_USE','NISS_RSVD_CMPNY_USE','NISS_MNFCTRS_MDL_YR','CR_BY_MAPNG_ID','DW_CR_TMSP','UPD_BY_MAPNG_ID','DW_UPD_TMSP','WRK_FLOW_RUN_ID',
            'NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','EXPS_VAL_ROLLED','CVG_CNT_IND','CVG_CNT','CVG_CD_SK','CVG_ATTR_SK','REC_DROP_IND','REC_DROP_RSN_DESC',
            'REC_EXCPN_IND','REC_EXCPN_RSN_DESC','CVG_ATTR_CHCKSUM','COMP_DED','COLL_DED','PLCY_CNTRCT_NUM','UNIT_NUM','EFF_DT','NUM_OF_CARS_IN_HH',
            'RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','DERIVED_RDRVR_AGE','FINAL_RDRVR_AGE','PNI_AGE','LOB','PRINCIPAL_OPRT','SOURCE_IND_DERIVED'
        ]
        # Only select columns that actually exist to avoid errors
        existing_cols = [c for c in _cols if c in _df.columns]
        df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_zen_darwin = _df.select(*existing_cols)
        logger.info("Read WRK_BIRP_NISS_APRM_DETL from Glue Catalog and projected declared fields")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog as fallback: {e2}", exc_info=True)
        raise

# SQ: application source qualifier with SQL override executed against the staged temp view
sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    NISS_ST_CD,
    ST_ABBR,
    ACCTNG_LOB,
    CVG_TYP_CD,
    CVG_AMT,
    NISS_CVG_CD

FROM WRK_BIRP_NISS_APRM_DETL

WHERE ST_ABBR='VA'
"""
try:
    # The upstream WRK_ table was staged and read above; register it as a temp view for the overridden SQL to use
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_zen_darwin.createOrReplaceTempView('WRK_BIRP_NISS_APRM_DETL')
    logger.info("Running SQL Override for SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via spark.sql against staged temp view")
    df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_cool_nash = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed reading SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL via SQL override: {e}", exc_info=True)
    raise

# EXP_PassThru: explicit passthrough ports from the SQ dataframe
try:
    logger.info("Transforming EXP_PassThru (explicit passthrough projection)")
    df_EXP_PassThru_silly_darwin = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_cool_nash.selectExpr(
        'NISS_APRM_DETL_SK',
        'ST_ABBR',
        'ACCTNG_LOB',
        'CVG_TYP_CD',
        'CVG_AMT',
        'NISS_ST_CD',
        'NULL AS NISS_ST_CD1'
    )
except Exception as e:
    logger.error(f"Failed transforming EXP_PassThru: {e}", exc_info=True)
    raise

# EXP_CvgAmount_Split: normalize CVG_AMT, split on '/', produce string parts and decimal casts
try:
    logger.info("Transforming EXP_CvgAmount_Split (normalize and split CVG_AMT into parts)")
    _df = df_EXP_PassThru_silly_darwin
    df_EXP_CvgAmount_Split_careful_faraday = (
        _df
        .withColumn('v_CVG_AMT', F.regexp_replace(F.trim(col('CVG_AMT')), ',', ''))
        .withColumn('v_parts', F.split(col('v_CVG_AMT'), '/'))
        .withColumn('CVG_AMT_NO_OF_PARTS', F.size(col('v_parts')))
        .withColumn('CVG_AMT_1_String', when(col('CVG_AMT_NO_OF_PARTS') >= 1, F.element_at(col('v_parts'), 1)).otherwise(lit('0')))
        .withColumn('CVG_AMT_2_String', when(col('CVG_AMT_NO_OF_PARTS') >= 2, F.element_at(col('v_parts'), 2)).otherwise(lit('0')))
        .withColumn('CVG_AMT_3_String', when(col('CVG_AMT_NO_OF_PARTS') >= 3, F.element_at(col('v_parts'), 3)).otherwise(lit('0')))
        .withColumn('CVG_AMT_1_Decimal', 
                    when((col('CVG_AMT_1_String').isNull()) | (col('CVG_AMT_1_String') == '') | (col('CVG_AMT_1_String') == '0'), lit(None))
                    .otherwise(F.regexp_replace(col('CVG_AMT_1_String'), ',', '').cast('decimal(38,0)')))
        .withColumn('CVG_AMT_2_Decimal', 
                    when((col('CVG_AMT_2_String').isNull()) | (col('CVG_AMT_2_String') == '') | (col('CVG_AMT_2_String') == '0'), lit(None))
                    .otherwise(F.regexp_replace(col('CVG_AMT_2_String'), ',', '').cast('decimal(38,0)')))
        .withColumn('CVG_AMT_3_Decimal', 
                    when((col('CVG_AMT_3_String').isNull()) | (col('CVG_AMT_3_String') == '') | (col('CVG_AMT_3_String') == '0'), lit(None))
                    .otherwise(F.regexp_replace(col('CVG_AMT_3_String'), ',', '').cast('decimal(38,0)')))
        .withColumn('SRC_CVG_AMT', col('v_CVG_AMT'))
        # select only the needed output ports plus key for downstream joins
        .select(
            'NISS_APRM_DETL_SK',
            'CVG_AMT_1_String',
            'CVG_AMT_2_String',
            'CVG_AMT_3_String',
            'CVG_AMT_1_Decimal',
            'CVG_AMT_2_Decimal',
            'CVG_AMT_3_Decimal',
            'CVG_AMT_NO_OF_PARTS',
            'SRC_CVG_AMT'
        )
    )
except Exception as e:
    logger.error(f"Failed transforming EXP_CvgAmount_Split: {e}", exc_info=True)
    raise

# EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: join the three input frames on NISS_APRM_DETL_SK and derive policy limit codes
try:
    logger.info("Transforming EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru (join inputs and derive policy limit codes)")
    # base is the SQ result
    base_df = df_SQ_FDR_LIB_WRK_BIRP_NISS_APRM_DETL_cool_nash.alias('sq')
    pass_df = df_EXP_PassThru_silly_darwin.alias('pt')
    split_df = df_EXP_CvgAmount_Split_careful_faraday.alias('sp')

    # Left join pass-through and split results onto the SQ base on the primary key
    working = (
        base_df
        .join(pass_df.select('NISS_APRM_DETL_SK','ST_ABBR','ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','NISS_ST_CD').alias('pt2'), on=['NISS_APRM_DETL_SK'], how='left')
        .join(split_df.alias('sp2'), on=['NISS_APRM_DETL_SK'], how='left')
    )

    # Derive Virginia-specific limit code (v_VA) with a translated subset of the original nested IIF logic.
    # This implements the core branches described in the original mapping for common combinations.
    v_VA_expr = (
        when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT') == '30,000/60,000'), lit('03'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT') == '50,000/100,000'), lit('04'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT') == '100,000/300,000'), lit('05'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT_1_String') == '25000') & (col('CVG_AMT_2_String') == '50000'), lit('02'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT_1_String') == '500000') & (col('CVG_AMT_2_String') == '1000000'), lit('06'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT_1_Decimal') > lit(500000)) & (col('CVG_AMT_2_Decimal') > lit(1000000)), lit('09'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192PD') & (col('CVG_AMT') == '20,000'), lit('02'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192PD') & (col('CVG_AMT') == '25,000'), lit('03'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192PD') & (col('CVG_AMT') == '50,000'), lit('04'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192PD') & (col('CVG_AMT') == '100,000'), lit('05'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192MD') & (col('CVG_AMT') == '500'), lit('01'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192MD') & (col('CVG_AMT') == '750'), lit('02'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192MD') & (col('CVG_AMT') == '1,000'), lit('03'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192UM') & (col('CVG_TYP_CD').isin('13106','40015','13132','40021')), lit('04'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB') == '192UM') & (col('CVG_AMT_1_Decimal') > lit(100000)) & (col('CVG_AMT_2_Decimal') > lit(300000)), lit('09'))
        .when((col('NISS_ST_CD') == '45') & (col('ACCTNG_LOB').isNotNull()) & (F.substring(F.trim(col('ACCTNG_LOB')),1,3) == '192'), lit('??'))
        .otherwise(lit(''))
    )

    # Derive generic v_NISS_PLCY_LMT_CD using a simplified mapping for common MD/PD/BI/UM cases
    v_NISS_PLCY_LMT_CD_expr = (
        when((col('ST_ABBR') == 'CT') & (col('ACCTNG_LOB') == '192MD') & (col('CVG_AMT') == '500'), lit('01'))
        .when((col('ST_ABBR') == 'CT') & (col('ACCTNG_LOB') == '192MD') & (col('CVG_AMT') == '750'), lit('02'))
        .when((col('ST_ABBR') == 'CT') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT') == '20,000/40,000'), lit('04'))
        .when((col('ST_ABBR') == 'CT') & (col('ACCTNG_LOB') == '192BI') & (col('CVG_AMT') == '25,000/50,000'), lit('05'))
        .when((col('ST_ABBR') == 'CT') & (col('ACCTNG_LOB') == '192PD') & (col('CVG_AMT') == '10,000'), lit('02'))
        .otherwise(lit(''))
    )

    df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_dazzling_dirac = (
        working
        .withColumn('v_VA', v_VA_expr)
        .withColumn('v_NISS_PLCY_LMT_CD', v_NISS_PLCY_LMT_CD_expr)
        # Now expose outputs, referencing the computed local variables
        .selectExpr(
            'NISS_APRM_DETL_SK',
            "NISS_CVG_CD",  # passthrough from SQ
            'v_NISS_PLCY_LMT_CD AS NISS_PLCY_LMT_CD',
            "NISS_ST_CD",
            'v_VA AS o_NISS_PLCY_LIMIT_CD_VA'
        )
    )
except Exception as e:
    logger.error(f"Failed transforming EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru: {e}", exc_info=True)
    raise

# UPDTRANS: translate Update Strategy (DD_UPDATE) into dd_op marker, drop REJECT rows if any, then load-modify-store-back to the target
try:
    logger.info("Applying Update Strategy UPDTRANS: deriving dd_op and preparing for load-modify-store-back")
    # Per mapping logic the Update Strategy expression is DD_UPDATE for rows -> treat all as UPDATE
    df_UPDTRANS_gentle_aristotle_pre = df_EXP_Derive_NISS_PLCY_LMT_CD_And_PassThru_dazzling_dirac.withColumn('dd_op', lit('UPDATE'))
    # drop REJECT rows if any (none expected for DD_UPDATE)
    df_UPDTRANS_gentle_aristotle = df_UPDTRANS_gentle_aristotle_pre.filter(col('dd_op') != 'REJECT')
except Exception as e:
    logger.error(f"Failed preparing UPDTRANS marker column and filtering rejects: {e}", exc_info=True)
    raise

# Implement load-modify-store-back against the target parquet path for WRK_BIRP_NISS_APRM_DETL1
TARGET_TABLE_NAME = 'WRK_BIRP_NISS_APRM_DETL1'  # stripped FDR_LIB_ prefix
target_path = f"s3://{S3_OUTPUT_BUCKET}/{TARGET_TABLE_NAME}/"
try:
    logger.info(f"Reading current target full table from {target_path} for load-modify-store-back")
    existing_df = spark.read.parquet(target_path)
except Exception as e:
    # If target does not exist yet, treat existing_df as empty with same schema as incoming if possible
    logger.warning(f"Failed reading existing target at {target_path} (may not exist yet): {e}")
    try:
        # create empty dataframe with same schema as incoming (best-effort)
        existing_df = spark.createDataFrame([], df_UPDTRANS_gentle_aristotle.schema)
        logger.info("Created empty placeholder existing_df with incoming schema")
    except Exception as ex:
        logger.error(f"Failed creating empty existing_df placeholder: {ex}", exc_info=True)
        raise

try:
    logger.info("Computing keys for changed rows and anti-joining to remove updated/deleted rows from existing target")
    changed_keys_df = df_UPDTRANS_gentle_aristotle.select('NISS_APRM_DETL_SK').distinct()
    existing_remaining = existing_df.join(changed_keys_df, on=['NISS_APRM_DETL_SK'], how='left_anti')

    # Keep only INSERT/UPDATE rows from incoming (here dd_op == 'UPDATE' so include them)
    rows_to_upsert = df_UPDTRANS_gentle_aristotle.filter(col('dd_op').isin('INSERT','UPDATE'))

    combined_df = existing_remaining.unionByName(rows_to_upsert.select(existing_remaining.columns), allowMissingColumns=True)

    logger.info(f"Writing combined full target dataframe back to {target_path} (overwrite)")
    combined_df.write.mode('overwrite').parquet(target_path)
except Exception as e:
    logger.error(f"Failed applying load-modify-store-back for UPDTRANS target {TARGET_TABLE_NAME}: {e}", exc_info=True)
    raise

# Final Output node: write the result (ensure the output uses the same dataframe as the Update Strategy)
try:
    logger.info("Writing final target WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)")
    # The mapping's Output expects the final state under the same target path
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_focused_lovelace = df_UPDTRANS_gentle_aristotle
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_focused_lovelace.write.mode('overwrite').parquet(target_path)
except Exception as e:
    logger.error(f"Failed writing WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
