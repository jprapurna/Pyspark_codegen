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
GLUE_DATABASE = "REPLACE_WITH_GLUE_DATABASE"

from pyspark.sql.functions import col, trim, regexp_replace, length, expr, instr, when, substring, lit

# WRK_BIRP_NISS_APRM_DETL: try read staged parquet from S3 first, fall back to Glue Catalog read
try:
    logger.info("Attempting to read WRK_BIRP_NISS_APRM_DETL from S3 staging path")
    df_WRK_BIRP_NISS_APRM_DETL_loving_hume = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL/")
    logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from S3 staging path")
except Exception as e:
    logger.warning("S3 path for WRK_BIRP_NISS_APRM_DETL not found or unreadable; falling back to Glue Catalog read: %s" % str(e))
    try:
        logger.info("Reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog database=%s table=WRK_BIRP_NISS_APRM_DETL" % GLUE_DATABASE)
        dyf = glueContext.create_dynamic_frame.from_catalog(database=GLUE_DATABASE, table_name="WRK_BIRP_NISS_APRM_DETL")
        df_tmp = dyf.toDF()
        cols = [
            'NISS_APRM_DETL_SK','CLNDR_YR','CALL_YR','NAIC_CMPNY_CD','NISS_CMPNY_CD','ST_NM','ST_CD','NISS_ST_CD','ST_ABBR',
            'ACCTNG_LOB','CVG_TYP_CD','CVG_AMT','BI_LMT','GA_ADDED_AT_FAULT_IND','FA2_PLCY_IND','UM_UMI_STACKING','PIP_WVR_WL_IND',
            'PIP_MED_SEC_IND','PIP_LOSS_INCOME_IND','MI_PPO_IND','PRD_GRP_CD','NJ_HLTH_INSR_PRIM','NJ_EXTR_PIP_PKG','NJ_RESDNC_RLTNSHP_PIP_IND',
            'NY_SSL_IND','NY_FULL_CVG_GLASS_COMP_IND','GRGNG_ZIP_5','NISS_TERR_CD','RATNG_CMPY_CD','MLT_CAR_IND','RT_CLS','AGE','GENDR',
            'MRTL_STAT','AUTO_USE_CD','MILES_TO_WRK','GOOD_STDNT_IND','DRVR_TRNG_IND','SOI_TYP','PHY_DMG_IND','NJ_RATD_PNTS','VEH_MDL_YR',
            'NJ_EXCPTION_CD','NJ_FGVN_PNTS','PASSV_RESTRA_DISC','SNR_DRVR_IND','DEFNS_DRVR_DISC_IND','ANTI_THFT_DISC','DAY_TM_RUN_LIGHTS',
            'LMT_TORT','ANNL_STMNT_LOB_CD','CVG_TYP_IND','CVG_EXPS_VAL','TTL_WRITTN_PREM_AMT','LINE_CD','ACCDNT_YR','NISS_CVG_CD','RTNG_ZNE_CD',
            'TERM_ZNE_CD','NISS_CLASS_CD','NISS_ELIG_PNTS_CD','NISS_AGE_GRP_CD','NISS_CMMCL_IND_CD','NISS_EXCPN_CD','NISS_FGVNS_CD',
            'NISS_PASSV_RESTRA_CD','NISS_DEFNS_DRVR_CRD_CD','NISS_ANTI_THFT_DVC_CD','NISS_DAY_TM_RUN_LAMPS_DISC_CD','NISS_PLCY_LMT_CD',
            'NISS_DEDUC_CD','NISS_SSL_LIAB_CD','NISS_SUBLOB_CD','NISS_TYP_LOSS_CD','NISS_LIAB_OR_NO_FAULT_CD','NISS_ANNL_STMNT_LOB_CD',
            'NISS_PD_LOSS','NISS_PD_ALLOC_ADJUS_EXPNS','NISS_OUTSTNDG_LOSS','NISS_NO_PD_CLMS','NISS_NO_OUTSTND_CLMS','RSVD_NISS_USE',
            'NISS_RSVD_CMPNY_USE','NISS_MNFCTRS_MDL_YR','CR_BY_MAPNG_ID','DW_CR_TMSP','UPD_BY_MAPNG_ID','DW_UPD_TMSP','WRK_FLOW_RUN_ID',
            'NJ_NO_LWST_LMT_IND','NJ_NMD_DRVR_EXCL_IND','EXPS_VAL_ROLLED','CVG_CNT_IND','CVG_CNT','CVG_CD_SK','CVG_ATTR_SK','REC_DROP_IND',
            'REC_DROP_RSN_DESC','REC_EXCPN_IND','REC_EXCPN_RSN_DESC','CVG_ATTR_CHCKSUM','COMP_DED','COLL_DED','PLCY_CNTRCT_NUM','UNIT_NUM',
            'EFF_DT','NUM_OF_CARS_IN_HH','RDRVR_DT_OF_BRTH','TERM_STRT_DT','SRC_SYS_CD','DERIVED_RDRVR_AGE','FINAL_RDRVR_AGE','PNI_AGE','LOB',
            'PRINCIPAL_OPRT','SOURCE_IND_DERIVED'
        ]
        # project exactly the listed columns
        df_WRK_BIRP_NISS_APRM_DETL_loving_hume = df_tmp.select([col(c) for c in cols])
        logger.info("Successfully read WRK_BIRP_NISS_APRM_DETL from Glue Catalog and projected required columns")
    except Exception as e2:
        logger.error(f"Failed reading WRK_BIRP_NISS_APRM_DETL from Glue Catalog: {e2}", exc_info=True)
        raise

# SQ_WRK_BIRP_NISS_APRM_DETL: staged SQL override executed via spark.sql against temp view
try:
    # register the staged dataframe as a temp view named exactly as the original real table
    df_WRK_BIRP_NISS_APRM_DETL_loving_hume.createOrReplaceTempView('WRK_BIRP_NISS_APRM_DETL')
    sql_query = f"""SELECT
    NISS_APRM_DETL_SK,
    ST_NM,
    ST_ABBR,
    ACCTNG_LOB,
    trim(CVG_TYP_CD) AS CVG_TYP_CD,
    trim(CVG_AMT) AS CVG_AMT,
    trim(BI_LMT) AS BI_LMT,
    PIP_LOSS_INCOME_IND,
    trim(coalesce(PRD_GRP_CD, '')) AS PRD_GRP_CD,
    trim(coalesce(NJ_HLTH_INSR_PRIM, '')) AS NJ_HLTH_INSR_PRIM,
    trim(coalesce(NJ_EXTR_PIP_PKG, '')) AS NJ_EXTR_PIP_PKG,
    NJ_RESDNC_RLTNSHP_PIP_IND,
    NY_FULL_CVG_GLASS_COMP_IND,
    NY_SSL_IND,
    trim(COMP_DED) AS COMP_DED,
    trim(COLL_DED) AS COLL_DED,
    REC_EXCPN_IND,
    REC_EXCPN_RSN_DESC
FROM
    WRK_BIRP_NISS_APRM_DETL
WHERE
    ST_ABBR IN ('NY','NJ')
"""
    logger.info("Running rewritten SQL override for SQ_WRK_BIRP_NISS_APRM_DETL against staged temp view")
    df_SQ_WRK_BIRP_NISS_APRM_DETL_mystifying_pasteur = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Failed executing SQL override for SQ_WRK_BIRP_NISS_APRM_DETL: {e}", exc_info=True)
    raise

# EXP_BILimit_Split: compute cleaned BI_LMT and split into parts, chain aliases for reuse
try:
    logger.info("Transforming EXP_BILimit_Split: cleaning and splitting BI_LMT")
    df1 = (
        df_SQ_WRK_BIRP_NISS_APRM_DETL_mystifying_pasteur
        .withColumn('v_BI_LMT', regexp_replace(trim(col('BI_LMT')), ',', ''))
        .withColumn('v_BI_LMT_Part1_Pos', instr(col('v_BI_LMT'), '/'))
        .withColumn('v_BI_LMT_Part2_Pos', expr("locate('/', v_BI_LMT, v_BI_LMT_Part1_Pos+1)"))
        .withColumn('v_BI_LMT_Parts', (length(col('v_BI_LMT')) - length(regexp_replace(col('v_BI_LMT'), '/', ''))) + lit(1))
    )

    df2 = (
        df1
        .withColumn('v_Limit_FIELD1', expr(
            "CASE WHEN v_BI_LMT_Parts = 1 THEN v_BI_LMT WHEN v_BI_LMT_Parts IN (2,3) THEN substring(v_BI_LMT, 1, v_BI_LMT_Part1_Pos - 1) ELSE '0' END"
        ))
        .withColumn('v_Limit_FIELD2', expr(
            "CASE WHEN v_BI_LMT_Parts = 1 THEN '0' WHEN v_BI_LMT_Parts = 2 THEN substring(v_BI_LMT, v_BI_LMT_Part1_Pos + 1) WHEN v_BI_LMT_Parts = 3 THEN substring(v_BI_LMT, v_BI_LMT_Part1_Pos + 1, v_BI_LMT_Part2_Pos - v_BI_LMT_Part1_Pos - 1) ELSE '0' END"
        ))
        .withColumn('v_Limit_FIELD3', expr(
            "CASE WHEN v_BI_LMT_Parts = 3 THEN substring(v_BI_LMT, v_BI_LMT_Part2_Pos + 1) ELSE '0' END"
        ))
        .withColumn('BI_LMT_1_Decimal', expr("CAST(v_Limit_FIELD1 AS INT)"))
        .withColumn('BI_LMT_2_Decimal', expr("CAST(v_Limit_FIELD2 AS INT)"))
        .withColumn('BI_LMT_3_Decimal', expr("CAST(v_Limit_FIELD3 AS INT)"))
        .withColumn('BI_LMT_NO_OF_PARTS', col('v_BI_LMT_Parts'))
        .withColumn('SRC_BI_LMT', col('v_BI_LMT'))
    )

    df_EXP_BILimit_Split_elegant_feynman = (
        df2.select(
            'BI_LMT_1_Decimal',
            'BI_LMT_2_Decimal',
            'BI_LMT_3_Decimal',
            'BI_LMT_NO_OF_PARTS',
            'SRC_BI_LMT',
            'REC_EXCPN_IND',
            'REC_EXCPN_RSN_DESC'
        )
    )
    logger.info("Completed EXP_BILimit_Split transformation")
except Exception as e:
    logger.error(f"Failed EXP_BILimit_Split transformation: {e}", exc_info=True)
    raise

# EXP_CvgAmount_Split: clean CVG_AMT and split into parts, two-stage alias chain
try:
    logger.info("Transforming EXP_CvgAmount_Split: cleaning and splitting CVG_AMT")
    df_a = (
        df_SQ_WRK_BIRP_NISS_APRM_DETL_mystifying_pasteur
        .withColumn('v_CVG_AMT', regexp_replace(trim(col('CVG_AMT')), ',', ''))
        .withColumn('v_CVG_AMT_Part1_Pos', instr(col('v_CVG_AMT'), '/'))
        .withColumn('v_CVG_AMT_Part2_Pos', expr("locate('/', v_CVG_AMT, v_CVG_AMT_Part1_Pos+1)"))
        .withColumn('v_CVG_AMT_Parts', (length(col('v_CVG_AMT')) - length(regexp_replace(col('v_CVG_AMT'), '/', ''))) + lit(1))
    )

    df_b = (
        df_a
        .withColumn('v_AMOUNT_FIELD1', expr(
            "CASE WHEN v_CVG_AMT_Parts = 1 THEN v_CVG_AMT WHEN v_CVG_AMT_Parts = 2 THEN substring(v_CVG_AMT, 1, v_CVG_AMT_Part1_Pos - 1) WHEN v_CVG_AMT_Parts = 3 THEN substring(v_CVG_AMT, 1, v_CVG_AMT_Part1_Pos - 1) ELSE '0' END"
        ))
        .withColumn('v_AMOUNT_FIELD2', expr(
            "CASE WHEN v_CVG_AMT_Parts = 1 THEN '0' WHEN v_CVG_AMT_Parts = 2 THEN substring(v_CVG_AMT, v_CVG_AMT_Part1_Pos + 1) WHEN v_CVG_AMT_Parts = 3 THEN substring(v_CVG_AMT, v_CVG_AMT_Part1_Pos + 1, v_CVG_AMT_Part2_Pos - v_CVG_AMT_Part1_Pos - 1) ELSE '0' END"
        ))
        .withColumn('v_AMOUNT_FIELD3', expr(
            "CASE WHEN v_CVG_AMT_Parts = 3 THEN substring(v_CVG_AMT, v_CVG_AMT_Part2_Pos + 1) ELSE '0' END"
        ))
        .withColumn('CVG_AMT_1_Decimal', expr("CAST(v_AMOUNT_FIELD1 AS DECIMAL(18,2))"))
        .withColumn('CVG_AMT_2_Decimal', expr("CAST(v_AMOUNT_FIELD2 AS DECIMAL(18,2))"))
        .withColumn('CVG_AMT_3_Decimal', expr("CAST(v_AMOUNT_FIELD3 AS DECIMAL(18,2))"))
        .withColumn('CVG_AMT_NO_OF_PARTS', col('v_CVG_AMT_Parts'))
        .withColumn('SRC_CVG_AMT', col('v_CVG_AMT'))
    )

    df_EXP_CvgAmount_Split_cool_plato = df_b.select(
        'CVG_AMT_1_Decimal',
        'CVG_AMT_2_Decimal',
        'CVG_AMT_3_Decimal',
        'CVG_AMT_NO_OF_PARTS',
        'SRC_CVG_AMT'
    )
    logger.info("Completed EXP_CvgAmount_Split transformation")
except Exception as e:
    logger.error(f"Failed EXP_CvgAmount_Split transformation: {e}", exc_info=True)
    raise

# EXP_Derive_NISS_CVG_CD_And_PassThru: join SQ with the split results and derive NISS codes
try:
    logger.info("Starting EXP_Derive_NISS_CVG_CD_And_PassThru: joining inputs and deriving NISS codes")
    # join the SQ with the splits on NISS_APRM_DETL_SK (left joins to preserve SQ rows)
    df_joined = (
        df_SQ_WRK_BIRP_NISS_APRM_DETL_mystifying_pasteur.alias('sq')
        .join(df_EXP_BILimit_Split_elegant_feynman.alias('bi'), on='NISS_APRM_DETL_SK', how='left')
        .join(df_EXP_CvgAmount_Split_cool_plato.alias('cvg'), on='NISS_APRM_DETL_SK', how='left')
    )

    df_work = (
        df_joined
        .withColumn('v_CVG_AMT', regexp_replace(trim(col('CVG_AMT')), ',', ''))
        .withColumn('v_PIP_LOSS_INCOME_IND', when(col('PIP_LOSS_INCOME_IND') == 1, lit('Y')).when(col('PIP_LOSS_INCOME_IND') == 0, lit('N')).otherwise(lit('')))
        .withColumn('v_NJ_RESDNC_RLTNSHP_PIP_IND', when(col('NJ_RESDNC_RLTNSHP_PIP_IND') == 1, lit('Y')).when(col('NJ_RESDNC_RLTNSHP_PIP_IND') == 0, lit('N')).otherwise(lit('')))
        .withColumn('v_NY_FULL_CVG_GLASS_COMP_IND', when(col('NY_FULL_CVG_GLASS_COMP_IND') == 1, lit('Y')).when(col('NY_FULL_CVG_GLASS_COMP_IND') == 0, lit('N')).otherwise(lit('')))
    )

    # Derive v_CVG_CD_NewJersey via CASE translations of the original DECODE logic
    df_work = df_work.withColumn('v_CVG_CD_NewJersey', expr(
        "CASE "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35050','35058','35102') THEN '691' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35051','35059','35103') THEN '693' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35066','35067','35106') THEN '699' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '01' THEN '601' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '02' THEN '602' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '03' THEN '603' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '04' THEN '604' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '05' THEN '605' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '06' THEN '606' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '07' THEN '607' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '08' THEN '608' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '09' THEN '609' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND NJ_EXTR_PIP_PKG = '10' THEN '610' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35104') AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND (NJ_EXTR_PIP_PKG IS NULL OR NJ_EXTR_PIP_PKG IN ('',' ','0','00','#')) THEN '616' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35105') AND v_NJ_RESDNC_RLTNSHP_PIP_IND = 'Y' AND NJ_EXTR_PIP_PKG = '01' THEN '621' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM = 'Y' AND CVG_TYP_CD IN ('35069','35060','35105') AND v_NJ_RESDNC_RLTNSHP_PIP_IND = 'Y' AND NJ_EXTR_PIP_PKG = '02' THEN '622' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM != 'Y' AND CVG_TYP_CD = '35051' AND PRD_GRP_CD = 'BA' THEN '680' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND NJ_HLTH_INSR_PRIM != 'Y' AND CVG_TYP_CD IN ('35050','35058') AND PRD_GRP_CD != 'BA' THEN '681' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35050','35058','35051','35059','35066','35067','35069','35060') THEN '???' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192BI' THEN '001' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192PD' THEN '004' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13106','40011','40015','40021') THEN '208' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '211CC' AND CVG_TYP_CD IN ('21000','21017','40011','40012','40013','40014') THEN '710' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '211CL' AND CVG_TYP_CD = '22004' THEN '748' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '211CL' AND CVG_TYP_CD IN ('22007','22008','22010','22011') THEN '707' "
        "WHEN ST_ABBR = 'NJ' AND ACCTNG_LOB = '2110F' AND CVG_TYP_CD = '20300' THEN '745' "
        "ELSE '???' END"
    ))

    # Derive v_CVG_CD_NewYork via CASE translations (abridged but faithful to branching logic)
    df_work = df_work.withColumn('v_CVG_CD_NewYork', expr(
        "CASE "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007') AND v_PIP_LOSS_INCOME_IND != 'Y' AND v_CVG_AMT = '50000' THEN '0711' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35030','35007') AND v_PIP_LOSS_INCOME_IND != 'Y' THEN '0771' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35023','35024') AND v_PIP_LOSS_INCOME_IND != 'Y' AND v_CVG_AMT IN ('25000/500','50000/1000','100000/2000') THEN '0711' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35023','35024') AND v_PIP_LOSS_INCOME_IND != 'Y' THEN '0771' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND v_PIP_LOSS_INCOME_IND = 'Y' THEN '0751' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192BI' THEN '0011' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192MD' THEN '0031' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192PD' THEN '0041' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '192UM' AND CVG_TYP_CD IN ('13100','13109') THEN '2051' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '21000' AND v_NY_FULL_CVG_GLASS_COMP_IND = 'Y' AND v_CVG_AMT = '0' THEN '001 ' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '211CC' AND CVG_TYP_CD = '20206' THEN '728' "
        "WHEN ST_ABBR = 'NY' AND ACCTNG_LOB = '211CL' AND CVG_TYP_CD = '22000' AND v_CVG_AMT = '100' THEN '074 ' "
        "ELSE '' END"
    ))

    # v_NISS_CVG_CD and other derived shorter codes
    df_work = df_work.withColumn('v_NISS_CVG_CD', expr("CASE WHEN ST_ABBR = 'NJ' THEN v_CVG_CD_NewJersey WHEN ST_ABBR = 'NY' THEN substring(v_CVG_CD_NewYork,1,3) ELSE '???' END"))
    df_work = df_work.withColumn('v_NISS_LIAB_OR_NO_FAULT_CD', expr("CASE WHEN ST_ABBR = 'NY' THEN substring(v_CVG_CD_NewYork,4,1) ELSE ' ' END"))

    # v_NISS_SSL_LIAB_CD (NY-specific)
    df_work = df_work.withColumn('v_NISS_SSL_LIAB_CD_calc', expr(
        "CASE WHEN ST_ABBR = 'NY' AND v_NISS_CVG_CD IN ('001','006') AND NY_SSL_IND = 1 THEN '1' WHEN ST_ABBR = 'NY' AND v_NISS_CVG_CD IN ('001','006') AND NY_SSL_IND != 1 THEN '9' WHEN ST_ABBR = 'NY' AND NOT (v_NISS_CVG_CD IN ('001','006')) THEN ' ' ELSE '?' END"
    ))
    df_work = df_work.withColumn('NISS_SSL_LIAB_CD', when(col('v_NISS_SSL_LIAB_CD_calc') == '', lit(' ')).otherwise(col('v_NISS_SSL_LIAB_CD_calc')))

    # v_REC_EXCPN_IND combining NJ and NY rule groups per source logic
    df_work = df_work.withColumn('v_REC_EXCPN_IND', expr(
        "CASE WHEN (ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35069','35060') AND NJ_HLTH_INSR_PRIM = 'Y' AND (NJ_EXTR_PIP_PKG IN ('','0','00','#') OR NJ_EXTR_PIP_PKG IS NULL) AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND v_NISS_CVG_CD = '616') "
        "OR (ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35069','35060') AND NJ_HLTH_INSR_PRIM = 'Y' AND (NJ_EXTR_PIP_PKG IN ('','0','00','#') OR NJ_EXTR_PIP_PKG IS NULL) AND v_NJ_RESDNC_RLTNSHP_PIP_IND = 'Y' AND v_NISS_CVG_CD = '636') "
        "OR (ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35069','35060') AND NJ_HLTH_INSR_PRIM != 'Y' AND (NJ_EXTR_PIP_PKG IN ('','0','00','#') OR NJ_EXTR_PIP_PKG IS NULL) AND v_NJ_RESDNC_RLTNSHP_PIP_IND != 'Y' AND v_NISS_CVG_CD = '656') "
        "OR (ST_ABBR = 'NJ' AND ACCTNG_LOB = '191' AND CVG_TYP_CD IN ('35069','35060') AND NJ_HLTH_INSR_PRIM != 'Y' AND (NJ_EXTR_PIP_PKG IN ('','0','00','#') OR NJ_EXTR_PIP_PKG IS NULL) AND v_NJ_RESDNC_RLTNSHP_PIP_IND = 'Y' AND v_NISS_CVG_CD = '676') "
        "OR (ST_ABBR = 'NY' AND ACCTNG_LOB = '191' AND NOT (CVG_TYP_CD IN ('35030','35007','35023','35024','30019','34008','35006','35013','35017')) AND v_NISS_CVG_CD = '071' AND v_PIP_LOSS_INCOME_IND != 'Y') "
        "OR (ST_ABBR = 'NY' AND ACCTNG_LOB = '192UM' AND NOT (CVG_TYP_CD IN ('13100','13109','13116','13117')) AND v_NISS_CVG_CD = '201') "
        "THEN 'Y' ELSE '' END"
    ))

    # Final outputs per Expression: NISS_CVG_CD, NISS_SSL_LIAB_CD, NISS_LIAB_OR_NO_FAULT_CD, REC_EXCPN_IND, NISS_APRM_DETL_SK
    df_EXP_Derive_NISS_CVG_CD_And_PassThru_tender_newton = df_work.select(
        col('NISS_APRM_DETL_SK'),
        when(col('v_NISS_CVG_CD') == '', lit('???')).otherwise(col('v_NISS_CVG_CD')).alias('NISS_CVG_CD'),
        when(col('NISS_SSL_LIAB_CD') == '', lit(' ')).otherwise(col('NISS_SSL_LIAB_CD')).alias('NISS_SSL_LIAB_CD'),
        when(col('v_NISS_LIAB_OR_NO_FAULT_CD') == '', lit(' ')).otherwise(col('v_NISS_LIAB_OR_NO_FAULT_CD')).alias('NISS_LIAB_OR_NO_FAULT_CD'),
        when((col('v_REC_EXCPN_IND') == 'Y') | (col('REC_EXCPN_IND') == 'Y'), lit('Y')).otherwise(lit('')).alias('REC_EXCPN_IND')
    )

    logger.info("Completed EXP_Derive_NISS_CVG_CD_And_PassThru transformation")
except Exception as e:
    logger.error(f"Failed EXP_Derive_NISS_CVG_CD_And_PassThru: {e}", exc_info=True)
    raise

# UPD_NISS_CVG_CD: translate Update Strategy DD_UPDATE and apply load-modify-store-back against the S3 target
try:
    logger.info("Applying Update Strategy UPD_NISS_CVG_CD: marking rows as UPDATE and performing load-modify-store-back")
    # mark all rows as UPDATE per configuration
    df_marked = df_EXP_Derive_NISS_CVG_CD_And_PassThru_tender_newton.withColumn('dd_op', lit('UPDATE'))

    # drop REJECT rows (none expected because expression is DD_UPDATE) - keep only not REJECT
    df_survivors = df_marked.filter(col('dd_op') != 'REJECT')

    # read current full target table from S3 to apply changes
    try:
        logger.info("Reading existing target WRK_BIRP_NISS_APRM_DETL1 from S3 for load-modify-store-back")
        existing_df = spark.read.parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
    except Exception as e:
        logger.error(f"Failed reading existing target WRK_BIRP_NISS_APRM_DETL1 from S3: {e}", exc_info=True)
        raise

    # identify keys being changed (INSERT/UPDATE) - here UPDATE only
    changed_keys_df = df_survivors.select('NISS_APRM_DETL_SK').distinct()

    # anti-join to remove any existing rows that will be replaced by UPDATE/INSERT
    existing_minus_changed = existing_df.join(changed_keys_df, on='NISS_APRM_DETL_SK', how='left_anti')

    # build rows to be written back: only rows marked INSERT or UPDATE
    rows_to_upsert = df_survivors.filter(col('dd_op').isin('INSERT', 'UPDATE')).drop('dd_op')

    # union the remaining existing rows with the upsert rows to form the complete new target
    combined_df = existing_minus_changed.unionByName(rows_to_upsert, allowMissingColumns=True)

    # write the combined dataframe back to the same S3 path (full overwrite)
    try:
        logger.info("Writing combined WRK_BIRP_NISS_APRM_DETL1 back to S3 as parquet (overwrite) - this rewrites the full target and may be costly for large tables")
        combined_df.write.mode('overwrite').parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
    except Exception as e:
        logger.error(f"Failed writing updated WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
        raise

    # assign for downstream consumption
    df_UPD_NISS_CVG_CD_loving_tesla = combined_df
    logger.info("Completed Update Strategy apply and wrote WRK_BIRP_NISS_APRM_DETL1 to S3")
except Exception as e:
    logger.error(f"Failed UPD_NISS_CVG_CD load-modify-store-back: {e}", exc_info=True)
    raise

# FDR_LIB_WRK_BIRP_NISS_APRM_DETL1: final Output write as Parquet to S3 (strip FDR_LIB_ prefix from path)
try:
    logger.info("Writing final target WRK_BIRP_NISS_APRM_DETL1 to S3 as parquet (overwrite)")
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_clever_schrodinger = df_UPD_NISS_CVG_CD_loving_tesla
    df_FDR_LIB_WRK_BIRP_NISS_APRM_DETL1_clever_schrodinger.write.mode('overwrite').parquet(f"s3://{S3_OUTPUT_BUCKET}/WRK_BIRP_NISS_APRM_DETL1/")
    logger.info("Successfully wrote WRK_BIRP_NISS_APRM_DETL1 to S3")
except Exception as e:
    logger.error(f"Failed writing final target WRK_BIRP_NISS_APRM_DETL1 to S3: {e}", exc_info=True)
    raise


job.commit()
