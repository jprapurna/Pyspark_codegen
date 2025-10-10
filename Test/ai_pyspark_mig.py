import logging
from pyspark.sql.functions import col, when, trim, coalesce

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Step 1: Load source data from Unity Catalog tables
    logging.info(\"Loading source data from Unity Catalog tables...\")
    source_df_1 = spark.table(\"catalog.source_db.WRK_BIRP_NISS_APRM_DETL\")
    source_df_2 = spark.sql(\"\"\"
        SELECT
            NISS_APRM_DETL_SK,
            ST_ABBR,
            ST_CD,
            RATNG_CMPY_CD,
            MLT_CAR_IND,
            RT_CLS,
            FINAL_RDRVR_AGE,
            GENDR,
            MRTL_STAT,
            AUTO_USE_CD,
            MILES_TO_WRK,
            GOOD_STDNT_IND,
            DRVR_TRNG_IND,
            SOI_TYP,
            ACCTNG_LOB,
            CVG_TYP_CD,
            CASE WHEN ST_ABBR='FL' AND FINAL_RDRVR_AGE!='' THEN
                CASE 
                    WHEN CAST(FINAL_RDRVR_AGE AS INT) < 25 AND RATNG_CMPY_CD IN ('2F', '2M') AND MRTL_STAT='M' AND AUTO_USE_CD!='FRM' THEN '1620'
                    WHEN CAST(FINAL_RDRVR_AGE AS INT) < 25 AND RATNG_CMPY_CD IN ('2F', '2M') AND MRTL_STAT='M' AND AUTO_USE_CD='FRM' THEN '1623'
                    WHEN CAST(FINAL_RDRVR_AGE AS INT) < 25 AND RATNG_CMPY_CD IN ('2F', '2M') AND MRTL_STAT='M' AND AUTO_USE_CD!='FRM' THEN '1622'
                END
            END AS NISS_CLASS_CD_FL
        FROM catalog.source_db.FDR_WRK_BIRP_NISS_APRM_DETL
        WHERE ST_ABBR NOT IN ('NY', 'NJ') AND SOURCE_IND_DERIVED='FARMERS'
    \"\"\")

    # Step 2: Apply transformations
    logging.info(\"Applying transformations...\")
    transformed_df = source_df_2.select(
        col(\"NISS_APRM_DETL_SK\"),
        col(\"ST_ABBR\"),
        col(\"ST_CD\"),
        col(\"RATNG_CMPY_CD\"),
        col(\"MLT_CAR_IND\"),
        col(\"RT_CLS\"),
        col(\"FINAL_RDRVR_AGE\"),
        col(\"GENDR\"),
        col(\"MRTL_STAT\"),
        col(\"AUTO_USE_CD\"),
        col(\"MILES_TO_WRK\"),
        col(\"GOOD_STDNT_IND\"),
        col(\"DRVR_TRNG_IND\"),
        col(\"SOI_TYP\"),
        col(\"ACCTNG_LOB\"),
        col(\"CVG_TYP_CD\"),
        when(col(\"AUTO_USE_CD\").isNull(), \"\").otherwise(col(\"AUTO_USE_CD\")).alias(\"AUTO_USE_CD\"),
        col(\"FINAL_RDRVR_AGE\").cast(\"int\").alias(\"IAGE\"),
        col(\"MILES_TO_WRK\").cast(\"int\").alias(\"IMILES_TO_WRK\"),
        when(
            (~col(\"ST_ABBR\").isin(\"NY\", \"NJ\", \"NC\", \"AR\", \"SD\", \"PA\")) & (col(\"ACCTNG_LOB\") == \"1923D\"), \"941400\"
        ).when(
            (col(\"ST_ABBR\") == \"AR\") & (col(\"ACCTNG_LOB\") == \"191\"), \"946400\"
        ).when(
            (col(\"ST_ABBR\") == \"SD\") & (col(\"ACCTNG_LOB\") == \"1923D\"), \"946400\"
        ).when(
            (col(\"ST_ABBR\") == \"PA\") & (col(\"ACCTNG_LOB\") == \"191\") & (col(\"CVG_TYP_CD\").isin(\"35043\", \"35046\")), \"999700\"
        ).when(
            (col(\"ST_ABBR\") == \"PA\") & (col(\"ACCTNG_LOB\") == \"1923D\") & (col(\"CVG_TYP_CD\").isin(\"35043\", \"35046\")), \"999800\"
        ).when(
            (col(\"ST_ABBR\") == \"PA\") & (~col(\"ACCTNG_LOB\").isin(\"191\", \"1923D\")) & (col(\"CVG_TYP_CD\").isin(\"35043\", \"35046\")), \"999900\"
        ).when(
            (col(\"ST_ABBR\") == \"NC\") & (col(\"ACCTNG_LOB\") == \"191\"), \"960400\"
        ).when(
            (col(\"ST_ABBR\") == \"NC\") & (col(\"ACCTNG_LOB\") == \"1923D\"), \"960700\"
        ).otherwise(\"\").alias(\"v_CLASS_CD_Indemnity\"),
        when(
            (~col(\"ST_ABBR\").isin(\"NY\", \"NJ\", \"NC\", \"MI\", \"MT\", \"PA\")) &
            (col(\"RATNG_CMPY_CD\").isin(\"N\", \"B\", \"K\", \"J\", \"L\")) &
            (col(\"ST_CD\").isin(\"04\", \"07\", \"08\", \"10\", \"11\", \"12\", \"13\", \"14\", \"15\", \"16\", \"18\", \"21\", \"22\", \"23\", \"24\", \"25\", \"26\", \"27\", \"28\", \"29\", \"30\", \"31\", \"32\", \"33\", \"35\", \"36\", \"37\", \"38\", \"39\", \"40\", \"41\", \"42\", \"44\", \"45\", \"46\", \"47\", \"48\", \"49\", \"50\", \"51\", \"53\", \"54\", \"55\", \"56\"))
        ).when(
            (col(\"AUTO_USE_CD\") == \"FRM\") & (col(\"IAGE\") < 25), \"1623\"
        ).when(
            (col(\"AUTO_USE_CD\") == \"FRM\") & (col(\"IAGE\") >= 25) & (col(\"IAGE\") < 65), \"1621\"
        ).when(
            (col(\"AUTO_USE_CD\") == \"FRM\") & (col(\"IAGE\") >= 65), \"1624\"
        ).when(
            (col(\"AUTO_USE_CD\") != \"FRM\") & (col(\"IAGE\") < 25), \"1620\"
        ).when(
            (col(\"AUTO_USE_CD\") != \"FRM\") & (col(\"IAGE\") >= 25) & (col(\"IAGE\") < 65), \"1616\"
        ).when(
            (col(\"AUTO_USE_CD\") != \"FRM\") & (col(\"IAGE\") >= 65), \"1625\"
        ).otherwise(\"\").alias(\"v_CLASS_CD_Auto_1\"),
        when(col(\"ST_ABBR\") == \"FL\", col(\"NISS_CLASS_CD_FL\")).otherwise(\"\").alias(\"v_CLASS_CD_Auto_1A\")
    )

    # Step 3: Update strategy
    logging.info(\"Applying update strategy...\")
    update_df = transformed_df.filter(col(\"NISS_APRM_DETL_SK\").isNotNull())

    # Step 4: Write to target Unity Catalog table
    logging.info(\"Writing data to target Unity Catalog table...\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.WRK_BIRP_NISS_APRM_DETL\")
    update_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.WRK_BIRP_NISS_APRM_DETL\")

    logging.info(\"ETL process completed successfully.\")

except Exception as e:
    logging.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise