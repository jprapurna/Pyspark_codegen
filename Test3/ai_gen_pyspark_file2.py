import logging
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load data from Unity Catalog source table
    logger.info(\"Loading data from Unity Catalog source table: catalog.source_db.File_SAF_Baseinteractiongroup\")
    source_df = spark.table(\"catalog.source_db.File_SAF_Baseinteractiongroup\")
    logger.info(f\"Source data loaded successfully with {source_df.count()} records.\")

    # Step 2: Apply transformations based on lineage plan
    logger.info(\"Applying transformations to the source data.\")
    
    # Example transformation: Filter records where InteractionGroupKey_Cd is not null
    transformed_df = source_df.filter(F.col(\"InteractionGroupKey_Cd\").isNotNull())
    
    # Example transformation: Add a new column for data quality check
    transformed_df = transformed_df.withColumn(\"DataQualityFlag\", F.when(F.col(\"InteractionGroupKey_Cd\").isNotNull(), F.lit(\"Valid\")).otherwise(F.lit(\"Invalid\")))
    
    # Example transformation: Rename columns for clarity
    transformed_df = transformed_df.withColumnRenamed(\"InteractionGroupKey_Cd\", \"InteractionGroupKeyCode\") \\
                                   .withColumnRenamed(\"InteractionGroupBusinessKey_Cd\", \"InteractionGroupBusinessKeyCode\")
    
    # Example transformation: Aggregate data (if applicable)
    # For demonstration, let's assume we need to count records grouped by InteractionGroupKeyCode
    aggregated_df = transformed_df.groupBy(\"InteractionGroupKeyCode\").agg(F.count(\"*\").alias(\"RecordCount\"))
    
    logger.info(\"Transformations applied successfully.\")

    # Step 3: Write transformed data to Unity Catalog target table
    target_table = \"catalog.target_db.transformed_interactiongroup\"
    logger.info(f\"Writing transformed data to Unity Catalog target table: {target_table}\")
    
    # Drop the target table if it exists
    spark.sql(f\"DROP TABLE IF EXISTS {target_table}\")
    
    # Write the data in Delta format
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_table)
    logger.info(f\"Transformed data written successfully to {target_table}.\")

    # Step 4: Write aggregated data to another Unity Catalog target table (if required)
    aggregated_target_table = \"catalog.target_db.aggregated_interactiongroup\"
    logger.info(f\"Writing aggregated data to Unity Catalog target table: {aggregated_target_table}\")
    
    # Drop the aggregated target table if it exists
    spark.sql(f\"DROP TABLE IF EXISTS {aggregated_target_table}\")
    
    # Write the aggregated data in Delta format
    aggregated_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(aggregated_target_table)
    logger.info(f\"Aggregated data written successfully to {aggregated_target_table}.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise


import logging
from pyspark.sql import functions as F
import psycopg2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load data from Unity Catalog source table
    logger.info(\"Loading data from Unity Catalog source table: catalog.source_db.File_SAF_Baseinteractiongroup\")
    source_df = spark.table(\"catalog.source_db.File_SAF_Baseinteractiongroup\")

    # Step 2: Apply transformations based on lineage plan
    logger.info(\"Applying transformations to the source data\")
    
    # Example transformation: Add a new column with concatenated values
    transformed_df = source_df.withColumn(
        \"InteractionGroupFullKey\",
        F.concat_ws(\"_\", F.col(\"InteractionGroupKey_Cd\"), F.col(\"InteractionGroupBusinessKey_Cd\"))
    )
    
    # Example transformation: Filter rows based on a condition
    transformed_df = transformed_df.filter(F.col(\"PolicyPremiumChange_Am\") > 0)
    
    # Example transformation: Aggregate data
    aggregated_df = transformed_df.groupBy(\"InteractionGroupKey_Cd\").agg(
        F.sum(\"PolicyPremiumChange_Am\").alias(\"TotalPolicyPremiumChange\"),
        F.count(\"*\").alias(\"RecordCount\")
    )
    
    # Example transformation: Join with another Unity Catalog table
    logger.info(\"Joining with another Unity Catalog table: catalog.source_db.PolicyDetails\")
    policy_details_df = spark.table(\"catalog.source_db.PolicyDetails\")
    joined_df = aggregated_df.join(
        policy_details_df,
        aggregated_df[\"InteractionGroupKey_Cd\"] == policy_details_df[\"PolicyKey_Cd\"],
        \"left\"
    ).select(
        aggregated_df[\"InteractionGroupKey_Cd\"],
        aggregated_df[\"TotalPolicyPremiumChange\"],
        aggregated_df[\"RecordCount\"],
        policy_details_df[\"PolicyType_Cd\"]
    )
    
    # Step 3: Write the transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table: catalog.target_db.TransformedInteractionGroup\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.TransformedInteractionGroup\")
    joined_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.TransformedInteractionGroup\")
    
    logger.info(\"Data successfully written to Unity Catalog target table\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise


import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog
    logger.info(\"Loading source data from Unity Catalog table: catalog.source_db.source_table\")
    source_df = spark.table(\"catalog.source_db.source_table\")

    # Step 2: Transformation Logic
    logger.info(\"Applying transformations to generate unique row IDs\")
    
    # Define a window specification for row numbering
    window_spec = Window.orderBy(F.lit(1))  # Order by a constant to generate sequential IDs
    
    # Add sequence transformation columns
    transformed_df = source_df.withColumn(\"seq_baseinteractiongroup_row_id\", F.row_number().over(window_spec)) \\
                               .withColumn(\"NEXTVAL\", F.col(\"seq_baseinteractiongroup_row_id\") + 1) \\
                               .withColumn(\"CURRVAL\", F.col(\"seq_baseinteractiongroup_row_id\"))
    
    # Log the schema of the transformed DataFrame
    logger.info(\"Transformed DataFrame schema:\")
    transformed_df.printSchema()

    # Step 3: Write transformed data to Unity Catalog target table
    target_table = \"catalog.target_db.target_table\"
    logger.info(f\"Dropping existing target table if it exists: {target_table}\")
    spark.sql(f\"DROP TABLE IF EXISTS {target_table}\")
    
    logger.info(f\"Writing transformed data to Unity Catalog target table: {target_table}\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_table)

    logger.info(\"ETL workflow completed successfully.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL workflow: {str(e)}\", exc_info=True)
    raise


import logging
from pyspark.sql import functions as F

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog tables
    logger.info(\"Loading source data from Unity Catalog tables...\")
    exp_anchor_ids_df = spark.table(\"catalog.source_db.exp_anchor_ids\")
    seq_baseinteractiongroup_row_id_df = spark.table(\"catalog.source_db.seq_baseinteractiongroup_row_id\")

    # Step 2: Perform transformations as per the lineage plan
    logger.info(\"Performing transformations...\")

    # Passthrough fields
    exp_anchor_ids_df = exp_anchor_ids_df.select(\"NEXTVAL\", \"LoadEvent_Id\")

    # Transformation: Calculate intermediate variable 'v_loadevent_id'
    exp_anchor_ids_df = exp_anchor_ids_df.withColumn(
        \"v_loadevent_id\",
        F.expr(\"TO_DECIMAL(RPAD(to_char(LoadEvent_Id), 18, '0'))\")
    )

    # Transformation: Calculate final row ID 'o_row_id'
    exp_anchor_ids_df = exp_anchor_ids_df.withColumn(
        \"o_row_id\",
        F.expr(\"v_loadevent_id + NEXTVAL\")
    )

    # Step 3: Join with sequence data if required (based on lineage plan)
    logger.info(\"Joining with sequence data...\")
    transformed_df = exp_anchor_ids_df.join(
        seq_baseinteractiongroup_row_id_df,
        on=\"NEXTVAL\",  # Assuming NEXTVAL is the join key
        how=\"inner\"
    )

    # Step 4: Write the transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table...\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.SAF_Baseinteractiongroup\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.SAF_Baseinteractiongroup\")

    logger.info(\"ETL workflow completed successfully.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL workflow: {str(e)}\")
    raise


import logging
from pyspark.sql.functions import col, when, lit, upper, isnull, expr

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog table
    logger.info(\"Loading source data from Unity Catalog table...\")
    source_df = spark.table(\"catalog.source_db.source_table\")

    # Step 2: Apply transformations based on the lineage plan
    logger.info(\"Applying transformations to flag records based on conditions...\")

    # Passthrough fields
    transformed_df = source_df.select(
        col(\"InteractionGroupKey_Cd\"),
        col(\"SequenceStart_It\"),
        col(\"Source_Cd\")
    )

    # Transformation: Local variable for existing max sequence start
    transformed_df = transformed_df.withColumn(
        \"V_Existing_Max_SequenceStart_It\",
        when(
            col(\"Source_Cd\") == \"ARM\",
            expr(\":LKP.Lkp_Interactiongrpkeycd_Max_Sequence_ARM(UPPER(InteractionGroupKey_Cd))\")
        )
    )

    # Transformation: Local variable for max sequence start
    transformed_df = transformed_df.withColumn(
        \"Max_SequenceStart_It\",
        when(
            isnull(col(\"V_Existing_Max_SequenceStart_It\")),
            lit(0)
        ).otherwise(col(\"V_Existing_Max_SequenceStart_It\"))
    )

    # Transformation: Derived field to flag incremental records
    transformed_df = transformed_df.withColumn(
        \"Incremental_Flag\",
        when(
            col(\"Source_Cd\") == \"ARM\",
            when(
                col(\"SequenceStart_It\") > col(\"Max_SequenceStart_It\"),
                lit(\"Y\")
            ).otherwise(lit(\"N\"))
        ).otherwise(lit(\"Y\"))
    )

    # Step 3: Write transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table...\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.target_table\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.target_table\")

    logger.info(\"ETL workflow completed successfully.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL workflow: {e}\")
    raise


import logging
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Step 1: Load source data from Unity Catalog table
    logging.info(\"Loading source data from Unity Catalog table: catalog.source_db.source_table\")
    source_df = spark.table(\"catalog.source_db.source_table\")

    # Step 2: Apply transformations
    logging.info(\"Applying transformations to source data\")

    # Passthrough fields (all fields are passed through without changes)
    passthrough_fields = [
        'LoadEvent_Id', 'InteractionGroupKey_Cd', 'InteractionGroupBusinessKey_Cd', 'InteractionThreadKey_Cd',
        'PartyKey_Cd', 'AgentOfRecord_Nb', 'AgentOfRecord_Nm', 'AgentOfRecordSource_Cd', 'AgentOfRecordPhone_Nb',
        'AlternateTelephone_Nb', 'BestContactTime_Cd', 'BestContactTimeTranslationRuleTable_Id', 'BusinessName_Ds',
        'CallBack_Ts', 'CommercialPGIndustry_Ds', 'CommercialPGOtherIndustry_Ds', 'CommercialPGYearsInBusiness_Ds',
        'CommercialPGCurrentInsured_Cd', 'CommercialPGCurrentInsuredTranslationRuleTable_Id', 'CommercialPGOtherProducts_Ds',
        'ContactFirst_Nm', 'ContactLast_Nm', 'ContactFull_Nm', 'EmailAddress_Nm', 'EnterpriseCustomer_Nb', 'EcifContact_Nb',
        'FullPolicy_Nb', 'Language_Ds', 'OperationalHousehold_Nb', 'PartyLifecycleStatus_Cd', 'PartyLifecycleStatusTranslationRuleTable_Id',
        'PolicyProcessingPlatform_Nm', 'PreferredTelephone_Nb', 'Quote_Nb', 'QuotePlatform_Nm', 'RatedState_Cd',
        'RatedStateTranslationRuleTable_Id', 'TimeZone_Cd', 'Zip_Cd', 'Effective_Dt', 'Expiration_Dt', 'Transaction_Ts',
        'Revision_Ts', 'SequenceStart_It', 'SequenceEnd_It', 'Source_Cd', 'InteractionGrpLogiclBusnKey_Cd', 'AnalyticHousehold_Nb',
        'AnalyticParty_Nb', 'BillingAccount_Nb', 'ContactPrefix_Nm', 'ContactMiddle_Nm', 'ContactSuffix_Nm', 'InteractionProduct_Cd',
        'InteractionProductTranslationRuleTable_Id', 'PolicyEffective_Dt', 'PolicyPremiumChange_Am', 'PolicyPremiumChange_Pc',
        'PolicyRenewal_Dt', 'PolicyRenewalPremium_Am', 'PolicyType_Cd', 'PolicyTypeTranslationRuleTable_Id', 'QuoteEvent_Nb',
        'FullTermPremium_Am', 'AgencyOfRecord_Nb', 'HouseholdBusinessKey_Cd', 'Opportunity_Nb', 'OpportunityDetail_Nb',
        'ActivePnCPolicyOwner_Cd', 'ActivePnCPolicyOwnerTranslationRuleTable_Id', 'CallEligibility_Cd', 'CallEligibilityTranslationRuleTable_Id',
        'ContactBirth_Dt', 'ContactGender_Cd', 'ContactGenderTranslationRuleTable_Id', 'ContactPermitted_Cd', 'ContactPermittedTranslationRuleTable_Id',
        'ContactSmokerStatus_Cd', 'ContactSmokerStatusTranslationRuleTable_Id', 'InteractionAddlProduct_Ds', 'InteractionAuto_Cd',
        'InteractionAutoTranslationRuleTable_Id', 'InteractionBusinessAuto_Cd', 'InteractionBusinessAutoTranslationRuleTable_Id',
        'InteractionBusinessLiab_Cd', 'InteractionBusinessLiabTranslationRuleTable_Id', 'InteractionBusinessProperty_Cd',
        'InteractionBusinessPropertyTranslationRuleTable_Id', 'InteractionCondominium_Cd', 'InteractionCondominiumTranslationRuleTable_Id',
        'InteractionHomeowners_Cd', 'InteractionHomeownersTranslationRuleTable_Id', 'InteractionIDTheft_Cd', 'InteractionIDTheftTranslationRuleTable_Id',
        'InteractionLifeEvent_Ds', 'InteractionMotorcycle_Cd', 'InteractionMotorcycleTranslationRuleTable_Id', 'InteractionRenters_Cd',
        'InteractionRentersTranslationRuleTable_Id', 'LapseInCoverage_Cd', 'LapseInCoverageTranslationRuleTable_Id', 'PriorCarrier_Nm',
        'QuoteLifeCycleStatus_Cd', 'QuoteLifeCycleStatusTranslationRuleTable_Id', 'AgencyOfRecord_Nm', 'TargetKeyLevel_Cd',
        'TargetKeyLevelTranslationRuleTable_Id', 'Producer_Nb', 'EmailJob_Nm', 'EmailFromAddress_Nm', 'EmailFrom_Nm', 'EmailMultiPart_Cd',
        'EmailMultiPartTranslation_Id', 'EmailJobPreviewURL_Tt', 'EmailSendDefExtKey_Tt', 'EmailSubjectLine_Tt', 'EmailCreative_Nb',
        'WebPreviewURL_Tt', 'SourceSubscriber_Nb', 'EmailList_Nb', 'NationwideAccount_Cd', 'NationwideAccountTranslation_Id',
        'EmailCollateral_Nb', 'NWAccountLevel1_Cd', 'NWAccountLevel1Translation_Id', 'NWAccountLevel2_Cd', 'NWAccountLevel2Translation_Id',
        'AnalyticDistrPartner_Id', 'DistrPartnerRelationship_Id', 'DistrPartnerSalesTerritory_Id', 'AgreementRole_Id', 'DistrPartner_Nb',
        'SalesTerritory_Cd'
    ]

    # Select passthrough fields
    transformed_df = source_df.select(*passthrough_fields)

    # Step 3: Sort data based on InteractionGroupKey_Cd and SequenceStart_It
    logging.info(\"Sorting data based on InteractionGroupKey_Cd and SequenceStart_It\")
    sorted_df = transformed_df.orderBy(F.col(\"InteractionGroupKey_Cd\"), F.col(\"SequenceStart_It\"))

    # Step 4: Write the transformed data to Unity Catalog target table
    target_table = \"catalog.target_db.target_table\"
    logging.info(f\"Dropping existing target table if exists: {target_table}\")
    spark.sql(f\"DROP TABLE IF EXISTS {target_table}\")

    logging.info(f\"Writing transformed data to Unity Catalog target table: {target_table}\")
    sorted_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_table)

    logging.info(\"ETL process completed successfully\")

except Exception as e:
    logging.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise


import logging
from pyspark.sql.functions import md5, concat_ws, col, ltrim, rtrim, to_date, to_timestamp, lit

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog
    logger.info(\"Loading source data from Unity Catalog table.\")
    source_df = spark.table(\"catalog.source_db.source_table\")

    # Step 2: Apply transformations
    logger.info(\"Applying transformations to source data.\")

    # List of fields for MD5 hash calculation
    fields_for_md5 = [
        \"InteractionGroupKey_Cd\", \"InteractionGroupBusinessKey_Cd\", \"InteractionGrpLogiclBusnKey_Cd\",
        \"InteractionThreadKey_Cd\", \"PartyKey_Cd\", \"AgentOfRecord_Nb\", \"AgentOfRecord_Nm\",
        \"AgentOfRecordSource_Cd\", \"AgentOfRecordPhone_Nb\", \"AlternateTelephone_Nb\", \"AnalyticHousehold_Nb\",
        \"AnalyticParty_Nb\", \"BestContactTime_Cd\", \"BestContactTime_Tp\", \"BusinessName_Ds\", \"CallBack_Ts\",
        \"CommercialPGIndustry_Ds\", \"CommercialPGOtherIndustry_Ds\", \"CommercialPGYearsInBusiness_Ds\",
        \"CommercialPGCurrentInsured_Cd\", \"CommercialPGCurrentInsured_Tp\", \"CommercialPGOtherProducts_Ds\",
        \"ContactPrefix_Nm\", \"ContactFirst_Nm\", \"ContactMiddle_Nm\", \"ContactLast_Nm\", \"ContactFull_Nm\",
        \"ContactSuffix_Nm\", \"EmailAddress_Nm\", \"EnterpriseCustomer_Nb\", \"EcifContact_Nb\", \"FullPolicy_Nb\",
        \"FullTermPremium_Am\", \"InteractionProduct_Cd\", \"InteractionProduct_Tp\", \"Language_Ds\",
        \"OperationalHousehold_Nb\", \"PartyLifecycleStatus_Cd\", \"PartyLifecycleStatus_Tp\", \"PolicyEffective_Dt\",
        \"PolicyPremiumChange_Am\", \"PolicyPremiumChange_Pc\", \"PolicyProcessingPlatform_Nm\", \"PolicyRenewal_Dt\",
        \"PolicyRenewalPremium_Am\", \"PolicyType_Cd\", \"PolicyType_Tp\", \"PreferredTelephone_Nb\", \"Quote_Nb\",
        \"QuoteEvent_Nb\", \"QuotePlatform_Nm\", \"RatedState_Cd\", \"RatedState_Tp\", \"TimeZone_Cd\", \"Zip_Cd\",
        \"Source_Cd\", \"AgencyOfRecord_Nb\", \"HouseholdBusinessKey_Cd\", \"Opportunity_Nb\", \"OpportunityDetail_Nb\",
        \"ActivePnCPolicyOwner_Cd\", \"ActivePnCPolicyOwner_Tp\", \"CallEligibility_Cd\", \"CallEligibility_Tp\",
        \"ContactBirth_Dt\", \"ContactGender_Cd\", \"ContactGender_Tp\", \"ContactPermitted_Cd\", \"ContactPermitted_Tp\",
        \"ContactSmokerStatus_Cd\", \"ContactSmokerStatus_Tp\", \"InteractionAddlProduct_Ds\", \"InteractionAuto_Cd\",
        \"InteractionAuto_Tp\", \"InteractionBusinessAuto_Cd\", \"InteractionBusinessAuto_Tp\", \"InteractionBusinessLiab_Cd\",
        \"InteractionBusinessLiab_Tp\", \"InteractionBusinessProperty_Cd\", \"InteractionBusinessProperty_Tp\",
        \"InteractionCondominium_Cd\", \"InteractionCondominium_Tp\", \"InteractionHomeowners_Cd\",
        \"InteractionHomeowners_Tp\", \"InteractionIDTheft_Cd\", \"InteractionIDTheft_Tp\", \"InteractionLifeEvent_Ds\",
        \"InteractionMotorcycle_Cd\", \"InteractionMotorcycle_Tp\", \"InteractionRenters_Cd\", \"InteractionRenters_Tp\",
        \"LapseInCoverage_Cd\", \"LapseInCoverage_Tp\", \"PriorCarrier_Nm\", \"QuoteLifeCycleStatus_Cd\",
        \"QuoteLifeCycleStatus_Tp\", \"BillingAccount_Nb\", \"AgencyOfRecord_Nm\", \"TargetKeyLevel_Cd\",
        \"TargetKeyLevel_Tp\", \"Producer_Nb\", \"EmailJob_Nm\", \"EmailFromAddress_Nm\", \"EmailFrom_Nm\",
        \"EmailMultiPart_Cd\", \"EmailMultiPart_Tp\", \"EmailJobPreviewURL_Tt\", \"EmailSendDefExtKey_Tt\",
        \"EmailSubjectLine_Tt\", \"EmailCreative_Nb\", \"WebPreviewURL_Tt\", \"SourceSubscriber_Nb\", \"EmailList_Nb\",
        \"NationwideAccount_Cd\", \"NationwideAccount_Tp\", \"EmailCollateral_Nb\", \"NWAccountLevel1_Cd\",
        \"NWAccountLevel1_Tp\", \"NWAccountLevel2_Cd\", \"NWAccountLevel2_Tp\", \"AnalyticDistrPartner_Id\",
        \"DistrPartnerRelationship_Id\", \"DistrPartnerSalesTerritory_Id\", \"AgreementRole_Id\", \"DistrPartner_Nb\",
        \"SalesTerritory_Cd\"
    ]

    # Apply MD5 transformation
    transformed_df = source_df.withColumn(
        \"md5_baseinteractiongroup\",
        md5(concat_ws(
            \"\",
            *[ltrim(rtrim(col(field))).cast(\"string\") for field in fields_for_md5]
        ))
    )

    # Step 3: Write transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table.\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.target_table\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.target_table\")

    logger.info(\"ETL process completed successfully.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {e}\")
    raise


import logging
from pyspark.sql.functions import col, when, lit, trim, ltrim, rtrim

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Step 1: Load source data from Unity Catalog
    logging.info(\"Loading source data from Unity Catalog...\")
    source_df = spark.table(\"catalog.source_db.source_table\")

    # Step 2: Apply transformations
    logging.info(\"Applying transformations...\")
    
    # Passthrough fields
    passthrough_fields = [
        \"BestContactTime_Cd\", \"BestContactTimeTranslationRuleTable_Id\", \"CommercialPGCurrentInsured_Cd\",
        \"CommercialPGCurrentInsuredTranslationRuleTable_Id\", \"PartyLifecycleStatus_Cd\",
        \"PartyLifecycleStatusTranslationRuleTable_Id\", \"RatedState_Cd\", \"RatedStateTranslationRuleTable_Id\",
        \"InteractionProduct_Cd\", \"InteractionProductTranslationRuleTable_Id\", \"PolicyType_Cd\",
        \"PolicyTypeTranslationRuleTable_Id\", \"ActivePnCPolicyOwner_Cd\", \"ActivePnCPolicyOwnerTranslationRuleTable_Id\",
        \"CallEligibility_Cd\", \"CallEligibilityTranslationRuleTable_Id\", \"ContactGender_Cd\",
        \"ContactGenderTranslationRuleTable_Id\", \"ContactPermitted_Cd\", \"ContactPermittedTranslationRuleTable_Id\",
        \"ContactSmokerStatus_Cd\", \"ContactSmokerStatusTranslationRuleTable_Id\", \"InteractionAuto_Cd\",
        \"InteractionAutoTranslationRuleTable_Id\", \"InteractionBusinessAuto_Cd\",
        \"InteractionBusinessAutoTranslationRuleTable_Id\", \"InteractionBusinessLiab_Cd\",
        \"InteractionBusinessLiabTranslationRuleTable_Id\", \"InteractionBusinessProperty_Cd\",
        \"InteractionBusinessPropertyTranslationRuleTable_Id\", \"InteractionCondominium_Cd\",
        \"InteractionCondominiumTranslationRuleTable_Id\", \"InteractionHomeowners_Cd\",
        \"InteractionHomeownersTranslationRuleTable_Id\", \"InteractionIDTheft_Cd\",
        \"InteractionIDTheftTranslationRuleTable_Id\", \"InteractionMotorcycle_Cd\",
        \"InteractionMotorcycleTranslationRuleTable_Id\", \"InteractionRenters_Cd\",
        \"InteractionRentersTranslationRuleTable_Id\", \"LapseInCoverage_Cd\", \"LapseInCoverageTranslationRuleTable_Id\",
        \"QuoteLifeCycleStatus_Cd\", \"QuoteLifeCycleStatusTranslationRuleTable_Id\", \"TargetKeyLevel_Cd\",
        \"TargetKeyLevelTranslationRuleTable_Id\", \"EmailMultiPart_Cd\", \"EmailMultiPartTranslation_Id\",
        \"NationwideAccount_Cd\", \"NationwideAccountTranslation_Id\", \"NWAccountLevel1_Cd\",
        \"NWAccountLevel1Translation_Id\", \"NWAccountLevel2_Cd\", \"NWAccountLevel2Translation_Id\"
    ]
    
    transformed_df = source_df.select(*[col(field) for field in passthrough_fields])

    # Transform fields with custom logic
    transformed_df = transformed_df.withColumn(
        \"v_BestContactTime_Tp\",
        when(
            col(\"BestContactTimeTranslationRuleTable_Id\").isNull() | 
            col(\"BestContactTime_Cd\").isNull() | 
            (col(\"BestContactTime_Cd\") == \"\"),
            lit(0)
        ).otherwise(lit(1))  # Replace with actual lookup logic
    ).withColumn(
        \"v_CommercialPGCurrentInsured_Tp\",
        when(
            col(\"CommercialPGCurrentInsuredTranslationRuleTable_Id\").isNull() | 
            col(\"CommercialPGCurrentInsured_Cd\").isNull() | 
            (col(\"CommercialPGCurrentInsured_Cd\") == \"\"),
            lit(0)
        ).otherwise(lit(1))  # Replace with actual lookup logic
    ).withColumn(
        \"v_PartyLifecycleStatus_Tp\",
        when(
            col(\"PartyLifecycleStatusTranslationRuleTable_Id\").isNull() | 
            col(\"PartyLifecycleStatus_Cd\").isNull() | 
            (col(\"PartyLifecycleStatus_Cd\") == \"\"),
            lit(0)
        ).otherwise(lit(1))  # Replace with actual lookup logic
    ).withColumn(
        \"v_RatedState_Tp\",
        when(
            col(\"RatedStateTranslationRuleTable_Id\").isNull() | 
            col(\"RatedState_Cd\").isNull() | 
            (col(\"RatedState_Cd\") == \"\"),
            lit(0)
        ).otherwise(lit(1))  # Replace with actual lookup logic
    ).withColumn(
        \"v_InteractionProduct_Tp\",
        when(
            col(\"InteractionProductTranslationRuleTable_Id\").isNull() | 
            col(\"InteractionProduct_Cd\").isNull() | 
            (col(\"InteractionProduct_Cd\") == \"\"),
            lit(0)
        ).otherwise(lit(1))  # Replace with actual lookup logic
    )
    # Add similar transformations for other fields as per the lineage plan

    # Step 3: Write transformed data to Unity Catalog target table
    logging.info(\"Writing transformed data to Unity Catalog target table...\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.target_table\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.target_table\")

    logging.info(\"ETL process completed successfully.\")

except Exception as e:
    logging.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise


import logging
from pyspark.sql.functions import col, lit, when

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Log the start of the ETL process
    logger.info(\"Starting ETL process for exp_Assign_Defaults transformation.\")

    # Load source data from Unity Catalog table
    logger.info(\"Loading source data from Unity Catalog table.\")
    source_df = spark.table(\"catalog.source_db.source_table\")

    # Define default values
    v_default_nm = \"\"
    v_default_string = \"@\"
    v_default_number = 0

    # Apply transformations
    logger.info(\"Applying transformations to assign default values.\")
    transformed_df = source_df.select(
        col(\"AgencyOfRecord_Nb\"),
        col(\"InteractionGrpLogiclBusnKey_Id\"),
        col(\"InteractionGrpLogiclBusnKey_Cd\"),
        col(\"Incremental_Flag\"),
        col(\"NWAccountLevel2_Cd\"),
        when(col(\"AssignedToAgent_Nb\").isNotNull(), col(\"AssignedToAgent_Nb\")).otherwise(lit(v_default_nm)).alias(\"o_AssignedToAgent_Nb\"),
        when(col(\"AssignedToAgentRole_Cd\").isNotNull(), col(\"AssignedToAgentRole_Cd\")).otherwise(lit(v_default_string)).alias(\"o_AssignedToAgentRole_Cd\"),
        when(col(\"AssignedToAgentCTM_Nb\").isNotNull(), col(\"AssignedToAgentCTM_Nb\")).otherwise(lit(v_default_nm)).alias(\"o_AssignedToAgentCTM_Nb\"),
        when(col(\"CreatedByAgentRole_Cd\").isNotNull(), col(\"CreatedByAgentRole_Cd\")).otherwise(lit(v_default_string)).alias(\"o_CreatedByAgentRole_Cd\"),
        when(col(\"CreatedByAgentCTM_Nb\").isNotNull(), col(\"CreatedByAgentCTM_Nb\")).otherwise(lit(v_default_nm)).alias(\"o_CreatedByAgentCTM_Nb\"),
        when(col(\"RestrictedAgent_Nb\").isNotNull(), col(\"RestrictedAgent_Nb\")).otherwise(lit(v_default_nm)).alias(\"o_RestrictedAgent_Nb\"),
        when(col(\"RestrictedAgentRole_Cd\").isNotNull(), col(\"RestrictedAgentRole_Cd\")).otherwise(lit(v_default_string)).alias(\"o_RestrictedAgentRole_Cd\"),
        when(col(\"RestrictedAgentCTM_Nb\").isNotNull(), col(\"RestrictedAgentCTM_Nb\")).otherwise(lit(v_default_nm)).alias(\"o_RestrictedAgentCTM_Nb\"),
        when(col(\"ActivityGrouping_Tt\").isNotNull(), col(\"ActivityGrouping_Tt\")).otherwise(lit(v_default_nm)).alias(\"o_ActivityGrouping_Tt\"),
        when(col(\"ActivityTopic_Cd\").isNotNull(), col(\"ActivityTopic_Cd\")).otherwise(lit(v_default_string)).alias(\"o_ActivityTopic_Cd\"),
        when(col(\"AdditionalTopicDescription_Tt\").isNotNull(), col(\"AdditionalTopicDescription_Tt\")).otherwise(lit(v_default_nm)).alias(\"o_AdditionalTopicDescription_Tt\"),
        when(col(\"ActualInteractionMedium_Cd\").isNotNull(), col(\"ActualInteractionMedium_Cd\")).otherwise(lit(v_default_string)).alias(\"o_ActualInteractionMedium_Cd\"),
        when(col(\"BusinessObjectSource_Cd\").isNotNull(), col(\"BusinessObjectSource_Cd\")).otherwise(lit(v_default_string)).alias(\"o_BusinessObjectSource_Cd\"),
        when(col(\"BusinessObject_Nb\").isNotNull(), col(\"BusinessObject_Nb\")).otherwise(lit(v_default_nm)).alias(\"o_BusinessObject_Nb\"),
        when(col(\"BusinessObject_Cd\").isNotNull(), col(\"BusinessObject_Cd\")).otherwise(lit(v_default_string)).alias(\"o_BusinessObject_Cd\"),
        when(col(\"InteractionActivity_Cd\").isNotNull(), col(\"InteractionActivity_Cd\")).otherwise(lit(v_default_string)).alias(\"o_InteractionActivity_Cd\")
    )

    # Log transformation completion
    logger.info(\"Transformations applied successfully.\")

    # Drop the target table if it exists
    logger.info(\"Dropping target table if it exists.\")
    spark.sql(\"DROP TABLE IF EXISTS catalog.target_db.target_table\")

    # Write transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table.\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(\"catalog.target_db.target_table\")

    # Log the completion of the ETL process
    logger.info(\"ETL process completed successfully.\")

except Exception as e:
    # Log any errors that occur during the ETL process
    logger.error(f\"An error occurred during the ETL process: {e}\")
    raise


import logging
from pyspark.sql.functions import col, when, upper, lit, expr

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Step 1: Load source data from Unity Catalog table
    logger.info(\"Loading source data from Unity Catalog table...\")
    source_df = spark.table(\"catalog.source_db.srt_to_sort_data_based_in_interactiongroupkey_cd_and_sequencestartit_source_cd\")

    # Step 2: Apply transformations based on the lineage plan
    logger.info(\"Applying transformations...\")
    
    # Passthrough fields
    passthrough_fields = [
        \"InteractionGroupKey_Cd\", \"InteractionGroupBusinessKey_Cd\", \"InteractionThreadKey_Cd\",
        \"PartyKey_Cd\", \"InteractionGrpLogiclBusnKey_Cd\", \"LoadEvent_Id\", \"Source_Cd\", \"HouseholdBusinessKey_Cd\"
    ]
    transformed_df = source_df.select(*[col(field) for field in passthrough_fields])

    # Derived fields using lookup logic and conditional checks
    transformed_df = transformed_df.withColumn(
        \"InteractionGroup_Id\",
        when((col(\"InteractionGroupKey_Cd\") == \"@\") | col(\"InteractionGroupKey_Cd\").isNull(), lit(0))
        .otherwise(expr(\"LKP_EDW_SHARED_ANCHOR_ID(Source_Cd, UPPER(InteractionGroupKey_Cd), 'InteractionGroup_Id')\"))
    ).withColumn(
        \"InteractionGroupBusinessKey_Id\",
        when((col(\"InteractionGroupBusinessKey_Cd\") == \"@\") | col(\"InteractionGroupBusinessKey_Cd\").isNull(), lit(0))
        .otherwise(expr(\"LKP_EDW_SHARED_ANCHOR_ID('ALL', UPPER(InteractionGroupBusinessKey_Cd), 'InteractionGroupBusinessKey_Id')\"))
    ).withColumn(
        \"InteractionThread_Id\",
        when((col(\"InteractionThreadKey_Cd\") == \"@\") | col(\"InteractionThreadKey_Cd\").isNull(), lit(0))
        .otherwise(expr(\"LKP_EDW_SHARED_ANCHOR_ID(Source_Cd, UPPER(InteractionThreadKey_Cd), 'InteractionThread_Id')\"))
    ).withColumn(
        \"Party_Id\",
        when((col(\"PartyKey_Cd\") == \"@\") | col(\"PartyKey_Cd\").isNull(), lit(0))
        .otherwise(expr(\"LKP_EDW_SHARED_ANCHOR_ID(Source_Cd, UPPER(PartyKey_Cd), 'Party_Id')\"))
    ).withColumn(
        \"InteractionGrpLogiclBusnKey_Id\",
        when((col(\"InteractionGrpLogiclBusnKey_Cd\") == \"@\") | col(\"InteractionGrpLogiclBusnKey_Cd\").isNull(), lit(0))
        .otherwise(expr(\"LKP_EDW_SHARED_ANCHOR_ID('ALL', UPPER(InteractionGrpLogiclBusnKey_Cd), 'InteractionGrpLogiclBusnKey_Id')\"))
    ).withColumn(
        \"HouseholdBusinessKey_Id\",
        when((col(\"HouseholdBusinessKey_Cd\") == \"@\") | col(\"HouseholdBusinessKey_Cd\").isNull(), lit(0))
        .otherwise(expr(\"LKP_EDW_SHARED_ANCHOR_ID('ALL', UPPER(HouseholdBusinessKey_Cd), 'HouseholdBusinessKey_Id')\"))
    )

    # Step 3: Write transformed data to Unity Catalog target table
    logger.info(\"Writing transformed data to Unity Catalog target table...\")
    target_table = \"catalog.target_db.SAF_Baseinteractiongroup\"
    spark.sql(f\"DROP TABLE IF EXISTS {target_table}\")
    transformed_df.write.format(\"delta\").mode(\"overwrite\").saveAsTable(target_table)

    logger.info(\"ETL process completed successfully.\")

except Exception as e:
    logger.error(f\"An error occurred during the ETL process: {str(e)}\")
    raise