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
  