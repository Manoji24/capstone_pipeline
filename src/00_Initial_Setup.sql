-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Intial Setup:
-- MAGIC This notebook creates the Objects required for the CAPSTONE project.
-- MAGIC
-- MAGIC - Catalog
-- MAGIC - Database
-- MAGIC - Volume

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
COMMENT 'This is the catalog for the CAPSTONE project.'

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

SELECT CURRENT_CATALOG();

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS CAPSTONE.BRONZE;
CREATE SCHEMA IF NOT EXISTS CAPSTONE.SILVER;
CREATE SCHEMA IF NOT EXISTS CAPSTONE.GOLD;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # External Volume
-- MAGIC
-- MAGIC  **Unable to create External Volume as I do not have cloud account**
-- MAGIC
-- MAGIC #### Steps:
-- MAGIC - Create role with describe, read and write permission in the policy
-- MAGIC - Provide cross account access using Trustrelation
-- MAGIC - Followed by running the following script
-- MAGIC
-- MAGIC _databricks storage-credentials create --json '{\"name\": \"sc_external_s3_capstone\", \"aws_iam_role\": {\"role_arn\": \"arn for iam role\"}}'_
-- MAGIC
-- MAGIC _CREATE EXTERNAL LOCATION EXT_S3_CAPSTONE URL 's3://capstone-proj-src/'
-- MAGIC      WITH (CREDENTIAL sc_external_s3_capstone)
-- MAGIC      COMMENT 'Capstone Src data path in S3';_

-- COMMAND ----------

-- CREATE EXTERNAL LOCATION EXT_S3_CAPSTONE URL 's3://capstone-proj-src/' 
-- WITH STORAGE CREDENTIAL sc_external_s3_capstone
-- -- COMMENT 'CAPSTONE data stored in S3'

CREATE EXTERNAL LOCATION IF NOT EXISTS EXT_S3_CAPSTONE URL 's3://capstone-proj-src/'
     WITH (CREDENTIAL sc_external_s3_capstone)
     COMMENT 'Capstone Src data path in S3';

-- COMMAND ----------



CREATE EXTERNAL VOLUME IF NOT EXISTS CAPSTONE.BRONZE.RAW
        COMMENT 'This is the external volume for source data'
        LOCATION 's3://capstone-proj-src/'

-- COMMAND ----------

LIST '/Volumes/capstone/bronze/raw'

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS CAPSTONE.BRONZE.RAWTEST
        COMMENT 'This is the external volume for source data'

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS CAPSTONE.BRONZE.HISTORY
        COMMENT 'This is the volume for storing audit log data'

-- COMMAND ----------

LIST '/Volumes/capstone/bronze/rawtest' 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def create_directory_ifnotexists(file_path):
-- MAGIC   try:
-- MAGIC     dbutils.fs.ls(file_path)
-- MAGIC   except Exception as e:
-- MAGIC     if "java.io.FileNotFoundException" in str(e):
-- MAGIC         dbutils.fs.mkdirs(file_path)
-- MAGIC     else:
-- MAGIC         raise e

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC base_raw_path = "/Volumes/CAPSTONE/bronze/raw/files/landing"               # root landing zone
-- MAGIC transactions_path = f"{base_raw_path}/transactions_raw"
-- MAGIC customers_path = f"{base_raw_path}/customers_raw"
-- MAGIC products_path = f"{base_raw_path}/products_raw"
-- MAGIC
-- MAGIC create_directory_ifnotexists(base_raw_path)
-- MAGIC create_directory_ifnotexists(transactions_path)
-- MAGIC create_directory_ifnotexists(customers_path)
-- MAGIC create_directory_ifnotexists(products_path)

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls -lrt /Volumes/CAPSTONE/bronze/raw/files/landing/*/
-- MAGIC

-- COMMAND ----------
