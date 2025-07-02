CREATE OR ALTER TASK COBRE_GOLD_DB.DBT_PROD.run_mms_billing_dbt_project
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 6 * * * America/Bogota'
AS
  EXECUTE DBT PROJECT COBRE_GOLD_DB.DBT_PROD.mms_billing args='run --target prod --models mms_billing';