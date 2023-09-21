# Databricks notebook source
# MAGIC %md
# MAGIC # Implement CICD flow by databrciks cli tool
# MAGIC - [Azure Databricks overview](https://learn.microsoft.com/en-us/azure/databricks/archive/dev-tools/cli/)
# MAGIC - [Azure Jobs document](https://learn.microsoft.com/en-us/azure/databricks/archive/dev-tools/cli/jobs-cli)
# MAGIC - [CICD with AzureDevOps and databricks](https://www.databricks.com/blog/2021/09/20/part-1-implementing-ci-cd-on-databricks-using-databricks-notebooks-and-azure-devops.html)

# COMMAND ----------

! databricks --help

# COMMAND ----------

! databricks jobs --help

# COMMAND ----------

! databricks jobs list

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC # Configure by shell script, create the configuration file directly.
# MAGIC
# MAGIC # Replace HOST and TOKEN with your actual values
# MAGIC HOST=https://dbc-75330423-305e.cloud.databricks.com/
# MAGIC TOKEN=dapie2abca78cf1cae76d3e5fa979ea735bf
# MAGIC
# MAGIC # Write the file
# MAGIC cat > ~/.databrickscfg <<EOF
# MAGIC [DEFAULT]
# MAGIC host = $HOST
# MAGIC token = $TOKEN
# MAGIC EOF

# COMMAND ----------

# MAGIC %sh
# MAGIC cat ~/.databrickscfg

# COMMAND ----------

! pwd
! ls

# COMMAND ----------

# MAGIC %sh 
# MAGIC # Check if the configuration goes right
# MAGIC
# MAGIC databricks fs ls dbfs:/ #test for configuration

# COMMAND ----------

! databricks jobs list

# COMMAND ----------

! databricks jobs --help

# COMMAND ----------

# Get the json template of specific job with id
! databricks jobs get --job-id 294569905577050

# COMMAND ----------

# MAGIC %sh
# MAGIC JOB_ID=294569905577050
# MAGIC databricks jobs run-now --job-id $JOB_ID
# MAGIC databricks runs list

# COMMAND ----------

# MAGIC %sh 
# MAGIC databricks jobs configure --version=2.1

# COMMAND ----------

# MAGIC %sh
# MAGIC JOB_ID=294569905577050
# MAGIC databricks jobs run-now --job-id $JOB_ID 
# MAGIC databricks runs list

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get install jq

# COMMAND ----------

! databricks runs get-output --

# COMMAND ----------

# MAGIC %sh
# MAGIC JOB_ID=294569905577050
# MAGIC output=$(databricks jobs run-now --job-id $JOB_ID)
# MAGIC echo $output
# MAGIC run_id=$(echo $output | jq -r '.run_id')
# MAGIC databricks runs get --run-id $run_id
# MAGIC task_ids=$(databricks runs get --run-id $run_id | jq ".tasks[].run_id")
# MAGIC for task_id in $task_ids
# MAGIC do
# MAGIC     databricks runs get-output --run-id $run_id --task-id $task_id
# MAGIC done

# COMMAND ----------

# MAGIC %sh 
# MAGIC #!/bin/bash
# MAGIC
# MAGIC JOB_ID=294569905577050
# MAGIC
# MAGIC # Function to check if the run is completed
# MAGIC function is_run_completed {
# MAGIC   status=$(databricks runs get --run-id $run_id | jq -r '.state.life_cycle_state')
# MAGIC   if [ "$status" == "TERMINATED" ] || [ "$status" == "SKIPPED" ] || [ "$status" == "INTERNAL_ERROR" ]; then
# MAGIC     return 1
# MAGIC   else
# MAGIC     return 0
# MAGIC   fi
# MAGIC }
# MAGIC
# MAGIC
# MAGIC output=$(databricks jobs run-now --job-id $JOB_ID)
# MAGIC echo $output
# MAGIC run_id=$(echo $output | jq -r '.run_id')
# MAGIC
# MAGIC # Poll the completed status
# MAGIC while true
# MAGIC do
# MAGIC   echo "Checking status..."
# MAGIC   if is_run_completed; then
# MAGIC     echo "Run completed."
# MAGIC     break
# MAGIC   else
# MAGIC     sleep 1
# MAGIC   fi
# MAGIC done

# COMMAND ----------


