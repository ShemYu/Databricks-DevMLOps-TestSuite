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
# MAGIC   if [ "$status" = "TERMINATED" ] || [ "$status" = "SKIPPED" ] || [ "$status" = "INTERNAL_ERROR" ]; then
# MAGIC     return 0
# MAGIC   else
# MAGIC     return 1
# MAGIC   fi
# MAGIC }
# MAGIC
# MAGIC # Function to print the "Run Report" header
# MAGIC function display_header {
# MAGIC     local header="Run Report"
# MAGIC     local width=40  # This can be adjusted
# MAGIC     local side_count=$(( (width - ${#header} - 2) / 2 ))  # Calculate the number of '=' signs on each side
# MAGIC
# MAGIC     # Print '=' signs, header text, and then more '=' signs
# MAGIC     printf "%${side_count}s" | tr ' ' '='
# MAGIC     printf " %s " "$header"
# MAGIC     printf "%${side_count}s\n" | tr ' ' '='
# MAGIC }
# MAGIC
# MAGIC # Start the run on databricks
# MAGIC output=$(databricks jobs run-now --job-id $JOB_ID)
# MAGIC echo $output
# MAGIC run_id=$(echo $output | jq -r '.run_id')
# MAGIC
# MAGIC # Poll the completed status
# MAGIC echo "[Info][Databircks] Checking status..."
# MAGIC while true
# MAGIC do
# MAGIC   if is_run_completed; then
# MAGIC     echo "[Info][Databricks] Run completed."
# MAGIC     break
# MAGIC   else
# MAGIC     echo "[Info][Databricks] Running ..."
# MAGIC     sleep 2
# MAGIC   fi
# MAGIC done
# MAGIC
# MAGIC # Print the header at the start of your script or whenever appropriate
# MAGIC display_header
# MAGIC
# MAGIC databricks runs get --run-id $run_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Start run by template
# MAGIC - [Databricks one-time submit API document](https://docs.databricks.com/api/workspace/jobs/submit)

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC # 294569905577050  test2_
# MAGIC # 136366796426721  test_pipeline
# MAGIC
# MAGIC # Get job manifest by job-id
# MAGIC manifest_json=$(databricks jobs get --job-id 294569905577050)
# MAGIC
# MAGIC # Write the file
# MAGIC cat > ../deploy/test2_.json <<EOF
# MAGIC $manifest_json
# MAGIC EOF
# MAGIC
# MAGIC # For testing output
# MAGIC cat ../deploy/test2_.json

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks runs submit --help

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks runs submit --json-file ../deploy/test2_.json

# COMMAND ----------

# MAGIC %sh
# MAGIC USR_NAME=shauns4y@gmail.com
# MAGIC NAME="Shem Yu"
# MAGIC NOTEBOOK_TASK="Untitled Notebook 2023-09-18 16_48_42"
# MAGIC EXIST_CLUSTER_ID="0915-021615-dvxnyad4"
# MAGIC
# MAGIC cat > ../deploy/workflow_template.json <<EOF
# MAGIC {
# MAGIC   "run_as": {
# MAGIC     "user_name": $USR_NAME
# MAGIC   },
# MAGIC   "name": $NAME,
# MAGIC   "email_notifications": {},
# MAGIC   "notification_settings": {
# MAGIC     "no_alert_for_skipped_runs": false,
# MAGIC     "no_alert_for_canceled_runs": false,
# MAGIC     "alert_on_last_attempt": false
# MAGIC   },
# MAGIC   "webhook_notifications": {},
# MAGIC   "max_concurrent_runs": 1,
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC         "task_key": "test2_",
# MAGIC         "run_if": "ALL_SUCCESS",
# MAGIC         "notebook_task": {
# MAGIC           "notebook_path": $NOTEBOOK_TASK,
# MAGIC           "source": "GIT"
# MAGIC         },
# MAGIC         "existing_cluster_id": $EXIST_CLUSTER_ID,
# MAGIC         "timeout_seconds": 0,
# MAGIC         "email_notifications": {},
# MAGIC         "notification_settings": {
# MAGIC           "no_alert_for_skipped_runs": false,
# MAGIC           "no_alert_for_canceled_runs": false,
# MAGIC           "alert_on_last_attempt": false
# MAGIC         }
# MAGIC     },e
# MAGIC   ],
# MAGIC   "git_source": {
# MAGIC       "git_url": "https://github.com/ShemYu/Databricks-DevMLOps-TestSuite",
# MAGIC       "git_provider": "gitHub",
# MAGIC       "git_branch": "main"
# MAGIC   },
# MAGIC   "format": "MULTI_TASK"
# MAGIC }
# MAGIC EOF
# MAGIC
# MAGIC databricks runs submit --json-file ../deploy/test2_.json

# COMMAND ----------


