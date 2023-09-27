# Databricks notebook source
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
# MAGIC databricks clusters list
# MAGIC databricks clusters get --cluster-id 0915-021615-dvxnyad4

# COMMAND ----------

# Databricks notebook source
import os
os.environ['DATABRICKS_HOST'] = "https://dbc-75330423-305e.cloud.databricks.com/"
os.environ['DATABRICKS_TOKEN'] = "dapie2abca78cf1cae76d3e5fa979ea735bf"

# COMMAND ----------

new_cluster_config = """
{
  "cluster_id": "0915-021615-dvxnyad4",
  "creator_user_name": "yuehhsuntsai@gmail.com",
  "driver": {
    "private_ip": "10.62.200.98",
    "node_id": "12ecbabd185f46138ed791310415651e",
    "instance_id": "i-0c78d6264b89d2998",
    "start_timestamp": 1695780560563,
    "node_aws_attributes": {
      "is_spot": false
    },
    "node_attributes": {
      "is_spot": false
    },
    "host_private_ip": "10.62.194.181"
  },
  "spark_context_id": 3007070280200869203,
  "driver_healthy": true,
  "jdbc_port": 10000,
  "cluster_name": "DS Training Cluster (4core,31GB,x) (Shem)",
  "spark_version": "12.2.x-cpu-ml-scala2.12",
  "spark_conf": {
    "spark.master": "local[*, 4]",
    "spark.databricks.cluster.profile": "singleNode"
  },
  "aws_attributes": {
    "first_on_demand": 1,
    "availability": "ON_DEMAND",
    "zone_id": "auto",
    "spot_bid_price_percent": 100,
    "ebs_volume_count": 0
  },
  "node_type_id": "i3.xlarge",
  "driver_node_type_id": "i3.xlarge",
  "custom_tags": {
    "ResourceClass": "SingleNode"
  },
  "autotermination_minutes": 15,
  "enable_elastic_disk": true,
  "disk_spec": {
    "disk_count": 0
  },
  "cluster_source": "UI",
  "single_user_name": "shauns4y@gmail.com",
  "enable_local_disk_encryption": false,
  "instance_source": {
    "node_type_id": "i3.xlarge"
  },
  "driver_instance_source": {
    "node_type_id": "i3.xlarge"
  },
  "data_security_mode": "SINGLE_USER",
  "runtime_engine": "STANDARD",
  "effective_spark_version": "12.2.x-cpu-ml-scala2.12",
  "state": "RUNNING",
  "state_message": "",
  "start_time": 1694744176080,
  "last_state_loss_time": 1695780635036,
  "last_activity_time": 1695781787756,
  "last_restarted_time": 1695780635064,
  "num_workers": 0,
  "cluster_memory_mb": 31232,
  "cluster_cores": 4.0,
  "default_tags": {
    "Vendor": "Databricks",
    "Creator": "yuehhsuntsai@gmail.com",
    "ClusterName": "DS Training Cluster (4core,31GB,x) (Shem)",
    "ClusterId": "0915-021615-dvxnyad4"
  },
  "init_scripts_safe_mode": false
}
"""
# Existing cluster ID where integration test will be executed
existing_cluster_id = '0915-021615-dvxnyad4'
# Path to the notebook with the integration test
notebook_path = '/Untitled Notebook 2023-09-18 16_48_42'
repo_path = '/Repos/shauns4y@gmail.com/Databricks-DevMLOps-TestSuite'


repos_path_prefix='/Repos/shauns4y@gmail.com/Databricks-DevMLOps-TestSuite'
git_url = 'https://github.com/ShemYu/Databricks-DevMLOps-TestSuite'
provider = 'gitHub'
branch = 'main'

# COMMAND ----------

# COMMAND ----------
from argparse import ArgumentParser
import sys
p = ArgumentParser()

p.add_argument("--branch_name", required=False, type=str)
p.add_argument("--pr_branch", required=False, type=str)

namespace = p.parse_known_args(sys.argv + [ '', ''])[0]
branch_name = namespace.branch_name
print('Branch Name: ', branch_name)
pr_branch = namespace.pr_branch
print('PR Branch: ', pr_branch)

# COMMAND ----------

# for testing, manually assign the argument
branch_name = "main"
pr_branch = "main"

# COMMAND ----------

# COMMAND ----------
import json
import time
from datetime import datetime

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService, ReposService

# Let's create Databricks CLI API client to be able to interact with Databricks REST API
# [Warning] This function will get config from environment variables
config = EnvironmentVariableConfigProvider().get_config()
api_client = _get_api_client(config, command_name="cicdtemplates-")

#Let's checkout the needed branch
if branch_name == 'merge':
  branch = pr_branch
else:
  branch = branch_name
print('Using branch: ', branch)
  
#Let's create Repos Service
repos_service = ReposService(api_client)

# Let's store the path for our new Repo
_b = branch.replace('/','_')
repo_path = f'{repos_path_prefix}_{_b}_{str(datetime.now().microsecond)}'
print('Checking out the following repo: ', repo_path)

# Let's clone our GitHub Repo in Databricks using Repos API
repo = repos_service.create_repo(url=git_url, provider=provider, path=repo_path)

# COMMAND ----------

try:
  repos_service.update_repo(id=repo['id'], branch=branch)

  #Let's create a jobs service to be able to start/stop Databricks jobs
  jobs_service = JobsService(api_client)

  notebook_task = {'notebook_path': repo_path + notebook_path}
  #new_cluster = json.loads(new_cluster_config)

  # Submit integration test job to Databricks REST API
  res = jobs_service.submit_run(run_name="xxx", existing_cluster_id=existing_cluster_id,  notebook_task=notebook_task, )
  run_id = res['run_id']
  print(run_id)

  #Wait for the job to complete
  while True:
      status = jobs_service.get_run(run_id)
      print(status)
      result_state = status["state"].get("result_state", None)
      if result_state:
          print(result_state)
          assert result_state == "SUCCESS"
          break
      else:
          time.sleep(5)
finally:
    print("Done")

# COMMAND ----------

repos_service.delete_repo(id=repo['id'])

# COMMAND ----------

from databricks_cli.pipelines

# COMMAND ----------


