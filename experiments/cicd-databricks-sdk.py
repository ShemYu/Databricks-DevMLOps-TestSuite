# Databricks notebook source
# MAGIC %sh 
# MAGIC /local_disk0/.ephemeral_nfs/envs/pythonEnv-53c31099-3a5f-48eb-845d-99e74c29cd1c/bin/python -m pip install --upgrade pip
# MAGIC pip install --upgrade requests databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os


HOST = "https://dbc-75330423-305e.cloud.databricks.com/"
TOKEN = "dapie2abca78cf1cae76d3e5fa979ea735bf"


from databricks.sdk import WorkspaceClient

w = WorkspaceClient(host=HOST, token=TOKEN)

for c in w.clusters.list():
  print(c.cluster_name)

# COMMAND ----------

d = w.dbutils.fs.ls('/')

for f in d:
  print(f.path)

# COMMAND ----------

import sys
from pathlib import Path



new_cluster_config = open("../config/new_cluster_config.json", "r").read()

import configparser

config = configparser.ConfigParser()
config.read("../config/config.ini")

existing_cluster_id = config["TRAINING"]["existing_cluster_id"]
# Path to the notebook with the integration test
notebook_path = config["TRAINING"]["notebook_path"]
repo_path = config["TRAINING"]["repo_path"]
repos_path_prefix = config["TRAINING"]["repos_path_prefix"]
git_url = config["TRAINING"]["git_url"]
provider = config["TRAINING"]["provider"]
branch = config["TRAINING"]["branch"]

# COMMAND ----------

from databricks.sdk.service.jobs import Task, NotebookTask, Source, GitSource


job_name            = "databricks-sdk-testing"
description         = "Testing jobs"
existing_cluster_id = existing_cluster_id
notebook_path       = notebook_path
task_key            = "my-key"

print("Attempting to create the job. Please wait...\n")

j = w.jobs.create(
  name = job_name,
  tasks = [
    Task(
      description = description,
      existing_cluster_id = existing_cluster_id,
      notebook_task = NotebookTask(
        base_parameters = dict(""),
        notebook_path = notebook_path,
        source = Source("WORKSPACE")
      ),
      task_key = task_key
    )
  ]
)

print(f"View the job at {w.config.host}/#job/{j.job_id}\n")

# COMMAND ----------

print("Attempting to create the job. Please wait...\n")

j = w.jobs.create(
  name = job_name,
  tasks = [
    Task(
      description = "Step 1. Jobs description.",
      existing_cluster_id = existing_cluster_id, # Runner 
      notebook_task = NotebookTask(
        base_parameters = {"Test": 123},
        notebook_path = notebook_path,
        source = GitSource(git_url=git_url, git_provider=provider, git_branch=branch)
      ),
      task_key = "Step 1."
    )
  ]
)

print(f"View the job at {w.config.host}/#job/{j.job_id}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC (*, access_control_list: Optional[List[databricks.sdk.service.iam.AccessControlRequest]]=None, compute: Optional[List[databricks.sdk.service.jobs.JobCompute]]=None, continuous: Optional[databricks.sdk.service.jobs.Continuous]=None, email_notifications: Optional[databricks.sdk.service.jobs.JobEmailNotifications]=None, format: Optional[databricks.sdk.service.jobs.Format]=None, git_source: Optional[databricks.sdk.service.jobs.GitSource]=None, health: Optional[databricks.sdk.service.jobs.JobsHealthRules]=None, job_clusters: Optional[List[databricks.sdk.service.jobs.JobCluster]]=None, max_concurrent_runs: Optional[int]=None, name: Optional[str]=None, notification_settings: Optional[databricks.sdk.service.jobs.JobNotificationSettings]=None, parameters: Optional[List[databricks.sdk.service.jobs.JobParameterDefinition]]=None, queue: Optional[databricks.sdk.service.jobs.QueueSettings]=None, run_as: Optional[databricks.sdk.service.jobs.JobRunAs]=None, schedule: Optional[databricks.sdk.service.jobs.CronSchedule]=None, tags: Optional[Dict[str, str]]=None, tasks: Optional[List[databricks.sdk.service.jobs.Task]]=None, timeout_seconds: Optional[int]=None, trigger: Optional[databricks.sdk.service.jobs.TriggerSettings]=None, webhook_notifications: Optional[databricks.sdk.service.jobs.WebhookNotifications]=None)
# MAGIC
# MAGIC
