import os
import sys
from pathlib import Path
import configparser
from databricks.sdk.service.jobs import Task, NotebookTask, Source, GitSource, GitProvider, TaskDependency
from databricks.sdk import WorkspaceClient
from argparse import ArgumentParser

p = ArgumentParser()

p.add_argument("--branch_name", required=False, type=str)
p.add_argument("--pr_branch", required=False, type=str)

namespace = p.parse_known_args(sys.argv + [ '', ''])[0]
branch_name = namespace.branch_name
print('Branch Name: ', branch_name)
pr_branch = namespace.pr_branch
print('PR Branch: ', pr_branch)
#Let's checkout the needed branch
if branch_name == 'merge':
  branch = pr_branch
else:
  branch = branch_name
print('Using branch: ', branch)

sys.path.append(Path(__file__).parent)

host = os.environ.get("HOST")
token = os.environ.get("TOKEN")
new_cluster_config = open("config/new_cluster_config.json", "r").read()
config = configparser.ConfigParser()
config.read("config/config.ini")


existing_cluster_id = config["ADMIN"]["existing_cluster_id"]
repo_path = config["ADMIN"]["repo_path"]
repos_path_prefix = config["ADMIN"]["repos_path_prefix"]
git_url = config["ADMIN"]["git_url"]
provider = config["ADMIN"]["provider"]
branch = branch

feature_engineering_notebook_path = config["FEATURE_ENGINEERING"]["notebook_path"]
feature_engineering_description = "Feature engineering is the process of transforming raw data into features that better represent \
    the underlying patterns in the data to predictive models. This step often involves techniques like normalization, \
    binning, and encoding, and can significantly boost a model's performance by highlighting essential information. \
    Effective feature engineering requires domain knowledge, experimentation, and iteration to fine-tune the features \
    for optimal results."
training_notebook_path = config["TRAINING"]["notebook_path"]
training_description = "The training step in machine learning involves feeding a dataset into an algorithm to tune and \
    optimize the model's parameters. This process allows the model to learn patterns and relationships within the data, \
    preparing it for accurate predictions on new, unseen data."

w = WorkspaceClient()
git_config = GitSource(
    git_url = git_url,
    git_provider = GitProvider(provider),
    git_branch=branch
)
feature_engineering = Task(
    task_key="feature-engineering",
    description = training_description,
    existing_cluster_id = existing_cluster_id,
    notebook_task = NotebookTask(
        base_parameters = dict(""),
        notebook_path = feature_engineering_notebook_path,
        source = Source("GIT")
    ),
)
training = Task(
    task_key="training",
    depends_on=[TaskDependency(task_key="feature-engineering")],
    description = training_description,
    existing_cluster_id = existing_cluster_id,
    notebook_task = NotebookTask(
        base_parameters = dict(""),
        notebook_path = training_notebook_path,
        source = Source("GIT")
    ),
)



print("Attempting to create the job. Please wait...\n")

j = w.jobs.create(
  name = "CT pipeline testing",
  tasks = [
    feature_engineering,
    training
  ],
  git_source=git_config
)
# print(f"View the job at {w.config.host}/#job/{j.job_id}\n")
os.environ.setdefault("JOBID", j.job_id.__str__())
print(os.environ.get("JOBID"))


ct_job = w.jobs.get(job_id=j.job_id)
print(ct_job)
run = w.jobs.run_now_and_wait(job_id=j.job_id)
print(run)

