### Databricks Python SDK

#### Overview
The script involves working with Databricks Jobs, including:
- Creating jobs
- Managing dependencies(define pipeline)
- Executing tasks in a pipeline

#### Notes
- Ensure that all necessary dependencies are installed and available in the execution environment (e.g., Databricks SDK, configparser).
- Sensitive information like the `TOKEN` should be securely managed.
- Consider handling potential exceptions for robustness, especially around API calls and file I/O.

#### Parsing Arguments
- Accepts optional `--branch_name` and `--pr_branch` arguments.
- Determines the branch to use based on `branch_name` and `pr_branch`.

##### Example code
```python
p = ArgumentParser()

p.add_argument("--branch_name", required=False, type=str)
p.add_argument("--pr_branch", required=False, type=str)
```

#### Environment and Configuration Handling
- Extracts `HOST` and `TOKEN` from the environment variables.
- Reads and utilizes configuration details (like paths and cluster IDs) from "`config/config.ini`" and "`config/new_cluster_config.json`".
- The config files contain various parameters for feature engineering and training, such as notebook paths and descriptions.

##### Example code
```python
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
```
##### config.ini Example
```ini
[ADMIN]
existing_cluster_id=0915-021615-dvxnyad4
repo_path=/Repos/shauns4y@gmail.com/Databricks-DevMLOps-TestSuite
repos_path_prefix=/Repos/shauns4y@gmail.com/Databricks-DevMLOps-TestSuite
git_url=https://github.com/ShemYu/Databricks-DevMLOps-TestSuite
provider=gitHub
branch=main

[FEATURE_ENGINEERING]
notebook_path=src/datapipeline
feature_table_name=

[TRAINING]
notebook_path=src/modelpipeline
project_name=
OUTPUT_MODEL_ID=
```

#### Repo Definition

Described the connect information of the repo will use.

```python
w = WorkspaceClient()
git_config = GitSource(
    git_url = git_url,
    git_provider = GitProvider(provider),
    git_branch=branch
)
```

#### Tasks Definition
1. **Feature Engineering Task**
   - Described as a crucial step in transforming raw data into a format better suited for modeling.
   - Utilizes a Databricks notebook, path fetched from configuration.
    
   ##### Example code
   ```python
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
    ```

2. **Training Task**
   - The machine learning model is trained using data, presumably processed in the feature engineering step.
   - It is dependent on the completion of the feature engineering task.
   - Also utilizes a Databricks notebook, path fetched from configuration.
   
   ##### Example code

   ```python
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
    ```

#### Workspace and Jobs Management via Databricks SDK
- Uses the `WorkspaceClient` to manage Databricks jobs.
- Utilizes the GitSource class to specify details about the git repository to use, including `git_url`, `git_provider`, and `git_branch`.

#### Job Creation and Execution
- Computes a SHA256 hash using the current timestamp, used to create a unique pipeline name.
- Creates a new job using tasks defined (feature engineering and training) and Git configurations.
  
    ```python    
    print("Attempting to create the job. Please wait...\n")
    j = w.jobs.create(
    name = pipeline_name,
    tasks = [
        feature_engineering,
        training
    ],
    git_source=git_config
    )
    print(f"View the job at {w.config.host}/#job/{j.job_id}\n")
    os.environ.setdefault("JOBID", j.job_id.__str__())
    ```

- Retrieves job details and attempts to execute it, waiting for its completion with a 1-day timeout.
    
    ```python
    ct_job = w.jobs.get(job_id=j.job_id)
    run = w.jobs.run_now_and_wait(job_id=j.job_id, timeout=timedelta(days=1))
    ```
  
#### Benefits
- Implement the CT process in CICD with high usability through the Python SDK.
- It's easy to define Tasks and connect them into a pipeline using the Python SDK.

#### Backwards
- The current documentation is incomplete, and one needs to trace the code to understand the SDK call methods.
