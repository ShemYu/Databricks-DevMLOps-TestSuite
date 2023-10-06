import os
from databricks.sdk import WorkspaceClient


job_id = os.environ.get("JOBID")
w = WorkspaceClient()
ct_job = w.jobs.get(job_id=job_id)
print(ct_job)
run = w.jobs.run_now_and_wait(job_id=job_id)
print(run)