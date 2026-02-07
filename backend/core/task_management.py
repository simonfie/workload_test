from celery.signals import task_prerun, task_postrun, task_failure
from celery.result import AsyncResult
from core.models import JobTask
from django.utils.timezone import now
from minio import Minio
import json
import logging
import io

# management script for all workers, now creates a row for every task and after the task finishes, it uploads it to minio
# it is only necessary to then import this file into the tasks' definition file and the worker will register these


BUCKET_NAME = "test-bucket"


logger = logging.getLogger(__name__)


minio_client = Minio(
    "localhost:9000",       
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

@task_prerun.connect
def create_job_task(sender=None, task_id=None, task=None, args=None, kwargs=None, **extra):
    # don't create database rows for internal celery tasks like chord_unlock, ...
    if sender.name.startswith("celery."):
        return

    # only for testing
    skip_tasks = ["log_to_minio"]
    task_name = sender.name.split(".")[-1]  # short task name
    if task_name in skip_tasks:
        return

    # extract job_id
    job_id = kwargs.get("job_id") or (args[0] if args else None)
    if not job_id:
        return


    # create JobTask if it doesn't exist
    JobTask.objects.get_or_create(
        task_id=task_id,
        defaults={
            "job_id": job_id,
            "name": sender.name.split(".")[-1],  # short task name
            "state": "PENDING",
            "total_tasks_in_job": 23,
        }

    )
    logger.info("PENDING")


@task_postrun.connect
def upload_logs(sender=None, task_id=None, error_msg=None, traceback=None, retval=None, **kwargs):
    try:
        task = JobTask.objects.get(task_id=task_id)
        result = AsyncResult(task_id)

        if result.state == "FAILURE":
            # update the log with the error message and time
            error_msg = str(result.result)
            traceback = str(result.traceback)
            task.log += f"\n [{now().isoformat()}] ERROR: {error_msg}\n"
            task.save(update_fields=["log", "updated_at"])
            error_msg = str(error_msg)

        elif result.state == "SUCCESS":
            task.log += f"\n [{now().isoformat()}] SUCCESS: {retval}"
            task.save(update_fields=["log", "updated_at"])
        

        data = {
            "job_id": task.job_id,
            "task": task.name,
            "updated_at": str(task.updated_at),
            "error": error_msg,
            "retval": str(retval),
            "traceback": traceback,
            "state": result.state,
            "log": task.log,
        }

        content = json.dumps(data, indent=2).encode()
        minio_client.put_object(
            BUCKET_NAME,
            f"{task.job_id}/{task.name}/log.json",
            io.BytesIO(content),
            len(content),
            content_type="application/json",
        )
    except JobTask.DoesNotExist:
        pass