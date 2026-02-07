from celery import shared_task

import time
import logging
import core.task_management

import io
import datetime
import json


logger = logging.getLogger(__name__)


# @shared_task(bind=True)
# def log_to_minio(self, index: int):
#     folder_name = f"test_{index}"
#     file_name = "result.json"
    
#     dummy_data = {
#         "task_index": index,
#         "result": "dummy data"
#     }

#     data_bytes = json.dumps(dummy_data).encode("utf-8")
#     data_io = io.BytesIO(data_bytes)

#     object_path = f"{folder_name}/{file_name}"

#     minio_client.put_object(
#         BUCKET_NAME,
#         object_path,
#         data_io,
#         length=len(data_bytes),
#         content_type="application/json"
#     )

#     return {"task_index": index, "status": "done"}


''''first group'''
@shared_task(bind=True)
def first_group_task1(self, job_id):
    total_steps = 3
    for step, msg in enumerate(["Preparing", "Running", "Finalizing"], start=1):
        logger.info(msg)
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep(1)
    return {"job_id": job_id, "status": "done", "task": "first_group_task1"}


@shared_task(bind=True)
def first_group_task2(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Working"
    })
    time.sleep(5)
    return {"job_id": job_id, "status": "done", "task": "first_group_task2"}


@shared_task(bind=True)
def first_group_task3(self, job_id):
    total_steps = 3
    for step, msg in enumerate(["Starting", "Halfway done", "Finishing"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "first_group_task3"}


''''second group'''
@shared_task(bind=True)
def second_group_task1(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Processing"
    })
    time.sleep(3)
    return {"job_id": job_id, "status": "done", "task": "second_group_task1"}


@shared_task(bind=True)
def second_group_task2(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Step 1", "Step 2"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "second_group_task2"}


@shared_task(bind=True)
def second_group_task3(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Running"
    })
    time.sleep(4)
    return {"job_id": job_id, "status": "done", "task": "second_group_task3"}


''''third group'''
@shared_task(bind=True)
def third_group_task1(self, job_id):
    total_steps = 3
    for step, msg in enumerate(["Step 1", "Step 2", "Step 3"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep([1, 2, 1][step-1])
    return {"job_id": job_id, "status": "done", "task": "third_group_task1"}


@shared_task(bind=True)
def third_group_task2(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Halfway", "Almost done"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep([3, 2][step-1])
    return {"job_id": job_id, "status": "done", "task": "third_group_task2"}


@shared_task(bind=True)
def third_group_task3(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Processing"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "third_group_task3"}


''''fourth group'''
@shared_task(bind=True)
def fourth_group_task1(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Running", "Finishing"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep(3)
    return {"job_id": job_id, "status": "done", "task": "fourth_group_task1"}


@shared_task(bind=True)
def fourth_group_task2(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Working"
    })
    time.sleep(7)
    return {"job_id": job_id, "status": "done", "task": "fourth_group_task2"}


@shared_task(bind=True)
def fourth_group_task3(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Starting", "Ending"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep(3)
    return {"job_id": job_id, "status": "done", "task": "fourth_group_task3"}


''''fifth group'''
@shared_task(bind=True)
def fifth_group_task1(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Processing"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "fifth_group_task1"}


@shared_task(bind=True)
def fifth_group_task2(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Running"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "fifth_group_task2"}


@shared_task(bind=True)
def fifth_group_task3(self, job_id):
    total_steps = 1
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": total_steps,
        "message": "Executing"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "fifth_group_task3"}


''''first chain'''
@shared_task(bind=True)
def first_chain_task1(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Running", "Finishing"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep([4, 3][step-1])
    return {"job_id": job_id, "status": "done", "task": "first_chain_task1"}


@shared_task(bind=True)
def first_chain_task2(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Processing"
    })
    time.sleep(6)
    return {"job_id": job_id, "status": "done", "task": "first_chain_task2"}


@shared_task(bind=True)
def first_chain_task3(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Halfway", "Done"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep([1, 1][step-1])
    return {"job_id": job_id, "status": "done", "task": "first_chain_task3"}


@shared_task(bind=True)
def first_chain_task4(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Working"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "first_chain_task4"}


''''standalone tasks'''
@shared_task(bind=True)
def standalone_task1(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Starting"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "standalone_task1"}


@shared_task(bind=True)
def standalone_task2(self, job_id):
    self.update_state(state="RUNNING", meta={
        "job_id": job_id,
        "step": 1,
        "steps_total": 1,
        "message": "Running"
    })
    time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "standalone_task2"}


@shared_task(bind=True)
def standalone_task3(self, job_id):
    total_steps = 2
    for step, msg in enumerate(["Halfway", "Almost done"], start=1):
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        time.sleep([1, 2][step-1])
    return {"job_id": job_id, "status": "done", "task": "standalone_task3"}


@shared_task(bind=True)
def standalone_task4(self, job_id):
    total_steps = 3
    for step, msg in enumerate(["Executing", "Working", "Finishing"], start=1):
        logger.info(msg)
        self.update_state(state="RUNNING", meta={
            "job_id": job_id,
            "step": step,
            "steps_total": total_steps,
            "message": msg
        })
        if step == 1:
            raise RuntimeError(f"Simulated error at step {step}")
        time.sleep(2)
    return {"job_id": job_id, "status": "done", "task": "standalone_task4"}
