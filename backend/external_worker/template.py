from minio import Minio
from celery import Celery, shared_task
from celery.signals import worker_process_init
import time
from kombu import Queue
from pathlib import Path

import sys, os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
import django
django.setup()
from external_worker.LoggingBase import LoggingBase
# from workload_test.backend.core import task_management
from core import task_management

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
import os

TASK_NAME=os.path.splitext(os.path.basename(__file__))[0]

app = Celery(TASK_NAME)
app.conf.broker_url = "amqp://guest:guest@localhost:5672//"
app.conf.result_backend = "db+postgresql://postgres:postgres@localhost:5432/celery_results"
app.conf.result_extended = True
app.conf.task_queues = [Queue(TASK_NAME)]
# because this worker is being run from /backend it would otherwise also discover the main tasks
app.autodiscover_tasks(["external_worker.template"])


@worker_process_init.connect
def init_model(**kwargs):
    logger.info("Loading model...")
    time.sleep(5)
    logger.info("Model loaded")

minio_client = Minio(
    "localhost:9000",       
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)


# to log each step it is only necessary to input a custom message, step current step of the task will be incremented inside the method
@shared_task(name=f"{TASK_NAME}.tasks.get_template", bind=True, queue="template", base=LoggingBase)
def get_template(self, job_id):
    self.init_progress(5)

    self.update_step("Starting template task")
    time.sleep(2)

    self.update_step("Loading and preprocessing audio")
    time.sleep(6)

    self.update_step("Beginning interference")
    time.sleep(6)

    self.update_step("Finalizing result data")
    time.sleep(3)

    return "Template done"



    
