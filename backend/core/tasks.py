from celery import shared_task
import time
import logging


logger = logging.getLogger(__name__)

@shared_task()
def task_a():
    logger.info("TASK A EXECUTED")
    return "A done"


@shared_task
def task_b():
    time.sleep(3)
    return "B done"

@shared_task
def task_c():
    time.sleep(10)
    return "C done"