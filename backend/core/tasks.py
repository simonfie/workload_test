from celery import shared_task
import time
import logging
from .progress import register_task, update_task, finish_task


logger = logging.getLogger(__name__)

''''first group'''
@shared_task(bind=True)
def first_group_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=3)

    logger.info(f"TASK first_group_task1 EXECUTED job_id: {job_id}")

    update_task(job_id, task_id, state="RUNNING", step=1, message="Preparing")
    time.sleep(1)

    update_task(job_id, task_id, state="RUNNING", step=2, message="Running")
    time.sleep(1)

    update_task(job_id, task_id, state="RUNNING", step=3, message="Finalizing")
    time.sleep(1)

    finish_task(job_id, task_id)
    return "first_group_task_1 done"


@shared_task(bind=True)
def first_group_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK first_group_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Working")
    time.sleep(5)

    finish_task(job_id, task_id)
    return "first_group_task_2 done"

@shared_task(bind=True)
def first_group_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK first_group_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Starting")
    time.sleep(2)

    update_task(job_id, task_id, step=2, message="Halfway done")
    time.sleep(2)

    update_task(job_id, task_id, step=3, message="Finishing")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "first_group_task_3 done"


''''second group'''
@shared_task(bind=True)
def second_group_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK second_group_task1 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Processing")
    time.sleep(3)

    finish_task(job_id, task_id)
    return "second_group_task_1 done"

@shared_task(bind=True)
def second_group_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=2)

    logger.info("TASK second_group_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Step 1")
    time.sleep(2)

    update_task(job_id, task_id, step=2, message="Step 2")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "second_group_task_2 done"

@shared_task(bind=True)
def second_group_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK second_group_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Running")
    time.sleep(4)

    finish_task(job_id, task_id)
    return "second_group_task_3 done"


''''third group'''
@shared_task(bind=True)
def third_group_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=3)

    logger.info("TASK third_group_task1 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Step 1")
    time.sleep(1)

    update_task(job_id, task_id, step=2, message="Step 2")
    time.sleep(2)

    update_task(job_id, task_id, step=3, message="Step 3")
    time.sleep(1)

    finish_task(job_id, task_id)
    return "third_group_task_1 done"

@shared_task(bind=True)
def third_group_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=2)

    logger.info("TASK third_group_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Halfway")
    time.sleep(3)

    update_task(job_id, task_id, step=2, message="Almost done")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "third_group_task_2 done"

@shared_task(bind=True)
def third_group_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK third_group_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Processing")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "third_group_task_3 done"


''''fourth group'''
@shared_task(bind=True)
def fourth_group_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=2)

    logger.info("TASK fourth_group_task1 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Running")
    time.sleep(3)

    update_task(job_id, task_id, step=2, message="Finishing")
    time.sleep(3)

    finish_task(job_id, task_id)
    return "fourth_group_task_1 done"

@shared_task(bind=True)
def fourth_group_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK fourth_group_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Working")
    time.sleep(7)

    finish_task(job_id, task_id)
    return "fourth_group_task_2 done"

@shared_task(bind=True)
def fourth_group_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK fourth_group_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Starting")
    time.sleep(3)

    update_task(job_id, task_id, step=2, message="Ending")
    time.sleep(3)

    finish_task(job_id, task_id)
    return "fourth_group_task_3 done"


''''fifth group'''
@shared_task(bind=True)
def fifth_group_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK fifth_group_task1 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Processing")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "fifth_group_task_1 done"

@shared_task(bind=True)
def fifth_group_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK fifth_group_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Running")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "fifth_group_task_2 done"

@shared_task(bind=True)
def fifth_group_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK fifth_group_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Executing")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "fifth_group_task_3 done"


''''first chain'''
@shared_task(bind=True)
def first_chain_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=2)

    logger.info("TASK first_chain_task1 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Running")
    time.sleep(4)

    update_task(job_id, task_id, step=2, message="Finishing")
    time.sleep(3)

    finish_task(job_id, task_id)
    return "first_chain_task_1 done"

@shared_task(bind=True)
def first_chain_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK first_chain_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Processing")
    time.sleep(6)

    finish_task(job_id, task_id)
    return "first_chain_task_2 done"

@shared_task(bind=True)
def first_chain_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name, steps_total=2)

    logger.info("TASK first_chain_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Halfway")
    time.sleep(1)

    update_task(job_id, task_id, step=2, message="Done")
    time.sleep(1)

    finish_task(job_id, task_id)
    return "first_chain_task_3 done"

@shared_task(bind=True)
def first_chain_task4(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK first_chain_task4 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Working")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "first_chain_task_4 done"


'''standalone tasks'''
@shared_task(bind=True)
def standalone_task1(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK standalone_task1 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Starting")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "standalone_task1 done"

@shared_task(bind=True)
def standalone_task2(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK standalone_task2 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Running")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "standalone_task2 done"

@shared_task(bind=True)
def standalone_task3(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK standalone_task3 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Halfway")
    time.sleep(1)
    update_task(job_id, task_id, step=2, message="Almost done")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "standalone_task3 done"

@shared_task(bind=True)
def standalone_task4(self, job_id):
    task_id = self.request.id
    register_task(job_id, task_id, self.name)

    logger.info("TASK standalone_task4 EXECUTED")
    update_task(job_id, task_id, state="RUNNING", step=1, message="Executing")
    time.sleep(2)
    update_task(job_id, task_id, step=2, message="Finishing")
    time.sleep(2)

    finish_task(job_id, task_id)
    return "standalone_task4 done"