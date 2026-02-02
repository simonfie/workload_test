from celery import shared_task
import time
import logging


logger = logging.getLogger(__name__)

''''first group'''
@shared_task()
def first_group_task1():
    logger.info("TASK first_group_task1 EXECUTED")
    time.sleep(3)
    return "first_group_task_1 done"


@shared_task()
def first_group_task2():
    logger.info("TASK first_group_task2 EXECUTED")
    time.sleep(5)
    return "first_group_task_2 done"

@shared_task()
def first_group_task3():
    logger.info("TASK first_group_task3 EXECUTED")
    time.sleep(6)
    return "first_group_task_3 done"


''''second group'''
@shared_task()
def second_group_task1():
    logger.info("TASK second_group_task1 EXECUTED")
    time.sleep(3)
    return "second_group_task_1 done"


@shared_task()
def second_group_task2():
    logger.info("TASK second_group_task2 EXECUTED")
    time.sleep(4)
    return "second_group_task_2 done"

@shared_task()
def second_group_task3():
    logger.info("TASK second_group_task3 EXECUTED")
    time.sleep(4)
    return "second_group_task_3 done"


''''third group'''
@shared_task()
def third_group_task1():
    logger.info("TASK third_group_task1 EXECUTED")
    time.sleep(4)
    return "third_group_task_1 done"


@shared_task()
def third_group_task2():
    logger.info("TASK third_group_task2 EXECUTED")
    time.sleep(5)
    return "third_group_task_2 done"

@shared_task()
def third_group_task3():
    logger.info("TASK third_group_task3 EXECUTED")
    time.sleep(2)
    return "third_group_task_3 done"

''''fourth group'''
@shared_task()
def fourth_group_task1():
    logger.info("TASK fourth_group_task1 EXECUTED")
    time.sleep(6)
    return "fourth_group_task_1 done"


@shared_task()
def fourth_group_task2():
    logger.info("TASK fourth_group_task2 EXECUTED")
    time.sleep(7)
    return "fourth_group_task_2 done"

@shared_task()
def fourth_group_task3():
    logger.info("TASK fourth_group_task3 EXECUTED")
    time.sleep(6)
    return "fourth_group_task_3 done"

''''fifth group'''
@shared_task()
def fifth_group_task1():
    logger.info("TASK fifth_group_task1 EXECUTED")
    time.sleep(3)
    return "fifth_group_task_1 done"


@shared_task()
def fifth_group_task2():
    logger.info("TASK fifth_group_task2 EXECUTED")
    time.sleep(4)
    return "fifth_group_task_2 done"

@shared_task()
def fifth_group_task3():
    logger.info("TASK fifth_group_task3 EXECUTED")
    time.sleep(2)
    return "fifth_group_task_3 done"

''''first chain'''
@shared_task()
def first_chain_task1():
    logger.info("TASK first_chain_task1 EXECUTED")
    time.sleep(7)
    return "first_chain_task_1 done"

@shared_task()
def first_chain_task2():
    logger.info("TASK first_chain_task2 EXECUTED")
    time.sleep(6)
    return "first_chain_task_2 done"

@shared_task()
def first_chain_task3():
    logger.info("TASK first_chain_task3 EXECUTED")
    time.sleep(2)
    return "first_chain_task_3 done"

@shared_task()
def first_chain_task4():
    logger.info("TASK first_chain_task4 EXECUTED")
    time.sleep(5)
    return "first_chain_task_4 done"

'''standalone tasks'''

@shared_task()
def standalone_task1():
    logger.info("TASK standalone_task1 EXECUTED")
    time.sleep(2)
    return "standalone_task1 done"

@shared_task()
def standalone_task2():
    logger.info("TASK standalone_task2 EXECUTED")
    time.sleep(2)
    return "standalone_task2 done"

@shared_task()
def standalone_task3():
    logger.info("TASK standalone_task3 EXECUTED")
    time.sleep(3)
    return "standalone_task3 done"

@shared_task()
def standalone_task4():
    logger.info("TASK standalone_task4 EXECUTED")
    time.sleep(4)
    return "standalone_task4 done"