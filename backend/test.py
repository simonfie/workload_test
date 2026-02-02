import os
import django
import redis

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

from celery import chord, chain, signature, group
from celery.canvas import StampingVisitor
from core.tasks import first_group_task1, first_group_task2, first_group_task3, second_group_task1, second_group_task2, second_group_task3, third_group_task1, third_group_task2, third_group_task3, fourth_group_task1, fourth_group_task2, fourth_group_task3, fifth_group_task1, fifth_group_task2, fifth_group_task3, first_chain_task1, first_chain_task2, first_chain_task3, first_chain_task4, standalone_task1, standalone_task2, standalone_task3, standalone_task4
import uuid
from datetime import datetime


# tested celery stamping for possible monitoring
class InGroupVisitor(StampingVisitor):
    def __init__(self, total_tasks):
        self.workflow_id = str(uuid.uuid4())
        self.total_tasks = total_tasks

    def on_signature(self, sig, **kwargs):
        return {
            "workflow_id": self.workflow_id,
            "workflow_total": self.total_tasks
        }

    def on_chord(self, chord, **kwargs):
        return {
            "workflow_id": self.workflow_id,
            "workflow_total": self.total_tasks
        }

# to test different workers on different tasks
external_signature = signature(
        "external.tasks.final_task",
        queue="external"
    )

# testing of different workflows
# workflow_chord = chord([task_a.s(),task_b.s(),task_c.s()],
#     external_signature
# )

# workflow_group_chain = chain(group(task_a.si(), task_b.si(), task_c.si()), external_signature)
# workflow_link = group(task_a.s())

job_id = 10

group1 = group(first_group_task1.si(job_id), first_group_task2.si(job_id), first_group_task3.si(job_id))
group2 = group(second_group_task1.si(job_id), second_group_task2.si(job_id), second_group_task3.si(job_id))
group3 = group(third_group_task1.si(job_id), third_group_task2.si(job_id), third_group_task3.si(job_id))
group4 = group(fourth_group_task1.si(job_id), fourth_group_task2.si(job_id), fourth_group_task3.si(job_id))
group5 = group(fifth_group_task1.si(job_id), fifth_group_task2.si(job_id), fifth_group_task3.si(job_id))

chain1 = chain(first_chain_task1.si(job_id), first_chain_task2.si(job_id), first_chain_task3.si(job_id), first_chain_task4.si(job_id))
chain2 = chain(standalone_task1.si(job_id), standalone_task2.si(job_id), standalone_task3.si(job_id), standalone_task4.si(job_id))

complex_workflow = chord(group(chord(group1, group2),chord(group3, chain1)), group(group4, chain2, group5))


r = redis.Redis(decode_responses=True)


if __name__ == "__main__":
        try:
            # N = 10000
            # print("Running logging test...")
            # start = datetime.now()
            # for _ in range(N):
            #     task_a.delay()

            # job_id = uuid.uuid4().hex

            # simple id for testing monitoring without API calls
            

            r.hset(f"job:{job_id}", mapping={
                 "total_tasks": 23,
                 "completed_tasks": 0
            })

            # 
            result = complex_workflow.apply_async()


        except Exception as e:
             print(f"Error: {e}") 

