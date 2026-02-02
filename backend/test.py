import os
import django

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

group1 = group(first_group_task1.si(), first_group_task2.si(), first_group_task3.si())
group2 = group(second_group_task1.si(), second_group_task2.si(), second_group_task3.si())
group3 = group(third_group_task1.si(), third_group_task2.si(), third_group_task3.si())
group4 = group(fourth_group_task1.si(), fourth_group_task2.si(), fourth_group_task3.si())
group5 = group(fifth_group_task1.si(), fifth_group_task2.si(), fifth_group_task3.si())

chain1 = chain(first_chain_task1.si(), first_chain_task2.si(), first_chain_task3.si(), first_chain_task4.si())
chain2 = chain(standalone_task1.si(), standalone_task2.si(), standalone_task3.si(), standalone_task4.si())

complex_workflow = chord(group(chord(group1, group2),chord(group3, chain1)), group(group4, chain2, group5))


if __name__ == "__main__":
        try:
            # N = 10000
            # print("Running logging test...")
            # start = datetime.now()
            # for _ in range(N):
            #     task_a.delay()

            result = complex_workflow.apply_async()


        except Exception as e:
             print(f"Error: {e}") 

