import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

from celery import chord, chain, signature, group
from celery.canvas import StampingVisitor
from core.tasks import task_a, task_b, task_c
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
workflow_chord = chord([task_a.s(),task_b.s(),task_c.s()],
    external_signature
)

# workflow_group_chain = chain(group(task_a.si(), task_b.si(), task_c.si()), external_signature)
workflow_link = group(task_a.s())


if __name__ == "__main__":
        try:
            N = 10000
            print("Running logging test...")
            start = datetime.now()
            for _ in range(N):
                task_a.delay()

            # print("Creating graph...")
            # with open('graph.dot', 'w') as fh:
            #     res.parent.parent.graph.to_dot(fh)
        except Exception as e:
             print(f"Error: {e}") 

