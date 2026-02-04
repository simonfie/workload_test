import os
import django
import sys
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

from celery.result import AsyncResult
# from celery_app import app
from core.models import JobTask




def get_progress(job_id):
    job_tasks = JobTask.objects.filter(job_id=job_id)

    if not job_tasks.exists():
        return {
            "job_id": job_id,
            "completed": 0,
            "total": 0,
            "percent": 0,
            "tasks": [],
        }

    completed = 0
    tasks_progress = []
    total_tasks = 0


    for task in job_tasks:
        result = AsyncResult(task.task_id)

        task_state = result.state
        if task_state in ["SUCCESS", "FAILURE"]:
            completed += 1

        total_tasks = task.total

        if task_state == "RUNNING" and isinstance(result.info, dict):
            current_step = result.info.get("step")
            total_steps = result.info.get("steps_total")
            msg = result.info.get("message")
        else:
            current_step = total_steps = msg = None


        tasks_progress.append({
            "task_name": task.name,
            "state": task_state,
            "current_step": current_step,
            "total_steps": total_steps,
            "msg": msg
        })

    percent_done = int(completed / total_tasks * 100) if total_tasks else 0

    return {
        "job_id": job_id,
        "completed": completed,
        "total": total_tasks,
        "percent": percent_done,
        "tasks": tasks_progress,
    }

if __name__ == "__main__":
    job_id = sys.argv[1]
    print("Starting progress tracking...")
    while(True):
        progress = get_progress(job_id)
        print(f"{progress['completed']}/{progress['total']} tasks done ({progress['percent']}%)")
        for t in progress["tasks"]:
            print(f" - {t['task_name']}: {t['state']} {t['current_step']}/{t['total_steps']} -- {t['msg']}")

        if progress["completed"] >= progress["total"]:
            if progress["total"] == 0:
                print("job not found")
                continue
            print("Job finished!")
            break
        time.sleep(1)
