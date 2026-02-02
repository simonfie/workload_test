import time
import redis
import os

redis_app = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

def task_key(job_id, task_id):
    return f"job:{job_id}:task:{task_id}"

def register_task(job_id, task_id, name, steps_total=1):
    redis_app.sadd(f"job:{job_id}:tasks", task_id)
    redis_app.hset(task_key(job_id, task_id), mapping={
        "task":name,
        "state": "PENDING",
        "step": 0,
        "steps_total": steps_total,
        "message": "",
        "updated_at": time.time(),
    })

def update_task(job_id, task_id, **fields):
    fields["updated_at"] = time.time()
    redis_app.hset(task_key(job_id, task_id), mapping=fields)

def finish_task(job_id, task_id):
    update_task(job_id, task_id, state="SUCCESS")
    redis_app.hincrby(f"job:{job_id}", "completed_tasks", 1)


def get_progress(job_id):
    tasks_ids = redis_app.smembers(f"job:{job_id}:tasks")

    running = []
    completed = 0

    for tid in tasks_ids:
        task = redis_app.hgetall(task_key(job_id, tid))
        if task["state"] == "RUNNING":
            running.append(task)
        if task["state"] == "SUCCESS":
            completed += 1
    try:
        total = int(redis_app.hget(f"job:{job_id}", "total_tasks"))
    except TypeError as e:
        print(f"job_id: {job_id} is not currently running, run test")
        return

    return {
        "progress": completed / total if total else 0,
        "running_tasks": running, 
        "completed_tasks": completed,
        "total_tasks": total,
    }

if __name__ == "__main__":
    print("Starting task progress monitor...")

    while(True):
        time.sleep(1)

        print(get_progress(10))



