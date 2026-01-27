from celery import Celery

# for monitoring completed tasks with a progress bar
# https://docs.celeryq.dev/en/latest/userguide/monitoring.html

app = Celery("monitor", broker="amqp://guest:guest@localhost:5672//")
app.conf.task_send_sent_event = True 

# TODO: find a better solution to define the monitored task than to write them individually
tasks_to_monitor = {
    'core.tasks.task_a',
    'core.tasks.task_b',
    'core.tasks.task_c',
    'external.tasks.final_task'
}

completed = set()      
task_names = {}       

def handle_sent(event):
    """Triggered when a task is sent to the broker"""
    uuid = event['uuid']
    name = event.get('name', 'UNKNOWN')
    task_names[uuid] = name

    headers = event.get("workflow_id", {})
    print(f"event: {event}")
    workflow_id = headers.get("workflow_id")
    total = headers.get("workflow_total")

    print(f"TASK SENT: {name} [{uuid}] --- WF_ID {workflow_id} TOTAL TASKS: {total}")

def handle_succeeded(event):
    """Triggered when a task succeeds"""
    uuid = event['uuid']
    name = task_names.get(uuid, 'UNKNOWN')
    if name in tasks_to_monitor:
        completed.add(uuid)
        print(f"TASK SUCCEEDED: {name} [{uuid}]")
        print(f"Progress: {len(completed)}/{len(tasks_to_monitor)} tasks completed")


def handle_received(event):
    uuid = event['uuid']
    name = task_names.get(uuid, 'UNKNOWN')
    print(event)
    print(f"TASK RECEIVED: {name} [{uuid}]")

if __name__ == "__main__":
    print("Starting Celery task monitor...")
    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-sent': handle_sent,
            'task-succeeded': handle_succeeded,
            'task-received': handle_received
        })
        recv.capture(limit=None, timeout=None, wakeup=True)
