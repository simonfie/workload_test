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
    uuid = event['uuid']
    name = event.get('name', 'UNKNOWN')
    root_id = event.get('root_id', 'UNKNOWN')
    parent_id = event.get('parent_id', 'UNKNOWN')
    task_names[uuid] = name


    print(f"TASK SENT: {name} [{uuid}] --- ROOT_ID {root_id} PARENT_ID: {parent_id}")

def handle_succeeded(event):
    uuid = event['uuid']
    name = task_names.get(uuid, 'UNKNOWN')
    # a simple progress bar for tasks
    if name in tasks_to_monitor:
        completed.add(uuid)
        print(f"TASK SUCCEEDED: {name} [{uuid}]")
        print(f"Progress: {len(completed)}/{len(tasks_to_monitor)} tasks completed")


def handle_received(event):
    uuid = event['uuid']
    name = task_names.get(uuid, 'UNKNOWN')
    root_id = event.get('root_id', 'UNKNOWN')
    parent_id = event.get('parent_id', 'UNKNOWN')
    # print(event)
    print(f"TASK RECEIVED: {name} [{uuid}] --- ROOT_ID {root_id} PARENT_ID: {parent_id}")

if __name__ == "__main__":
    print("Starting Celery task monitor...")
    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-sent': handle_sent,
            'task-succeeded': handle_succeeded,
            'task-received': handle_received
        })
        recv.capture(limit=None, timeout=None, wakeup=True)
