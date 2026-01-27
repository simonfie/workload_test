import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")

celery_app = Celery("app")
celery_app.config_from_object("django.conf:settings", namespace="CELERY")
celery_app.autodiscover_tasks()
celery_app.conf.task_send_sent_event = True
celery_app.conf.worker_send_task_events = True
celery_app.conf.result_extented = True
