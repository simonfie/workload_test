import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()
from celery import group
from core.tasks import log_to_minio 


tasks_group = group([log_to_minio.si(i) for i in range(10000)])
result = tasks_group.apply_async()
