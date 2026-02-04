from django.db import models

# models.py
class JobTask(models.Model):
    job_id = models.CharField(max_length=64, db_index=True)
    task_id = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=200)
    state = models.CharField(max_length=20, default="PENDING")
    current = models.IntegerField(default=0)
    total = models.IntegerField(default=1)
    updated_at = models.DateTimeField(auto_now=True)

