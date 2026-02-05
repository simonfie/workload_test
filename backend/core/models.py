from django.db import models

# models.py
class JobTask(models.Model):
    job_id = models.CharField(max_length=64, db_index=True)
    task_id = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=200)
    state = models.CharField(max_length=20, default="PENDING")
    total_tasks_in_job = models.IntegerField(default=1)
    updated_at = models.DateTimeField(auto_now=True)
    # using textfield as the log will progressively grow
    log = models.TextField(default="", blank=True)
