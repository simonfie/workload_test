import logging
from django.utils.timezone import now
from celery import current_task


class JobTaskDBHandler(logging.Handler):
    def emit(self, record):
        task_id = getattr(record, "task_id", None)
        if not task_id:
            return

        try:
            # needs to be loaded "lazily"
            from core.models import JobTask
            
            job_task = JobTask.objects.get(task_id=task_id)
            job_task.log += f"[{now().isoformat()}] {record.levelname}: {record.getMessage()}\n "
            job_task.save(update_fields=["log", "updated_at"])
        except JobTask.DoesNotExist:
            pass

class CeleryTaskFilter(logging.Filter):
    def filter(self, record):
        if not current_task:
            return False
        record.task_id = current_task.request.id
        return True
