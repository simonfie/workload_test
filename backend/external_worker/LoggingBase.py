import logging
from celery import Task

logger = logging.getLogger(__name__)

# class to simplify logging
# initiated with the total steps of the task
# then each updated_step increments step, takes a custom message to log and also append to the "log" row in JobTask table with the logger
# lastly updates the result_backend (also a DB in this case) which is used for progress tracking in progress.py
class LoggingBase(Task):
    abstract = True

    def init_progress(self, steps_total: int):
        self.steps_total = steps_total
        self._current_step = 1

    def update_step(self, message="", state="RUNNING"):
        logger.info(message)

        self.update_state(
            state=state,
            meta={
                "step": self._current_step,
                "steps_total": self.steps_total,
                "message": message,
            }
        )

        self._current_step += 1
