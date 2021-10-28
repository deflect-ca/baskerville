from baskerville.models.labeler import Labeler
from baskerville.models.pipeline_tasks.tasks_base import Task


class LabelTask(Task):
    def run(self):
        labeler = Labeler(
            self.config.database,
            spark=self.spark,
            logger=self.logger,
            **self.config.labeler
        )

        labeler.start()
        labeler.join()
