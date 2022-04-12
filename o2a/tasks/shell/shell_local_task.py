from typing import Any, Dict
from o2a.converter.task import Task
from airflow.utils.trigger_rule import TriggerRule


class ShellLocalTask(Task):
    """Class for Hive Local execution Task"""

    def __init__(
            self,
            task_id: str,
            template_name: str,
            trigger_rule: str =
            TriggerRule.ONE_SUCCESS,
            template_params: Dict[str, Any] = None
    ):
        super().__init__(task_id, template_name, trigger_rule, template_params)

    @staticmethod
    def required_imports() -> set[str]:
        return {"from airflow.utils import dates", "from airflow.operators import bash"}
