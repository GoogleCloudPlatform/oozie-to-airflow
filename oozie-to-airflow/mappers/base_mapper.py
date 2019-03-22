# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.utils.trigger_rule import TriggerRule


class BaseMapper:
    def __init__(self, oozie_node, task_id, trigger_rule=TriggerRule.ALL_SUCCESS, params={}, **kwargs):
        self.params = params
        self.oozie_node = oozie_node
        self.task_id = task_id
        self.trigger_rule = trigger_rule

    def convert_to_text(self):
        """
        Returns a python operator equivalent string. This will be appended to
        the file.
        """
        raise NotImplementedError("Not Implemented")

    def convert_to_airflow_op(self):
        """
        Return a corresponding Airflow Operator python object. As of now, it is
        not used, but it could come in handy in the future for extending the
        program.
        """
        raise NotImplementedError("Not Implemented")

    @staticmethod
    def required_imports():
        """
        Returns a list of strings that are the import statement that python will
        write to use.

        Ex: returns ['from airflow.operators import bash_operator']
        """
        raise NotImplementedError("Not Implemented")

    def get_task_id(self):
        """
        Returns the task_id of the operator, this can be handy if any prepare
        statements need to be used or any reordering of operators.
        """
        return self.task_id
