# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
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
"""All WF EL functions"""

from typing import Optional, Set

from jinja2 import pass_context

from airflow.models import TaskInstance, DagRun, DAG
from airflow.utils.db import provide_session
from airflow import AirflowException


def _reverse_task_map(task_map: dict) -> dict:
    """
    Given a map {oozie_node: [airflow_node1, airflow_node2]} it returns
    reversed map {airflow_node1: oozie_node, airflow_node2: oozie_node}.
    :param task_map: oozie to airflow task map
    :return: reversed task map
    """
    new_map = dict()
    for oozie_node, airflow_tasks in task_map.items():
        new_map.update({t: oozie_node for t in airflow_tasks})
    return new_map


@pass_context
def conf(context=None, key: str = None):
    """
    It returns the value of the workflow job configuration property for the
    current workflow job, or an empty string if undefined.
    This has the effect that some parameters cannot be templated, and thus
    this will fail.
    """
    try:
        return context[key]
    except KeyError:
        raise AirflowException(f"Property {key} not found in workflow configuration.")


@pass_context
def user(context=None):
    """
    Returns gloabl user name, DAG owner or raises error
    if there is more than one DAG's owner.
    """
    owner = context.get("user.name", None)
    if owner:
        return owner

    dag: Optional[DAG] = context.get("dag", None)
    if dag is None:
        raise AirflowException("No dag reference in context.")

    owners: Set[str] = {t.owner for t in dag.tasks}
    if len(owners) > 1:
        raise AirflowException("DAG owner is ambiguous.")

    if not owners:
        raise AirflowException("DAG has no owner.")

    owner = owners.pop()
    return owner


@pass_context
@provide_session
def last_error_node(context=None, session=None) -> str:
    """
    It returns the name of the last workflow action node that exit with an ERROR
    exit state, or an empty string if no action has exited with ERROR state in the
    current workflow job.
    """
    drun: Optional[DagRun] = context.get("dag_run", None)
    if drun is None:
        raise AirflowException("No dag_run reference in context.")

    dag_id = drun.dag_id
    ti = TaskInstance  # pylint:disable=invalid-name
    last_failed_task = (
        session.query(TaskInstance)
        .filter(ti.dag_id == dag_id)
        .filter(ti.task_id.endswith("_error"))
        .order_by(ti.execution_date.asc())
        .first()
    )

    if not last_failed_task:
        return ""

    task_name: str = last_failed_task.task_id

    task_map: Optional[dict] = context.get("task_map", None)
    if task_map is None:
        raise AirflowException("No task map!")

    reversed_map: dict = _reverse_task_map(task_map)
    return reversed_map.get(task_name, "")


def app_path():
    """
    It returns the workflow application path for the current workflow job.
    This has the effect that some parameters cannot be templated, and thus
    this will fail.
    """


def group():
    """
    It returns the group/ACL for the current workflow job.
    In Airflow, I believe it uses RBAC vs ACL, need to discuss.
    """


def callback(state_variable):  # pylint: disable=unused-argument
    """
    It returns the callback URL for the current workflow action node, stateVar
    can be a valid exit state (=OK= or ERROR ) for the action or a token to be
    replaced with the exit state by the remote system executing the task.
    When a computation/processing tasks is started by Oozie, Oozie provides a
    unique callback URL to the task, the task should invoke the given URL to
    notify its completion.
    For cases that the task failed to invoke the callback URL for any reason
    (i.e. a transient network failure) or when the type of task cannot invoke
    the callback URL upon completion, Oozie has a mechanism to poll
    computation/processing tasks for completion.
    """


def transition(node):  # pylint: disable=unused-argument
    """
    It returns the transition taken by the specified workflow action node, or
    an empty string if the action has not being executed or it has not completed
    yet.
    """


def error_code(node):  # pylint: disable=unused-argument
    """
    It returns the error code for the specified action node, or an empty string if
    the action node has not exited with ERROR state.
    Each type of action node must define its complete error code list.
    """


def error_message(node):  # pylint: disable=unused-argument
    """
    It returns the error message for the specified action node, or an empty string
    if no action node has not exited with ERROR state.
    The error message can be useful for debugging and notification purposes.
    """
    # TODO: return proper message instead of a dummy one.
    return f"Dummy error message for node {node}."


def run():
    """
    It returns the run number for the current workflow job, normally 0 unless the
    workflow job is re-run, in which case indicates the current run.
    """


def action_data(node):  # pylint: disable=unused-argument
    """
    This function is only applicable to action nodes that produce output data on
    completion.
    The output data is in a Java Properties format and via this EL function it
    is available as a Map.
    """


def action_external_id(node):  # pylint: disable=unused-argument
    """
    It returns the external Id for an action node, or an empty string if the
    action has not being executed or it has not completed yet.
    """


def action_tracker_uri(node):  # pylint: disable=unused-argument
    """
    It returns the tracker URI for an action node, or an empty string if the action
    has not being executed or it has not completed yet.
    """


def action_external_status(node):  # pylint: disable=unused-argument
    """
    It returns the external status for an action node, or an empty string if the
    action has not being executed or it has not completed yet.
    """
