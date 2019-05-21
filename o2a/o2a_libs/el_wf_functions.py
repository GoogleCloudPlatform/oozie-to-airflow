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
# pylint: disable=unused-argument


def wf_id():
    """
    It returns the workflow job ID for the current workflow job.

    In airflow it can be found using Jinja templating for `run_id`
    This has the effect that some parameters cannot be templated, and thus
    this will fail.
    """
    return "{{ run_id }}"


def wf_name():
    """
    It returns the workflow application name for the current workflow job.

    This has the effect that some parameters cannot be templated, and thus
    this will fail.

    :return: Current DAG id.
    """
    return "{{ dag.dag_id }}"


def wf_app_path():
    """
    It returns the workflow application path for the current workflow job.
    This has the effect that some parameters cannot be templated, and thus
    this will fail.
    """


def wf_conf():
    """
    It returns the value of the workflow job configuration property for the
    current workflow job, or an empty string if undefined.

    This has the effect that some parameters cannot be templated, and thus
    this will fail.
    """


def wf_user():
    """
    Returns the user name that started the current workflow job.

    """
    return "{{ params.user.name }}"


def wf_group():
    """
    It returns the group/ACL for the current workflow job.

    In Airflow, I believe it uses RBAC vs ACL, need to discuss.
    """


def wf_callback(state_variable):
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


def wf_transition(node):
    """
    It returns the transition taken by the specified workflow action node, or
    an empty string if the action has not being executed or it has not completed
    yet.
    """


def wf_last_error_node():
    """
    It returns the name of the last workflow action node that exit with an ERROR
    exit state, or an empty string if no action has exited with ERROR state in the
    current workflow job.
    """


def wf_error_code(node):
    """
    It returns the error code for the specified action node, or an empty string if
    the action node has not exited with ERROR state.

    Each type of action node must define its complete error code list.
    """


def wf_error_message(message):
    """
    It returns the error message for the specified action node, or an empty string
    if no action node has not exited with ERROR state.

    The error message can be useful for debugging and notification purposes.
    """


def wf_run():
    """
    It returns the run number for the current workflow job, normally 0 unless the
    workflow job is re-run, in which case indicates the current run.
    """


def wf_action_data(node):
    """
    This function is only applicable to action nodes that produce output data on
    completion.

    The output data is in a Java Properties format and via this EL function it
    is available as a Map.
    """


def wf_action_external_id(node):
    """
    It returns the external Id for an action node, or an empty string if the
    action has not being executed or it has not completed yet.
    """


def wf_action_tracker_uri(node):
    """
    It returns the tracker URI for an action node, or an empty string if the action
    has not being executed or it has not completed yet.
    """


def wf_action_external_status(node):
    """
    It returns the external status for an action node, or an empty string if the
    action has not being executed or it has not completed yet.
    """
