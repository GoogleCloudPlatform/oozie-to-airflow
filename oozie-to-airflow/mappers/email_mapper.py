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
import os
import jinja2
import logging

from airflow.operators import email_operator
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.action_mapper import ActionMapper
from o2a_libs import hdfs_helper_functions
from utils import el_utils
from utils import xml_utils

# Required nodes are determined by the Oozie XML Schema
REQ_NODES = ['to', 'subject', 'body']

TO_TAG = 'to'
SUBJECT_TAG = 'subject'
BODY_TAG = 'body'
CC_TAG = 'cc'
BCC_TAG = 'bcc'
CONTENT_TAG = 'content_type'
ATTACH_TAG = 'attachment'

MIXED = 'mixed'


class EmailMapper(ActionMapper):
    """
    Converts an email oozie node to Airflow operator.

    In order to use this, the user must specify an Airflow connection to use, and
    provide the password there.

    An action node looks like this, but we get passed the <email> node.

    <action name="[NODE-NAME]">
        <email xmlns="uri:oozie:email-action:0.2">
            <to>[COMMA-SEPARATED-TO-ADDRESSES]</to>
            <cc>[COMMA-SEPARATED-CC-ADDRESSES]</cc> <!-- cc is optional -->
            <bcc>[COMMA-SEPARATED-BCC-ADDRESSES]</bcc> <!-- bcc is optional -->
            <subject>[SUBJECT]</subject>
            <body>[BODY]</body>
            <content_type>[CONTENT-TYPE]</content_type> <!-- content_type is optional -->
            <attachment>[COMMA-SEPARATED-HDFS-FILE-PATHS]</attachment> <!-- attachment is optional -->
        </email>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    """

    def __init__(self, oozie_node, task_id,
                 trigger_rule=TriggerRule.ALL_SUCCESS, params={}, template='email.tpl'):
        ActionMapper.__init__(self, oozie_node, task_id, trigger_rule)

        self.template = template
        self.params = params

        # Parse required nodes first
        self.check_required_nodes(oozie_node, REQ_NODES)
        to = xml_utils.find_nodes_by_tag(oozie_node, TO_TAG)[0]
        # to is a templated field in Apache Airflow 1.10.0, however requires a list
        # so it is easier to just replace with variable.
        self.to = el_utils.replace_el_with_var(to.text, params=self.params, quote=False)
        self.to = self.to.split(',')

        subject = xml_utils.find_nodes_by_tag(oozie_node, SUBJECT_TAG)[0]
        # subject is a templated field in Apache Airflow 1.10.0
        self.subject = el_utils.convert_el_to_jinja(subject.text, quote=True)

        body = xml_utils.find_nodes_by_tag(oozie_node, BODY_TAG)[0]
        # body is a templated field in Apache Airflow 1.10.0
        self.body = el_utils.convert_el_to_jinja(body.text, quote=True)

        # Onto parsing optional nodes
        cc = xml_utils.find_nodes_by_tag(oozie_node, CC_TAG)
        if cc:
            self.cc = el_utils.replace_el_with_var(cc[0].text, params=self.params, quote=False)
            # Split by comma into python list
            self.cc = self.cc.split(",")

        bcc = xml_utils.find_nodes_by_tag(oozie_node, BCC_TAG)
        if bcc:
            self.bcc = el_utils.replace_el_with_var(bcc[0].text, params=self.params, quote=False)
            self.bcc = self.bcc.split(",")

        content_type = xml_utils.find_nodes_by_tag(oozie_node, CONTENT_TAG)
        if content_type:
            # Don't quote this since the quote is already in the template (required by jinja default option)
            self.content_type = el_utils.replace_el_with_var(content_type[0].text, params=self.params, quote=False)

        attachment = xml_utils.find_nodes_by_tag(oozie_node, ATTACH_TAG)
        if attachment:
            self.attachment = el_utils.replace_el_with_var(attachment[0].text, params=self.params, quote=False)
            self.attachment = self.attachment.split(",")

    def convert_to_text(self):
        template_loader = jinja2.FileSystemLoader(
            searchpath=os.path.join(ROOT_DIR, 'templates/'))
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template(self.template)
        return template.render(**self.__dict__)

    def convert_to_airflow_op(self):
        return email_operator.EmailOperator(
            task_id=self.task_id,
            trigger_rule=self.trigger_rule,
            params=self.params,
            # Required for EmailOperator
            to=self.to,
            subject=self.subject,
            html_content=self.body,
            # Optional for EmailOperator
            cc=self.cc or None,
            bcc=self.bcc or None,
            files=hdfs_helper_functions.read_hdfs_files(self.attachment) or None,
            mime_subtype=self.content_type or MIXED,
        )

    def check_required_nodes(self, oozie_node, node_tag_list):
        for tag in node_tag_list:
            nodes = xml_utils.find_nodes_by_tag(oozie_node, tag)
            if len(nodes) == 0:
                logging.WARNING("Oozie Node {} missing"
                                " required tag {}.".format(oozie_node.tag, tag))

    @staticmethod
    def required_imports():
        return ['from airflow.operators import email_operator']
