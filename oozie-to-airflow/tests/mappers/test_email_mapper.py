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

import ast
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from mappers import email_mapper


class TestEmailMapper(unittest.TestCase):

    def _create_xml_jinja(self):
        email = ET.Element('email')
        to = ET.SubElement(email, 'to')
        cc = ET.SubElement(email, 'cc')
        bcc = ET.SubElement(email, 'bcc')
        subject = ET.SubElement(email, 'subject')
        body = ET.SubElement(email, 'body')
        content_type = ET.SubElement(email, 'content_type')
        attachment = ET.SubElement(email, 'attachment')

        to.text = '${to}'
        cc.text = '${cc}'
        bcc.text = '${bcc}'
        subject.text = '${subject}'
        body.text = '${body}'
        content_type.text = '${content_type}'
        attachment.text = '${attachment}'

        self.et = ET.ElementTree(email)

    def _create_xml_no_jinja(self):
        email = ET.Element('email')
        to = ET.SubElement(email, 'to')
        cc = ET.SubElement(email, 'cc')
        bcc = ET.SubElement(email, 'bcc')
        subject = ET.SubElement(email, 'subject')
        body = ET.SubElement(email, 'body')
        content_type = ET.SubElement(email, 'content_type')
        attachment = ET.SubElement(email, 'attachment')

        to.text = 'bob@apache.org,not.bob@apache.org'
        cc.text = 'john@apache.org,jeff@apache.org'
        bcc.text = 'secret@apache.org,secret2@apache.org'
        subject.text = 'test_subject'
        body.text = 'this is a test body'
        content_type.text = 'text/plain'
        attachment.text = '/file/in/hdfs.txt'

        self.et = ET.ElementTree(email)

    def test_create_mapper_no_jinja(self):
        self._create_xml_no_jinja()
        mapper = email_mapper.EmailMapper(oozie_node=self.et.getroot(),
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        # make sure everything is getting initialized correctly
        self.assertEqual('test_id', mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)

        self.assertEqual(['bob@apache.org', 'not.bob@apache.org'], mapper.to)
        self.assertEqual(['john@apache.org', 'jeff@apache.org'], mapper.cc)
        self.assertEqual(['secret@apache.org', 'secret2@apache.org'],
                         mapper.bcc)
        self.assertEqual('\'test_subject\'', mapper.subject)
        self.assertEqual('\'this is a test body\'', mapper.body)
        self.assertEqual('text/plain', mapper.content_type)
        self.assertEqual(['/file/in/hdfs.txt'], mapper.attachment)

    def test_create_mapper_jinja(self):
        self._create_xml_jinja()
        # test jinja templating
        params = {'to': 'user@apache.org,user2@apache.org',
                  'cc': 'bob@apache.org',
                  'bcc': 'jeff@apache.org',
                  'subject': 'test_subject',
                  'content_type': 'text/plain',
                  'body': 'this is a test body',
                  'attachment': '/file/in/hdfs.txt'}

        mapper = email_mapper.EmailMapper(oozie_node=self.et.getroot(),
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY,
                                          params=params)
        self.assertEqual('test_id', mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        # First two are parameterized in oozie
        self.assertEqual('\'{{ params.subject }}\'', mapper.subject)
        self.assertEqual('\'{{ params.body }}\'', mapper.body)
        # Not parameterized so must replace variable with actual
        self.assertEqual(['user@apache.org', 'user2@apache.org'], mapper.to)
        self.assertEqual(['bob@apache.org'], mapper.cc)
        self.assertEqual(['jeff@apache.org'], mapper.bcc)
        self.assertEqual('text/plain', mapper.content_type)
        self.assertEqual(['/file/in/hdfs.txt'], mapper.attachment)

    def _create_xml_required(self):
        email = ET.Element('email')
        to = ET.SubElement(email, 'to')
        subject = ET.SubElement(email, 'subject')
        body = ET.SubElement(email, 'body')

        to.text = 'bob@apache.org,not.bob@apache.org'
        subject.text = 'test_subject'
        body.text = 'this is a test body'

        self.et = ET.ElementTree(email)

    def test_convert_to_text(self):
        self._create_xml_no_jinja()
        mapper = email_mapper.EmailMapper(oozie_node=self.et.getroot(),
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    def test_convert_to_text_defaults(self):
        self._create_xml_required()
        mapper = email_mapper.EmailMapper(oozie_node=self.et.getroot(),
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        # Throws a syntax error if doesn't parse correctly
        # This time the defaults are from email.tpl
        ast.parse(mapper.convert_to_text())

    @mock.patch('logging.WARNING')
    def test_check_required_nodes(self, log_mock):
        self._create_xml_required()
        req_nodes = ['to', 'subject', 'body']
        mapper = email_mapper.EmailMapper(oozie_node=self.et.getroot(),
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        mapper.check_required_nodes(mapper.oozie_node, req_nodes)
        self.assertEqual(0, log_mock.call_count)

    @mock.patch('logging.WARNING')
    def test_check_required_nodes_missing(self, log_mock):
        self._create_xml_required()
        EXTRA_TAG = 'extra_tag'
        req_nodes = ['to', 'subject', 'body', EXTRA_TAG]
        mapper = email_mapper.EmailMapper(oozie_node=self.et.getroot(),
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        mapper.check_required_nodes(mapper.oozie_node, req_nodes)
        log_mock.assert_called_once_with("Oozie Node {} missing"
                                         " required tag {}.".format(mapper.oozie_node.tag, EXTRA_TAG))

    def test_required_imports(self):
        imps = email_mapper.EmailMapper.required_imports()
        imp_str = '\n'.join(imps)
        ast.parse(imp_str)
