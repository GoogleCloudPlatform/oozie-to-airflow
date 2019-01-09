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
import io
import unittest

import oozie_converter
from converter import parsed_node
from definitions import TPL_PATH
import jinja2
from mappers import dummy_mapper


class TestOozieConverter(unittest.TestCase):

    def test_parse_args_input(self):
        FILE_NAME = '/tmp/does.not.exist'
        args = oozie_converter.parse_args(['-i', FILE_NAME])
        self.assertEquals(args.input, FILE_NAME)

    def test_parse_args_prop_file(self):
        FILE_NAME = '/tmp/does.not.exist'
        PROP_FILE_NAME = '/tmp/job.properties'
        args = oozie_converter.parse_args(['-i', FILE_NAME,
                                           '-p', PROP_FILE_NAME])
        self.assertEquals(args.input, FILE_NAME)
        self.assertEquals(args.properties, PROP_FILE_NAME)

    def test_parse_args_output_file(self):
        FILE_NAME = '/tmp/does.not.exist'
        OUT_FILE_NAME = '/tmp/out.py'
        args = oozie_converter.parse_args(['-i', FILE_NAME,
                                           '-o', OUT_FILE_NAME])
        self.assertEquals(args.input, FILE_NAME)
        self.assertEquals(args.output, OUT_FILE_NAME)

    def test_parse_args_user(self):
        FILE_NAME = '/tmp/does.not.exist'
        USER = 'oozie_test'
        args = oozie_converter.parse_args(['-i', FILE_NAME,
                                           '-u', USER])
        self.assertEquals(args.input, FILE_NAME)
        self.assertEquals(args.user, USER)

    def test_write_operators(self):
        node = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task1'))
        ops = {'task1': node}

        fp = io.StringIO()
        oozie_converter.write_operators(fp, ops, indent=0)
        fp.seek(0)

        self.assertEqual(node.operator.convert_to_text(), fp.read())

    def test_write_relations(self):
        relations = ['task1.set_downstream(task2)', 'task2.set_upstream(task1)']

        fp = io.StringIO()
        oozie_converter.write_relations(fp, relations, indent=0)
        fp.seek(0)

        expected = '\n'.join(relations) + '\n'
        self.assertEqual(expected, fp.read())

    def test_write_dependencies(self):
        depends = ['import airflow', 'from jaws import thriller']

        fp = io.StringIO()
        oozie_converter.write_relations(fp, depends, indent=0)
        fp.seek(0)

        expected = '\n'.join(depends) + '\n'
        self.assertEqual(expected, fp.read())

    def test_write_dag_header(self):
        DAG_NAME = 'dag_name'
        TEMPLATE = 'dag.tpl'

        fp = io.StringIO()
        oozie_converter.write_dag_header(fp, DAG_NAME, template=TEMPLATE)
        fp.seek(0)

        template_loader = jinja2.FileSystemLoader(searchpath=TPL_PATH)
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(TEMPLATE)
        expected = template.render(dag_name=DAG_NAME)

        self.assertEqual(expected, fp.read())

