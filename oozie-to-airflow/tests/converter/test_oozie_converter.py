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
from converter.converter import OozieConverter
from converter.mappers import CONTROL_MAP, ACTION_MAP
from definitions import TPL_PATH
import jinja2
from mappers import dummy_mapper
from tests.utils.test_paths import EXAMPLE_DEMO_PATH


class TestOozieConverter(unittest.TestCase):
    def setUp(self):
        self.converter = OozieConverter(
            dag_name="test_dag",
            input_directory_path=EXAMPLE_DEMO_PATH,
            output_directory_path="/tmp",
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
        )

    def test_parse_args_input_output_file(self):
        input_dir = "/tmp/does.not.exist/"
        output_dir = "/tmp/out/"
        args = oozie_converter.parse_args(["-i", input_dir, "-o", output_dir])
        self.assertEqual(args.input_directory_path, input_dir)
        self.assertEqual(args.output_directory_path, output_dir)

    def test_parse_args_user(self):
        input_dir = "/tmp/does.not.exist"
        output_dir = "/tmp/out/"
        user = "oozie_test"
        args = oozie_converter.parse_args(["-i", input_dir, "-o", output_dir, "-u", user])
        self.assertEqual(args.user, user)

    def test_write_operators(self):
        node = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=None, task_id="task1"))
        ops = {"task1": node}

        fp = io.StringIO()
        self.converter.write_operators(fp=fp, operators=ops, indent=0)
        fp.seek(0)

        self.assertEqual(node.operator.convert_to_text(), fp.read())

    def test_write_relations(self):
        relations = ["task1.set_downstream(task2)", "task2.set_upstream(task1)"]

        fp = io.StringIO()
        OozieConverter.write_relations(fp, relations, indent=0)
        fp.seek(0)

        expected = "\n".join(relations) + "\n"
        self.assertEqual(expected, fp.read())

    def test_write_dependencies(self):
        depends = ["import airflow", "from jaws import thriller"]

        fp = io.StringIO()
        OozieConverter.write_relations(fp, depends, indent=0)
        fp.seek(0)

        expected = "\n".join(depends) + "\n"
        self.assertEqual(expected, fp.read())

    def test_write_dag_header(self):
        dag_name = "dag_name"
        template = "dag.tpl"

        fp = io.StringIO()
        OozieConverter.write_dag_header(
            fp, dag_name, template=template, schedule_interval=1, start_days_ago=1
        )
        fp.seek(0)

        template_loader = jinja2.FileSystemLoader(searchpath=TPL_PATH)
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(template)
        expected = template.render(dag_name=dag_name, schedule_interval=1, start_days_ago=1)

        self.assertEqual(expected, fp.read())
