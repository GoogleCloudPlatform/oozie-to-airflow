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
"""Tests for all EL to Jinja parser"""

import unittest
from unittest.mock import MagicMock

from parameterized import parameterized
from jinja2 import Template

from airflow import AirflowException

import o2a.o2a_libs.functions as functions
from o2a.o2a_libs.el_parser import translate


class TestElParser(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "{{fs.exists(wf.action_data('hdfs-lookup')['outputPath'] / 2)}}",
                "${fs:exists(wf:actionData('hdfs-lookup')['outputPath']/2)}",
            ),
            (
                "{{wf.action_data('shell-node')['my_output'] == 'Hello Oozie'}}",
                "${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}",
            ),
            ("{{1 > (4 / 2)}}", "${1 > (4/2)}"),
            ("{{4.0 >= 3}}", "${4.0 >= 3}"),
            ("{{100.0 == 100}}", "${100.0 == 100}"),
            ("{{(10 * 10) != 100}}", "${(10*10) ne 100}"),
            ("{{'a' < 'b'}}", "${'a' < 'b'}"),
            ("{{'hip' > 'hit'}}", "${'hip' gt 'hit'}"),
            ("{{4 > 3}}", "${4 > 3}"),
            ("{{1.2E4 + 1.4}}", "${1.2E4 + 1.4}"),
            ("{{10 % 4}}", "${10 mod 4}"),
            ("{{3 / 4}}", "${3 div 4}"),
            ("{{pageContext.request.contextPath}}", "${pageContext.request.contextPath}"),
            ("{{sessionScope.cart.numberOfItems}}", "${sessionScope.cart.numberOfItems}"),
            ("{{param['mycom.productId']}}", "${param['mycom.productId']}"),
            ('{{header["host"]}}', '${header["host"]}'),
            ("{{departments[deptName]}}", "${departments[deptName]}"),
            (
                "{{requestScope['javax.servlet.forward.servlet_path']}}",
                "${requestScope['javax.servlet.forward.servlet_path']}",
            ),
            ("{{customer.lName}}", "#{customer.lName}"),
            ("{{customer.calcTotal}}", "#{customer.calcTotal}"),
            ('{{params["jump.to"] == "ssh"}}', '${wf:conf("jump.to") eq "ssh"}'),
            ('{{params["jump.to"] == "parallel"}}', '${wf:conf("jump.to") eq "parallel"}'),
            ('{{params["jump.to"] == "single"}}', '${wf:conf("jump.to") eq "single"}'),
            (
                '{{print("ok") if "bool" == "bool" else print("not ok")}}',
                '${"bool" == "bool" ? print("ok") : print("not ok")}',
            ),
            ('{{print("ok") if f(2) else print("not ok")}}', '${f(2) ? print("ok") : print("not ok")}'),
            ('{{print("ok") if !False else print("not ok")}}', '${!false ? print("ok") : print("not ok")}'),
            ("some pure text {{coord.user()}}", "some pure text ${coord:user()}"),
            (
                "{{nameNode}}/user/{{wf.user()}}/{{examplesRoot}}/output-data/{{outputDir}}",
                "${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}",
            ),
            ("{{YEAR}}/{{MONTH}}/{{DAY}}/{{HOUR}}", "${YEAR}/${MONTH}/${DAY}/${HOUR}"),
            ("pure text without any.function wf:function()", "pure text without any.function wf:function()"),
            (
                "{{fs.file_size('/usr/foo/myinputdir') > 10 * 1024 ** 3}}",
                "${fs:fileSize('/usr/foo/myinputdir') gt 10 * GB}",
            ),
            (
                '{{hadoop.counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}}',
                '${hadoop:counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}',
            ),
            ('{{hadoop.counters("pig-node")["JOB_GRAPH"]}}', '${hadoop:counters("pig-node")["JOB_GRAPH"]}'),
            (
                "{{wf.action_data('shell-node')['my_output'] == 'Hello Oozie'}}",
                "${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}",
            ),
            ("{{coord.data_out('output')}}", "${coord:dataOut('output')}"),
            ("{{coord.current(0)}}", "${coord:current(0)}"),
            ('{{coord.offset(-42,"MINUTE")}}', '${coord:offset(-42, "MINUTE")}'),
            (
                "{{(wf.action_data('java1')['datelist'] == EXPECTED_DATE_RANGE)}}",
                "${(wf:actionData('java1')['datelist'] == EXPECTED_DATE_RANGE)}",
            ),
            (
                "{{wf.action_data('getDirInfo')['dir.num-files'] > 23 or"
                " wf.action_data('getDirInfo')['dir.age'] > 6}}",
                "${wf:actionData('getDirInfo')['dir.num-files'] gt 23 || "
                "wf:actionData('getDirInfo')['dir.age'] gt 6}",
            ),
            ("{{'${'}}", "#{'${'}"),
            ("{{'${'}}", "${'${'}"),
            (
                "{{function()}} literal and {{other_function()}}",
                "${function()} literal and #{OtherFunction()}",
            ),
            ("{{value ~ 'xxx'}}", "${concat(value, 'xxx')}"),
            ("{{2 != 4}}", "${2 ne 4}"),
            ("{{2 < 4}}", "${2 lt 4}"),
            ("{{2 <= 4}}", "${2 le 4}"),
            ("{{2 >= 4}}", "${2 ge 4}"),
            ("{{2 and 4}}", "${2 && 4}"),
            ("{{f('x') == True}}", "${f('x') == true}"),
            ("{{f('x') == False}}", "${f('x') == false}"),
            ("{{f('x') == None}}", "${f('x') == null}"),
            ("{{'aaa' ~ 'bbb'}}", "${concat('aaa', 'bbb')}"),
            ("{{run_id}}", "${wf:id()}"),
            ("{{dag.dag_id}}", "${wf:name()}"),
            ("{{wf.user()}}", "${wf:user()}"),
            ("{{' aaabbb '.strip()}}", "${trim(' aaabbb ')}"),
            ("{{value.strip()}}", "${trim(value)}"),
            ('{{macros.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")}}', "${timestamp()}"),
            ("{{url_encode('%')}}", "${urlEncode('%')}"),
            ("test_dir/test2.zip#test_zip_dir", "test_dir/test2.zip#test_zip_dir"),
            ("/path/with/two/els/{{var1}}/{{var2}}.tar", "/path/with/two/els/${var1}/${var2}.tar"),
        ]
    )
    def test_translations(self, output_sentence, input_sentence):
        translation = translate(input_sentence, functions_module="")
        output_sentence = output_sentence
        self.assertEqual(translation, output_sentence)

    @parameterized.expand(
        [
            ("Job name is ${'test_job'}", "Job name is {{'test_job'}}", "Job name is test_job", {}),
            ("${2 + 4}", "{{2 + 4}}", "6", {}),
            ("${name == 'name'}", "{{name == 'name'}}", "True", dict(name="name")),
            (
                "${firstNotNull('first', 'second')}",
                "{{functions.first_not_null('first','second')}}",
                "first",
                {"functions": functions},
            ),
            ("${concat('aaa', 'bbb')}", "{{'aaa' ~ 'bbb'}}", "aaabbb", {}),
            ("${urlEncode('%')}", "{{functions.url_encode('%')}}", "%25", {"functions": functions}),
            ("${trim(' aaa ')}", "{{' aaa '.strip()}}", "aaa", {}),
            ("${trim(value)}", "{{value.strip()}}", "aaa", dict(value="aaa")),
            ("${trim(value)}", "{{value.strip()}}", "aaa", dict(value="aaa")),
            ("${wf:id()}", "{{run_id}}", "xxx", {"run_id": "xxx"}),
            (
                "${wf:user()}",
                "{{functions.wf.user()}}",
                "user_name",
                {"functions": functions, "dag": MagicMock(tasks=[MagicMock(owner="user_name")])},
            ),
            ("${wf:name()}", "{{dag.dag_id}}", "xxx", {"dag": MagicMock(dag_id="xxx")}),
            (
                "${wf:conf('key')}",
                "{{params['key']}}",
                "value",
                {"params": {"key": "value"}, "functions": functions},
            ),
        ]
    )
    def test_rendering(self, input_sentence, translation, output_sentence, kwargs):
        translated = translate(input_sentence, functions_module="functions")

        self.assertEqual(translation, translated)
        render = Template(translated).render(**kwargs)

        self.assertEqual(output_sentence, render)

    @parameterized.expand(
        [
            (
                "{{functions.wf.last_error_node()}}",
                "${wf:lastErrorNode()}",
                {"dag_run": None, "functions": functions},
            ),
            ("{{functions.wf.user()}}", "${wf:user()}", {"dag": None, "functions": functions}),
            (
                "{{functions.wf.user()}}",
                "${wf:user()}",
                {
                    "dag": MagicMock(tasks=[MagicMock(owner="name1"), MagicMock(owner="name2")]),
                    "functions": functions,
                },
            ),
        ]
    )
    def test_error_rendering(self, translation, input_sentence, kwargs):
        translated = translate(input_sentence, functions_module="functions")

        self.assertEqual(translated, translation)

        with self.assertRaises(AirflowException):
            Template(translated).render(**kwargs)
