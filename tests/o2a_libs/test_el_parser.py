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

from o2a.o2a_libs.el_parser import translate
import o2a.o2a_libs.functions as functions


class TestElParser(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "${fs:exists(wf:actionData('hdfs-lookup')['outputPath']/2)}",
                "{{fs.exists(wf.action_data('hdfs-lookup')['outputPath'] / 2)}}",
            ),
            (
                "${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}",
                "{{wf.action_data('shell-node')['my_output'] == 'Hello Oozie'}}",
            ),
            ("${1 > (4/2)}", "{{1 > (4 / 2)}}"),
            ("${4.0 >= 3}", "{{4.0 >= 3}}"),
            ("${100.0 == 100}", "{{100.0 == 100}}"),
            ("${(10*10) ne 100}", "{{(10 * 10) != 100}}"),
            ("${'a' < 'b'}", "{{'a' < 'b'}}"),
            ("${'hip' gt 'hit'}", "{{'hip' > 'hit'}}"),
            ("${4 > 3}", "{{4 > 3}}"),
            ("${1.2E4 + 1.4}", "{{1.2E4 + 1.4}}"),
            ("${10 mod 4}", "{{10 % 4}}"),
            ("${3 div 4}", "{{3 / 4}}"),
            ("${pageContext.request.contextPath}", "{{params['pageContext'].request.contextPath}}"),
            ("${sessionScope.cart.numberOfItems}", "{{params['sessionScope'].cart.numberOfItems}}"),
            ("${param['mycom.productId']}", "{{params['param']['mycom.productId']}}"),
            ('${header["host"]}', """{{params['header']["host"]}}"""),
            ("${departments[deptName]}", "{{params['departments'][params['deptName']]}}"),
            (
                "${requestScope['javax.servlet.forward.servlet_path']}",
                "{{params['requestScope']['javax.servlet.forward.servlet_path']}}",
            ),
            ("#{customer.lName}", "{{params['customer'].lName}}"),
            ("#{customer.calcTotal}", "{{params['customer'].calcTotal}}"),
            ('${wf:conf("jump.to") eq "ssh"}', '{{params["jump.to"] == "ssh"}}'),
            ('${wf:conf("jump.to") eq "parallel"}', '{{params["jump.to"] == "parallel"}}'),
            ('${wf:conf("jump.to") eq "single"}', '{{params["jump.to"] == "single"}}'),
            (
                '${"bool" == "bool" ? print("ok") : print("not ok")}',
                '{{print("ok") if "bool" == "bool" else print("not ok")}}',
            ),
            ('${f(2) ? print("ok") : print("not ok")}', '{{print("ok") if f(2) else print("not ok")}}'),
            ('${!false ? print("ok") : print("not ok")}', '{{print("ok") if !False else print("not ok")}}'),
            ("some pure text ${coord:user()}", "some pure text {{coord.user()}}"),
            (
                "${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}",
                "{{params['nameNode']}}/user/{{params['userName']}}"
                "/{{params['examplesRoot']}}/output-data/{{params['outputDir']}}",
            ),
            (
                "${YEAR}/${MONTH}/${DAY}/${HOUR}",
                "{{params['YEAR']}}/{{params['MONTH']}}/{{params['DAY']}}/{{params['HOUR']}}",
            ),
            ("pure text without any.function wf:function()", "pure text without any.function wf:function()"),
            (
                "${fs:fileSize('/usr/foo/myinputdir') gt 10 * GB}",
                "{{fs.file_size('/usr/foo/myinputdir') > 10 * 1024 ** 3}}",
            ),
            (
                '${hadoop:counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}',
                '{{hadoop.counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}}',
            ),
            ('${hadoop:counters("pig-node")["JOB_GRAPH"]}', '{{hadoop.counters("pig-node")["JOB_GRAPH"]}}'),
            (
                "${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}",
                "{{wf.action_data('shell-node')['my_output'] == 'Hello Oozie'}}",
            ),
            ("${coord:dataOut('output')}", "{{coord.data_out('output')}}"),
            ("${coord:current(0)}", "{{coord.current(0)}}"),
            ('${coord:offset(-42, "MINUTE")}', '{{coord.offset(-42,"MINUTE")}}'),
            (
                "${(wf:actionData('java1')['datelist'] == EXPECTED_DATE_RANGE)}",
                "{{(wf.action_data('java1')['datelist'] == params['EXPECTED_DATE_RANGE'])}}",
            ),
            (
                "${wf:actionData('getDirInfo')['dir.num-files'] gt 23 || "
                "wf:actionData('getDirInfo')['dir.age'] gt 6}",
                "{{wf.action_data('getDirInfo')['dir.num-files'] > 23 or "
                "wf.action_data('getDirInfo')['dir.age'] > 6}}",
            ),
            ("#{'${'}", "{{'${'}}"),
            ("${'${'}", "{{'${'}}"),
            (
                "${function()} literal and #{OtherFunction()}",
                "{{function()}} literal and {{other_function()}}",
            ),
            ("${concat(value, 'xxx')}", "{{params['value'] ~ 'xxx'}}"),
            ("${2 ne 4}", "{{2 != 4}}"),
            ("${2 lt 4}", "{{2 < 4}}"),
            ("${2 le 4}", "{{2 <= 4}}"),
            ("${2 ge 4}", "{{2 >= 4}}"),
            ("${2 && 4}", "{{2 and 4}}"),
            ("${f('x') == true}", "{{f('x') == True}}"),
            ("${f('x') == false}", "{{f('x') == False}}"),
            ("${f('x') == null}", "{{f('x') == None}}"),
            ("${concat('aaa', 'bbb')}", "{{'aaa' ~ 'bbb'}}"),
            ("${wf:id()}", "{{run_id}}"),
            ("${wf:name()}", "{{dag.dag_id}}"),
            ("${wf:user()}", "{{params['userName']}}"),
            ("${trim(' aaabbb ')}", "{{' aaabbb '.strip()}}"),
            ("${trim(value)}", "{{params['value'].strip()}}"),
            ("${timestamp()}", '{{macros.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")}}'),
            ("${urlEncode('%')}", "{{url_encode('%')}}"),
        ]
    )
    def test_translations(self, input_sentence, output_sentence):
        translation = translate(input_sentence)
        output_sentence = output_sentence
        self.assertEqual(translation, output_sentence)

    @parameterized.expand(
        [
            ("Job name is ${'test_job'}", "Job name is {{'test_job'}}", "Job name is test_job", {}),
            ("${2 + 4}", "{{2 + 4}}", "6", {}),
            ("${name == 'name'}", "{{params['name'] == 'name'}}", "True", {"params": dict(name="name")}),
            (
                "${firstNotNull('first', 'second')}",
                "{{functions.first_not_null('first','second')}}",
                "first",
                {"functions": functions},
            ),
            ("${concat('aaa', 'bbb')}", "{{'aaa' ~ 'bbb'}}", "aaabbb", {}),
            ("${urlEncode('%')}", "{{functions.url_encode('%')}}", "%25", {"functions": functions}),
            ("${trim(' aaa ')}", "{{' aaa '.strip()}}", "aaa", {}),
            ("${trim(value)}", "{{params['value'].strip()}}", "aaa", {"params": dict(value="aaa")}),
            ("${wf:id()}", "{{run_id}}", "xxx", {"run_id": "xxx"}),
            ("${wf:name()}", "{{dag.dag_id}}", "xxx", {"dag": MagicMock(dag_id="xxx")}),
            (
                "${wf:lastErrorNode()}",
                "{{functions.wf.last_error_node()}}",
                "",
                {"dag_run": MagicMock(dag_id="test_id"), "functions": functions},
            ),
            (
                "${wf:conf('key')}",
                "{{params['key']}}",
                "value",
                {"params": dict(key="value"), "functions": functions},
            ),
        ]
    )
    def test_rendering(self, input_sentence, translation, output_sentence, kwargs):
        translated = translate(input_sentence, functions_module="functions")

        self.assertEqual(translated, translation)
        render = Template(translated).render(**kwargs)

        self.assertEqual(render, output_sentence)

    @parameterized.expand(
        [
            (
                "${wf:lastErrorNode()}",
                "{{functions.wf.last_error_node()}}",
                {"dag_run": None, "functions": functions},
            )
        ]
    )
    def test_error_rendering(self, input_sentence, translation, kwargs):
        translated = translate(input_sentence, functions_module="functions")

        self.assertEqual(translated, translation)

        with self.assertRaises(AirflowException):
            Template(translated).render(**kwargs)
