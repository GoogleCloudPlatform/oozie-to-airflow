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
"""Tests for all EL to Jinjia parser"""

import unittest
from collections import namedtuple

from parameterized import parameterized

from jinja2 import Template

from o2a.o2a_libs.el_parser import translate
import o2a.o2a_libs.functions as functions


Dag = namedtuple("Dag", ("dag_id"))


class TestElParser(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "${fs:exists(wf:actionData('hdfs-lookup')['outputPath']/2)}",
                "{{fs_exists(wf_action_data('hdfs-lookup')['outputPath'] / 2)}}",
            ),
            (
                "${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}",
                "{{wf_action_data('shell-node')['my_output'] == 'Hello Oozie'}}",
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
            ("${pageContext.request.contextPath}", "{{page_context.request.context_path}}"),
            ("${sessionScope.cart.numberOfItems}", "{{session_scope.cart.number_of_items}}"),
            ("${param['mycom.productId']}", "{{param['mycom.productId']}}"),
            ('${header["host"]}', '{{header["host"]}}'),
            ("${departments[deptName]}", "{{departments[dept_name]}}"),
            (
                "${requestScope['javax.servlet.forward.servlet_path']}",
                "{{request_scope['javax.servlet.forward.servlet_path']}}",
            ),
            ("#{customer.lName}", "{{customer.l_name}}"),
            ("#{customer.calcTotal}", "{{customer.calc_total}}"),
            ('${wf:conf("jump.to") eq "ssh"}', '{{conf == "ssh"}}'),
            ('${wf:conf("jump.to") eq "parallel"}', '{{conf == "parallel"}}'),
            ('${wf:conf("jump.to") eq "single"}', '{{conf == "single"}}'),
            (
                '${"bool" == bool ? print("ok") : print("not ok")}',
                '{{print("ok") if "bool" == bool else print("not ok")}}',
            ),
            ('${f(x) ? print("ok") : print("not ok")}', '{{print("ok") if f(x) else print("not ok")}}'),
            ('${!false ? print("ok") : print("not ok")}', '{{print("ok") if !False else print("not ok")}}'),
            ("some pure text ${coord:user()}", "some pure text {{coord_user()}}"),
            (
                "${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}",
                "{{name_node}}/user/{{params.props.merged.user.name}}/{{examples_root}}/output-data/{{output_dir}}",  # noqa  # pylint: disable=line-too-long
            ),
            ("${YEAR}/${MONTH}/${DAY}/${HOUR}", "{{year}}/{{month}}/{{day}}/{{hour}}"),
            ("pure text without any.function wf:function()", "pure text without any.function wf:function()"),
            (
                "${fs:fileSize('/usr/foo/myinputdir') gt 10 * GB}",
                "{{fs_file_size('/usr/foo/myinputdir') > 10 * gb}}",
            ),
            (
                '${hadoop:counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}',
                '{{hadoop_counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}}',
            ),
            ('${hadoop:counters("pig-node")["JOB_GRAPH"]}', '{{hadoop_counters("pig-node")["JOB_GRAPH"]}}'),
            (
                "${wf:actionData('shell-node')['my_output'] eq 'Hello Oozie'}",
                "{{wf_action_data('shell-node')['my_output'] == 'Hello Oozie'}}",
            ),
            ("${coord:dataOut('output')}", "{{coord_data_out('output')}}"),
            ("${coord:current(0)}", "{{coord_current(0)}}"),
            ('${coord:offset(-42, "MINUTE")}', '{{coord_offset(-42,"MINUTE")}}'),
            (
                "${(wf:actionData('java1')['datelist'] == EXPECTED_DATE_RANGE)}",
                "{{(wf_action_data('java1')['datelist'] == expected_date_range)}}",
            ),
            (
                '${(hadoop:counters("mr-node")[RECORDS][MAP_IN] == hadoop:counters("mr-node")'
                '[RECORDS][MAP_OUT]) and (hadoop:counters("mr-node")[RECORDS][REDUCE_IN] == '
                'hadoop:counters("mr-node")[RECORDS][REDUCE_OUT]) and (hadoop:counters("mr-node")'
                "[RECORDS][GROUPS] gt 0)}",
                '{{(hadoop_counters("mr-node")[records][map_in] == hadoop_counters("mr-node")'
                '[records][map_out]) and (hadoop_counters("mr-node")[records][reduce_in] == '
                'hadoop_counters("mr-node")[records][reduce_out]) and (hadoop_counters("mr-node")'
                "[records][groups] > 0)}}",
            ),
            (
                "${fs:exists(concat(concat(concat(concat(concat(nameNode, '/user/'),"
                "wf:user()), '/'), examplesRoot), '/output-data/demo/mr-node')) == 'true'}",
                "{{fs_exists(concat(concat(concat(concat(concat(name_node,'/user/'),"
                "params.props.merged.user.name),'/'),examples_root),'/output-data/demo/mr-node')) == 'true'}}",  # noqa  # pylint: disable=line-too-long
            ),
            (
                "${wf:actionData('getDirInfo')['dir.num-files'] gt 23 || "
                "wf:actionData('getDirInfo')['dir.age'] gt 6}",
                "{{wf_action_data('getDirInfo')['dir.num-files'] > 23 or "
                "wf_action_data('getDirInfo')['dir.age'] > 6}}",
            ),
            ("#{'${'}", "{{'${'}}"),
            ("${'${'}", "{{'${'}}"),
            ("${function()} literal and #{OtherFunction}", "{{function()}} literal and {{other_function}}"),
            ("${2 ne 4}", "{{2 != 4}}"),
            ("${2 lt 4}", "{{2 < 4}}"),
            ("${2 le 4}", "{{2 <= 4}}"),
            ("${2 ge 4}", "{{2 >= 4}}"),
            ("${2 && 4}", "{{2 and 4}}"),
            ("${f(x) == true}", "{{f(x) == True}}"),
            ("${f(x) == false}", "{{f(x) == False}}"),
            ("${f(x) == null}", "{{f(x) == None}}"),
        ]
    )
    def test_translations(self, input_sentence, output_sentence):
        translation = translate(input_sentence)
        output_sentence = output_sentence
        self.assertEqual(translation, output_sentence)

    @parameterized.expand(
        [
            ("Job name is ${'test_job'}", "Job name is test_job", {}),
            ("${2 + 4}", "6", {}),
            ("${name == 'name'}", "True", {"name": "name"}),
            # ("${concat('aaa', 'bbb')}", "aaabbb", {"functions": functions}),
            # ("${trim('  aaa  ')}", "aaa", {"functions": functions}),
            ("${wf:id()}", "xxx", {"run_id": "xxx", "functions": functions}),
            ("${wf:name()}", "xxx", {"dag": Dag(dag_id="xxx"), "functions": functions}),
        ]
    )
    def test_rendering(self, input_sentence, output_sentence, kwargs):
        translation = translate(input_sentence, functions_module="functions")
        render = Template(translation).render(**kwargs)

        self.assertEqual(render, output_sentence)
