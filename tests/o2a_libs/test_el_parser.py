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
            ("${pageContext.request.contextPath}", "{{JOB_PROPS['pageContext'].request.contextPath}}"),
            ("${sessionScope.cart.numberOfItems}", "{{JOB_PROPS['sessionScope'].cart.numberOfItems}}"),
            ("${param['mycom.productId']}", "{{JOB_PROPS['param']['mycom.productId']}}"),
            ('${header["host"]}', """{{JOB_PROPS['header']["host"]}}"""),
            ("${departments[deptName]}", "{{JOB_PROPS['departments'][JOB_PROPS['deptName']]}}"),
            (
                "${requestScope['javax.servlet.forward.servlet_path']}",
                "{{JOB_PROPS['requestScope']['javax.servlet.forward.servlet_path']}}",
            ),
            ("#{customer.lName}", "{{JOB_PROPS['customer'].lName}}"),
            ("#{customer.calcTotal}", "{{JOB_PROPS['customer'].calcTotal}}"),
            ('${wf:conf("jump.to") eq "ssh"}', '{{wf_conf("jump.to") == "ssh"}}'),
            ('${wf:conf("jump.to") eq "parallel"}', '{{wf_conf("jump.to") == "parallel"}}'),
            ('${wf:conf("jump.to") eq "single"}', '{{wf_conf("jump.to") == "single"}}'),
            (
                '${"bool" == "bool" ? print("ok") : print("not ok")}',
                '{{print("ok") if "bool" == "bool" else print("not ok")}}',
            ),
            ('${f(2) ? print("ok") : print("not ok")}', '{{print("ok") if f(2) else print("not ok")}}'),
            ('${!false ? print("ok") : print("not ok")}', '{{print("ok") if !False else print("not ok")}}'),
            ("some pure text ${coord:user()}", "some pure text {{coord_user()}}"),
            # (
            #     "${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}",
            #     "{{JOB_PROPS['nameNode']}}/user/{{JOB_PROPS['params'].props.merged.user.name}}
            #     /{{JOB_PROPS['examplesRoot']}}/output-data/{{JOB_PROPS['outputDir']}}",
            # ),
            # ("${YEAR}/${MONTH}/${DAY}/${HOUR}", "{{JOB_PROPS['YEAR]}}/{{job_properties.month}}/
            # "{{job_properties.day}}/{{job_properties.hour}}"),
            ("pure text without any.function wf:function()", "pure text without any.function wf:function()"),
            (
                "${fs:fileSize('/usr/foo/myinputdir') gt 10 * GB}",
                "{{fs_file_size('/usr/foo/myinputdir') > 10 * 1073741824}}",
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
                "{{(wf_action_data('java1')['datelist'] == JOB_PROPS['EXPECTED_DATE_RANGE'])}}",
            ),
            # (
            #     "${fs:exists(concat(concat(concat(concat(concat(nameNode, '/user/'),"
            #     "wf:user()), '/'), examplesRoot), '/output-data/demo/mr-node')) == 'true'}",
            #     "{{fs_exists(job_properties.name_node ~ '/user/' ~ "
            #     "params.props.merged.user.name ~ '/' ~ examples_root "
            #     "~ '/output-data/demo/mr-node') == 'true'}}",
            # ),
            (
                "${wf:actionData('getDirInfo')['dir.num-files'] gt 23 || "
                "wf:actionData('getDirInfo')['dir.age'] gt 6}",
                "{{wf_action_data('getDirInfo')['dir.num-files'] > 23 or "
                "wf_action_data('getDirInfo')['dir.age'] > 6}}",
            ),
            ("#{'${'}", "{{'${'}}"),
            ("${'${'}", "{{'${'}}"),
            (
                "${function()} literal and #{OtherFunction()}",
                "{{function()}} literal and {{other_function()}}",
            ),
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
            ("${wf:user()}", "{{params.props.merged.user.name}}"),
            ("${trim(' aaabbb ')}", "{{' aaabbb '.strip()}}"),
            ("${trim(value)}", "{{JOB_PROPS['value'].strip()}}"),
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
            ("Job name is ${'test_job'}", "Job name is test_job", {}),
            ("${2 + 4}", "6", {}),
            ("${name == 'name'}", "True", {"JOB_PROPS": dict(name="name")}),
            ("${firstNotNull('first', 'second')}", "first", {"functions": functions}),
            ("${concat('aaa', 'bbb')}", "aaabbb", {}),
            ("${urlEncode('%')}", "%25", {"functions": functions}),
            ("${trim('  aaa  ')}", "aaa", {}),
            ("${trim(value)}", "aaa", {"JOB_PROPS": dict(value="aaa")}),
            ("${wf:id()}", "xxx", {"run_id": "xxx"}),
            ("${wf:name()}", "xxx", {"dag": Dag(dag_id="xxx")}),
            # ("${wf:conf('key')}", "value", {"conf": dict(key="value"), "functions": functions}),
        ]
    )
    def test_rendering(self, input_sentence, output_sentence, kwargs):
        translation = translate(input_sentence, functions_module="functions")

        render = Template(translation).render(**kwargs)

        self.assertEqual(render, output_sentence)
