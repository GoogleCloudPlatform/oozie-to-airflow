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

from parameterized import parameterized

from o2a.o2a_libs.el_parser import translate


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
            ('${wf:conf("jump.to") eq "ssh"}', '{{wf_conf("jump.to") == "ssh"}}'),
            ('${wf:conf("jump.to") eq "parallel"}', '{{wf_conf("jump.to") == "parallel"}}'),
            ('${wf:conf("jump.to") eq "single"}', '{{wf_conf("jump.to") == "single"}}'),
            (
                '${"bool" == bool ? print("ok") : print("not ok")}',
                '{{print("ok") if "bool" == bool else print("not ok")}}',
            ),
            ("some pure text ${coord:user()}", "some pure text{{coord_user()}}"),
            (
                "${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}",
                "{{name_node}}/user/{{wf_user()}}/{{examples_root}}/output-data/{{output_dir}}",
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
                "wf_user()),'/'),examples_root),'/output-data/demo/mr-node')) == 'true'}}",
            ),
            (
                "${wf:actionData('getDirInfo')['dir.num-files'] gt 23 || "
                "wf:actionData('getDirInfo')['dir.age'] gt 6}",
                "{{wf_action_data('getDirInfo')['dir.num-files'] > 23 or "
                "wf_action_data('getDirInfo')['dir.age'] > 6}}",
            ),
            ("#{'${'}", "{{'${'}}"),
            ("${'${'}", "{{'${'}}"),
            ("${function()} literal and #{OtherFunction}", "{{function()}} literal and{{other_function}}"),
        ]
    )
    def test_translations(self, input_sentence, output_sentence):
        # TODO: for now the translation is non-determinist, difference in number of space
        translation = translate(input_sentence).replace(" ", "")
        output_sentence = output_sentence.replace(" ", "")
        self.assertEqual(translation, output_sentence)
