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
import unittest
from xml.etree import ElementTree as ET

from mappers import prepare_mixin


class TestPrepareMixin(unittest.TestCase):
    delete_path1 = '/examples/output-data/demo/pig-node'
    delete_path2 = '/examples/output-data/demo/pig-node2'
    mkdir_path1 = '/examples/input-data/demo/pig-node'
    mkdir_path2 = '/examples/input-data/demo/pig-node2'

    def setUp(self):
        pig = ET.Element('pig')
        name_node = ET.SubElement(pig, 'name-node')
        self.et = ET.ElementTree(pig)
        name_node.text = 'hdfs://localhost:8020'

    def test_with_prepare(self):
        pig = self.et.getroot()
        prepare = ET.SubElement(pig, 'prepare')
        delete1 = ET.SubElement(prepare, 'delete')
        delete2 = ET.SubElement(prepare, 'delete')
        mkdir1 = ET.SubElement(prepare, 'mkdir')
        mkdir2 = ET.SubElement(prepare, 'mkdir')
        delete1.set('path', '${nameNode}%s' % self.delete_path1)
        delete2.set('path', '${nameNode}%s' % self.delete_path2)
        mkdir1.set('path', '${nameNode}%s' % self.mkdir_path1)
        mkdir2.set('path', '${nameNode}%s' % self.mkdir_path2)
        cluster = 'my-cluster'
        region = 'europe-west3'
        prepare = prepare_mixin.PrepareMixin().get_prepare_command(oozie_node=self.et.getroot(),
                                                                   params={'dataproc_cluster': cluster,
                                                                           'gcp_region': region})
        self.assertEqual("$DAGS_FOLDER/../data/prepare.sh -c {0} -r {1} -d \"{2} {3}\" -m \"{4} {5}\""
                         .format(cluster, region, self.delete_path1, self.delete_path2, self.mkdir_path1,
                                 self.mkdir_path2), prepare)

    def test_no_prepare(self):
        cluster = 'my-cluster'
        region = 'europe-west3'
        prepare = prepare_mixin.PrepareMixin().get_prepare_command(oozie_node=self.et.getroot(),
                                                                   params={'dataproc_cluster': cluster,
                                                                           'gcp_region': region})
        self.assertEqual("", prepare)

