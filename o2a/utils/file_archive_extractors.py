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
"""Mapper for File and Archive nodes"""
from typing import List
from xml.etree.ElementTree import Element

from o2a.o2a_libs.src.o2a_lib import el_parser
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet


class HdfsPathProcessor:
    def __init__(self, props: PropertySet):
        self.props = props

    @staticmethod
    def check_path_for_comma(path: str) -> None:
        """
        Raises exception if path has comma
        :param path: path to check
        :return: None
        """
        if "," in path:
            raise Exception(f"There should not be ',' in the path {path}")

    def preprocess_path_to_hdfs(self, path: str):
        if path.startswith("/"):
            return self.props.merged["nameNode"] + path
        return self.props.merged["oozie.wf.application.path"] + "/" + path


def split_by_hash_sign(path: str) -> List[str]:
    """
    Checks if the path contains maximum one hash.
    :param path: path to check
    :return: path split into array on the hash
    """
    if "#" in path:
        split_path = path.split("#")
        if len(split_path) > 2:
            raise Exception(f"There should be maximum one '#' in the path {path}")
        return split_path
    return [path]


class FileExtractor:
    """ Extracts all file paths from an Oozie node """

    def __init__(self, oozie_node: Element, props: PropertySet):
        self.files: List[str] = []
        self.hdfs_files: List[str] = []
        self.file_path_processor = HdfsPathProcessor(props=props)
        self.oozie_node = oozie_node
        self.props = props

    def parse_node(self):
        file_nodes: List[Element] = self.oozie_node.findall("file")

        for file_node in file_nodes:
            file_path = el_parser.translate(file_node.text)
            self.add_file(file_path)

        return self.files, self.hdfs_files

    def add_file(self, oozie_file_path: str) -> None:
        """
        Adds file to the list of files for this action.

        :param oozie_file_path: oozie file path to add
        :return: None
        """
        self.file_path_processor.check_path_for_comma(oozie_file_path)
        split_by_hash_sign(oozie_file_path)
        self.files.append(oozie_file_path)
        self.hdfs_files.append(self.file_path_processor.preprocess_path_to_hdfs(oozie_file_path))


class ArchiveExtractor:
    """ Extracts all archive paths from an Oozie node """

    ALLOWED_EXTENSIONS = [".zip", ".gz", ".tar.gz", ".tar", ".jar"]

    def __init__(self, oozie_node: Element, props: PropertySet):
        self.archives: List[str] = []
        self.hdfs_archives: List[str] = []
        self.archive_path_processor = HdfsPathProcessor(props=props)
        self.oozie_node = oozie_node
        self.props = props

    def parse_node(self):
        archive_nodes: List[Element] = self.oozie_node.findall("archive")
        if archive_nodes:
            for archive_node in archive_nodes:
                archive_path = el_parser.translate(archive_node.text)
                self.add_archive(archive_path)
        return self.archives, self.hdfs_archives

    def _check_archive_extensions(self, oozie_archive_path: str) -> List[str]:
        """
        Checks if the archive path is correct archive path.
        :param oozie_archive_path: path to check
        :return: path split on hash
        """
        split_path = split_by_hash_sign(oozie_archive_path)
        archive_path = split_path[0]
        extension_accepted = False
        for extension in self.ALLOWED_EXTENSIONS:
            if archive_path.endswith(extension):
                extension_accepted = True
        if not extension_accepted:
            raise Exception(
                "The path {} cannot be accepted as archive as it does not have one "
                "of the extensions: {}".format(archive_path, self.ALLOWED_EXTENSIONS)
            )
        return split_path

    def add_archive(self, oozie_archive_path: str):
        self.archive_path_processor.check_path_for_comma(oozie_archive_path)
        self._check_archive_extensions(oozie_archive_path)
        self.archives.append(oozie_archive_path)
        self.hdfs_archives.append(self.archive_path_processor.preprocess_path_to_hdfs(oozie_archive_path))
