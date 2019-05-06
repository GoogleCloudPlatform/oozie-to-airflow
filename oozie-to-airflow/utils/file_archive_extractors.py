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
from typing import Dict, List
from xml.etree.ElementTree import Element

from utils.el_utils import replace_el_with_var

ARCHIVE_EXTENSIONS = [".zip", ".gz", ".tar.gz", ".tar", ".jar"]


def preprocess_path_to_hdfs(path: str, params: Dict[str, str]):
    if path.startswith("/"):
        return params["nameNode"] + path
    return params["oozie.wf.application.path"] + "/" + path


def split_by_hash_sign(path: str) -> List[str]:
    """
    Checks if the path contains maximum one hash.
    :param path: path to check
    :return: path split into array on the hash
    """
    if "#" in path:
        split_path = path.split("#")
        if len(split_path) > 2:
            raise Exception("There should be maximum one '#' in the path {}".format(path))
        return split_path
    return [path]


def extract_file_paths(oozie_node: Element, params: Dict[str, str], hdfs_path: bool):
    file_nodes: List[Element] = oozie_node.findall("file")
    files = []
    for file_node in file_nodes:
        file_path = replace_el_with_var(file_node.text, params=params, quote=False)
        file_path = preprocess_path_to_hdfs(file_path, params) if hdfs_path else file_path
        files.append(file_path)

    return files


def _check_archive_extensions(oozie_archive_path: str) -> List[str]:
    """
    Checks if the archive path is correct archive path.
    :param oozie_archive_path: path to check
    :return: path split on hash
    """
    split_path = split_by_hash_sign(oozie_archive_path)
    archive_path = split_path[0]
    if not any(archive_path.endswith(extension) for extension in ARCHIVE_EXTENSIONS):
        raise Exception(
            "The path {} cannot be accepted as archive as it does not have one "
            "of the extensions: {}".format(archive_path, ARCHIVE_EXTENSIONS)
        )
    return split_path


def extract_archive_paths(oozie_node: Element, params: Dict[str, str], hdfs_path: bool):
    """ Extracts all archive paths from an Oozie node """
    archives = []
    archive_nodes: List[Element] = oozie_node.findall("archive")
    if archive_nodes:
        for archive_node in archive_nodes:
            archive_path = replace_el_with_var(archive_node.text, params=params, quote=False)
            _check_archive_extensions(archive_path)
            file_path = preprocess_path_to_hdfs(archive_path, params) if hdfs_path else archive_path
            archives.append(file_path)

    return archives
