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
from typing import Dict


class HdfsPathProcessor:
    def __init__(self, params: Dict[str, str]):
        self.params = params

    @staticmethod
    def split_by_hash_sign(path: str) -> [str]:
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

    @staticmethod
    def check_path_for_comma(path: str) -> None:
        """
        Raises exception if path has comma
        :param path: path to check
        :return: None
        """
        if "," in path:
            raise Exception("There should not be ',' in the path {}".format(path))

    def preprocess_path_to_hdfs(self, path: str):
        if path.startswith("/"):
            return self.params["nameNode"] + path
        else:
            return self.params["oozie.wf.application.path"] + "/" + path


class FileMixin:
    def __init__(self, params: Dict[str, str]):
        self.files = None
        self.hdfs_files = None
        self.file_path_processor = HdfsPathProcessor(params=params)

    def add_file(self, oozie_file_path: str) -> None:
        """
        Adds file to the list of files for this action.

        :param oozie_file_path: oozie file path to add
        :return: None
        """
        self.file_path_processor.check_path_for_comma(oozie_file_path)
        self.file_path_processor.split_by_hash_sign(oozie_file_path)
        self.files = "{},{}".format(self.files, oozie_file_path) if self.files else oozie_file_path
        self.hdfs_files = (
            self.hdfs_files + "," + self.file_path_processor.preprocess_path_to_hdfs(oozie_file_path)
            if self.hdfs_files
            else self.file_path_processor.preprocess_path_to_hdfs(oozie_file_path)
        )


class ArchiveMixin:

    ALLOWED_EXTENSIONS = [".zip", ".gz", ".tar.gz", ".tar", ".jar"]

    def __init__(self, params: Dict[str, str]):
        self.archives = None
        self.hdfs_archives = None
        self.archive_path_processor = HdfsPathProcessor(params=params)

    def _check_archive_extensions(self, oozie_archive_path: str) -> [str]:
        """
        Checks if the archive path is correct archive path.
        :param oozie_archive_path: path to check
        :return: path split on hash
        """
        split_path = self.archive_path_processor.split_by_hash_sign(oozie_archive_path)
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
        self.archives = (
            "{},{}".format(self.archives, oozie_archive_path) if self.archives else oozie_archive_path
        )
        self.hdfs_archives = (
            self.hdfs_archives + "," + self.archive_path_processor.preprocess_path_to_hdfs(oozie_archive_path)
            if self.hdfs_archives
            else self.archive_path_processor.preprocess_path_to_hdfs(oozie_archive_path)
        )
