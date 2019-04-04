import os
from typing import Set
from xml.etree.ElementTree import Element

import jinja2
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.action_mapper import ActionMapper
from mappers.file_archive_mixins import FileMixin, ArchiveMixin
from mappers.prepare_mixin import PrepareMixin
from utils import el_utils, xml_utils


class PigMapper(ActionMapper, PrepareMixin, ArchiveMixin, FileMixin):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        task_id: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params=None,
        template_file_name: str = "pig.tpl",
        **kwargs,
    ):
        ArchiveMixin.__init__(self, params=params)
        FileMixin.__init__(self, params=params)
        ActionMapper.__init__(
            self, oozie_node=oozie_node, task_id=task_id, trigger_rule=trigger_rule, **kwargs
        )
        if params is None:
            params = dict()
        self.template = template_file_name
        self.params = params
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self.properties = {}
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        res_man_text = self.oozie_node.find("resource-manager").text
        name_node_text = self.oozie_node.find("name-node").text
        script = self.oozie_node.find("script").text
        self.resource_manager = el_utils.replace_el_with_var(res_man_text, params=self.params, quote=False)
        self.name_node = el_utils.replace_el_with_var(name_node_text, params=self.params, quote=False)
        self.script_file_name = el_utils.replace_el_with_var(script, params=self.params, quote=False)
        self._parse_config()
        self._parse_params()

    def _parse_params(self):
        param_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, "param")
        if param_nodes:
            self.params_dict = {}
            for node in param_nodes:
                param = el_utils.replace_el_with_var(node.text, params=self.params, quote=False)
                key, value = param.split("=")
                self.params_dict[key] = value

    def _parse_config(self):
        config = self.oozie_node.find("configuration")
        if config:
            property_nodes = xml_utils.find_nodes_by_tag(config, "property")
            if property_nodes:
                for node in property_nodes:
                    name = node.find("name").text
                    value = el_utils.replace_el_with_var(
                        node.find("value").text, params=self.params, quote=False
                    )
                    self.properties[name] = value

    def convert_to_text(self) -> str:
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/"))
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(self.template)
        prepare_command = self.get_prepare_command(self.oozie_node, self.params)
        return template.render(prepare_command=prepare_command, **self.__dict__)

    def _add_symlinks(self, destination_pig_file):
        destination_pig_file.write("set mapred.create.symlink yes;\n")
        if self.files:
            destination_pig_file.write("set mapred.cache.file {};\n".format(self.hdfs_files))
        if self.archives:
            destination_pig_file.write("set mapred.cache.archives {};\n".format(self.hdfs_archives))

    def copy_extra_assets(self, input_directory_path: str, output_directory_path: str):
        self._validate_paths(input_directory_path, output_directory_path)
        source_pig_file_path = os.path.join(input_directory_path, self.script_file_name)
        destination_pig_file_path = os.path.join(output_directory_path, self.script_file_name)
        self._copy_pig_script_with_path_injection(destination_pig_file_path, source_pig_file_path)

    def _copy_pig_script_with_path_injection(self, destination_pig_file_path, source_pig_file_path):
        with open(destination_pig_file_path, "w") as destination_pig_file:
            with open(source_pig_file_path, "r") as source_pig_file:
                pig_script = source_pig_file.read()
                if self.files or self.archives:
                    self._add_symlinks(destination_pig_file)
                destination_pig_file.write(pig_script)

    @staticmethod
    def _validate_paths(input_directory_path, output_directory_path):
        if not input_directory_path:
            raise Exception("The input_directory_path should be set and is {}".format(input_directory_path))
        if not output_directory_path:
            raise Exception("The output_directory_path should be set and is {}".format(output_directory_path))

    def convert_to_airflow_op(self):
        pass

    @staticmethod
    def required_imports() -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
