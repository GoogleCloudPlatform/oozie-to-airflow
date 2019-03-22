import os

import jinja2
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.action_mapper import ActionMapper
from mappers.prepare_mixin import PrepareMixin
from utils import el_utils, xml_utils


class PigMapper(ActionMapper, PrepareMixin):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(
        self, oozie_node, task_id, trigger_rule=TriggerRule.ALL_SUCCESS, params=None, template="pig.tpl"
    ):
        ActionMapper.__init__(self, oozie_node, task_id, trigger_rule)
        if params is None:
            params = {}
        self.template = template
        self.params = params
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        res_man_text = self.oozie_node.find("resource-manager").text
        name_node_text = self.oozie_node.find("name-node").text
        script_text = self.oozie_node.find("script").text
        self.resource_manager = el_utils.replace_el_with_var(res_man_text, params=self.params, quote=False)
        self.name_node = el_utils.replace_el_with_var(name_node_text, params=self.params, quote=False)
        self.script = el_utils.replace_el_with_var(script_text, params=self.params, quote=False)
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
            self.properties = {}
            property_nodes = xml_utils.find_nodes_by_tag(config, "property")
            if property_nodes:
                for node in property_nodes:
                    name = node.find("name").text
                    value = el_utils.replace_el_with_var(
                        node.find("value").text, params=self.params, quote=False
                    )
                    self.properties[name] = value

    def convert_to_text(self):
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/"))
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(self.template)
        prepare_command = self.get_prepare_command(self.oozie_node, self.params)
        return template.render(prepare_command=prepare_command, **self.__dict__)

    def convert_to_airflow_op(self):
        pass

    @staticmethod
    def required_imports():
        return ["from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"]
