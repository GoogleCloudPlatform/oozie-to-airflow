import logging
import os

import jinja2
from airflow.utils.trigger_rule import TriggerRule

import oozie_converter
from definitions import ROOT_DIR
from mappers.action_mapper import ActionMapper
from utils import el_utils, xml_utils


class SubworkflowMapper(ActionMapper):
    """
    Converts a Sub-workflow Oozie node to an Airflow task.
    """

    IN_FILE_NAME_SUFFIX = "/workflow.xml"
    PROPERTIES_SUFFIX = "/job.properties"
    CONFIGURATION_SUFFIX = "/configuration.properties"
    OUT_FILE_NAME_TEMPLATE = "output/subdag_{}.py"

    def __init__(
        self,
        oozie_node,
        task_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        params=None,
        template="subwf.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, task_id, trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.template = template
        self.params = params
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self.dag_name = kwargs.get("dag_name") if "dag_name" in kwargs else None
        self.properties = {}
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        app_path = self.oozie_node.find("app-path").text
        app_path = el_utils.replace_el_with_var(app_path, params=self.params, quote=False)
        app_path = app_path.split("${wf:user()}/")[1]  # TODO hacky: we want to extract "examples/pig"
        app_name = app_path.split("/")[-1]
        logging.info(f"Converting subworkflow from {app_path}")
        self._parse_config()
        converter = oozie_converter.OozieSubworkflowConverter(
            in_file_name=app_path + self.IN_FILE_NAME_SUFFIX,
            out_file_name=self.OUT_FILE_NAME_TEMPLATE.format(app_name),
            properties=app_path + self.PROPERTIES_SUFFIX,
            configuration=app_path + self.CONFIGURATION_SUFFIX,
            start_days_ago=0,
            dag_name=f"{self.dag_name}.{self.task_id}",
            config_properties=self.get_config_properties(),
        )
        converter.convert()

    def get_config_properties(self):
        propagate_configuration = self.oozie_node.find("propagate-configuration")
        # Below the `is not None` is necessary due to Element's __bool__() return value: `len(self._children) != 0`,
        # and `propagate_configuration` is an empty node so __bool__() will always return False.
        return self.properties if propagate_configuration is not None else {}

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

    def convert_to_text(self):
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/"))
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(self.template)
        return template.render(**self.__dict__)

    def convert_to_airflow_op(self):
        pass

    @staticmethod
    def required_imports():
        return [
            "from airflow.utils import dates",
            "from airflow.contrib.operators import dataproc_operator",
            "from airflow.operators.subdag_operator import SubDagOperator",
            "from subdag_test import sub_dag",
        ]
