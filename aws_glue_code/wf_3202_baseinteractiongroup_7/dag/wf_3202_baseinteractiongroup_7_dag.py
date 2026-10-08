import os
from dagfactory import DagFactory

config_path = os.path.join(os.path.dirname(__file__), "wf_3202_baseinteractiongroup_7_dag.yaml")
dag_factory = DagFactory(config_path)
dag_factory.generate_dags(globals())
