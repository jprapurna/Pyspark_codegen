import os
from dagfactory import DagFactory

config_path = os.path.join(os.path.dirname(__file__), "wf_4201_calculate_interaction_facts 6_dag.yaml")
dag_factory = DagFactory(config_path)
dag_factory.generate_dags(globals())
