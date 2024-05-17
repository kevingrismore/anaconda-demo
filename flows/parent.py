from prefect import flow

from dag_builder import DAGBuilder
from graph_parser import Graph


@flow
def parent():
    graph = Graph("graph.json")
    dag = DAGBuilder()

    for node in graph.nodes:
        dag.add_deployment(
            task_id=node.id,
            task_name=node.name,
            flow_name="conda-build",
            deployment_name=f"conda-build-{node.platform}",
            depends_on=node.depends_on,
        )

    dag.run()
