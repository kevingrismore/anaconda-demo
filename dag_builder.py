from collections import defaultdict, deque
from typing import Any, Callable

from prefect import task, get_run_logger
from prefect.client.schemas import FlowRun, State
from prefect.deployments import run_deployment
from prefect.utilities.collections import visit_collection
from prefect.settings import PREFECT_UI_URL

from pydantic import BaseModel, Field, root_validator


@task
def run_deployment_task(
    name: str,
    parameters: dict,
    get_persisted_result: bool = False,
    as_subflow: bool = False,
    propagate_failure: bool = True,
) -> None | State | Any:
    """
    Run a deployment. Optionally:
    - Return the persisted result data. Requires result persistence to be enabled on the deployed flow.
    - Run the deployment as a subflow of the flow this task is run in. Clutters the flow run graph a bit, up to taste.
    - Propagate the failure of the deployment to the parent flow. If the deployment fails, the parent flow will fail.
    """
    flow_run: FlowRun = run_deployment(
        name=name, parameters=parameters, as_subflow=as_subflow
    )

    get_run_logger().info(
        f"View the flow run created by this task at {get_flow_run_ui_url(flow_run.id)}"
    )

    if propagate_failure and (
        flow_run.state.is_failed() or flow_run.state.is_crashed()
    ):
        return flow_run.state

    if get_persisted_result:
        return flow_run.state.result()


def get_flow_run_ui_url(flow_run_id) -> str:
    return f"{PREFECT_UI_URL.value()}/flow-runs/flow-run/{flow_run_id}"


class ResultPlaceholder(BaseModel):
    """Task result model"""

    task_id: str = Field(..., description="Task id")


class DAGTask(BaseModel):
    """Task model"""

    id: int = Field(..., description="Task id")
    name: str = Field(..., description="Task name")
    task_function: Callable = Field(..., description="Task function")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Task parameters"
    )
    depends_on: list[int] = Field(
        default_factory=list, description="List of task ids that this task depends on"
    )
    result: ResultPlaceholder = None

    @root_validator(pre=True)
    def prepare_result(cls, values):
        task_id = values.get("id")
        values["result"] = ResultPlaceholder(task_id=task_id)
        return values


class DAGBuilder(BaseModel):
    """
    Add task or deployment runs to be called within a flow. Run the dag with .run().
    
    Since a DAG in Prefect 2 is an arbitrary construct, and number of DAGs may be built
    and run within a single flow.
    """
    tasks: list[DAGTask] = Field(
        default_factory=list, description="Tasks to be executed"
    )
    futures: dict = Field(default_factory=dict)

    def _replace_result_placeholders(self, x):
        if isinstance(x, ResultPlaceholder):
            return self.futures[x.task_id].result()

        return x

    def add_task(
        self,
        id: int,
        name: str,
        task_function: Callable,
        parameters: dict = None,
        depends_on: list[str] = None,
    ) -> DAGTask:
        """
        Add a task to the DAG.
        """
        task = DAGTask(
            id=id,
            name=name,
            task_function=task_function,
            parameters={} if not parameters else parameters,
            depends_on=[] if not depends_on else depends_on,
        )
        self.tasks.append(task)
        return task

    def add_deployment(
        self,
        task_id: int,
        task_name: str,
        flow_name: str,
        deployment_name: str,
        depends_on: list[str],
        get_persisted_result: bool = False,
        as_subflow: bool = False,
        propagate_failure: bool = True,
        parameters: dict | None = None,
    ) -> DAGTask:
        """
        Add a task to the DAG that runs a deployment.
        """
        return self.add_task(
            id=task_id,
            name=task_name,
            task_function=run_deployment_task,
            parameters={
                "name": f"{flow_name}/{deployment_name}",
                "parameters": parameters,
                "get_persisted_result": get_persisted_result,
                "as_subflow": as_subflow,
                "propagate_failure": propagate_failure,
            },
            depends_on=depends_on,
        )

    def _find_concurrent_order(self) -> dict[int, set[int]]:
        """
        Prefect tasks must be submitted in the right order, since downstream tasks need
        results from upstream tasks in order to wait for them.
        """
        graph = defaultdict(list)
        in_degree = defaultdict(int)
        task_levels = defaultdict(set)

        # Build the graph and in-degree map
        for task in self.tasks:
            for dependency in task.depends_on:
                graph[dependency].append(task.id)
                in_degree[task.id] += 1

        # Initialize the queue with tasks having no dependencies
        queue = deque([(task.id, 0) for task in self.tasks if in_degree[task.id] == 0])

        while queue:
            task_id, level = queue.popleft()
            task_levels[level].add(task_id)

            for dependent in graph[task_id]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append((dependent, level + 1))

        return task_levels

    def run(self):
        """
        Find the order of tasks to be submitted. If the input to a task is a result from
        an upstream task or is a data structure that contains a result from an upstream task,
        replace the result placeholder with the actual result.

        Submit all the tasks at each level of concurrency and save their futures by task id.
        """
        concurrent_order = self._find_concurrent_order()
        for level in sorted(concurrent_order.keys()):
            tasks_to_run: list[DAGTask] = [
                task for task in self.tasks if task.id in concurrent_order[level]
            ]
            for task in tasks_to_run:
                wait_for = [self.futures[dependency] for dependency in task.depends_on]

                task.parameters = visit_collection(
                    task.parameters, self._replace_result_placeholders, return_data=True
                )

                task_function = task.task_function.with_options(
                    name=task.id, task_run_name=task.name
                )

                self.futures[task.id] = task_function.submit(
                    **task.parameters,
                    wait_for=wait_for,
                )
