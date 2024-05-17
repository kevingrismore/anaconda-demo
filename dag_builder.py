from collections import defaultdict, deque
from typing import Any, Callable

from prefect import task
from prefect.client.schemas import FlowRun
from prefect.deployments import run_deployment
from prefect.utilities.collections import visit_collection

from pydantic import BaseModel, Field, root_validator


@task
def run_deployment_task(name: str, parameters: dict):
    flow_run: FlowRun = run_deployment(name=name, parameters=parameters)
    return flow_run.state.result()


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
    ):
        """Add a task to the DAG"""
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
        parameters: dict | None = None,
    ):
        return self.add_task(
            id=task_id,
            name=task_name,
            task_function=run_deployment_task,
            parameters={
                "name": f"{flow_name}/{deployment_name}",
                "parameters": parameters,
            },
            depends_on=depends_on,
        )

    def _find_concurrent_order(self):
        graph = defaultdict(list)  # Task -> List of tasks depending on it
        in_degree = defaultdict(int)  # Task -> Number of dependencies
        task_levels = defaultdict(
            set
        )  # Level -> Tasks that can be executed at this level

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
