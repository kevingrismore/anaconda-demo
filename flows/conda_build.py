from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run
from prefect.settings import PREFECT_UI_URL

@flow
def conda_build(name: str):
    get_run_logger().info(f"View the parent flow run at {PREFECT_UI_URL.value()}/flow-runs/flow-run/{flow_run.get_id()}")
    clone_repo()
    build(name)
    write_artifact()

@task
def clone_repo():
    get_run_logger().info("Cloning repo")

@task
def build(name: str):
    get_run_logger().info(f"Building {name}")

@task
def write_artifact():
    get_run_logger().info("Writing artifact")