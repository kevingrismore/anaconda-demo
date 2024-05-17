from prefect import flow


@flow
def conda_build(name: str = "something"):
    print(f"Building {name}")
