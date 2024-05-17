from prefect import flow

WORK_POOLS = [
    "universal",
    "mac",
    "linux",
]

if __name__ == "__main__":
    conda_build_flow = flow.from_source(
        source="https://github.com/kevingrismore/anaconda-demo.git",
        entrypoint="flows/conda_build.py:conda_build",
    )

    parent_flow = flow.from_source(
        source="https://github.com/kevingrismore/anaconda-demo.git",
        entrypoint="flows/parent.py:parent",
    )

    for pool in WORK_POOLS:
        conda_build_flow.deploy(
            name=f"conda-build-{pool}",
            work_pool_name=pool,
            build=False,
            ignore_warnings=True,
        )

    parent_flow.deploy(
        name="conda-parent",
        work_pool_name="universal",
        build=False,
        ignore_warnings=True,
    )
