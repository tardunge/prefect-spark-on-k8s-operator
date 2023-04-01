"""
    A module to define flows interacting with spark-on-k8s-operator
    resources created in Kubernetes.
"""
from typing import Any, Dict

from prefect import flow, task

from prefect_spark_on_k8s_operator.app import SparkApplication


@flow
async def run_spark_application(
    spark_application: SparkApplication,
) -> Dict[str, Any]:
    """Flow for running a spark application using spark-on-k8s-operator.

    Args:
        spark_application: The `SparkApplication` block that specifies the
            application run params.

    Returns:
        The a dict of logs from driver pod after the application reached a
            terminal state which can be COMPLETED, FAILED, UNKNOWN
            (for `time_out seconds`).

    Raises:
        RuntimeError: If the created spark application attains a failed/unknown status.

    Example:

        ```python
        import asyncio

        from prefect_kubernetes.credentials import KubernetesCredentials
        from prefect_spark_on_k8s_operator import (
            SparkApplication,
            run_spark_application,
        )

        app = SparkApplication.from_yaml_file(
            credentials=KubernetesCredentials.load("k8s-creds"),
            manifest_path="path/to/job.yaml",
        )


        if __name__ == "__main__":
            # run the flow
            asyncio.run(run_spark_application(app))
        ```
    """
    spark_application_run = await task(spark_application.trigger.aio)(spark_application)

    await task(spark_application_run.wait_for_completion.aio)(spark_application_run)

    return await task(spark_application_run.fetch_result.aio)(spark_application_run)
