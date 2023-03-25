"""This is an example tasks module"""
from prefect import task


@task
def hello_prefect_spark_on_k8s_operator() -> str:
    """
    Sample task that says hello!

    Returns:
        A greeting for your collection
    """
    return "Hello, prefect-spark-on-k8s-operator!"


@task
def goodbye_prefect_spark_on_k8s_operator() -> str:
    """
    Sample task that says goodbye!

    Returns:
        A farewell for your collection
    """
    return "Goodbye, prefect-spark-on-k8s-operator!"
