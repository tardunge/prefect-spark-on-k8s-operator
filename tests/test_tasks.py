from prefect import flow

from prefect_spark_on_k8s_operator.tasks import (
    goodbye_prefect_spark_on_k8s_operator,
    hello_prefect_spark_on_k8s_operator,
)


def test_hello_prefect_spark_on_k8s_operator():
    @flow
    def test_flow():
        return hello_prefect_spark_on_k8s_operator()

    result = test_flow()
    assert result == "Hello, prefect-spark-on-k8s-operator!"


def goodbye_hello_prefect_spark_on_k8s_operator():
    @flow
    def test_flow():
        return goodbye_prefect_spark_on_k8s_operator()

    result = test_flow()
    assert result == "Goodbye, prefect-spark-on-k8s-operator!"
