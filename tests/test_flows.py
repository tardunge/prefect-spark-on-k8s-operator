from prefect_spark_on_k8s_operator.flows import hello_and_goodbye


def test_hello_and_goodbye_flow():
    result = hello_and_goodbye()
    assert result == "Done"
