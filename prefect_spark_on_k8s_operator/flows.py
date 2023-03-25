"""This is an example flows module"""
from prefect import flow

from prefect_spark_on_k8s_operator.blocks import Sparkonk8soperatorBlock
from prefect_spark_on_k8s_operator.tasks import (
    goodbye_prefect_spark_on_k8s_operator,
    hello_prefect_spark_on_k8s_operator,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    Sparkonk8soperatorBlock.seed_value_for_example()
    block = Sparkonk8soperatorBlock.load("sample-block")

    print(hello_prefect_spark_on_k8s_operator())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_spark_on_k8s_operator())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
