import pytest

from prefect_spark_on_k8s_operator.constants import SparkApplicationModel as model

constants = model()


def test_constant_attribution_error():
    with pytest.raises(AttributeError):
        constants.KIND = "KIND"
