from . import _version
from prefect_spark_on_k8s_operator.app import (  # noqa F401
    SparkApplication,
)
from prefect_spark_on_k8s_operator.flows import (  # noqa F401
    run_spark_application,
)

__version__ = _version.get_versions()["version"]
