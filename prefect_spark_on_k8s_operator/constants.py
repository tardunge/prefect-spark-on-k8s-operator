"""Module to define constants required for SparkApplication"""
from typing import Any, Dict, Final


class SparkApplicationModel:
    """This class contains constants required for SparkApplication
    manifest defintion.
    We don't have any spark-on-k8s-operator SparkApplication model for Python.
    """

    __slots__ = ()

    # SparkApplication api-resource specific constants.
    GROUP: Final[str] = "sparkoperator.k8s.io"
    VERSION: Final[str] = "v1beta2"
    PLURAL: Final[str] = "sparkapplications"

    # SparkApplication manifest specific constants.
    # We are using constants for now till open api v3
    # model generator supports python.
    KIND: Final[str] = "kind"
    METADATA: Final[str] = "metadata"
    NAME: Final[str] = "name"
    RESTART_POLICY_KEY: Final[str] = "restartPolicy"
    RESTART_POLICY: Final[Dict[str, Any]] = {"restartPolicy": {"type": "Never"}}
    SPEC: Final[str] = "spec"
    TYPE: Final[str] = "type"
    TEMPLATE: Final[str] = "template"
    NEVER: Final[str] = "Never"

    # spark-on-k8s-operator supported kinds.
    SPARK_APPLICATION_KIND: Final[str] = "SparkApplication"
    SCHEDULED_SPARK_APPLICATION_KIND: Final[str] = "ScheduledSparkApplication"
    SPARK_APPLICATION_KINDS = [SPARK_APPLICATION_KIND, SCHEDULED_SPARK_APPLICATION_KIND]

    # spark application types
    JAVA: Final[str] = "Java"
    PYTHON: Final[str] = "Python"
    SCALA: Final[str] = "Scala"
    SPARK_APPLICATION_TYPES = [JAVA, PYTHON, SCALA]

    # runtime manifest keys
    DETAILS: Final[str] = "details"
    SUCCESS: Final[str] = "Success"
    STATUS: Final[str] = "status"
    APPLICATION_STATE: Final[str] = "applicationState"
    STATE: Final[str] = "state"
    ERROR_MESSAGE: Final[str] = "errorMessage"
    SPARK_APPLICATION_ID: Final[str] = "sparkApplicationId"
    SUBMISSION_ID: Final[str] = "submissionID"

    # spark-on-k8s application terminal states.
    COMPLETED: Final[str] = "COMPLETED"
    FAILED: Final[str] = "FAILED"
    UNKNOWN: Final[str] = "UNKNOWN"

    LABELS_TEMPLATE: Final[str] = ",".join(
        [
            "spark-app-selector=${app_id}",
            "sparkoperator.k8s.io/app-name=${name}",
            "sparkoperator.k8s.io/submission-id=${submission_id}",
            "spark-role=driver",
        ]
    )
    SPARK_DRIVER_CONAINER_NAME: Final[str] = "spark-kubernetes-driver"
