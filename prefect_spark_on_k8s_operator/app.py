"""Module to define SparkApplication and monitor its Run"""

import random
import string
from asyncio import sleep
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Type, Union

import yaml
from prefect.blocks.abstract import JobBlock, JobRun
from prefect.utilities.asyncutils import sync_compatible
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.custom_objects import (
    create_namespaced_custom_object,
    delete_namespaced_custom_object,
    get_namespaced_custom_object_status,
)
from prefect_kubernetes.pods import list_namespaced_pod, read_namespaced_pod_log
from pydantic import Field
from typing_extensions import Self

from prefect_spark_on_k8s_operator.constants import SparkApplicationModel as model

constants = model()


def generate_pod_selectors(name: str, app_id: str, submission_id: str) -> List[str]:
    """Generates pod selector labels given the
    name, app_id and submission_id of the spark application.
    """
    template = string.Template(constants.LABELS_TEMPLATE)
    labels = template.safe_substitute(
        name=name, app_id=app_id, submission_id=submission_id
    )
    return labels


class SparkApplication(JobBlock):
    """A block representing a spark application configuration.
    The object instance can be created by `from_yaml_file` classmethod.
    Below are the additional attributes which can be passed to it if you want to
    change the `SparkApplicationRun` behaviour.

    Attributes:
        manifest:
            The spark application manifest(spark-on-k8s-operator v1Beta2 API spec)
            to run. This dictionary can be produced
            using `yaml.safe_load`.
        credentials:
            The credentials to configure a client from.
        delete_after_completion:
            Whether to delete the application after it has completed.
            Defaults to `True`.
        interval_seconds:
            The number of seconds to wait between application status checks.
            Defaults to `5` seconds.
        namespace:
            The namespace to create and run the application in. Defaults to `default`.
        timeout_seconds:
            The number of seconds to wait for the application in UNKNOWN
            state before timing out.
            Defaults to `600` (10 minutes).
        collect_driver_logs:
            Whether to collect driver logs after completion.
            By default this is done only upon failures. Logs for successful runs will
            be collected by setting this option to True.
            Defaults to `False`.
        api_kwargs:
            Additional arguments to include in Kubernetes API calls.
    """

    # Duplicated description until griffe supports pydantic Fields.
    manifest: Dict[str, Any] = Field(
        default=...,
        title="SparkApplication Manifest",
        description=(
            "The spark application manifest(as per spark-on-k8s-operator API spec)"
            " to run. This dictionary can be produced "
            "using `yaml.safe_load`."
        ),
    )
    api_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        title="Additional API Arguments",
        description="Additional arguments to include in Kubernetes API calls.",
        example={"pretty": "true"},
    )
    credentials: KubernetesCredentials = Field(
        default=..., description="The credentials to configure a client from."
    )
    delete_after_completion: bool = Field(
        default=True,
        description="Whether to delete the application after it has completed.",
    )
    interval_seconds: int = Field(
        default=5,
        description="The number of seconds to wait between application status checks.",
    )
    namespace: str = Field(
        default="default",
        description="The namespace to create and run the application in.",
    )
    timeout_seconds: Optional[int] = Field(
        default=600,
        description="The number of seconds to wait for the application in "
        "UNKNOWN state before timing out.",
    )
    collect_driver_logs: Optional[bool] = Field(
        default=False,
        description=(
            "Whether to collect driver logs after completion."
            " By default this is done only upon failures."
            " Logs for successful runs will be collected by setting this option to True"
        ),
    )

    _block_type_name = "Spark On K8s Operator"
    _block_type_slug = "spark-on-k8s-operator"
    _logo_url = "https://images.ctfassets.net/zscdif0zqppk/35vNcprr3MmIlkrKxxCiah/1d720b4b50dfa8876198cf21730cf123/Kubernetes_logo_without_workmark.svg.png?h=250"  # noqa: E501
    _documentation_url = "https://tardunge.github.io/prefect-spark-on-k8s-operator/app/#prefect_spark_on_k8s_operator.app.SparkApplication"  # noqa
    name: str = ""

    @sync_compatible
    async def trigger(self) -> "SparkApplicationRun":
        """Apply the spark application and return a `SparkApplicationRun` object.

        Returns:
            SparkApplicationRun object.
        """

        # randomize the application run instance name.
        name = (
            self.manifest.get(constants.METADATA).get(constants.NAME)
            + "-"
            + "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        )
        self.manifest.get(constants.METADATA)[constants.NAME] = name

        manifest = await create_namespaced_custom_object.fn(
            kubernetes_credentials=self.credentials,
            group=constants.GROUP,
            version=constants.VERSION,
            plural=constants.PLURAL,
            body=self.manifest,
            namespace=self.namespace,
            **self.api_kwargs,
        )
        self.logger.info(
            "Created spark application: "
            f"{manifest.get(constants.METADATA).get(constants.NAME)}"
        )

        self.manifest = manifest
        self.name = manifest.get(constants.METADATA).get(constants.NAME)
        return SparkApplicationRun(spark_application=self)

    @classmethod
    def from_yaml_file(
        cls: Type[Self], manifest_path: Union[Path, str], **kwargs
    ) -> Self:
        """Create a `SparkApplication` from a YAML file.
        Supports manifests of SparkApplication or ScheduledSparkApplication Kind only.
        If a `ScheduledSparkApplication` is provided, it is converted to a
        `SparkApplication` Kind. It forcefully sets the restartPolicy of the application
        to 'Never'.

        Args:
            manifest_path: The YAML file to create the `SparkApplication` from.

        Returns:
            A SparkApplicationRun object.
        """
        with open(manifest_path, "r") as yaml_stream:
            yaml_dict = yaml.safe_load(yaml_stream)

        # convert ScheduledSparkApplication to SparkApplication
        # as the schedules are handled by prefect.
        if yaml_dict.get(constants.KIND) == constants.SCHEDULED_SPARK_APPLICATION_KIND:
            yaml_dict[constants.KIND] = constants.SPARK_APPLICATION_KIND
            yaml_dict[constants.SPEC] = yaml_dict[constants.SPEC][constants.TEMPLATE]

        if (
            yaml_dict.get(constants.KIND) not in constants.SPARK_APPLICATION_KINDS
            or yaml_dict.get(constants.SPEC).get(constants.TYPE)
            not in constants.SPARK_APPLICATION_TYPES
        ):
            raise TypeError(
                "The provided manifest has either unsupport kind or spec.type"
            )

        # Forcefully set restartPolicy to Never. Schedules and retries
        # should be dictated by prefect flow.
        yaml_dict.get(constants.SPEC).update(constants.RESTART_POLICY)
        return cls(manifest=yaml_dict, **kwargs)


class SparkApplicationRun(JobRun[Dict[str, Any]]):
    """A container representing a run of a spark application."""

    def __init__(
        self,
        spark_application: "SparkApplication",
    ):
        self.application_logs = None

        self._completed = False
        self._timed_out = False
        self._terminal_state = ""
        self._error_msg = "The Run has not encounterend any errors."
        self._spark_application = spark_application
        self._status = None
        self._cleanup_status = False

    async def _cleanup(self) -> bool:
        """Deletes the resources created by the spark application.
        Produces Resourceleak warning in case the delete was unsuccessful.
        """
        deleted = await delete_namespaced_custom_object.fn(
            kubernetes_credentials=self._spark_application.credentials,
            group=constants.GROUP,
            version=constants.VERSION,
            plural=constants.PLURAL,
            name=self._spark_application.name,
            namespace=self._spark_application.namespace,
            **self._spark_application.api_kwargs,
        )
        status = deleted.get(constants.STATUS) == constants.SUCCESS
        if status:
            self.logger.info(
                "cleaned up resources for app: "
                f"{deleted.get(constants.DETAILS).get(constants.NAME)}"
            )
        else:
            self.logger.warning(
                f"Resource leak: failed to clean up, {self._spark_application.name}",
            )

        return status

    async def _fetch_status(self) -> Dict[str, Any]:
        """Reads the runtime status of the spark application."""
        self._status = await get_namespaced_custom_object_status.fn(
            kubernetes_credentials=self._spark_application.credentials,
            group=constants.GROUP,
            version=constants.VERSION,
            plural=constants.PLURAL,
            name=self._spark_application.name,
            namespace=self._spark_application.namespace,
            **self._spark_application.api_kwargs,
        )
        return self._status

    @sync_compatible
    async def wait_for_completion(self):
        """Waits for the application to reach a terminal state.
        The terminal states currently used are:
        COMPLETED:
            The application has run successfully.
        FAILED:
            The application has encounterend some error.
        UNKNOWN:
            The application is in UNKNOWN state. This happens if the
            Kubernetes node hosting the driver pod crashes. If the
            state doesn't change for `timeout_seconds`, then the application
            is terminated.

        Raises:
            RuntimeError: If the application fails or in unknown state
                for timeout_seconds.
        """
        self.application_logs = {}

        # wait for the status to change from ""(empty string) to something.
        while True:
            status = await self._fetch_status()
            if constants.STATUS not in status:
                await sleep(1)
                continue
            break

        # wait for the application to reach a terminal_state
        # which is either COMPLETED or FAILED
        # or UNKNOWN(for timeout_seconds)
        while not self._completed:
            status = await self._fetch_status()
            app_state = (
                status.get(constants.STATUS)
                .get(constants.APPLICATION_STATE)
                .get(constants.STATE)
            )
            self.logger.info(f"Last obeserved heartbeat: {app_state}")
            if app_state in [constants.COMPLETED, constants.FAILED]:
                self._completed = True
                self._terminal_state = app_state
                self.logger.info(f"{self._completed}")
                self.logger.info(f"{self._terminal_state}")

            # happens when node/kubelet crashes.
            # Stop the application if this state doesn't change until timeout_seconds.
            elif app_state == constants.UNKNOWN:
                timer_start = int(perf_counter())
                while not self._timed_out:
                    await sleep(self._spark_application.interval_seconds)
                    status = await self._fetch_status()
                    app_state = (
                        status.get(constants.STATUS)
                        .get(constants.APPLICATION_STATE)
                        .get(constants.STATE)
                    )
                    if app_state != constants.UNKNOWN:
                        timer_start = 0
                        break
                    if (
                        int(perf_counter()) - timer_start
                        > self._spark_application.timeout_seconds
                    ):
                        self._completed = True
                        self._timed_out = True
                        self._terminal_state = app_state
            else:
                await sleep(self._spark_application.interval_seconds)

        # restore the value after getting rid of loops.
        if self._terminal_state != constants.COMPLETED:
            self._completed = False

        _tail_logs = False
        if self._terminal_state == constants.FAILED:
            _tail_logs = True
        if self._spark_application.collect_driver_logs:
            _tail_logs = True
        if self._terminal_state == constants.UNKNOWN:
            _tail_logs = False

        if _tail_logs:
            self._error_msg = (
                self._status.get(constants.STATUS)
                .get(constants.APPLICATION_STATE)
                .get(constants.ERROR_MESSAGE, self._error_msg)
            )
            app_id = self._status.get(constants.STATUS).get(
                constants.SPARK_APPLICATION_ID
            )
            app_submission_id = self._status.get(constants.STATUS).get(
                constants.SUBMISSION_ID
            )

            v1_pod_list = await list_namespaced_pod.fn(
                kubernetes_credentials=self._spark_application.credentials,
                namespace=self._spark_application.namespace,
                label_selector=generate_pod_selectors(
                    self._spark_application.name,
                    app_id,
                    app_submission_id,
                ),
                **self._spark_application.api_kwargs,
            )
            self.logger.info(f"pod_list: {len(v1_pod_list.items)}")
            for pod in v1_pod_list.items:
                pod_name = pod.metadata.name
                self.logger.info(f"Capturing logs for pod {pod_name!r}.")
                self.application_logs[pod_name] = await read_namespaced_pod_log.fn(
                    kubernetes_credentials=self._spark_application.credentials,
                    namespace=self._spark_application.namespace,
                    pod_name=pod_name,
                    container=constants.SPARK_DRIVER_CONAINER_NAME,
                    **self._spark_application.api_kwargs,
                )

        if self._spark_application.delete_after_completion or self._timed_out:
            self._cleanup_status = await self._cleanup()

        if self._terminal_state != constants.COMPLETED:
            raise RuntimeError(
                "The SparkApplication run is not in a completed state,"
                f" last observed state was {self._terminal_state} - "
                f"possible errors: {self._error_msg}"
            )

    @sync_compatible
    async def fetch_result(self) -> Dict[str, Any]:
        """Returns the logs from driver pod when:
        `collect_driver_logs` is set to true
        or the application is not in COMPLETED state.

        Returns:
            A dict containing the driver pod name and its main container logs.
        """
        self.logger.info(f"logs: {self.application_logs}")
        if self._terminal_state != constants.COMPLETED:
            raise ValueError(
                "The SparkApplication run is not in a completed state,"
                f" last observed state was {self._terminal_state} - "
                f"possible errors: {self._error_msg}"
            )
        return self.application_logs
