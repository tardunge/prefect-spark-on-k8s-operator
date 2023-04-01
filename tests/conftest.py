from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml
from kubernetes.client import CoreV1Api, CustomObjectsApi
from kubernetes.client.models import V1ObjectMeta, V1Pod, V1PodList
from prefect.blocks.kubernetes import KubernetesClusterConfig
from prefect_kubernetes.credentials import KubernetesCredentials

BASEDIR = Path("tests")
GOOD_CONFIG_FILE_PATH = BASEDIR / "kube_config.yaml"
SPARK_APP_FIXTURES_BASEDIR = Path("tests/sample_spark_jobs")
SAMPLE_JOB_YAML = SPARK_APP_FIXTURES_BASEDIR / "sample_job.yaml"
COMPLETED_JOB_YAML = SPARK_APP_FIXTURES_BASEDIR / "completed_job_status.yaml"
FAILED_JOB_YAML = SPARK_APP_FIXTURES_BASEDIR / "failed_job_status.yaml"
EMPTY_JOB_YAML = SPARK_APP_FIXTURES_BASEDIR / "empty_job_status.yaml"
HUNG_JOB_YAML = SPARK_APP_FIXTURES_BASEDIR / "hung_job_status.yaml"


@pytest.fixture
def kube_config_dict():
    return yaml.safe_load(GOOD_CONFIG_FILE_PATH.read_text())


@pytest.fixture
def sample_spark_app():
    return yaml.safe_load(SAMPLE_JOB_YAML.read_text())


@pytest.fixture
def completed_spark_app():
    return yaml.safe_load(COMPLETED_JOB_YAML.read_text())


@pytest.fixture
def failed_spark_app():
    return yaml.safe_load(FAILED_JOB_YAML.read_text())


@pytest.fixture
def empty_spark_app():
    return yaml.safe_load(EMPTY_JOB_YAML.read_text())


@pytest.fixture
def hung_spark_app():
    return yaml.safe_load(HUNG_JOB_YAML.read_text())


@pytest.fixture
def deleted_spark_app():
    return {"status": "Success", "details": {"name": "spark-pi"}}


@pytest.fixture
def v1_pod_list():
    return V1PodList(
        items=[V1Pod(metadata=(V1ObjectMeta(name="spark-pi-khha-driver")))]
    )


@pytest.fixture
def pod_log():
    return "test-logs"


@pytest.fixture
def kubernetes_credentials(kube_config_dict):
    return KubernetesCredentials(
        cluster_config=KubernetesClusterConfig(
            context_name="test", config=kube_config_dict
        )
    )


@pytest.fixture
def _mock_kubernets_api_client(monkeypatch):
    custom_objects_client = MagicMock(
        spec=list(set(dir(CoreV1Api) + dir(CustomObjectsApi)))
    )

    @contextmanager
    def get_client(self, _):
        yield custom_objects_client

    monkeypatch.setattr(
        "prefect_kubernetes.credentials.KubernetesCredentials.get_client",
        get_client,
    )

    return custom_objects_client


@pytest.fixture
def mock_create_namespaced_custom_object(
    monkeypatch,
    sample_spark_app,
):
    mock_v1_job = AsyncMock(return_value=sample_spark_app)
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.create_namespaced_custom_object.fn",
        mock_v1_job,
    )
    return mock_v1_job


@pytest.fixture
def mock_get_namespaced_custom_object_status_completed(
    monkeypatch,
    completed_spark_app,
):
    mock_completed_job = AsyncMock(return_value=completed_spark_app)
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.get_namespaced_custom_object_status.fn",
        mock_completed_job,
    )
    return mock_completed_job


@pytest.fixture
def mock_get_namespaced_custom_object_status_failed(
    monkeypatch,
    failed_spark_app,
):
    mock_completed_job = AsyncMock(return_value=failed_spark_app)
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.get_namespaced_custom_object_status.fn",
        mock_completed_job,
    )
    return mock_completed_job


@pytest.fixture
def mock_get_namespaced_custom_object_status_init(
    monkeypatch,
    empty_spark_app,
    completed_spark_app,
):
    mock_empty_job = AsyncMock(
        side_effect=(2 * [empty_spark_app] + 2 * [completed_spark_app])
    )
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.get_namespaced_custom_object_status.fn",
        mock_empty_job,
    )
    return mock_empty_job


@pytest.fixture
def mock_get_namespaced_custom_object_status_unknown(
    monkeypatch,
    hung_spark_app,
):
    mock_hung_job = AsyncMock(return_value=hung_spark_app)
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.get_namespaced_custom_object_status.fn",
        mock_hung_job,
    )
    return mock_hung_job


@pytest.fixture
def mock_get_namespaced_custom_object_status_recovered(
    monkeypatch,
    hung_spark_app,
    completed_spark_app,
):
    mock_recovered_job = AsyncMock(
        side_effect=(4 * [hung_spark_app] + 2 * [completed_spark_app])
    )
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.get_namespaced_custom_object_status.fn",
        mock_recovered_job,
    )
    return mock_recovered_job


@pytest.fixture
def mock_delete_namespaced_custom_object(
    monkeypatch,
    deleted_spark_app,
):
    mock_deleted_job = AsyncMock(return_value=deleted_spark_app)
    monkeypatch.setattr(
        "prefect_kubernetes.custom_objects.delete_namespaced_custom_object.fn",
        mock_deleted_job,
    )
    return mock_deleted_job


@pytest.fixture
def mock_list_namespaced_pod(monkeypatch, v1_pod_list):
    mock_v1_pod_list = AsyncMock(return_value=v1_pod_list)
    monkeypatch.setattr(
        "prefect_kubernetes.pods.list_namespaced_pod.fn", mock_v1_pod_list
    )
    return mock_v1_pod_list


@pytest.fixture
def mock_read_namespaced_pod_log(monkeypatch, pod_log):
    mock_pod_log = AsyncMock(return_value=pod_log)
    monkeypatch.setattr(
        "prefect_kubernetes.pods.read_namespaced_pod_log.fn", mock_pod_log
    )
    return mock_pod_log
