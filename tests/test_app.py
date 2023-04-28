import pytest

from prefect_spark_on_k8s_operator.app import SparkApplication, generate_pod_selectors
from prefect_spark_on_k8s_operator.constants import SparkApplicationModel as model

constants = model()


def test_from_yaml_file(kubernetes_credentials):
    app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
    )
    assert app.manifest.get(constants.METADATA).get(constants.NAME) == "spark-pi"
    assert app.manifest.get(constants.KIND) == constants.SPARK_APPLICATION_KIND
    assert (
        app.manifest.get(constants.SPEC)
        .get(constants.RESTART_POLICY_KEY)
        .get(constants.TYPE)
        == constants.NEVER
    )


def test_from_yaml_file_scheduled_job(kubernetes_credentials):
    app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/scheduled_job.yaml",
    )
    assert app.manifest.get(constants.KIND) == constants.SPARK_APPLICATION_KIND
    assert (
        app.manifest.get(constants.SPEC).get(constants.TYPE)
        in constants.SPARK_APPLICATION_TYPES
    )
    assert (
        app.manifest.get(constants.SPEC)
        .get(constants.RESTART_POLICY_KEY)
        .get(constants.TYPE)
        == constants.NEVER
    )


def test_from_yaml_file_invalid_manifest(kubernetes_credentials):
    with pytest.raises(TypeError):
        SparkApplication.from_yaml_file(
            credentials=kubernetes_credentials,
            manifest_path="tests/sample_spark_jobs/invalid_kind.yaml",
        )
    with pytest.raises(TypeError):
        SparkApplication.from_yaml_file(
            credentials=kubernetes_credentials,
            manifest_path="tests/sample_spark_jobs/invalid_type.yaml",
        )


def test_generate_pod_selectors():
    assert generate_pod_selectors("spark-pi", "app_id", "submission_id") == (
        "spark-app-selector=app_id,"
        "sparkoperator.k8s.io/app-name=spark-pi,"
        "sparkoperator.k8s.io/submission-id=submission_id,"
        "spark-role=driver"
    )


async def test_trigger(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
    )

    app_run = await spark_app.trigger()
    assert (
        app_run._spark_application.manifest.get(constants.METADATA).get(constants.NAME)
        == app_run._spark_application.name
    )


async def test_wait_for_completion_completed(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_completed,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
    )

    app_run = await spark_app.trigger()
    await app_run.wait_for_completion()
    assert app_run._terminal_state == constants.COMPLETED
    assert app_run._completed


async def test_wait_for_completion_failed(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_failed,
    mock_list_namespaced_pod,
    mock_read_namespaced_pod_log,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
    )

    app_run = await spark_app.trigger()
    
    await app_run.wait_for_completion()

    assert app_run.application_logs.get("spark-pi-khha-driver") == "test-logs"
    assert not app_run._completed
    assert app_run._terminal_state == constants.FAILED

    with pytest.raises(RuntimeError):
        await app_run.fetch_result()


async def test_wait_for_completion_completed_with_logs(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_completed,
    mock_list_namespaced_pod,
    mock_read_namespaced_pod_log,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
        collect_driver_logs=True,
    )

    app_run = await spark_app.trigger()
    await app_run.wait_for_completion()
    assert app_run._completed
    assert app_run._terminal_state == constants.COMPLETED
    await app_run.fetch_result()
    assert app_run.application_logs.get("spark-pi-khha-driver") == "test-logs"


async def test_wait_for_completion_unknown(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_unknown,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
        interval_seconds=0,
        timeout_seconds=0,
    )
    app_run = await spark_app.trigger()

    await app_run.wait_for_completion()

    assert not app_run._completed
    assert app_run._terminal_state == constants.UNKNOWN
    assert len(app_run.application_logs) == 0

    with pytest.raises(RuntimeError):
        await app_run.fetch_result()


async def test_wait_for_completion_recovered(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_recovered,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
        interval_seconds=2,
        timeout_seconds=5,
    )
    app_run = await spark_app.trigger()
    status = await app_run._fetch_status()
    app_state = (
        status.get(constants.STATUS)
        .get(constants.APPLICATION_STATE)
        .get(constants.STATE)
    )
    assert app_state == constants.UNKNOWN
    await app_run.wait_for_completion()
    assert app_run._terminal_state == constants.COMPLETED


async def test_wait_for_completion_init(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_init,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
    )
    app_run = await spark_app.trigger()
    status = await app_run._fetch_status()
    assert constants.STATUS not in status
    await app_run.wait_for_completion()
    assert app_run._terminal_state == constants.COMPLETED


async def test_clean_up(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_completed,
    mock_delete_namespaced_custom_object,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
    )
    app_run = await spark_app.trigger()
    await app_run.wait_for_completion()
    assert app_run._cleanup_status
