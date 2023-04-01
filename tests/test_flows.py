from prefect_spark_on_k8s_operator import SparkApplication, run_spark_application


async def test_run_spark_application(
    kubernetes_credentials,
    _mock_kubernets_api_client,
    mock_create_namespaced_custom_object,
    mock_get_namespaced_custom_object_status_completed,
    mock_list_namespaced_pod,
    mock_read_namespaced_pod_log,
    mock_delete_namespaced_custom_object,
):
    spark_app = SparkApplication.from_yaml_file(
        credentials=kubernetes_credentials,
        manifest_path="tests/sample_spark_jobs/sample_job.yaml",
        collect_driver_logs=True,
    )
    result = await run_spark_application(spark_app)
    assert result.get("spark-pi-khha-driver") == "test-logs"
