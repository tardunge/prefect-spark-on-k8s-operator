# prefect-spark-on-k8s-operator

<p align="center">
    <!--- Insert a cover image here -->
    <!--- <br> -->
    <a href="https://pypi.python.org/pypi/prefect-spark-on-k8s-operator/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-spark-on-k8s-operator?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/tardunge/prefect-spark-on-k8s-operator/" alt="Stars">
        <img src="https://img.shields.io/github/stars/tardunge/prefect-spark-on-k8s-operator?color=0052FF&labelColor=090422" /></a>
    <a href="https://pypistats.org/packages/prefect-spark-on-k8s-operator/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-spark-on-k8s-operator?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/tardunge/prefect-spark-on-k8s-operator/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/tardunge/prefect-spark-on-k8s-operator?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

Visit the full docs [here](https://tardunge.github.io/prefect-spark-on-k8s-operator) to see additional examples and the API reference.

Prefect integrations for orchestrating and monitoring apache spark jobs on kubernetes using spark-on-k8s-operator.

## Welcome!

`prefect-spark-on-k8s-operator` is a collection of Prefect flows enabling orchestration, observation and management of `SparkApplication` custom kubernetes resources defined according to spark-on-k8s-operator CRD v1Beta2 API Spec.

Jump to [examples](#example-usage).


## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

### Installation

!!! warning
    This integration requires [prefect-kubernetes](https://prefecthq.github.io/prefect-kubernetes/) custom objects api which is [merged](https://github.com/PrefectHQ/prefect-kubernetes/pull/45) but not yet released.<br/>
    This is required to apply spark-on-k8s-operator crd `SparkApplication` into kubernetes cluster.
  
  
Once you have the `prefect-kubernetes` integration. You need to configure the kubernetes credentials as per `prefect-kubernetes` documentation.<br />
Install `prefect-spark-on-k8s-operator` with `pip`:

```bash
pip install prefect-spark-on-k8s-operator
```

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These flows are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Example Usage

#### Specify and run a SparkApplication from a yaml file

```python
import asyncio

from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_spark_on_k8s_operator import (
    SparkApplication,
    run_spark_application, # this is a flow
)

app = SparkApplication.from_yaml_file(
    credentials=KubernetesCredentials.load("k8s-creds"),
    manifest_path="path/to/spark_application.yaml",
)


if __name__ == "__main__":
    # run the flow
    asyncio.run(run_spark_application(app))
```

## Feedback

If you encounter any bugs while using `prefect-spark-on-k8s-operator`, feel free to open an issue in the [prefect-spark-on-k8s-operator](https://github.com/tardunge/prefect-spark-on-k8s-operator) repository.

If you have any questions or issues while using `prefect-spark-on-k8s-operator`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-spark-on-k8s-operator`](https://github.com/tardunge/prefect-spark-on-k8s-operator) for updates too!

## Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-spark-on-k8s-operator`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/tardunge/prefect-spark-on-k8s-operator/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
