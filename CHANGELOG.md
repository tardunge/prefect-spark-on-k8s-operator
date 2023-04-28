# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.1.2

Released on April 28th, 2023.

### Fixed

- fixed RuntimeError on wait_for_completion which will be raised only on fetch_results but not for wait_for_completion.
- fix formatting of logger in fetch_results.
- changed logo url.

## 0.1.1

Released on April 2nd, 2023.

### Changed

- update requirements.txt to prefect-kubernetes>=0.2.3
- update README.md

## 0.1.0

Released on April 1st, 2023.

### Added

- Initial implementation of spark-on-k8s-operator integration with prefect. - [#2](https://github.com/tardunge/prefect-spark-on-k8s-operator/pull/2)
