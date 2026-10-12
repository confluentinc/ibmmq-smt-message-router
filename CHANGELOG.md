# Change Log

## Unreleased

### Features and Fixes

- `JmsPropertyToHeader` SMT: return a new record instead of mutating the input, fail with `DataException` when `skip.missing=false`, no longer swallow errors, and add unit tests.
- Removed the self-managed Flink Table API router (`java-router/`); the supported options are Confluent Cloud Flink SQL and the JSON_PATH SMT.
- Flink SQL files: route unknown or missing `messageType` to a separate topic, matching the Java router.
- Documentation: correct `ExtractTopic$Header` configuration (`field`, no `topic.format`), remove contradictory and untested claims, and consolidate the SMT and Flink guides.
- The prebuilt `jms-property-to-header-smt-plugin.zip` is no longer committed; `make release-ci` builds it.

## v0.0.0 - Initial release

- JMS message routing with Flink SQL, a custom SMT, and the built-in JSON_PATH SMT.
