# Change Log

## Unreleased

### Features and Fixes

- `JmsPropertyToHeader` SMT: return a new record instead of mutating the input, fail with `DataException` when `skip.missing=false`, no longer swallow errors, and add unit tests.
- Flink Table API router: fix invalid DDL when API credentials are supplied, align `flink-connector-kafka` with Flink 1.20, use plain JSON (no `json-registry`, which only exists on Confluent Cloud), submit all routes as one job, and drop the unsupported `PRIMARY KEY` on plain Kafka sinks.
- Flink SQL files: route unknown or missing `messageType` to a separate topic, matching the Java router.
- Documentation: correct `ExtractTopic$Header` configuration (`field`, no `topic.format`), remove contradictory and untested claims, and consolidate the SMT and Flink guides.
- CI: build and test the Flink router as well as the SMT.
- The prebuilt `jms-property-to-header-smt-plugin.zip` is no longer committed; `make release-ci` builds it.

## v0.0.0 - Initial release

- JMS message routing with Flink SQL, a custom SMT, and the built-in JSON_PATH SMT.
