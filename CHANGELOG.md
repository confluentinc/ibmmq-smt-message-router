# Change Log

## Unreleased

### Features and Fixes

- Removed the self-managed Flink Table API router and the custom `JmsPropertyToHeader` SMT (with its plugin ZIP and Maven build). The supported options are Confluent Cloud Flink SQL and the built-in JSON_PATH SMT.
- Flink SQL files: route unknown or missing `messageType` to a separate topic instead of dropping it.
- Documentation: consolidated the SMT and Flink guides, removed contradictory and untested claims.

## v0.0.0 - Initial release

- JMS message routing with Flink SQL and the built-in JSON_PATH SMT.
