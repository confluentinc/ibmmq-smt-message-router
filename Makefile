.PHONY: show-args init-ci build test release-ci epilogue-ci testbreak-after help clean

help:
	@echo "JMS Message Routing to Kafka"
	@echo ""
	@echo "Available targets:"
	@echo "  show-args       - Display build arguments"
	@echo "  init-ci         - Initialize CI environment"
	@echo "  build           - Nothing to build"
	@echo "  test            - Run tests"
	@echo "  clean           - Clean build artifacts"
	@echo "  release-ci      - No release artifacts"
	@echo "  epilogue-ci     - CI epilogue tasks"
	@echo "  testbreak-after - Post-test cleanup"

show-args:
	@echo "Repository: ibmmq-smt-message-router"
	@echo "Type: JMS Routing Solutions"
	@echo "Contents: Flink SQL routing + JSON_PATH SMT docs for JMS Source Connectors"

init-ci:
	@echo "Nothing to initialize"

build:
	@echo "Nothing to build: documentation and Flink SQL only"

test:
	@echo "No tests"

clean:
	@echo "Nothing to clean"

release-ci:
	@echo "No release artifacts"

epilogue-ci:
	@echo "Running CI epilogue tasks..."
	@echo "No cleanup required"

testbreak-after:
	@echo "Running post-test cleanup..."
	@echo "No testbreak cleanup required"
