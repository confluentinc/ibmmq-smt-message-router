.PHONY: show-args init-ci build test release-ci epilogue-ci testbreak-after help clean

help:
	@echo "JMS Message Routing to Kafka"
	@echo ""
	@echo "Available targets:"
	@echo "  show-args       - Display build arguments"
	@echo "  init-ci         - Initialize CI environment"
	@echo "  build           - Build custom SMT plugin"
	@echo "  test            - Run tests"
	@echo "  clean           - Clean build artifacts"
	@echo "  release-ci      - Release artifacts"
	@echo "  epilogue-ci     - CI epilogue tasks"
	@echo "  testbreak-after - Post-test cleanup"

show-args:
	@echo "Repository: ibmmq-smt-message-router"
	@echo "Type: JMS Routing Solutions"
	@echo "Contents: Flink SQL routing + Custom SMT for JMS Source Connectors"

init-ci:
	@echo "Initializing CI environment..."
	@which mvn > /dev/null || (echo "Maven not found" && exit 1)
	@echo "Maven found"

build:
	@echo "Building custom SMT plugin..."
	cd jms-routing-smt && mvn clean package
	@echo "Build complete: jms-routing-smt/target/*.jar"

test:
	@echo "Running tests..."
	cd jms-routing-smt && mvn test
	@echo "Tests complete"

clean:
	@echo "Cleaning build artifacts..."
	cd jms-routing-smt && mvn clean
	rm -rf jms-routing-smt/target
	@echo "Clean complete"

release-ci:
	@echo "Releasing artifacts..."
	@echo "No automated release - plugin JAR in jms-routing-smt/target/"

epilogue-ci:
	@echo "Running CI epilogue tasks..."
	@echo "No cleanup required"

testbreak-after:
	@echo "Running post-test cleanup..."
	@echo "No testbreak cleanup required"
