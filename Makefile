.PHONY: show-args init-ci build test release-ci epilogue-ci testbreak-after help clean

help:
	@echo "JMS Message Routing to Kafka"
	@echo ""
	@echo "Available targets:"
	@echo "  show-args       - Display build arguments"
	@echo "  init-ci         - Initialize CI environment"
	@echo "  build           - Build custom SMT plugin and Flink router"
	@echo "  test            - Run tests"
	@echo "  clean           - Clean build artifacts"
	@echo "  release-ci      - Package the custom connector plugin ZIP"
	@echo "  epilogue-ci     - CI epilogue tasks"
	@echo "  testbreak-after - Post-test cleanup"

show-args:
	@echo "Repository: ibmmq-smt-message-router"
	@echo "Type: JMS Routing Solutions"
	@echo "Contents: Flink SQL routing + Custom SMT for JMS Source Connectors"

init-ci:
	@echo "Initializing CI environment..."
	@echo "Java version:"
	@java -version 2>&1 | head -1
	@echo "Maven version:"
	@mvn -version 2>&1 | head -1
	@echo "JAVA_HOME: $$JAVA_HOME"

build:
	@echo "Building custom SMT plugin..."
	cd jms-routing-smt && mvn -B clean package
	@echo "Building Flink Table API router..."
	cd jms-routing-smt/flink-routing/java-router && mvn -B clean package
	@echo "Build complete: jms-routing-smt/target/*.jar, jms-routing-smt/flink-routing/java-router/target/*.jar"

test:
	@echo "Running tests..."
	cd jms-routing-smt && mvn -B test
	cd jms-routing-smt/flink-routing/java-router && mvn -B test
	@echo "Tests complete"

clean:
	@echo "Cleaning build artifacts..."
	cd jms-routing-smt && mvn -B clean
	cd jms-routing-smt/flink-routing/java-router && mvn -B clean
	@echo "Clean complete"

release-ci:
	@echo "Packaging Confluent Cloud custom connector plugin ZIP..."
	rm -rf jms-routing-smt/target/plugin
	mkdir -p jms-routing-smt/target/plugin/lib
	cp jms-routing-smt/target/jms-property-to-header-smt-*.jar jms-routing-smt/target/plugin/lib/
	rm -f jms-routing-smt/target/plugin/lib/original-*.jar
	cd jms-routing-smt/target/plugin && zip -r ../jms-property-to-header-smt-plugin.zip lib
	@echo "Plugin: jms-routing-smt/target/jms-property-to-header-smt-plugin.zip"

epilogue-ci:
	@echo "Running CI epilogue tasks..."
	@echo "No cleanup required"

testbreak-after:
	@echo "Running post-test cleanup..."
	@echo "No testbreak cleanup required"
