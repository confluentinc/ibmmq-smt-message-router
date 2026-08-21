.PHONY: show-args init-ci build test release-ci epilogue-ci testbreak-after help

# This is a documentation/configuration example repository
# Most CI targets are no-ops since there's no code to build or test

help:
	@echo "IBM MQ SMT Message Router - Configuration Examples Repository"
	@echo ""
	@echo "Available targets:"
	@echo "  show-args       - Display build arguments"
	@echo "  init-ci         - Initialize CI environment"
	@echo "  build           - Build (no-op for config repo)"
	@echo "  test            - Run tests (validation)"
	@echo "  release-ci      - Release artifacts (no-op)"
	@echo "  epilogue-ci     - CI epilogue tasks"
	@echo "  testbreak-after - Post-test cleanup"

show-args:
	@echo "Repository: ibmmq-smt-message-router"
	@echo "Type: Configuration Examples"
	@echo "Contents: SMT routing patterns for IBM MQ Source Connector"

init-ci:
	@echo "Initializing CI environment..."
	@echo "No dependencies to install for configuration-only repository"

build:
	@echo "Building..."
	@echo "No build required - configuration examples only"

test: validate-json validate-examples

validate-json:
	@echo "Validating JSON configuration files..."
	@for file in examples/*.json; do \
		echo "Checking $$file..."; \
		python3 -m json.tool $$file > /dev/null || exit 1; \
	done
	@echo "✓ All JSON files are valid"

validate-examples:
	@echo "Validating example configurations..."
	@echo "Checking required files exist..."
	@test -f examples/basic-routing.json || (echo "Missing basic-routing.json" && exit 1)
	@test -f examples/routing-with-prefix.json || (echo "Missing routing-with-prefix.json" && exit 1)
	@test -f examples/multi-dimensional-routing.json || (echo "Missing multi-dimensional-routing.json" && exit 1)
	@test -f examples/routing-with-metadata.json || (echo "Missing routing-with-metadata.json" && exit 1)
	@test -f examples/conditional-routing.json || (echo "Missing conditional-routing.json" && exit 1)
	@test -f examples/README.md || (echo "Missing examples/README.md" && exit 1)
	@echo "✓ All required example files present"

release-ci:
	@echo "Releasing artifacts..."
	@echo "No artifacts to release - configuration examples only"

epilogue-ci:
	@echo "Running CI epilogue tasks..."
	@echo "No cleanup required"

testbreak-after:
	@echo "Running post-test cleanup..."
	@echo "No testbreak cleanup required"
