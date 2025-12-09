.PHONY: install lint test unit-test build clean clean-build type-mappings benchmark benchmark-aws test-converter-integration test-integration-rebuild publish

install:
	uv sync --all-extras

clean-build:
	rm -rf dist build deltacat.egg-info

clean: clean-build
	rm -rf .venv

build:
	uv build

rebuild: clean-build build

deploy-s3:
	./dev/deploy/aws/scripts/s3-build-and-deploy.sh

lint: install
	uv run pre-commit run --all-files

test: install
	uv run pytest -m "not integration"

unit-test: install
	uv run pytest -m "not integration and not benchmark"

test-converter-integration: install
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml rm -f
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml up -d
	sleep 3
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py
	export SPARK_LOCAL_IP="127.0.0.1"
	uv run pytest deltacat/tests/compute/converter/integration -vv

test-integration-rebuild:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml rm -f
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml build --no-cache

benchmark-aws: install
	uv run pytest deltacat/benchmarking/benchmark_parquet_reads.py --benchmark-only --benchmark-group-by=group,param:name

benchmark: install
	uv run pytest -m benchmark deltacat/benchmarking

type-mappings: install
	@echo "Generating type mappings..."
	uv run python deltacat/docs/autogen/schema/inference/generate_type_mappings.py
	@echo "Parsing type mappings to markdown..."
	uv run python deltacat/docs/autogen/schema/inference/parse_json_type_mappings.py generate_type_mappings_results.json
	@echo "Generating Python compatibility mapping..."
	uv run python deltacat/docs/autogen/schema/inference/parse_json_type_mappings.py generate_type_mappings_results.json --python
	@echo "Type mappings generation complete!"

publish: test build
	uv publish
