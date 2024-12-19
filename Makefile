venv:
	if [ ! -d "venv" ]; then \
  		if [[ '$(shell uname -m)' == 'arm64' ]]; then \
			/usr/bin/python3 -m venv venv; \
		else \
			python -m venv venv; \
		fi \
	fi

clean-build:
	rm -rf dist
	rm -rf build
	rm -rf deltacat.egg-info

clean-venv:
	rm -rf venv

clean: clean-build clean-venv

build: venv
	venv/bin/python setup.py sdist bdist_wheel

rebuild: clean-build build

deploy-s3:
	./dev/deploy/aws/scripts/s3-build-and-deploy.sh

install: venv
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r dev-requirements.txt

lint: install
	venv/bin/pre-commit run --all-files

test: install
	venv/bin/pytest -m "not integration"

test-integration: install
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml rm -f
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml up -d
	sleep 3
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py
	venv/bin/python -m pytest deltacat/tests/integ -v -m integration

test-integration-rebuild:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml rm -f
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml build --no-cache

benchmark-aws: install
	venv/bin/pytest deltacat/benchmarking/benchmark_parquet_reads.py --benchmark-only --benchmark-group-by=group,param:name
