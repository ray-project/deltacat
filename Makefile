clean:
	rm -rf venv

install:
	if [ ! -d "venv" ]; then \
  		if [[ '$(shell uname -m)' == 'arm64' ]]; then \
			/usr/bin/python3 -m venv venv; \
		else \
			python -m venv venv; \
		fi \
	fi
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r dev-requirements.txt

lint:
	venv/bin/pre-commit run --all-files

test-integration:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml rm -f
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml up -d
	sleep 10
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py
	venv/bin/python -m pytest deltacat/tests/integ

test-integration-rebuild:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml rm -f
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml build --no-cache
