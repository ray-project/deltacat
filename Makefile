test-integration:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml build
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml up -d
	sleep 30
	venv/bin/python -m pytest tests/