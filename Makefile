install:
	rm -rf venv
	python -m venv venv
	venv/bin/pip install -r requirements.txt

install-apple-silicon:
	rm -rf venv
	/usr/bin/python3 -m venv venv
	venv/bin/pip install -r requirements.txt


test-integration:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml build
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml up -d
	sleep 30
	venv/bin/python -m pytest tests/