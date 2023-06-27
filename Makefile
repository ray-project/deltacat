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

test-integration:
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml kill
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml build
	docker-compose -f dev/iceberg-integration/docker-compose-integration.yml up -d
	sleep 30
	venv/bin/python -m pytest deltacat/tests/integ
