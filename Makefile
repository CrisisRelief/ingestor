PYTHON_VERSION = "3.7.3"

virtualenv_init:
	pyenv virtualenv $(PYTHON_VERSION) "crisisapp-ingestor-$(PYTHON_VERSION)"

install_reqs_dev:
	pip install -r requirements-dev.txt

install_reqs:
	pip install -r requirements.txt

lint:
	flake8
	yamllint .
	pylint ingestion

test:
	py.test --verbose --color=yes tests

test_watch:
	pytest-watch --runner 'make test'
