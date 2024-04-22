dev-install:
	pip install poetry==1.7.1 && \
	poetry install --with dev;

install:
	pip install poetry==1.7.1 && \
	poetry install;

pre-commit:
	pre-commit run --all-files

checks:
	black --check emrflow/*
	isort emrflow/* --check-only
	pylint --fail-under=7 emrflow/*

lint:
	black emrflow/*
	isort emrflow/*
	pylint --fail-under=7 emrflow/*

local-test:
	PYTHONPATH=${PYTHONPATH}:.:tests \
	poetry run pytest --cov=emrflow/ --cov-fail-under=70 tests/

test:
	PYTHONPATH=${PYTHONPATH}:.:tests \
	poetry run pytest --cov=emrflow/ --cov-report xml:cov.xml --cov-fail-under=70 tests/unit_tests


clean:
	find ./ -name "*~" | xargs rm -v || :
	find ./ -name "*.pyc" | xargs rm -v || :
	find ./ -name "__pycache__" | xargs rm -rf || :
