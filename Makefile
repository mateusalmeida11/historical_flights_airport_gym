.PHONY : lock, tests, quality, version_patch, version_minor, version_major

quality:
	@echo "Starting the quality process..."
	@poetry install --with dev
	@poetry run pre-commit install
	@poetry run pre-commit run --all-files

lock:
	@echo "Starting de lock process"
	@python3 -m pip install -q poetry
	@poetry lock

test:
	@echo "Starting the tests process..."
	@poetry install --with dev
	@poetry run pytest --cov=formula_1_etl/ --cov-fail-under=70
	@poetry run coverage html

version_patch:
	@echo "Starting the version process..."
	@poetry install --with dev
	@poetry run bump2version patch

version_minor:
	@echo "Starting the version process..."
	@poetry install --with dev
	@poetry run bump2version minor

version_major:
	@echo "Starting the version process..."
	@poetry install --with dev
	@poetry run bump2version major
