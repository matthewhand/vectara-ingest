.PHONY: requirements requirements-extra requirements-all test lint format

# Soup Sieve 4 requires Python 3.11.5 or newer.
PYTHON_VERSION := 3.11.5
# uv resolves the CUDA builds from PyPI. Keep the deployable CPU lock entries.
STRIP_CUDA := awk '/^(cuda-|nvidia-)/ { skip = 1; next } /^[^[:space:]]/ { skip = 0 } !skip'

requirements:
	uv pip compile requirements.in -o requirements.txt --universal --python-version $(PYTHON_VERSION)
	$(STRIP_CUDA) requirements.txt > requirements.txt.tmp && mv requirements.txt.tmp requirements.txt

requirements-extra:
	uv pip compile requirements-extra.in -o requirements-extra.txt --universal --python-version $(PYTHON_VERSION)
	$(STRIP_CUDA) requirements-extra.txt > requirements-extra.txt.tmp && mv requirements-extra.txt.tmp requirements-extra.txt

requirements-all: requirements requirements-extra

# Run the unit test suite via pytest (declared in requirements-test.txt). The
# tests are unittest-based, so this is equivalent to CI's
# `python -m unittest discover -s tests -p "test_*.py"`.
test:
	python -m pytest tests/

# Ruff version, kept in sync with .github/workflows/python-lint-check.yml so
# local `make lint` and the CI lint job report identical findings.
RUFF_VERSION := 0.15.14

# Lint gate: Ruff with the repo's ruff.toml (E402 ignored). Black is not run —
# the tree isn't black-formatted and CI treats black as informational only.
# Ruff isn't a project dependency; run it through `uvx` (uv is already required
# for the requirements targets above) so this works in a fresh environment.
lint:
	uvx ruff@$(RUFF_VERSION) check .

# Apply Ruff's safe autofixes. (Deliberately no `black .`: it would reformat
# nearly the whole tree, which isn't the project's convention.)
format:
	uvx ruff@$(RUFF_VERSION) check --fix .
