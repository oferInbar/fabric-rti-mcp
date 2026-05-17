fmt: format check

format:
	ruff format defender_ah_mcp

check:
	ruff check defender_ah_mcp --fix

test:
	pytest

typecheck:
	ty check defender_ah_mcp

precommit: fmt typecheck test

ci:
	ruff format --check defender_ah_mcp
	ruff check defender_ah_mcp
	ty check defender_ah_mcp
	pytest tests

run:
	uvx .