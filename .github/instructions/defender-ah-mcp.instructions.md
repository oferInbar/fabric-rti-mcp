---
applyTo: "**"
---
# Project Coding Guidelines

This document outlines the coding standards, architectural patterns, and best practices for this project's codebase.
It's designed to help new engineers quickly understand our approach and contribute effectively.

## Project Overview

Defender Advanced Hunting MCP is an MCP server that exposes Microsoft 365 Defender Advanced Hunting
(via the Microsoft Graph Security API) as tools that agents can call to investigate threats across
devices, identities, email, and cloud apps. It is an internal project.

## Code Style and Formatting

### Python Version

- Python 3.10 is required for this project

### Formatting

- We use [ruff](https://docs.astral.sh/ruff/) for formatting and linting with a line length of 120 characters
- Run `make precommit` before committing to ensure formatting, linting, type checks, and tests pass

### Imports

- Group imports in the following order:
  1. Standard library imports
  2. Third-party library imports
  3. Project imports
- Within each group, use alphabetical ordering
- Use absolute imports for project modules (e.g. `package.submodule.module`), not relative imports (e.g. `..module`)
- Never use wildcard imports (e.g. `from package import *`)

Example:
```python
import os
from dataclasses import dataclass

import httpx
from mcp.server.fastmcp import FastMCP

from defender_ah_mcp.services.hunting import hunting_service
```

### Type Annotations

- Use type hints for all function parameters and return values
- Use generics (TypeVar) when appropriate
- Prefer composition of simple types over complex nested types
- Prefer built-in/primitive container types (e.g. `list[str]`, `dict[str, Any]`) over `typing` aliases (e.g. `List[str]`, `Dict[str, Any]`)
- Prefer union syntax for optionals (e.g. `str | None`) over `Optional[str]`

### Naming Conventions

- **Classes**: `PascalCase`
- **Functions/Methods**: `snake_case`
- **Variables**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private members**: Prefix with underscore (`_private_method`)
- **Type variables**: Single uppercase letter or `PascalCase` with descriptive name

### Comments and TODOs

- Use TODOs to mark areas that need improvement, but include specific details about what needs to be done
- For complex algorithms or non-obvious code, include explanatory comments
- Avoid commented-out code in the main branch

### Documentation
* DO NOT add obvious comments that repeat the code. Instead, focus on explaining the "why" behind complex logic or design decisions.
* DO NOT add top-level docstrings for modules or files. Focus on function / class docstrings instead.
* DO NOT add any other module-level documentation; keep documentation scoped to functions, classes, and methods.
* Be concise. Only document the bare minimum necessary to understand the code.

## Architecture & Design

### High-Level Structure
- The project is organized into modules, each responsible for a specific service (e.g., `hunting`)
- Each service declares its tools under `{service_name}_tools.py`. This should be a light wrapper around the service module named `{service_name}_service.py`.
- All logic goes in the service module (e.g., `hunting_service.py`), which contains the core functionality and business logic.
- If extra modules or classes are needed, make sure to split them out in a meaningful way (avoid `utils.py` or `helpers.py` files that contain unrelated functions).


### Development Guidelines
- Make sure to properly document each tool's function, its arguments, and expected output. If the output is complex, show an example of the output format.
- Avoid using `@tool` decorators; prefer a bootstrapping function (`service_module.register_tools(mcp_instance)`). This allows for better flexibility and decoupling.
- Minimize the number of tools. Too many tools confuse agents. Make sure each tool has a clear purpose and is not redundant with others.
- The current design allows stateless hosting. Do not share state in-memory between tools. Assume that each tool invocation is stateless and independent.
- You can use environment variables to "store" configuration in an idempotent way.
- Environment variables must be namespaced to avoid conflicts. Use the `DEFENDER_AH_` prefix for general-purpose configuration shared across the server, and feature-specific prefixes (e.g., `HUNTING_`, `DEFENDER_GRAPH_`) for narrowly scoped settings.
- When stumbling upon a common pattern, do your best to extract it into a reusable common module. This reduces code duplication and improves maintainability.
- ALWAYS RUN `make precommit` before committing to ensure that all checks pass and the code is formatted correctly. It is intentionally not hooked into a git pre-commit hook to keep the model flexible, so you need to run it manually.


### Complex Design Decisions
- When faced with a complex design decision, document the reasoning behind the chosen approach
- Be clear about the trade-offs and alternatives considered
- Review common practices in similar projects and patterns adoptable from other languages (like Rust) that can be applied to Python
- If the decision isn't clear-cut, consult with the owner: present the options with pros/cons and suggest at least 3 alternatives

## Testing Guidelines

### Test Structure
- Use pytest for all tests
- Group tests by module/functionality in the `tests/` directory
- Follow the Arrange-Act-Assert pattern for test structure
- Focus on testing specific service code (e.g. `hunting`). No need to test common code.
- Use `MagicMock` for mocking external dependencies. Mock all IO calls in unit tests.
