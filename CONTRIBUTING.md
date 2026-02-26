# Contributing to fassung

Thank you for your interest in contributing to `fassung`! This guide will help you get started with the development environment.

## Development

`fassung` uses `uv` for dependency management, `ruff` for formatting & linting, `pyright` for type checking, and `pytest` for testing.

### Setup

To install the dependencies including dev-dependencies, run:

```bash
uv sync --dev --locked
```


### Pre-commit hooks

`fassung` uses `prek` for pre-commit hooks. To install the hooks, run:

```bash
prek install
```

### Coding Standards

We use `ruff` for linting and formatting, and `pyright` for static type analysis.

To format the code:
```bash
uv run ruff format .
```

To lint the code:
```bash
uv run ruff check .
```

To run type checking:
```bash
uv run pyright
```

### Infrastructure

The project requires a PostgreSQL database for some tests. Create an `.env` file in the project root directory and add the following lines:

```bash
POSTGRES_USER=<your user>
POSTGRES_PASSWORD=<your password>
POSTGRES_DB=<your database>
```

Then you can start the database using Docker Compose:

```bash
docker compose up -d
```

### Testing

To run the tests with coverage and generate a report, run:

```bash
uv run pytest --cov --cov-report=term
```
