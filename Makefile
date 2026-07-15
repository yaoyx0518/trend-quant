.PHONY: test test-unit test-integration test-api test-all test-cov test-cov-html test-deps

# Install test dependencies
test-deps:
	pip install pytest pytest-cov pytest-mock pytest-asyncio httpx

# Unit tests only (fastest — pure functions, no IO)
test-unit:
	pytest tests/unit/ -v -m "unit" --tb=short

# Integration tests (real classes + isolated DB, filesystem)
test-integration:
	pytest tests/integration/ -v -m "integration" --tb=short

# API tests (FastAPI TestClient)
test-api:
	pytest tests/api/ -v -m "api" --tb=short

# All fast tests (exclude "slow" marker)
test:
	pytest tests/ -v -m "not slow" --tb=short

# Every test including slow ones
test-all:
	pytest tests/ -v --tb=short

# Coverage report (terminal)
test-cov:
	pytest tests/ -m "not slow" --cov --cov-report=term-missing

# Coverage report (HTML)
test-cov-html:
	pytest tests/ -m "not slow" --cov --cov-report=html
	@echo "Coverage report → htmlcov/index.html"
