# AGENTS.md

This file provides guidance for agentic coding agents working in this repository.

## Project Overview

**stock-mcp** is an open-source financial data service providing MCP and HTTP APIs for AI agents. It aggregates market data from multiple providers (Tushare, Akshare, Baostock, Yahoo, Finnhub, etc.) and exposes them through a unified plugin-based runtime.

- **Python**: 3.11-3.12
- **Package Manager**: uv
- **Web Framework**: FastAPI
- **MCP Framework**: FastMCP
- **Testing**: pytest

---

## Build/Lint/Test Commands

### Installation
```bash
uv sync --dev          # Install all dependencies including dev dependencies
cp .env.example .env    # Copy environment configuration
```

### Running the Service
```bash
# Local development with uv
uv run python -m uvicorn src.server.app:app --host 127.0.0.1 --port 9898

# Docker Compose
docker compose up -d --build

# MCP stdio mode
uv run python -c "import src.server.mcp.server as m; m.create_mcp_server().run(transport='stdio')"
```

### Testing
```bash
# Run all tests
uv run pytest

# Run single test file
uv run pytest tests/test_runtime_capability_registry.py

# Run single test function
uv run pytest tests/test_runtime_capability_registry.py::test_capability_registry_hides_capabilities_without_required_contracts

# Run tests matching pattern
uv run pytest -k "market"

# Run with verbose output
uv run pytest -v

# Run integration tests only (marked with @pytest.mark.integration)
uv run pytest -m integration
```

### Code Quality
```bash
# Format code (if ruff/black configured)
ruff format .

# Lint (if ruff configured)
ruff check .

# Type checking (if mypy configured)
mypy src/
```

---

## Architecture

### Directory Structure
```
src/server/
├── app.py                    # Thin entrypoint
├── auth_support.py           # Authentication helpers
├── capabilities/             # Business capability plugins
│   ├── code_export/
│   ├── filings/
│   ├── fundamental/
│   ├── market/
│   ├── money_flow/
│   ├── news/
│   └── technical/
├── config/
│   └── settings.py           # Pydantic settings (legacy)
├── core/
│   └── dependencies.py       # Dependency injection container
├── domain/                   # Domain services (symbol resolution, routing)
├── infrastructure/           # External connections (Redis, Postgres)
├── providers/                # Data source plugins
├── runtime/                  # Core runtime (auth, lifecycle, registries)
├── transports/               # Protocol adapters (HTTP, MCP)
└── utils/                    # Logging, helpers
```

### Plugin Architecture

#### Capability Plugins
Each capability follows this structure:
```
capabilities/<name>/
├── http.py       # FastAPI router factory (build_router(runtime) -> APIRouter)
├── mcp.py        # MCP tool registration function (register_<name>_tools(mcp))
├── plugin.py     # Plugin definition (CapabilityPlugin instance)
├── schemas.py    # Pydantic request/response models
└── service.py    # Business logic (optional service class)
```

#### Provider Plugins
Data sources declare which contracts they implement:
```python
# src/server/providers/contracts.py
REALTIME_PRICE = "realtime_price"
HISTORICAL_PRICE = "historical_price"
TECHNICAL_INDICATORS = "technical_indicators"
# ... etc
```

---

## Code Style Guidelines

### Imports
- Always use `from __future__ import annotations` at the top of Python files
- Use absolute imports from `src.server`: `from src.server.capabilities.market.service import ...`
- Group imports: stdlib → third-party → local (separated by blank lines)
- Sort imports alphabetically within groups

```python
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.server.runtime.models import RuntimeContext
from src.server.utils.logger import logger
```

### Type Annotations
- Use modern type hints with Python 3.11+ syntax
- Use `|` for union types: `dict[str, Any] | None` instead of `Optional[dict[str, Any]]`
- Use Pydantic for all API schemas and configuration
- Avoid `typing.Any` when possible; prefer explicit types

### Naming Conventions
| Element | Convention | Example |
|---------|-----------|--------|
| Classes | PascalCase | `MarketCapabilityService` |
| Functions/methods | snake_case | `get_real_time_price` |
| Variables | snake_case | `historical_prices` |
| Constants | SCREAMING_SNAKE_CASE | `REALTIME_PRICE` |
| Private members | _leading_underscore | `self._runtime` |
| Type variables | PascalCase | `T` in generics |

### Pydantic Schemas
```python
class GetHistoricalPricesRequest(BaseModel):
    symbol: str
    period: str = "30d"
    interval: str = "1d"

class MarketReportResponse(BaseModel):
    symbol: str
    info: dict[str, Any] | None = None
    timestamp: datetime
```

### Error Handling

#### Custom Exceptions
Define custom exceptions with structured error information:
```python
class SymbolResolutionError(Exception):
    def __init__(self, code: str, message: str, raw: str, candidates: list[str] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.raw = raw
        self.candidates = candidates or []

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "raw": self.raw,
            "candidates": self.candidates,
        }
```

#### HTTP Routes
```python
@router.get("/asset/info")
async def get_asset_info(symbol: str = Query(..., description="资产代码")):
    try:
        result = await service.get_asset_info(symbol)
        return result or {"error": f"Asset not found: {symbol}"}
    except Exception as exc:
        logger.error(f"Capability API error in get_asset_info: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get asset info: {exc}"
        ) from exc
```

#### MCP Tools
```python
@mcp.tool(tags={"market-quote"})
async def get_real_time_price(symbol: str, ctx: Context | None = None) -> Any:
    try:
        if ctx:
            await ctx.info(f"💹 获取实时价格: {symbol}")
        result = await service.get_real_time_price(symbol)
        if not result:
            return create_mcp_error_result(f"Price not found for {symbol}", error_code="NOT_FOUND")
        # ... return artifact response
    except SymbolResolutionError as exc:
        return create_symbol_error_response(exc, variant="real_time_price", name=f"{symbol} 实时报价")
    except Exception as exc:
        logger.error(f"Capability MCP error in get_real_time_price: {exc}")
        return create_mcp_error_result(str(exc))
```

### Logging
Use structlog via `src.server.utils.logger`:
```python
from src.server.utils.logger import logger

logger.info("Processing request", symbol=symbol, period=period)
logger.error(f"Failed to fetch data: {exc}", exc_info=True)
```

### Logging Configuration
- JSON output with `ensure_ascii=False` for emoji support
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Include context variables in log calls

### Runtime Context Pattern
Access shared runtime through dependency injection:
```python
from src.server.runtime import get_runtime_context

def my_function():
    runtime = get_runtime_context()
    # Use runtime.provider_facade, runtime.capability_registry, etc.
```

### MCP Artifact Response Pattern
Use the artifacts module for structured MCP responses:
```python
from src.server.transports.mcp.artifacts import (
    create_artifact_envelope,
    create_artifact_response,
    create_mcp_error_result,
    create_symbol_error_response,
)

# Return success with artifact
return create_artifact_response(
    summary=f"已获取 {symbol} 的K线数据，共 {count} 条。",
    artifact=artifact,
)

# Return error
return create_mcp_error_result(str(exc), error_code="INTERNAL_ERROR")
```

### HTTP Router Pattern
```python
def build_router(runtime: RuntimeContext) -> APIRouter:
    router = APIRouter(prefix="/api/v1/market", tags=["Market"])
    service = get_market_capability_service(runtime)

    @router.post("/endpoint", summary="描述")
    async def handler(request: RequestSchema):
        # implementation
        pass

    return router
```

### Plugin Definition Pattern
```python
from src.server.runtime.models import CapabilityPlugin

plugin = CapabilityPlugin(
    name="market",
    description="Market data capability",
    required_contracts=frozenset({REALTIME_PRICE, HISTORICAL_PRICE}),
    http_routers=(build_router,),
    mcp_registrars=(register_market_tools,),
)
```

---

## Configuration

### Environment Variables
- Use `pydantic_settings.BaseSettings` with `validation_alias` for env var mapping
- Nested config uses `__` delimiter: `REDIS__HOST` maps to `RedisConfig(host=...)`
- See `.env.example` for all available variables

### Key Settings
```python
class TimeoutConfig(BaseAppSettings):
    mcp_tool_seconds: float = Field(35.0, validation_alias="MCP_TOOL_TIMEOUT_SECONDS")
    provider_call_seconds: float = Field(12.0, validation_alias="PROVIDER_CALL_TIMEOUT_SECONDS")
```

---

## Testing Guidelines

### Test Structure
- Tests go in `tests/` directory
- One test file per module: `test_<module_name>.py`
- Use descriptive test names: `test_capability_registry_hides_capabilities_without_required_contracts`

### Test Patterns
```python
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.server.runtime.capability_registry import CapabilityRegistry

def test_something():
    registry = CapabilityRegistry(...)
    # assertions
    assert [plugin.name for plugin in registry.list_enabled()] == ["expected"]
```

### Pytest Markers
- `@pytest.mark.integration` for tests requiring external services/network

---

## Common Patterns

### Adding a New Capability
1. Create `src/server/capabilities/<name>/` directory
2. Add `schemas.py` with Pydantic request/response models
3. Add `service.py` with business logic
4. Add `http.py` with FastAPI router factory
5. Add `mcp.py` with MCP tool registration
6. Add `plugin.py` with `CapabilityPlugin` instance
7. Register in `src/server/capabilities/registry.py`

### Adding a New Provider
1. Define contracts in `src/server/providers/contracts.py`
2. Create provider plugin in `src/server/providers/`
3. Register in `src/server/providers/registry.py`

### Symbol Resolution
- Use `SymbolResolutionError` for symbol parsing failures
- Include candidates for typo suggestions
- Handle `SymbolResolutionError` in HTTP/MCP handlers for user-friendly errors
