# UltimaScraperBackend

`UltimaScraperBackend` is the FastAPI control plane and worker-service package
used by UltimaScraper (USR). Its published distribution is
`ultima-scraper-backend` and its Python import is `ultima_scraper_backend`.

The package owns USR's generic API, job execution, plugin runtime, queue
coordination, and archive-service integration. Product launchers, WebUI
code, Compose files, and end-to-end tests remain in the USR repository.

## Ownership boundary

- This package may depend on UltimaScraperCore, UltimaScraperDB, and
  UltimaScraperDBArchive.
- It must remain unaware of OnlySiterips and the Degen backend/database.
- OSR reaches the running USR service over its archive API; it does not import
  this package.

## Development

```bash
uv sync
uv run python -c "import ultima_scraper_backend"
```

USR keeps the integration test suite that exercises this package in the full
application runtime.

Set `USR_PLUGINS_DIR` to an explicit runtime catalog. For source-checkout
development, USR sets `USR_REPOSITORY_ROOT` and the backend resolves the
catalog as `<repository-root>/plugins`.
