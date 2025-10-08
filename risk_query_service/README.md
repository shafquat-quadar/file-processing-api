# Risk Query Service

A production-ready FastAPI microservice that exposes OneDrive-based risk action and permission reports to Microsoft Copilot Studio.

## Features

- Lazy TSV parsing with [Polars](https://www.pola.rs/) for 200k+ line datasets
- Supports locally synced OneDrive folders and Microsoft Graph fallback
- Cursor-based pagination and top-N summaries optimised for Copilot tools
- Swagger 2.0 export compatible with Copilot Studio Custom Connectors
- API key authentication via the `x-api-key` header

## Getting started

### 1. Clone and install

```bash
make install
```

### 2. Configure environment

Copy the template and edit as required:

```bash
cp .env.example .env
```

Key variables:

- `API_KEY`: Shared secret required in the `x-api-key` header.
- `ONEDRIVE_LOCAL_PATH`: Preferred mode – absolute path to the synced folder containing `RS_*` files.
- `MS_TENANT_ID`, `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, `MS_DRIVE_ID`, `MS_FOLDER_PATH`: Microsoft Graph credentials when a local path is unavailable.
- `ENABLE_CORS`: Set to `true` to allow browser-based integrations.

### 3. Run the service

```bash
make run
```

The API will be available at `http://127.0.0.1:8000`.

### 4. Authenticate requests

All endpoints require `x-api-key: <API_KEY>`.

### 5. Example requests

Fetch schema metadata:

```bash
curl -H "x-api-key: $API_KEY" http://127.0.0.1:8000/meta/schema
```

Query high-risk actions (50 row default):

```bash
curl -G \
  -H "x-api-key: $API_KEY" \
  --data-urlencode "risk_level=High" \
  --data-urlencode "limit=50" \
  http://127.0.0.1:8000/risk/actions/query
```

Summarise by risk level:

```bash
curl -G \
  -H "x-api-key: $API_KEY" \
  --data-urlencode "groupby=Risk Level" \
  http://127.0.0.1:8000/risk/actions/summary
```

### Swagger 2.0 for Copilot Studio

The standard OpenAPI 3 document is exposed at `/openapi.json`. A converted Swagger 2.0 document is available at `/swagger2.json`:

```bash
curl -H "x-api-key: $API_KEY" http://127.0.0.1:8000/swagger2.json
```

Use the Swagger 2.0 JSON when creating a Custom Connector (REST API) in Copilot Studio. Map the connector's security settings to pass the `x-api-key` header.

## Microsoft Graph mode

When `ONEDRIVE_LOCAL_PATH` is unset, the service automatically downloads the latest files from OneDrive via the Microsoft Graph API. Files are cached in `CACHE_DIR` and refreshed every 15 minutes per report type. Ensure the app registration has `Files.Read.All` application permissions.

## Development workflow

- `make test` – run the pytest suite (uses sample fixtures)
- `make docker` – build the production container image
- `make fmt` – placeholder target for future formatters

## Running inside Docker

```bash
docker build -t risk-query-service .
docker run --rm -p 8000:8000 --env-file .env risk-query-service
```

## Notes

- The service never loads entire files into memory; Polars lazy scans keep memory usage predictable.
- Latest report selection is cached for 60 seconds. File hashes guard cursor validity.
- Requests exceeding three seconds are flagged as `partial` with a continuation cursor.
