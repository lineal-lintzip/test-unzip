# Azure Durable Functions unzip worker for Azure Files UNC paths

This project accepts a source Azure Files UNC path to a ZIP file and a destination Azure Files UNC path, then runs an unzip workflow using Azure Durable Functions.

## Supported input format

Examples:

- `\\account.file.core.windows.net\incoming\drop\archive.zip`
- `\\account.file.core.windows.net\processed\unzipped-output`

These must be **Azure Files UNC paths**, not arbitrary on-prem SMB shares.

## What it does

- `POST /api/jobs` starts an orchestration.
- `GET /api/jobs/{instanceId}` returns orchestration state, custom status, and output.
- `POST /api/jobs/{instanceId}/terminate` terminates a job.
- `DELETE /api/jobs/{instanceId}` purges orchestration history.

Workflow:
1. Validate request.
2. Download ZIP from Azure Files using the Python SDK.
3. Extract ZIP to the function temp disk.
4. Upload extracted files to the destination Azure Files path.
5. Return summary output.

## Local development

1. Install Python 3.11+ and Azure Functions Core Tools.
2. Create and activate a virtual environment.
3. Install dependencies:
   `pip install -r requirements.txt`
4. Copy `local.settings.json.example` to `local.settings.json`.
5. Run Azurite or set a real `AzureWebJobsStorage` connection string.
6. Start the app:
   `func start`

## Example request

```bash
curl -X POST http://localhost:7071/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source_unc": "\\\\mystorage.file.core.windows.net\\incoming\\drop\\archive.zip",
    "destination_unc": "\\\\mystorage.file.core.windows.net\\processed\\archive-output",
    "zip_password": null,
    "overwrite": true
  }'
```

## Managed identity

The code uses `DefaultAzureCredential`, which works well with a managed identity in Azure. Grant the Function App identity data-plane rights on the storage account or specific file shares it needs.

A common starting role is a storage file data contributor/privileged contributor role, depending on your exact permissions model.

## Important deployment notes

- Durable Functions require a valid `AzureWebJobsStorage` account.
- Large ZIPs can exceed temp disk or function time/memory limits.
- For extremely large archives, consider chunking uploads or splitting the workflow further.
- If the storage accounts are network-restricted, configure VNet integration, private endpoints, and DNS accordingly.

## Portal integration notes

Your static site can:
- `POST /api/jobs` on submit
- poll `GET /api/jobs/{instanceId}`
- map `runtimeStatus` and `customStatus.progress` into your UI

## Response shape from status endpoint

```json
{
  "instanceId": "...",
  "name": "unzip_orchestrator",
  "runtimeStatus": "Running",
  "createdTime": "2026-04-02T10:00:00+00:00",
  "lastUpdatedTime": "2026-04-02T10:01:00+00:00",
  "input": {
    "source_unc": "\\\\account.file.core.windows.net\\incoming\\drop\\archive.zip",
    "destination_unc": "\\\\account.file.core.windows.net\\processed\\archive-output",
    "overwrite": true
  },
  "customStatus": {
    "status": "running",
    "stage": "extracted",
    "progress": 60
  },
  "output": null
}
```
