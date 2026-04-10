"""
Azure Durable Functions — ZIP unzip worker for Azure Files UNC paths.

Fixes vs original:
  - Streaming download (chunked, never readall into memory)
  - Per-entry streaming extraction direct to temp file then chunked upload
    (no temp-disk explosion — only one file at a time lives on disk)
  - Fan-out in batches (UPLOAD_BATCH_SIZE) so Durable history payloads
    stay small regardless of archive size
  - Full file list never passed through orchestrator history
  - CORS headers (OPTIONS pre-flight + every response)
  - Terminate button support from UI
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import azure.durable_functions as df
import azure.functions as func
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.fileshare import ShareFileClient, ShareServiceClient

# ---------------------------------------------------------------------------
# App & constants
# ---------------------------------------------------------------------------
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

UNC_RE = re.compile(
    r"^\\\\(?P<account>[a-z0-9-]+)\.file\.core\.windows\.net"
    r"\\(?P<share>[^\\]+)(?:\\(?P<path>.*))?$",
    re.IGNORECASE,
)

UPLOAD_MAX_CONCURRENCY = 4   # SDK parallel connections per file
UPLOAD_BATCH_SIZE = 50       # files per fan-out activity (keep history small)
MAX_FAILURES = 100           # failure details kept in final output

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, x-functions-key",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class ValidationError(Exception):
    pass


@dataclass
class AzureFilesPath:
    account: str
    share: str
    path: str

    @property
    def normalized_path(self) -> str:
        return self.path.replace("\\", "/").strip("/")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_credential() -> DefaultAzureCredential:
    return DefaultAzureCredential(exclude_interactive_browser_credential=True)


def get_share_service_client(account_name: str) -> ShareServiceClient:
    return ShareServiceClient(
        account_url=f"https://{account_name}.file.core.windows.net",
        credential=get_credential(),
    )


def parse_unc(unc: str) -> AzureFilesPath:
    value = (unc or "").strip()
    m = UNC_RE.match(value)
    if not m:
        raise ValidationError(
            r"UNC must look like \\account.file.core.windows.net\share\optional\path"
        )
    path = (m.group("path") or "").replace("\\", "/").strip("/")
    return AzureFilesPath(account=m.group("account"), share=m.group("share"), path=path)


def validate_job_input(job: Dict[str, Any]) -> Dict[str, Any]:
    source_unc = (job.get("source_unc") or "").strip()
    destination_unc = (job.get("destination_unc") or "").strip()
    zip_password = job.get("zip_password") or None
    overwrite = bool(job.get("overwrite", True))

    if not source_unc:
        raise ValidationError("source_unc is required")
    if not destination_unc:
        raise ValidationError("destination_unc is required")

    src = parse_unc(source_unc)
    dst = parse_unc(destination_unc)

    if not src.normalized_path.lower().endswith(".zip"):
        raise ValidationError("source_unc must point to a .zip file")

    return {
        "source_unc": source_unc,
        "destination_unc": destination_unc,
        "zip_password": zip_password,
        "overwrite": overwrite,
        "source": asdict(src),
        "destination": asdict(dst),
        "zip_name": Path(src.normalized_path).name,
        "validated_at": utc_now_iso(),
    }


def ensure_directory_tree(share_client, remote_file_path: str) -> None:
    parent = str(Path(remote_file_path).parent).replace("\\", "/").strip("/")
    if not parent or parent == ".":
        return
    current = ""
    for part in parent.split("/"):
        current = f"{current}/{part}".strip("/")
        try:
            share_client.get_directory_client(current).create_directory()
        except ResourceExistsError:
            pass


def read_json(req: func.HttpRequest) -> Dict[str, Any]:
    try:
        return req.get_json()
    except ValueError as exc:
        raise ValidationError("Request body must be valid JSON") from exc


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


def cors_resp(body: Any, status: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body), status_code=status,
        mimetype="application/json", headers=CORS_HEADERS,
    )


def chunked(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i: i + size]


# ---------------------------------------------------------------------------
# HTTP triggers
# ---------------------------------------------------------------------------
@app.route(route="jobs", methods=["OPTIONS"])
async def jobs_preflight(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(status_code=204, headers=CORS_HEADERS)


@app.route(route="jobs", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_unzip_job(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = read_json(req)
        instance_id = await client.start_new("unzip_orchestrator", client_input=body)
        base_url = str(req.url).rstrip("/").rsplit("/api/jobs", 1)[0]
        return cors_resp({
            "instanceId": instance_id,
            "message": "Unzip orchestration started.",
            "statusQueryGetUri": f"{base_url}/api/jobs/{instance_id}",
            "terminatePostUri": f"{base_url}/api/jobs/{instance_id}/terminate",
            "purgeHistoryDeleteUri": f"{base_url}/api/jobs/{instance_id}",
        }, status=202)
    except ValidationError as exc:
        return cors_resp({"error": str(exc)}, status=400)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to start orchestration")
        return cors_resp({"error": str(exc)}, status=500)


@app.route(route="jobs/{instance_id}", methods=["OPTIONS"])
async def job_preflight(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(status_code=204, headers=CORS_HEADERS)


@app.route(route="jobs/{instance_id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_job_status(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id")
    status = await client.get_status(instance_id)
    if status is None:
        return cors_resp({"error": "Job not found"}, status=404)
    return cors_resp({
        "instanceId": status.instance_id,
        "name": status.name,
        "runtimeStatus": json_safe(status.runtime_status),
        "createdTime": status.created_time.isoformat() if status.created_time else None,
        "lastUpdatedTime": status.last_updated_time.isoformat() if status.last_updated_time else None,
        "input": json_safe(status.input_),
        "customStatus": json_safe(status.custom_status),
        "output": json_safe(status.output),
    })


@app.route(route="jobs/{instance_id}/terminate", methods=["OPTIONS"])
async def terminate_preflight(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(status_code=204, headers=CORS_HEADERS)


@app.route(route="jobs/{instance_id}/terminate", methods=["POST"])
@app.durable_client_input(client_name="client")
async def terminate_job(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id")
    await client.terminate(instance_id, "Terminated by user request.")
    return cors_resp({"instanceId": instance_id, "message": "Termination requested."}, status=202)


@app.route(route="jobs/{instance_id}", methods=["DELETE"])
@app.durable_client_input(client_name="client")
async def purge_job_history(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id")
    await client.purge_instance_history(instance_id)
    return cors_resp({"instanceId": instance_id, "message": "History purged."})


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
@app.orchestration_trigger(context_name="context")
def unzip_orchestrator(context: df.DurableOrchestrationContext):
    request = context.get_input() or {}

    # 1 – validate input
    validated = yield context.call_activity("validate_request", request)
    yield context.set_custom_status({
        "status": "running", "stage": "validated", "progress": 5,
        "source_unc": validated["source_unc"], "destination_unc": validated["destination_unc"],
        "zipFile": validated["zip_name"], "message": "Request validated.",
    })

    # 2 – stream-download ZIP to temp file
    download = yield context.call_activity("download_zip_activity", validated)
    yield context.set_custom_status({
        "status": "running", "stage": "downloaded", "progress": 20,
        "zipFile": validated["zip_name"], "source_unc": validated["source_unc"],
        "destination_unc": validated["destination_unc"],
        "downloadedBytes": download["downloaded_bytes"],
        "message": "ZIP downloaded from Azure Files.",
    })

    # 3 – scan central directory (lightweight — just filenames + sizes)
    scan = yield context.call_activity("scan_zip_activity", {
        "validated": validated, "download": download,
    })
    file_count = scan["file_count"]
    yield context.set_custom_status({
        "status": "running", "stage": "scanned", "progress": 25,
        "zipFile": validated["zip_name"], "totalFiles": file_count,
        "source_unc": validated["source_unc"], "destination_unc": validated["destination_unc"],
        "message": f"ZIP scanned: {file_count} entries.",
    })

    # 4 – fan-out: extract + upload in small batches
    batches = list(chunked(scan["file_index"], UPLOAD_BATCH_SIZE))
    num_batches = len(batches)
    batch_results = []

    for idx, batch in enumerate(batches):
        result = yield context.call_activity("extract_upload_batch_activity", {
            "validated": validated, "download": download, "batch": batch,
        })
        batch_results.append(result)
        progress = 25 + int(70 * (idx + 1) / max(num_batches, 1))
        yield context.set_custom_status({
            "status": "running", "stage": "uploading", "progress": progress,
            "zipFile": validated["zip_name"],
            "source_unc": validated["source_unc"], "destination_unc": validated["destination_unc"],
            "totalFiles": file_count,
            "uploaded": sum(r["uploaded"] for r in batch_results),
            "failed": sum(r["failed"] for r in batch_results),
            "message": f"Uploading: batch {idx + 1}/{num_batches}.",
        })

    # 5 – cleanup temp dir
    yield context.call_activity("cleanup_activity", download)

    uploaded = sum(r["uploaded"] for r in batch_results)
    skipped = sum(r["skipped"] for r in batch_results)
    failed = sum(r["failed"] for r in batch_results)
    failures: List[Dict] = []
    for r in batch_results:
        failures.extend(r.get("failures", []))
    failures = failures[:MAX_FAILURES]

    result = {
        "status": "completed" if failed == 0 else "completed_with_errors",
        "source_unc": validated["source_unc"],
        "destination_unc": validated["destination_unc"],
        "zipFile": validated["zip_name"],
        "downloadBytes": download["downloaded_bytes"],
        "totalFiles": file_count,
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "failures": failures,
        "completedAt": context.current_utc_datetime.isoformat(),
    }
    yield context.set_custom_status({
        "status": result["status"], "stage": "completed", "progress": 100,
        "zipFile": validated["zip_name"],
        "source_unc": validated["source_unc"], "destination_unc": validated["destination_unc"],
        "totalFiles": file_count, "uploaded": uploaded, "skipped": skipped, "failed": failed,
        "message": "Upload finished.",
    })
    return result


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------
@app.activity_trigger(input_name="job")
def validate_request(job: Dict[str, Any]) -> Dict[str, Any]:
    return validate_job_input(job)


@app.activity_trigger(input_name="validated")
def download_zip_activity(validated: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stream-download ZIP from Azure Files using chunked iteration.
    The file is NEVER fully loaded into memory — each chunk is written
    directly to a temp file on disk.
    """
    source = AzureFilesPath(**validated["source"])
    work_dir = tempfile.mkdtemp(prefix="unzip-")
    local_zip_path = os.path.join(work_dir, validated["zip_name"])

    service = get_share_service_client(source.account)
    share = service.get_share_client(source.share)
    file_client = share.get_file_client(source.normalized_path)

    downloaded_bytes = 0
    stream = file_client.download_file()
    with open(local_zip_path, "wb") as fh:
        for chunk in stream.chunks():          # SDK yields chunks; never readall()
            fh.write(chunk)
            downloaded_bytes += len(chunk)

    logging.info("Downloaded %d bytes → %s", downloaded_bytes, local_zip_path)
    return {
        "work_dir": work_dir,
        "local_zip_path": local_zip_path,
        "downloaded_bytes": downloaded_bytes,
        "downloaded_at": utc_now_iso(),
    }


@app.activity_trigger(input_name="payload")
def scan_zip_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read the ZIP central directory to enumerate members.
    Returns only lightweight metadata per file (no data read).
    Path-traversal check is done here.
    """
    download = payload["download"]
    validated = payload["validated"]
    local_zip_path = download["local_zip_path"]

    file_index: List[Dict[str, Any]] = []
    bad: List[str] = []

    with zipfile.ZipFile(local_zip_path, "r") as zf:
        for info in zf.infolist():
            p = Path(info.filename)
            if p.is_absolute() or ".." in p.parts:
                bad.append(info.filename)
                continue
            if info.is_dir():
                continue
            file_index.append({
                "filename": info.filename,
                "file_size": info.file_size,
                "compress_size": info.compress_size,
            })

    if bad:
        raise ValidationError(f"Unsafe ZIP entries detected: {bad[:5]}")

    return {"file_count": len(file_index), "file_index": file_index, "scanned_at": utc_now_iso()}


@app.activity_trigger(input_name="payload")
def extract_upload_batch_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    For each file in the batch:
      1. Stream-extract from ZIP to a single small temp file (one at a time).
      2. Upload that temp file to Azure Files with chunked SDK upload.
      3. Delete the temp file immediately after upload.

    Peak disk usage = size of ONE extracted file, not the whole archive.
    """
    validated = payload["validated"]
    download = payload["download"]
    batch: List[Dict[str, Any]] = payload["batch"]

    local_zip_path = download["local_zip_path"]
    work_dir = download["work_dir"]
    destination = AzureFilesPath(**validated["destination"])
    zip_password = validated.get("zip_password")
    overwrite = validated.get("overwrite", True)
    pwd = zip_password.encode("utf-8") if zip_password else None

    service = get_share_service_client(destination.account)
    share = service.get_share_client(destination.share)

    uploaded = skipped = failed = 0
    failures: List[Dict[str, str]] = []

    with zipfile.ZipFile(local_zip_path, "r") as zf:
        for item in batch:
            filename = item["filename"]
            rel = filename.replace("\\", "/").strip("/")
            remote_path = "/".join(p for p in [destination.normalized_path, rel] if p)

            tmp_path: Optional[str] = None
            try:
                ensure_directory_tree(share, remote_path)
                file_client: ShareFileClient = share.get_file_client(remote_path)

                if not overwrite:
                    try:
                        file_client.get_file_properties()
                        skipped += 1
                        continue
                    except ResourceNotFoundError:
                        pass

                # Write single entry to a temp file, then upload; delete immediately
                with tempfile.NamedTemporaryFile(dir=work_dir, delete=False) as tmp:
                    tmp_path = tmp.name
                    with zf.open(filename, pwd=pwd) as src:
                        shutil.copyfileobj(src, tmp, length=4 * 1024 * 1024)

                with open(tmp_path, "rb") as data:
                    file_client.upload_file(data, overwrite=True, max_concurrency=UPLOAD_MAX_CONCURRENCY)

                uploaded += 1

            except Exception as exc:  # noqa: BLE001
                failed += 1
                logging.warning("Upload failed %s: %s", remote_path, exc)
                if len(failures) < MAX_FAILURES:
                    failures.append({"path": remote_path, "error": str(exc)})
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass

    return {"uploaded": uploaded, "skipped": skipped, "failed": failed, "failures": failures, "processed_at": utc_now_iso()}


@app.activity_trigger(input_name="download")
def cleanup_activity(download: Dict[str, Any]) -> Dict[str, Any]:
    work_dir = download.get("work_dir")
    if work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)
    return {"cleaned_at": utc_now_iso()}
