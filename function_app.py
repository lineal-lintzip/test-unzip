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
from azure.storage.fileshare import ShareServiceClient

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ---------------------------------------------------------------------------
# UNC parsing
# ---------------------------------------------------------------------------

UNC_RE = re.compile(
    r"^\\\\(?P<account>[a-z0-9-]+)\.file\.core\.windows\.net"
    r"\\(?P<share>[^\\]+)(?:\\(?P<path>.*))?$",
    re.IGNORECASE,
)


class ValidationError(Exception):
    pass


@dataclass
class AzureFilesPath:
    account: str
    share: str
    path: str

    @property
    def account_url(self) -> str:
        return f"https://{self.account}.file.core.windows.net"

    @property
    def normalized_path(self) -> str:
        return self.path.replace("\\", "/").strip("/")


def parse_unc(unc: str) -> AzureFilesPath:
    value = (unc or "").strip()
    m = UNC_RE.match(value)
    if not m:
        raise ValidationError(
            r"UNC must look like \\account.file.core.windows.net\share\path"
        )
    path = (m.group("path") or "").replace("\\", "/").strip("/")
    return AzureFilesPath(account=m.group("account"), share=m.group("share"), path=path)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Azure Files helpers
# ---------------------------------------------------------------------------

def get_share_service(account: str) -> ShareServiceClient:
    cred = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return ShareServiceClient(
        account_url=f"https://{account}.file.core.windows.net",
        credential=cred,
    )


def ensure_dirs(share_client, remote_path: str) -> None:
    parent = str(Path(remote_path).parent).replace("\\", "/").strip("/")
    if not parent or parent == ".":
        return
    current = ""
    for part in parent.split("/"):
        current = f"{current}/{part}".strip("/")
        try:
            share_client.get_directory_client(current).create_directory()
        except ResourceExistsError:
            pass


# ---------------------------------------------------------------------------
# HTTP triggers
# ---------------------------------------------------------------------------

@app.route(route="jobs", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_unzip_job(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return _json_response({"error": "Request body must be valid JSON"}, 400)

    try:
        instance_id = await client.start_new("unzip_orchestrator", client_input=body)
    except Exception as exc:
        logging.exception("Failed to start orchestration")
        return _json_response({"error": str(exc)}, 500)

    base = str(req.url).rstrip("/").rsplit("/api/jobs", 1)[0]
    return _json_response({
        "instanceId": instance_id,
        "statusQueryGetUri": f"{base}/api/jobs/{instance_id}",
        "terminatePostUri": f"{base}/api/jobs/{instance_id}/terminate",
    }, 202)


@app.route(route="jobs/{instance_id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_job_status(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id")
    status = await client.get_status(instance_id)
    if status is None:
        return _json_response({"error": "Job not found"}, 404)

    return _json_response({
        "instanceId": status.instance_id,
        "runtimeStatus": _safe(status.runtime_status),
        "createdTime": status.created_time.isoformat() if status.created_time else None,
        "lastUpdatedTime": status.last_updated_time.isoformat() if status.last_updated_time else None,
        "input": _safe(status.input_),
        "customStatus": _safe(status.custom_status),
        "output": _safe(status.output),
    }, 200)


@app.route(route="jobs/{instance_id}/terminate", methods=["POST"])
@app.durable_client_input(client_name="client")
async def terminate_job(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id")
    await client.terminate(instance_id, "Terminated by user.")
    return _json_response({"instanceId": instance_id, "message": "Terminated."}, 202)


@app.route(route="jobs/{instance_id}", methods=["DELETE"])
@app.durable_client_input(client_name="client")
async def purge_job_history(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get("instance_id")
    await client.purge_instance_history(instance_id)
    return _json_response({"instanceId": instance_id, "message": "History purged."}, 200)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

@app.orchestration_trigger(context_name="context")
def unzip_orchestrator(context: df.DurableOrchestrationContext):
    job = context.get_input() or {}

    # Step 1 — validate
    validated = yield context.call_activity("act_validate", job)
    yield context.set_custom_status({"stage": "validated", "progress": 5})

    # Step 2 — download
    download = yield context.call_activity("act_download", validated)
    yield context.set_custom_status({"stage": "downloaded", "progress": 30})

    # Step 3 — extract
    extracted = yield context.call_activity("act_extract", {
        "validated": validated,
        "download": download,
    })
    yield context.set_custom_status({
        "stage": "extracted",
        "progress": 60,
        "fileCount": extracted["file_count"],
    })

    # Step 4 — upload
    upload = yield context.call_activity("act_upload", {
        "validated": validated,
        "extracted": extracted,
    })

    status = "completed" if upload["failed"] == 0 else "completed_with_errors"
    yield context.set_custom_status({"stage": "done", "progress": 100, "status": status})

    return {
        "status": status,
        "source_unc": validated["source_unc"],
        "destination_unc": validated["destination_unc"],
        "totalFiles": extracted["file_count"],
        "uploaded": upload["uploaded"],
        "skipped": upload["skipped"],
        "failed": upload["failed"],
        "failures": upload["failures"],
        "completedAt": context.current_utc_datetime.isoformat(),
    }


# ---------------------------------------------------------------------------
# Activities
# ---------------------------------------------------------------------------

@app.activity_trigger(input_name="job")
def act_validate(job: Dict[str, Any]) -> Dict[str, Any]:
    source_unc = (job.get("source_unc") or "").strip()
    destination_unc = (job.get("destination_unc") or "").strip()
    overwrite = bool(job.get("overwrite", True))
    zip_password = job.get("zip_password") or None  # None means no password

    if not source_unc:
        raise ValidationError("source_unc is required")
    if not destination_unc:
        raise ValidationError("destination_unc is required")

    source = parse_unc(source_unc)
    destination = parse_unc(destination_unc)

    if not source.normalized_path.lower().endswith(".zip"):
        raise ValidationError("source_unc must point to a .zip file")

    return {
        "source_unc": source_unc,
        "destination_unc": destination_unc,
        "overwrite": overwrite,
        "zip_password": zip_password,
        "source": asdict(source),
        "destination": asdict(destination),
        "zip_name": Path(source.normalized_path).name,
    }


@app.activity_trigger(input_name="validated")
def act_download(validated: Dict[str, Any]) -> Dict[str, Any]:
    source = AzureFilesPath(**validated["source"])
    work_dir = tempfile.mkdtemp(prefix="unzip-")
    local_zip = os.path.join(work_dir, validated["zip_name"])

    svc = get_share_service(source.account)
    share = svc.get_share_client(source.share)
    file_client = share.get_file_client(source.normalized_path)

    with open(local_zip, "wb") as fh:
        data = file_client.download_file().readall()
        fh.write(data)

    logging.info("Downloaded %d bytes to %s", len(data), local_zip)
    return {"work_dir": work_dir, "local_zip": local_zip, "bytes": len(data)}


@app.activity_trigger(input_name="payload")
def act_extract(payload: Dict[str, Any]) -> Dict[str, Any]:
    download = payload["download"]
    validated = payload["validated"]
    extract_dir = os.path.join(download["work_dir"], "extracted")
    os.makedirs(extract_dir, exist_ok=True)

    zip_password = validated.get("zip_password")
    pwd = zip_password.encode("utf-8") if zip_password else None

    files = []
    with zipfile.ZipFile(download["local_zip"], "r") as zf:
        # Safety check - reject zip-slip entries
        bad = [m.filename for m in zf.infolist()
               if Path(m.filename).is_absolute() or ".." in Path(m.filename).parts]
        if bad:
            raise ValidationError(f"Unsafe zip entries: {bad[:3]}")

        zf.extractall(path=extract_dir, pwd=pwd)

    for dirpath, _, filenames in os.walk(extract_dir):
        for fname in filenames:
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, extract_dir).replace("\\", "/")
            files.append({"relative_path": rel, "size_bytes": os.path.getsize(full)})

    files.sort(key=lambda f: f["relative_path"])
    logging.info("Extracted %d files", len(files))
    return {
        "work_dir": download["work_dir"],
        "extract_dir": extract_dir,
        "file_count": len(files),
        "files": files,
    }


@app.activity_trigger(input_name="payload")
def act_upload(payload: Dict[str, Any]) -> Dict[str, Any]:
    validated = payload["validated"]
    extracted = payload["extracted"]
    destination = AzureFilesPath(**validated["destination"])
    overwrite = validated["overwrite"]

    svc = get_share_service(destination.account)
    share = svc.get_share_client(destination.share)

    logging.info("UPLOAD destination account=%s share=%s path=%s",
                 destination.account, destination.share, destination.normalized_path)

    # Ensure every segment of the destination folder exists
    if destination.normalized_path:
        current = ""
        for part in destination.normalized_path.split("/"):
            current = f"{current}/{part}".strip("/")
            try:
                share.get_directory_client(current).create_directory()
                logging.info("UPLOAD created directory: %s", current)
            except ResourceExistsError:
                logging.info("UPLOAD directory already exists: %s", current)

    uploaded = skipped = failed = 0
    failures: List[Dict[str, str]] = []

    for item in extracted["files"]:
        rel = item["relative_path"]
        local_path = os.path.join(extracted["extract_dir"], rel)
        remote_path = "/".join(p for p in [destination.normalized_path, rel] if p)

        logging.info("UPLOAD file: local=%s remote=%s", local_path, remote_path)

        try:
            ensure_dirs(share, remote_path)
            fc = share.get_file_client(remote_path)

            if not overwrite:
                try:
                    fc.get_file_properties()
                    skipped += 1
                    continue
                except ResourceNotFoundError:
                    pass

            with open(local_path, "rb") as data:
                fc.upload_file(data, overwrite=True)
            uploaded += 1
            logging.info("UPLOAD success: %s", remote_path)

        except Exception as exc:
            failed += 1
            failures.append({"path": remote_path, "error": str(exc)})
            logging.error("UPLOAD failed: %s | error: %s", remote_path, exc, exc_info=True)

    try:
        shutil.rmtree(extracted["work_dir"], ignore_errors=True)
    except Exception:
        pass

    logging.info("Upload complete: %d uploaded, %d skipped, %d failed", uploaded, skipped, failed)
    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "failures": failures[:50],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, list, dict)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _json_response(body: Any, status: int) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body),
        status_code=status,
        mimetype="application/json",
    )
