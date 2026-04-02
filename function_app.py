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
from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.fileshare import ShareServiceClient

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

UNC_RE = re.compile(
    r"^\\\\(?P<account>[a-z0-9-]+)\.file\.core\.windows\.net\\(?P<share>[^\\]+)(?:\\(?P<path>.*))?$",
    re.IGNORECASE,
)

MAX_PATHS_TO_RETURN = 100


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
        return self.path.replace('\\', '/').strip('/')


@dataclass
class FileEntry:
    relative_path: str
    size_bytes: int


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
    value = (unc or '').strip()
    match = UNC_RE.match(value)
    if not match:
        raise ValidationError(
            'UNC must look like \\\\account.file.core.windows.net\\share\\optional\\path'
        )

    path = (match.group('path') or '').replace('\\', '/').strip('/')
    return AzureFilesPath(
        account=match.group('account'),
        share=match.group('share'),
        path=path,
    )


def basename_from_unc(unc: str) -> str:
    parsed = parse_unc(unc)
    return Path(parsed.normalized_path).name


def validate_job_input(job: Dict[str, Any]) -> Dict[str, Any]:
    source_unc = (job.get('source_unc') or '').strip()
    destination_unc = (job.get('destination_unc') or '').strip()
    zip_password = job.get('zip_password')
    overwrite = bool(job.get('overwrite', True))

    if not source_unc:
        raise ValidationError('source_unc is required')
    if not destination_unc:
        raise ValidationError('destination_unc is required')

    source = parse_unc(source_unc)
    destination = parse_unc(destination_unc)

    if not source.normalized_path.lower().endswith('.zip'):
        raise ValidationError('source_unc must point to a .zip file in Azure Files')
    if source.account != destination.account:
        logging.info('Cross-account copy detected: %s -> %s', source.account, destination.account)

    return {
        'source_unc': source_unc,
        'destination_unc': destination_unc,
        'zip_password': zip_password,
        'overwrite': overwrite,
        'source': asdict(source),
        'destination': asdict(destination),
        'zip_name': Path(source.normalized_path).name,
        'validated_at': utc_now_iso(),
    }


def ensure_directory_tree(share_client, remote_file_path: str) -> None:
    parent = str(Path(remote_file_path).parent).replace('\\', '/').strip('/')
    if not parent or parent == '.':
        return

    current = ''
    for part in parent.split('/'):
        current = f"{current}/{part}".strip('/')
        directory_client = share_client.get_directory_client(current)
        try:
            directory_client.create_directory()
        except ResourceExistsError:
            pass


def download_source_zip(source: AzureFilesPath, local_zip_path: str) -> int:
    service = get_share_service_client(source.account)
    share = service.get_share_client(source.share)
    file_client = share.get_file_client(source.normalized_path)

    with open(local_zip_path, 'wb') as fh:
        stream = file_client.download_file()
        data = stream.readall()
        fh.write(data)
        return len(data)


def safe_extract_zip(local_zip_path: str, extract_dir: str, zip_password: Optional[str]) -> List[FileEntry]:
    extracted_files: List[FileEntry] = []
    with zipfile.ZipFile(local_zip_path, 'r') as zf:
        bad_entries = []
        for member in zf.infolist():
            member_path = Path(member.filename)
            if member_path.is_absolute() or '..' in member_path.parts:
                bad_entries.append(member.filename)
        if bad_entries:
            raise ValidationError(f'Unsafe zip entries detected: {bad_entries[:5]}')

        pwd = zip_password.encode('utf-8') if zip_password else None
        zf.extractall(path=extract_dir, pwd=pwd)

    for dirpath, _, filenames in os.walk(extract_dir):
        for filename in filenames:
            local_path = os.path.join(dirpath, filename)
            rel_path = os.path.relpath(local_path, extract_dir).replace('\\', '/')
            extracted_files.append(FileEntry(relative_path=rel_path, size_bytes=os.path.getsize(local_path)))

    extracted_files.sort(key=lambda f: f.relative_path)
    return extracted_files


def upload_extracted_files(
    extracted_root: str,
    destination: AzureFilesPath,
    files: List[Dict[str, Any]],
    overwrite: bool,
) -> Dict[str, Any]:
    service = get_share_service_client(destination.account)
    share = service.get_share_client(destination.share)

    uploaded = 0
    skipped = 0
    failed = 0
    failures: List[Dict[str, str]] = []

    for item in files:
        rel_path = item['relative_path']
        local_path = os.path.join(extracted_root, rel_path)
        remote_path = '/'.join(part for part in [destination.normalized_path, rel_path] if part)
        try:
            ensure_directory_tree(share, remote_path)
            file_client = share.get_file_client(remote_path)

            if not overwrite:
                try:
                    file_client.get_file_properties()
                    skipped += 1
                    continue
                except ResourceNotFoundError:
                    pass

            with open(local_path, 'rb') as data:
                file_client.upload_file(data, overwrite=True)
            uploaded += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failures.append({'path': remote_path, 'error': str(exc)})

    return {
        'uploaded': uploaded,
        'skipped': skipped,
        'failed': failed,
        'failures': failures[:MAX_PATHS_TO_RETURN],
    }


def read_json(req: func.HttpRequest) -> Dict[str, Any]:
    try:
        return req.get_json()
    except ValueError as exc:
        raise ValidationError('Request body must be valid JSON') from exc


@app.route(route='jobs', methods=['POST'])
@app.durable_client_input(client_name='client')
async def start_unzip_job(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    try:
        body = read_json(req)
        instance_id = await client.start_new('unzip_orchestrator', client_input=body)
        payload = {
            'instanceId': instance_id,
            'message': 'Unzip orchestration started.',
            'statusQueryGetUri': str(req.url).rstrip('/').rsplit('/api/jobs', 1)[0] + f'/api/jobs/{instance_id}',
            'terminatePostUri': str(req.url).rstrip('/').rsplit('/api/jobs', 1)[0] + f'/api/jobs/{instance_id}/terminate',
            'purgeHistoryDeleteUri': str(req.url).rstrip('/').rsplit('/api/jobs', 1)[0] + f'/api/jobs/{instance_id}',
        }
        return func.HttpResponse(json.dumps(payload), status_code=202, mimetype='application/json')
    except ValidationError as exc:
        return func.HttpResponse(json.dumps({'error': str(exc)}), status_code=400, mimetype='application/json')
    except Exception as exc:  # noqa: BLE001
        logging.exception('Failed to start orchestration')
        return func.HttpResponse(json.dumps({'error': str(exc)}), status_code=500, mimetype='application/json')


@app.route(route='jobs/{instance_id}', methods=['GET'])
@app.durable_client_input(client_name='client')
async def get_job_status(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get('instance_id')
    status = await client.get_status(instance_id)
    if status is None:
        return func.HttpResponse(json.dumps({'error': 'Job not found'}), status_code=404, mimetype='application/json')

    payload = {
        'instanceId': status.instance_id,
        'name': status.name,
        'runtimeStatus': status.runtime_status,
        'createdTime': status.created_time.isoformat() if status.created_time else None,
        'lastUpdatedTime': status.last_updated_time.isoformat() if status.last_updated_time else None,
        'input': status.input_,
        'customStatus': status.custom_status,
        'output': status.output,
    }
    return func.HttpResponse(json.dumps(payload), status_code=200, mimetype='application/json')


@app.route(route='jobs/{instance_id}/terminate', methods=['POST'])
@app.durable_client_input(client_name='client')
async def terminate_job(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get('instance_id')
    reason = 'Terminated by user request.'
    await client.terminate(instance_id, reason)
    return func.HttpResponse(json.dumps({'instanceId': instance_id, 'message': reason}), status_code=202, mimetype='application/json')


@app.route(route='jobs/{instance_id}', methods=['DELETE'])
@app.durable_client_input(client_name='client')
async def purge_job_history(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = req.route_params.get('instance_id')
    await client.purge_instance_history(instance_id)
    return func.HttpResponse(json.dumps({'instanceId': instance_id, 'message': 'History purged.'}), status_code=200, mimetype='application/json')


@app.orchestration_trigger(context_name='context')
def unzip_orchestrator(context: df.DurableOrchestrationContext):
    request = context.get_input() or {}

    validated = yield context.call_activity('validate_request', request)
    source_unc = validated['source_unc']
    destination_unc = validated['destination_unc']

    yield context.set_custom_status({
        'status': 'running',
        'stage': 'validated',
        'progress': 5,
        'source_unc': source_unc,
        'destination_unc': destination_unc,
        'zipFile': validated['zip_name'],
        'message': 'Request validated.',
    })

    download = yield context.call_activity('download_zip_activity', validated)
    yield context.set_custom_status({
        'status': 'running',
        'stage': 'downloaded',
        'progress': 30,
        'zipFile': validated['zip_name'],
        'source_unc': source_unc,
        'destination_unc': destination_unc,
        'download': download,
        'message': 'ZIP downloaded from source Azure Files share.',
    })

    extracted = yield context.call_activity('extract_zip_activity', {
        'validated': validated,
        'download': download,
    })
    yield context.set_custom_status({
        'status': 'running',
        'stage': 'extracted',
        'progress': 60,
        'zipFile': validated['zip_name'],
        'source_unc': source_unc,
        'destination_unc': destination_unc,
        'fileCount': extracted['file_count'],
        'sampleFiles': extracted['files'][:10],
        'message': 'ZIP extracted to temporary storage.',
    })

    upload = yield context.call_activity('upload_files_activity', {
        'validated': validated,
        'extracted': extracted,
    })

    result = {
        'status': 'completed' if upload['failed'] == 0 else 'completed_with_errors',
        'source_unc': source_unc,
        'destination_unc': destination_unc,
        'zipFile': validated['zip_name'],
        'downloadBytes': download['downloaded_bytes'],
        'totalFiles': extracted['file_count'],
        'uploaded': upload['uploaded'],
        'skipped': upload['skipped'],
        'failed': upload['failed'],
        'failures': upload['failures'],
        'completedAt': context.current_utc_datetime.isoformat(),
    }

    yield context.set_custom_status({
        'status': result['status'],
        'stage': 'completed',
        'progress': 100,
        'zipFile': validated['zip_name'],
        'source_unc': source_unc,
        'destination_unc': destination_unc,
        'totalFiles': extracted['file_count'],
        'uploaded': upload['uploaded'],
        'skipped': upload['skipped'],
        'failed': upload['failed'],
        'message': 'Upload finished.',
    })

    return result


@app.activity_trigger(input_name='job')
def validate_request(job: Dict[str, Any]) -> Dict[str, Any]:
    return validate_job_input(job)


@app.activity_trigger(input_name='validated')
def download_zip_activity(validated: Dict[str, Any]) -> Dict[str, Any]:
    source = AzureFilesPath(**validated['source'])
    instance_dir = tempfile.mkdtemp(prefix='unzip-download-')
    local_zip_path = os.path.join(instance_dir, validated['zip_name'])
    downloaded_bytes = download_source_zip(source, local_zip_path)
    return {
        'work_dir': instance_dir,
        'local_zip_path': local_zip_path,
        'downloaded_bytes': downloaded_bytes,
        'downloadedAt': utc_now_iso(),
    }


@app.activity_trigger(input_name='payload')
def extract_zip_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    validated = payload['validated']
    download = payload['download']
    extract_dir = os.path.join(download['work_dir'], 'extracted')
    os.makedirs(extract_dir, exist_ok=True)

    files = safe_extract_zip(
        download['local_zip_path'],
        extract_dir,
        validated.get('zip_password'),
    )

    return {
        'work_dir': download['work_dir'],
        'extract_dir': extract_dir,
        'file_count': len(files),
        'files': [asdict(f) for f in files[:MAX_PATHS_TO_RETURN]],
        'all_files': [asdict(f) for f in files],
        'extractedAt': utc_now_iso(),
    }


@app.activity_trigger(input_name='payload')
def upload_files_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    validated = payload['validated']
    extracted = payload['extracted']
    destination = AzureFilesPath(**validated['destination'])

    result = upload_extracted_files(
        extracted_root=extracted['extract_dir'],
        destination=destination,
        files=extracted['all_files'],
        overwrite=validated['overwrite'],
    )

    try:
        shutil.rmtree(extracted['work_dir'], ignore_errors=True)
    except Exception:  # noqa: BLE001
        logging.warning('Failed to delete temp work directory: %s', extracted['work_dir'])

    result['uploadedAt'] = utc_now_iso()
    return result
