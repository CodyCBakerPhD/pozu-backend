"""
Hourly CRON snapshot: reconstruct assets from JSONL buffers and upload to DANDI.

Schedule this via PythonAnywhere's "Scheduled tasks" to run once per hour:

    python /home/CodyCBakerPhD/mysite/cron_snapshot.py

What it does:
  1. Finds all completed JSONL buffer files (any hour tag that is not the current hour).
  2. For bbox buffers: writes each JSON record back as an individual .json file inside
     the dandiset's derivatives/incoming/ directory.
  3. For labels buffers: decodes each base64 record back to a binary .slp file inside
     the dandiset's derivatives/incoming/ directory.
  4. Moves the processed JSONL files to derivatives/snapshots/ for safe-keeping.
  5. Runs a single `dandi upload` per dandiset (batching all reconstructed files).
  6. On successful upload, removes the reconstructed files from derivatives/incoming/
     (the JSONL snapshots are kept as an audit trail).
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import uuid

# =============================================================================
# Config — must stay in sync with pozu_flask_app.py
# =============================================================================

VENV_BIN = "/home/CodyCBakerPhD/.virtualenvs/pozu/bin"
DANDI_BIN = f"{VENV_BIN}/dandi"

api_key_file_path = pathlib.Path("/home/CodyCBakerPhD/dandi_token")
EMBER_DANDI_API_KEY = api_key_file_path.read_text().strip()

BBOX_DANDISET_ROOT = pathlib.Path("/home/CodyCBakerPhD/mysite/000469")
LABELS_DANDISET_ROOT = pathlib.Path("/home/CodyCBakerPhD/mysite/000470")
DANDI_INSTANCE = "https://api-dandi.emberarchive.org/api"

# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# =============================================================================
# DANDI upload helper
# =============================================================================


def dandi_upload(dandiset_root: pathlib.Path) -> tuple[int, str, str]:
    """Run `dandi upload` inside *dandiset_root*. Returns (rc, stdout, stderr)."""
    env = os.environ.copy()
    env["EMBER_DANDI_API_KEY"] = EMBER_DANDI_API_KEY
    env["PATH"] = f"{VENV_BIN}:{env.get('PATH', '')}"

    cmd = [DANDI_BIN, "upload", "--dandi-instance", DANDI_INSTANCE]
    logger.info("Running dandi upload (cwd=%s)", dandiset_root)
    proc = subprocess.run(
        cmd,
        cwd=dandiset_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    logger.info("dandi upload rc=%d\nstdout: %s\nstderr: %s", proc.returncode, proc.stdout, proc.stderr)
    return proc.returncode, proc.stdout, proc.stderr


# =============================================================================
# Buffer processing helpers
# =============================================================================


def _complete_jsonl_files(buffer_dir: pathlib.Path, current_hour_tag: str) -> list[pathlib.Path]:
    """Return JSONL files in *buffer_dir* whose hour tag is not the current hour."""
    if not buffer_dir.exists():
        return []
    return sorted(f for f in buffer_dir.glob("*.jsonl") if current_hour_tag not in f.name)


def process_bbox_buffer(dandiset_root: pathlib.Path, current_hour_tag: str) -> list[pathlib.Path]:
    """Reconstruct individual .json files from completed bbox JSONL buffers.

    Returns the list of reconstructed file paths (to be cleaned up after upload).
    """
    buffer_dir = dandiset_root / "derivatives" / "buffer"
    complete_files = _complete_jsonl_files(buffer_dir, current_hour_tag)
    if not complete_files:
        return []

    incoming_dir = dandiset_root / "derivatives" / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir = dandiset_root / "derivatives" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    reconstructed: list[pathlib.Path] = []
    for jsonl_file in complete_files:
        logger.info("Processing bbox buffer: %s", jsonl_file.name)
        with jsonl_file.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                submission_id = record.get("submission_id") or uuid.uuid4().hex
                out_path = incoming_dir / f"id-{submission_id}.json"
                out_path.write_text(json.dumps(record, indent=2, sort_keys=True))
                reconstructed.append(out_path)

        shutil.move(str(jsonl_file), str(snapshots_dir / jsonl_file.name))
        logger.info("Archived bbox JSONL -> snapshots/%s (%d records)", jsonl_file.name, len(reconstructed))

    return reconstructed


def process_labels_buffer(dandiset_root: pathlib.Path, current_hour_tag: str) -> list[pathlib.Path]:
    """Write individual .json records from completed labels JSONL buffers.

    Returns the list of reconstructed file paths (to be cleaned up after upload).
    """
    buffer_dir = dandiset_root / "derivatives" / "buffer"
    complete_files = _complete_jsonl_files(buffer_dir, current_hour_tag)
    if not complete_files:
        return []

    incoming_dir = dandiset_root / "derivatives" / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir = dandiset_root / "derivatives" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    reconstructed: list[pathlib.Path] = []
    for jsonl_file in complete_files:
        logger.info("Processing labels buffer: %s", jsonl_file.name)
        with jsonl_file.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                submission_id = record.get("submission_id") or uuid.uuid4().hex
                out_path = incoming_dir / f"id-{submission_id}.json"
                out_path.write_text(json.dumps(record, indent=2, sort_keys=True))
                reconstructed.append(out_path)

        shutil.move(str(jsonl_file), str(snapshots_dir / jsonl_file.name))
        logger.info("Archived labels JSONL -> snapshots/%s (%d records)", jsonl_file.name, len(reconstructed))

    return reconstructed


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    current_hour_tag = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H")
    logger.info("cron_snapshot starting (current hour: %s)", current_hour_tag)

    bbox_files = process_bbox_buffer(BBOX_DANDISET_ROOT, current_hour_tag)
    if bbox_files:
        logger.info("Uploading %d bbox records to DANDI", len(bbox_files))
        rc, _, _ = dandi_upload(BBOX_DANDISET_ROOT)
        if rc == 0:
            for f in bbox_files:
                f.unlink(missing_ok=True)
            logger.info("bbox upload succeeded; cleaned up %d files", len(bbox_files))
        else:
            logger.error("bbox dandi upload failed (rc=%d); files left in incoming/ for retry", rc)

    labels_files = process_labels_buffer(LABELS_DANDISET_ROOT, current_hour_tag)
    if labels_files:
        logger.info("Uploading %d labels files to DANDI", len(labels_files))
        rc, _, _ = dandi_upload(LABELS_DANDISET_ROOT)
        if rc == 0:
            for f in labels_files:
                f.unlink(missing_ok=True)
            logger.info("labels upload succeeded; cleaned up %d files", len(labels_files))
        else:
            logger.error("labels dandi upload failed (rc=%d); files left in incoming/ for retry", rc)

    logger.info("cron_snapshot done")


if __name__ == "__main__":
    main()
