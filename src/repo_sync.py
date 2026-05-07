from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path("/data_nuevo/cobertura_integrada")


def ejecutar_sync_repo(
    output_dir: str,
    dig_tramite: str = "",
    *,
    project_root: Path | None = None,
    repo_root: str = "/data_nuevo/repo_grande/data/datos",
    backup_root: str | None = None,
    replace_existing_cc: bool = True,
    timeout_seconds: int = 1800,
) -> dict:
    root = Path(project_root or PROJECT_ROOT).resolve()
    script = root / "scripts" / "sync_coberturas_repo.py"
    if not script.exists():
        msg = f"No existe script de sync: {script}"
        return {"ok": False, "already_running": False, "returncode": -1, "stdout": "", "error": msg}

    cmd = [
        sys.executable,
        str(script),
        "--origen-root",
        output_dir,
        "--repo-root",
        repo_root,
        "--logs-dir",
        str(root / "logs"),
        "--state-db",
        str(root / "logs" / "cobertura_repo_sync.sqlite"),
    ]

    if backup_root:
        cmd.extend(["--backup-root", backup_root])
    else:
        cmd.extend(["--backup-root", str(root / "logs" / "sync_replaced_cc_backups")])

    if replace_existing_cc:
        cmd.append("--replace-existing-cc")

    cmd.append("--apply")

    if dig_tramite:
        cmd.extend(["--tramite", dig_tramite])

    try:
        completed = subprocess.run(
            cmd,
            cwd=str(root),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )
        stdout = completed.stdout or ""
        if completed.returncode != 0 and stdout:
            error = stdout[-2000:]
        else:
            error = ""
        return {
            "ok": completed.returncode == 0,
            "already_running": completed.returncode == 10,
            "returncode": completed.returncode,
            "stdout": stdout,
            "error": error,
        }
    except subprocess.TimeoutExpired as exc:
        msg = f"Timeout ejecutando sync: {exc}"
        return {"ok": False, "already_running": False, "returncode": -2, "stdout": "", "error": msg}
    except Exception as exc:
        msg = f"Error ejecutando sync: {exc}"
        return {"ok": False, "already_running": False, "returncode": -3, "stdout": "", "error": msg}
