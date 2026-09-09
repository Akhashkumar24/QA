"""MySQL-over-SSH-tunnel access for DB↔S3 reconciliation use cases.

Config (use-case YAML ``db:`` block)::

    db:
      ssm:                       # optional — auto port-forward when ssh port is closed
        instance_id: i-0123...
        region: ca-central-1
        profile: glue
      ssh:  {host: 127.0.0.1, port: 13408, username: ec2devuser, password: "${DB_SSH_PASSWORD}"}
      mysql: {host: db.example.rds.amazonaws.com, port: 3306, username: dbdevuser,
              password: "${DB_MYSQL_PASSWORD}", database: FlightScheduleDataStore}

``sshtunnel`` and ``mysql-connector-python`` are imported lazily (only the
db-reconciliation use cases need the ``.[db]`` extra).
"""

from __future__ import annotations

import socket
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_ssm_forwarding(ssm: dict[str, Any], ssh: dict[str, Any], *, max_wait: int = 30) -> None:
    """Start ``aws ssm start-session`` port forwarding if the SSH port is closed."""
    host, port = ssh["host"], int(ssh["port"])
    if _port_open(host, port):
        return
    cmd = [
        "aws", "ssm", "start-session",
        "--target", ssm["instance_id"],
        "--document-name", "AWS-StartPortForwardingSession",
        "--parameters", f"portNumber=22,localPortNumber={port}",
        "--region", ssm.get("region", "ca-central-1"),
    ]
    if ssm.get("profile"):
        cmd += ["--profile", ssm["profile"]]
    subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    waited = 0
    while waited < max_wait:
        if _port_open(host, port):
            return
        time.sleep(2)
        waited += 2
    raise TimeoutError(f"SSM forwarding started but {host}:{port} never opened ({max_wait}s)")


@contextmanager
def connect(db_conf: dict[str, Any]) -> Iterator[Any]:
    """Yield a live ``mysql.connector`` connection through an SSH tunnel."""
    import mysql.connector
    from sshtunnel import SSHTunnelForwarder

    ssh, mysql_conf = db_conf["ssh"], db_conf["mysql"]
    if db_conf.get("ssm"):
        ensure_ssm_forwarding(db_conf["ssm"], ssh)

    tunnel = SSHTunnelForwarder(
        (ssh["host"], int(ssh["port"])),
        ssh_username=ssh["username"],
        ssh_password=ssh.get("password"),
        ssh_pkey=ssh.get("pkey"),
        remote_bind_address=(mysql_conf["host"], int(mysql_conf.get("port", 3306))),
    )
    tunnel.start()
    conn = None
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            user=mysql_conf["username"],
            password=mysql_conf.get("password"),
            database=mysql_conf.get("database"),
            connection_timeout=30,
        )
        yield conn
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        try:
            tunnel.stop()
        except Exception:
            pass


def run_query(conn: Any, sql: str) -> tuple[list[str], list[dict[str, Any]]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = list(cur.column_names) if getattr(cur, "column_names", None) else (
            list(rows[0].keys()) if rows else []
        )
        return cols, rows
    finally:
        cur.close()


def minutes(hhmmss: str) -> int:
    """'HH:MM:SS' or ISO-8601 → minutes-from-midnight (DB stores smallint minutes)."""
    if "T" in hhmmss:
        from datetime import datetime

        dt = datetime.fromisoformat(hhmmss.replace("Z", "+00:00"))
        return dt.hour * 60 + dt.minute
    h, m, *_ = hhmmss.split(":")
    return int(h) * 60 + int(m)
