"""Self-contained HTML fragments for pytest-html reports.

One copy of the table/key-value builders that used to be pasted into every step
module. ``attach`` appends a fragment to the pytest-html ``extras`` list.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any

try:  # pytest-html is optional at import time
    from pytest_html import extras as _html_extras
except Exception:  # pragma: no cover
    _html_extras = None

_FONT = "'Segoe UI',Arial,sans-serif"


def esc(text: Any) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def attach(extras: list, html: str) -> None:
    """Append an HTML fragment to a pytest-html ``extras`` list (no-op if unavailable)."""
    if _html_extras is not None and extras is not None:
        extras.append(_html_extras.html(html))


def html_kv(pairs: Iterable[tuple[str, Any]], title: str = "") -> str:
    rows = "".join(
        f'<tr style="background:{"#f1f5f9" if i % 2 else "#fff"};">'
        f'<td style="border:1px solid #e2e8f0;padding:6px 12px;font-weight:600;'
        f'white-space:nowrap;color:#334155;">{esc(k)}</td>'
        f'<td style="border:1px solid #e2e8f0;padding:6px 12px;font-family:monospace;'
        f'color:#1e293b;">{esc(v)}</td></tr>'
        for i, (k, v) in enumerate(pairs)
    )
    head = f'<h4 style="margin:8px 0 4px;color:#1e293b;">{esc(title)}</h4>' if title else ""
    return (
        f'<div style="margin:12px 0;font-family:{_FONT};">{head}'
        f'<table style="border-collapse:collapse;font-size:13px;font-family:{_FONT};">'
        f"{rows}</table></div>"
    )


def html_table(
    columns: Sequence[str],
    rows: Sequence[Any],
    title: str = "",
    subtitle: str = "",
    *,
    limit: int = 100,
) -> str:
    head = f'<h4 style="margin:8px 0 4px;color:#1e293b;">{esc(title)}</h4>' if title else ""
    sub = (
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">{esc(subtitle)}</p>'
        if subtitle
        else ""
    )
    ths = "".join(
        f'<th style="border:1px solid #cbd5e1;padding:8px 12px;background:#3b82f6;'
        f'color:#fff;text-align:left;white-space:nowrap;font-weight:600;'
        f'position:sticky;top:0;">{esc(c)}</th>'
        for c in columns
    )
    body_rows = []
    for i, row in enumerate(rows[:limit]):
        bg = "#f1f5f9" if i % 2 else "#fff"
        cells = []
        for c in columns:
            val = row.get(c) if isinstance(row, dict) else row
            cell = (
                '<span style="color:#94a3b8;font-style:italic;">NULL</span>'
                if val is None
                else esc(val)
            )
            cells.append(
                f'<td style="border:1px solid #e2e8f0;padding:6px 12px;white-space:nowrap;">{cell}</td>'
            )
        body_rows.append(f'<tr style="background:{bg};">{"".join(cells)}</tr>')
    if len(rows) > limit:
        body_rows.append(
            f'<tr><td colspan="{len(columns)}" style="padding:8px;text-align:center;'
            f'color:#64748b;font-style:italic;">... {len(rows) - limit} more rows ...</td></tr>'
        )
    return (
        f'<div style="margin:12px 0;font-family:{_FONT};">{head}{sub}'
        f'<div style="overflow:auto;max-height:600px;">'
        f'<table style="border-collapse:collapse;width:100%;font-size:13px;font-family:{_FONT};">'
        f"<thead><tr>{ths}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"
        f'<p style="margin:4px 0 0;color:#64748b;font-size:12px;">Total rows: {len(rows)}</p></div>'
    )


def html_json(obj: Any, title: str = "", subtitle: str = "") -> str:
    head = f'<h4 style="margin:8px 0 4px;color:#1e293b;">{esc(title)}</h4>' if title else ""
    sub = (
        f'<p style="margin:2px 0 8px;color:#64748b;font-size:12px;">{esc(subtitle)}</p>'
        if subtitle
        else ""
    )
    pretty = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    return (
        f'<div style="margin:12px 0;font-family:{_FONT};">{head}{sub}'
        f'<pre style="background:#f8fafc;border:1px solid #e2e8f0;padding:12px;'
        f'border-radius:6px;overflow:auto;font-size:12px;line-height:1.5;max-height:640px;'
        f'color:#1e293b;white-space:pre-wrap;word-break:break-word;">{esc(pretty)}</pre></div>'
    )
