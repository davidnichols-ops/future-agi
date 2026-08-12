import csv
import io
import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

from django.http import HttpResponse

_CSV_FORMULA_TRIGGERS = ("=", "+", "-", "@", "\t", "\r")


def _format_csv_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(
            value,
            default=str,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=isinstance(value, dict),
        )
    if isinstance(value, str) and value.startswith(_CSV_FORMULA_TRIGGERS):
        return "'" + value
    return value


def bounded_page_csv_response(
    *,
    rows: Iterable[Mapping[str, Any]] | None,
    filename: str,
    metadata: Mapping[str, Any] | None = None,
) -> HttpResponse:
    """Serialize one finite list page and disclose any incomplete export."""

    page_rows = list(rows or ())
    fieldnames = list(dict.fromkeys(key for row in page_rows for key in row))
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(fieldnames)
    for row in page_rows:
        writer.writerow([_format_csv_cell(row.get(field)) for field in fieldnames])

    read_metadata = metadata or {}
    if (
        read_metadata.get("has_more")
        or read_metadata.get("total_rows_is_lower_bound")
        or read_metadata.get("query_complete") is False
    ):
        writer.writerow(
            [
                f"# export truncated after {len(page_rows)} rows; refine filters to export a complete bounded page"
            ]
        )

    response = HttpResponse(buffer.getvalue(), content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
