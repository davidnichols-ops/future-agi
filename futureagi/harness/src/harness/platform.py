"""Reporting a run to the platform, so it appears where every other run does.

The platform already has somewhere to put this. Its simulate pages read `RunTest`,
`TestExecution` and `CallExecution`, and the ingestion API that the hosted runner posts to builds
exactly those. So a harness run is not shown by drawing it again somewhere else; it is shown by
walking the same API, and the pages that already exist render it unchanged.

    provision  ──► a RunTest for this session, once
    start      ──► a TestExecution, once per run, so running twice gives two runs
    batch      ──► a CallExecution per scenario
    result     ──► what the scenario did, one call at a time
    recording  ──► the audio, where a spoken run left any

What is deliberately *not* sent: interruption counts, talk ratio, latency, scores. The backend
derives those from the transcript it is given, and a second implementation here would drift from
the one the rest of the platform is measured by. This reports only what the run observed.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Where runs are reported. Its own variables, because reporting and evaluating go to different
# places: the eval templates a run is scored against live on the hosted platform, while the runs
# themselves belong wherever the person is looking at them -- usually the backend beside this
# harness. Sharing FI_* for both means one of the two is always pointed at the wrong host.
# FI_* is the fallback, so a setup that genuinely uses one platform for both still works unchanged.
BASE_URL = ("HARNESS_PLATFORM_URL", "FI_BASE_URL")
API_KEY = ("HARNESS_PLATFORM_API_KEY", "FI_API_KEY")
SECRET_KEY = ("HARNESS_PLATFORM_SECRET_KEY", "FI_SECRET_KEY")


def _setting(names: tuple[str, ...]) -> str:
    """The first of these that is set, so the specific name wins over the shared one."""
    for name in names:
        found = os.environ.get(name, "").strip()
        if found:
            return found
    return ""

INGESTION = "/simulate/api/alk-simulate"

# Django appends a slash and cannot redirect a POST while keeping its body, so every path here
# carries one already. Without it the call fails as a 500 that reads like a server fault.
TIMEOUT_SECONDS = 120.0


class PlatformError(RuntimeError):
    """The platform refused or could not be reached, with enough detail to act on."""


@dataclass
class Reported:
    """Where a run ended up, so a caller can link to it."""

    run_test_id: str = ""
    test_execution_id: str = ""
    calls: dict[str, str] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)

    @property
    def url(self) -> str:
        """Where this run is on the platform, for anyone wanting to look at it."""
        return f"/dashboard/simulate/test/{self.run_test_id}/runs" if self.run_test_id else ""


def configured() -> str:
    """Why a run cannot be reported, or an empty string when it can."""
    missing = [names[0] for names in (BASE_URL, API_KEY, SECRET_KEY) if not _setting(names)]
    if missing:
        return f"{', '.join(missing)} not set, so this run stays local"
    return ""


class Platform:
    """The ingestion API, as the few calls a run actually makes."""

    def __init__(self, base: str = "", key: str = "", secret: str = "") -> None:
        self.base = (base or _setting(BASE_URL)).rstrip("/")
        self.key = key or _setting(API_KEY)
        self.secret = secret or _setting(SECRET_KEY)

    def _call(self, path: str, payload: dict[str, Any], method: str = "POST") -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base}{INGESTION}{path}",
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": self.key,
                "X-Secret-Key": self.secret,
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as answer:
                body = json.loads(answer.read().decode() or "{}")
        except urllib.error.HTTPError as refused:
            detail = refused.read().decode(errors="replace")[:400]
            raise PlatformError(f"{method} {path} failed ({refused.code}): {detail}") from refused
        except Exception as unreachable:  # noqa: BLE001 - reported, not handled
            raise PlatformError(f"{method} {path} could not be sent: {unreachable}") from unreachable
        # The platform wraps every answer; unwrap it here so callers read the payload itself.
        return body.get("result", body) if isinstance(body, dict) else {}

    def provision(self, name: str, personas: list[dict[str, Any]]) -> dict[str, Any]:
        return self._call("/run-tests/provision/", {"name": name, "personas": personas})

    def start(self, run_test_id: str) -> dict[str, Any]:
        return self._call(f"/run-tests/{run_test_id}/test-executions/", {})

    def batch(self, test_execution_id: str, count: int) -> dict[str, Any]:
        return self._call(f"/test-executions/{test_execution_id}/batch/", {"count": count})

    def result(self, call_execution_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._call(f"/call-executions/{call_execution_id}/result/", payload, method="PATCH")


def persona_of(scenario: Any) -> dict[str, Any]:
    """One scenario as the platform's persona record.

    ``persona`` is carried whole so the simulator prompt's placeholder resolves against the same
    person the scenario was written for, rather than a name reconstructed from it.
    """
    persona = getattr(scenario, "persona", None) or {}
    if hasattr(persona, "model_dump"):
        persona = persona.model_dump()
    elif not isinstance(persona, dict):
        persona = {}
    return {
        "name": str(persona.get("name") or getattr(scenario, "name", "") or "caller")[:255],
        "role": str(persona.get("role") or persona.get("occupation") or "")[:255],
        "situation": str(getattr(scenario, "instruction", "") or ""),
        "outcome": str(getattr(scenario, "tests", "") or ""),
        "persona": persona,
    }


# What the harness calls a speaker, and what a transcript row is called on the platform. Anything
# unrecognised is the person, because the agent's turns are the ones we name.
SPEAKERS = {
    "agent": "assistant",
    "assistant": "assistant",
    "bot": "assistant",
    "system": "system",
    "customer": "user",
    "caller": "user",
    "user": "user",
    "tester": "user",
}


def segments_of(result: Any) -> list[dict[str, Any]]:
    """A run's conversation as transcript rows, tool calls included.

    No timings are invented. A typed run has none to give, and a made-up millisecond would be
    indistinguishable from a measured one to everything downstream that averages them.
    """
    rows: list[dict[str, Any]] = []
    for turn in getattr(result, "exchanges", None) or []:
        said = str(turn.get("text") or "").strip()
        if not said:
            continue
        rows.append(
            {
                "speaker_role": SPEAKERS.get(str(turn.get("speaker", "")).lower(), "user"),
                "content": said,
            }
        )
    for call in getattr(result, "calls_detail", None) or []:
        rows.append(
            {
                "speaker_role": "tool_calls",
                "content": f"{call.get('name', '')}({json.dumps(call.get('arguments', {}), default=str)})",
            }
        )
        outcome = call.get("error") or call.get("result") or ""
        rows.append(
            {
                "speaker_role": "tool_call_result",
                "content": ("refused: " if call.get("refused") else "") + str(outcome)[:4000],
            }
        )
    return rows


def result_of(result: Any) -> dict[str, Any]:
    """One scenario's outcome, in the shape the ingestion API takes.

    Sub-goals travel in ``call_metadata`` rather than as free text: they are what this run
    actually decided, and a page showing one goal per column needs them named and separate.
    """
    checkpoints = [
        {
            "name": getattr(check, "name", ""),
            "kind": getattr(check, "kind", ""),
            "passed": bool(getattr(check, "passed", False)),
            "detail": str(getattr(check, "detail", ""))[:2000],
        }
        for check in getattr(result, "checkpoints", None) or []
    ]
    problems = list(getattr(result, "problems", None) or [])
    payload: dict[str, Any] = {
        # A scenario that never ran is not a scenario the agent failed, and the two must not
        # arrive as the same status.
        "status": "failed" if problems else "completed",
        "duration_seconds": max(0, int(getattr(result, "seconds", 0) or 0)),
        "ended_reason": (getattr(result, "ended", "") or "")[:10000],
        "call_summary": (getattr(result, "line", lambda: "")() or "")[:2000],
        "transcript": segments_of(result),
        "call_metadata": {
            "harness_scenario": getattr(result, "scenario", ""),
            "harness_passed": bool(getattr(result, "passed", False)),
            "harness_met": int(getattr(result, "met", 0) or 0),
            "harness_of": len(checkpoints),
            "harness_checkpoints": checkpoints,
            "harness_spent_usd": round(float(getattr(result, "spent_usd", 0.0) or 0.0), 4),
        },
    }
    if problems:
        payload["error_message"] = "; ".join(problems)[:2000]
    return payload


def report(
    results: list[Any],
    scenarios: list[Any],
    *,
    name: str,
    run_test_id: str = "",
    platform: Platform | None = None,
) -> Reported:
    """Report one suite run, and say where it landed.

    ``run_test_id`` is reused when the session already has one, so a second run adds a second
    execution to the same test rather than a second test with one run in it.
    """
    api = platform or Platform()
    reported = Reported(run_test_id=run_test_id)

    if not run_test_id:
        provisioned = api.provision(name, [persona_of(one) for one in scenarios])
        reported.run_test_id = str(provisioned.get("run_test_id", ""))
    if not reported.run_test_id:
        raise PlatformError("the platform returned no run test to report against")

    started = api.start(reported.run_test_id)
    reported.test_execution_id = str(started.get("test_execution_id", ""))

    claimed = api.batch(reported.test_execution_id, max(1, len(results)))
    ids = [str(one) for one in claimed.get("call_execution_ids", [])]

    # Calls come back in the order the scenarios were attached, which is the order they were run
    # in. Zip rather than assume equal length: a suite can be a subset of its own test.
    for call_execution_id, result in zip(ids, results, strict=False):
        try:
            api.result(call_execution_id, result_of(result))
            reported.calls[getattr(result, "scenario", "")] = call_execution_id
        except PlatformError as failed:
            reported.problems.append(f"{getattr(result, 'scenario', '?')}: {failed}")
    if len(ids) < len(results):
        reported.problems.append(
            f"the platform allocated {len(ids)} calls for {len(results)} scenarios, "
            "so the rest were not reported"
        )
    return reported


def remember(destination: Path, reported: Reported) -> None:
    """Keep where a session reports to, so its next run joins the same test."""
    (Path(destination) / "platform.json").write_text(
        json.dumps(
            {
                "run_test_id": reported.run_test_id,
                "test_execution_id": reported.test_execution_id,
                "url": reported.url,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def remembered(destination: Path) -> str:
    """The run test this session already has, or an empty string."""
    kept = Path(destination) / "platform.json"
    if not kept.exists():
        return ""
    try:
        return str(json.loads(kept.read_text(encoding="utf-8")).get("run_test_id", ""))
    except Exception:  # noqa: BLE001 - a damaged file just means provisioning again
        return ""
