"""The environment interface a generated world presents, wherever it is running.

The platform's simulation runners drive an ``EnvironmentAdapter``, so a generated world has to be
one. Importing that class from ``fi.simulate`` also runs that package's ``__init__``, which pulls
in its LiveKit dependency -- so a harness that never places a call still could not start without
the voice extra installed.

That is the wrong trade for a package meant to run on its own. So the real class is used when it
is importable, and an identical local one when it is not. A world built either way satisfies the
same interface; the only difference is whether it is literally the platform's class, which
matters solely to ``isinstance`` checks inside the platform.
"""

from __future__ import annotations

from abc import ABC
from typing import Any, Mapping, Optional

from pydantic import BaseModel, Field

try:  # pragma: no cover - which branch runs depends on what is installed
    from fi.simulate.environment import (
        EnvironmentAdapter,
        EnvironmentSnapshot,
        ToolExecutionResult,
    )

    PLATFORM_INTERFACE = True
except ImportError:  # pragma: no cover - the standalone case
    PLATFORM_INTERFACE = False

    class EnvironmentSnapshot(BaseModel):  # type: ignore[no-redef]
        """State, tools, artifacts, and events exposed by a simulation environment."""

        tools: list[dict[str, Any]] = Field(default_factory=list)
        artifacts: list[Any] = Field(default_factory=list)
        events: list[Any] = Field(default_factory=list)
        state: dict[str, Any] = Field(default_factory=dict)
        metadata: dict[str, Any] = Field(default_factory=dict)

    class ToolExecutionResult(BaseModel):  # type: ignore[no-redef]
        """Result from executing a tool call inside a local environment."""

        tool_call_id: Optional[str] = None
        tool_name: str
        content: str
        result: Any = None
        success: bool = True
        error: Optional[str] = None
        state_updates: dict[str, Any] = Field(default_factory=dict)
        artifacts: list[Any] = Field(default_factory=list)
        events: list[Any] = Field(default_factory=list)
        metadata: dict[str, Any] = Field(default_factory=dict)

        def to_tool_message(self) -> dict[str, Any]:
            return {
                "role": "tool",
                "tool_call_id": self.tool_call_id or self.tool_name,
                "content": self.content,
            }

    class EnvironmentAdapter(ABC):  # type: ignore[no-redef]
        """Base class for local simulation environments."""

        name = "environment"

        def reset(self, **context: Any) -> EnvironmentSnapshot:
            return EnvironmentSnapshot()

        def observe(self, **context: Any) -> EnvironmentSnapshot:
            return EnvironmentSnapshot()

        def handle_tool_call(
            self,
            tool_call: Mapping[str, Any],
            **context: Any,
        ) -> Optional[ToolExecutionResult]:
            return None


__all__ = [
    "EnvironmentAdapter",
    "EnvironmentSnapshot",
    "PLATFORM_INTERFACE",
    "ToolExecutionResult",
]
