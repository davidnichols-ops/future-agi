"""
Future-AGI Protect — The protection layer for AI agents.

This module provides the Protect pillar functionality including:
- Guardrail scanners (PII, jailbreak, injection, etc.)
- Vendor adapter integrations (Lakera, Presidio, Llama Guard, etc.)
- Model deployment conformance verification (cvconform)
- Inline protection via AgentCC Gateway
"""

from .scanners import (
    CVConformScanner,
    create_cvconform_scanner,
    get_scanner,
    list_scanners,
    SCANNER_REGISTRY,
)

__all__ = [
    "CVConformScanner",
    "create_cvconform_scanner",
    "get_scanner",
    "list_scanners",
    "SCANNER_REGISTRY",
]