"""
Future-AGI Protect Scanners

Built-in scanners for the Protect pillar.
"""

from .cvconform_scanner import CVConformScanner, create_cvconform_scanner

__all__ = [
    "CVConformScanner",
    "create_cvconform_scanner",
]

# Scanner registry for dynamic loading
SCANNER_REGISTRY = {
    "cvconform": {
        "class": CVConformScanner,
        "factory": create_cvconform_scanner,
        "description": "Differential conformance verification for CV model deployments",
        "supported_formats": [".pt", ".pth", ".onnx", ".mlmodel", ".engine", ".plan"],
        "targets": ["onnx", "coreml", "tensorrt"],
    },
}


def get_scanner(name: str, **kwargs) -> Any:
    """Get a scanner instance by name."""
    if name not in SCANNER_REGISTRY:
        raise ValueError(f"Unknown scanner: {name}. Available: {list(SCANNER_REGISTRY.keys())}")
    return SCANNER_REGISTRY[name]["factory"](**kwargs)


def list_scanners() -> List[Dict[str, Any]]:
    """List all available scanners with metadata."""
    return [
        {
            "name": name,
            "description": info["description"],
            "supported_formats": info["supported_formats"],
            "targets": info.get("targets", []),
        }
        for name, info in SCANNER_REGISTRY.items()
    ]