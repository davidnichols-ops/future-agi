"""
cvconform Scanner for Future-AGI Protect Pillar

Integrates cvconform differential conformance verification as a built-in
scanner for computer vision model deployment verification.

This scanner runs cvconform verify on model artifacts to detect silent
behavioral divergences between PyTorch reference and exported runtimes
(ONNX, CoreML, TensorRT).
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class CVConformResult:
    """Result of cvconform verification scan."""

    model_path: str
    source_format: str
    reference_runtime: str
    target_runtimes: List[str]
    overall_conformant: bool
    conformance_scores: Dict[str, float]
    findings: List[Dict[str, Any]]
    report_path: Optional[str] = None
    error: Optional[str] = None


class CVConformScanner:
    """
    Scanner that runs cvconform differential conformance verification.

    This scanner can be used in Future-AGI's Protect pipeline to verify
    that CV model exports (ONNX, CoreML, TensorRT) are faithful to the
    PyTorch reference model.
    """

    # Supported model formats
    SUPPORTED_FORMATS = {
        ".pt": "pytorch",
        ".pth": "pytorch",
        ".jit": "pytorch",
        ".onnx": "onnx",
        ".mlmodel": "coreml",
        ".mlpackage": "coreml",
        ".engine": "tensorrt",
        ".plan": "tensorrt",
    }

    # Available target runtimes
    AVAILABLE_TARGETS = ["onnx", "coreml", "tensorrt"]

    def __init__(
        self,
        reference: str = "pytorch",
        targets: Optional[List[str]] = None,
        seed: int = 0,
        num_samples: int = 1,
        tolerance_policy: Optional[Dict[str, Any]] = None,
        require_conformant: bool = True,
        output_dir: Optional[str] = None,
    ):
        """
        Initialize the cvconform scanner.

        Args:
            reference: Reference runtime ("pytorch", "onnx", "coreml")
            targets: List of target runtimes to verify
            seed: Random seed for reproducible synthetic inputs
            num_samples: Number of synthetic samples to generate
            tolerance_policy: Custom tolerance policy for comparisons
            require_conformant: Whether to fail on non-conformant results
            output_dir: Directory for report outputs
        """
        self.reference = reference
        self.targets = targets or ["onnx", "coreml"]
        self.seed = seed
        self.num_samples = num_samples
        self.tolerance_policy = tolerance_policy
        self.require_conformant = require_conformant
        self.output_dir = Path(output_dir) if output_dir else None

        # Validate targets
        for t in self.targets:
            if t not in self.AVAILABLE_TARGETS:
                raise ValueError(
                    f"Unsupported target runtime: {t}. "
                    f"Available: {self.AVAILABLE_TARGETS}"
                )

    def scan(self, model_path: str) -> CVConformResult:
        """
        Run cvconform verification on a model.

        Args:
            model_path: Path to the model file

        Returns:
            CVConformResult with verification details
        """
        model_path = Path(model_path)
        if not model_path.exists():
            return CVConformResult(
                model_path=str(model_path),
                source_format="unknown",
                reference_runtime=self.reference,
                target_runtimes=self.targets,
                overall_conformant=False,
                conformance_scores={},
                findings=[],
                error=f"Model not found: {model_path}",
            )

        # Detect source format
        source_format = self.SUPPORTED_FORMATS.get(model_path.suffix.lower())
        if source_format is None:
            # Try to detect from file content
            source_format = self._detect_format(model_path)
            if source_format is None:
                return CVConformResult(
                    model_path=str(model_path),
                    source_format="unknown",
                    reference_runtime=self.reference,
                    target_runtimes=self.targets,
                    overall_conformant=False,
                    conformance_scores={},
                    findings=[],
                    error=f"Cannot detect model format for {model_path.suffix}",
                )

        logger.info(
            "cvconform_scan_started",
            model=str(model_path),
            source_format=source_format,
            reference=self.reference,
            targets=self.targets,
        )

        try:
            # Import cvconform verify
            from cvconform import verify, verify_to_json

            # Prepare output path
            report_path = None
            if self.output_dir:
                self.output_dir.mkdir(parents=True, exist_ok=True)
                report_path = self.output_dir / f"{model_path.stem}_cvconform_report.json"

            # Run verification
            if report_path:
                report = verify_to_json(
                    model=str(model_path),
                    reference=self.reference,
                    targets=self.targets,
                    seed=self.seed,
                    num_samples=self.num_samples,
                    output_names=None,
                    source_kind=source_format,
                )
                # verify_to_json already writes to file, but we can also write our own
                with open(report_path, "w") as f:
                    json.dump(report, f, indent=2)
            else:
                report = verify(
                    model=str(model_path),
                    reference=self.reference,
                    targets=self.targets,
                    seed=self.seed,
                    num_samples=self.num_samples,
                    output_names=None,
                    source_kind=source_format,
                )

            # Extract results
            overall_conformant = True
            conformance_scores = {}
            findings = []

            for target, target_report in report.get("targets", {}).items():
                score = target_report.get("overall_score", 0.0)
                is_conformant = target_report.get("is_conformant", False)
                conformance_scores[target] = score
                if not is_conformant:
                    overall_conformant = False

                # Convert divergences to findings
                for div in target_report.get("divergences", []):
                    findings.append({
                        "target": target,
                        "type": "conformance_divergence",
                        "output": div.get("output", "unknown"),
                        "metric": div.get("metric", "unknown"),
                        "magnitude": div.get("magnitude", 0.0),
                        "threshold": div.get("threshold", 0.0),
                        "mechanism": div.get("mechanism", "unknown"),
                        "confidence": div.get("confidence", 0.0),
                        "first_divergence": div.get("first_divergence", ""),
                    })

            # Add root-cause analysis findings
            for finding in report.get("findings", []):
                if finding:
                    findings.append({
                        "target": finding.get("backend", "unknown"),
                        "type": "root_cause",
                        "affected": finding.get("affected", "unknown"),
                        "cause": finding.get("cause", "unknown"),
                        "mechanism": finding.get("mechanism", "unknown"),
                        "impact": finding.get("impact", ""),
                        "confidence": finding.get("confidence", 0.0),
                        "fixes": finding.get("fixes", []),
                        "details": finding.get("details", {}),
                    })

            result = CVConformResult(
                model_path=str(model_path),
                source_format=source_format,
                reference_runtime=self.reference,
                target_runtimes=self.targets,
                overall_conformant=overall_conformant,
                conformance_scores=conformance_scores,
                findings=findings,
                report_path=str(report_path) if report_path else None,
            )

            logger.info(
                "cvconform_scan_completed",
                model=str(model_path),
                overall_conformant=overall_conformant,
                scores=conformance_scores,
                num_findings=len(findings),
            )

            return result

        except Exception as e:
            logger.exception("cvconform_scan_failed", model=str(model_path), error=str(e))
            return CVConformResult(
                model_path=str(model_path),
                source_format=source_format,
                reference_runtime=self.reference,
                target_runtimes=self.targets,
                overall_conformant=False,
                conformance_scores={},
                findings=[],
                error=str(e),
            )

    def scan_batch(self, model_paths: List[str]) -> List[CVConformResult]:
        """
        Run cvconform verification on multiple models.

        Args:
            model_paths: List of paths to model files

        Returns:
            List of CVConformResult
        """
        results = []
        for path in model_paths:
            results.append(self.scan(path))
        return results

    def _detect_format(self, model_path: Path) -> Optional[str]:
        """Attempt to detect model format from file content."""
        try:
            # Try ONNX
            if model_path.suffix.lower() in [".onnx", ".pb"]:
                import onnx
                onnx.load(str(model_path))
                return "onnx"
        except Exception:
            pass

        try:
            # Try CoreML
            if model_path.suffix.lower() in [".mlmodel", ".mlpackage"]:
                import coremltools as ct
                ct.models.MLModel(str(model_path))
                return "coreml"
        except Exception:
            pass

        try:
            # Try PyTorch
            import torch
            obj = torch.load(str(model_path), map_location="cpu", weights_only=False)
            if hasattr(obj, "eval"):
                return "pytorch"
        except Exception:
            pass

        try:
            # Try TensorRT
            if model_path.suffix.lower() in [".engine", ".plan"]:
                import tensorrt as trt
                logger = trt.Logger(trt.Logger.WARNING)
                with open(str(model_path), "rb") as f:
                    runtime = trt.Runtime(logger)
                    engine = runtime.deserialize_cuda_engine(f.read())
                if engine is not None:
                    return "tensorrt"
        except Exception:
            pass

        return None

    def to_protect_format(self, result: CVConformResult) -> Dict[str, Any]:
        """
        Convert cvconform result to Future-AGI Protect scanner format.

        Returns a dict compatible with the Protect scanner output schema.
        """
        return {
            "scanner": "cvconform",
            "model": os.path.basename(result.model_path),
            "source_format": result.source_format,
            "reference": result.reference_runtime,
            "targets": result.target_runtimes,
            "passed": result.overall_conformant,
            "scores": result.conformance_scores,
            "findings": result.findings,
            "report_path": result.report_path,
            "error": result.error,
        }


def create_cvconform_scanner(
    reference: str = "pytorch",
    targets: Optional[List[str]] = None,
    **kwargs
) -> CVConformScanner:
    """Factory function to create a cvconform scanner."""
    return CVConformScanner(
        reference=reference,
        targets=targets,
        **kwargs
    )