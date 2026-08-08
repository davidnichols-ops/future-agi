from types import SimpleNamespace
from unittest.mock import MagicMock, call

from tfc.temporal.common import registry


def test_usage_temporal_registry_prefers_cloud(monkeypatch):
    cloud_registry = MagicMock()
    import_module = MagicMock(
        return_value=SimpleNamespace(get_workflows=cloud_registry)
    )
    monkeypatch.setattr(registry, "import_module", import_module)

    assert registry._load_usage_temporal_registry("get_workflows") is cloud_registry
    import_module.assert_called_once_with("ee.cloud.temporal")


def test_usage_temporal_registry_falls_back_to_legacy(monkeypatch):
    legacy_registry = MagicMock()
    import_module = MagicMock(
        side_effect=[
            ImportError("cloud module unavailable"),
            SimpleNamespace(get_activities=legacy_registry),
        ],
    )
    monkeypatch.setattr(registry, "import_module", import_module)

    assert registry._load_usage_temporal_registry("get_activities") is legacy_registry
    assert import_module.call_args_list == [
        call("ee.cloud.temporal"),
        call("ee.usage.temporal"),
    ]


def test_usage_temporal_registry_returns_none_when_unavailable(monkeypatch):
    import_module = MagicMock(side_effect=ImportError("module unavailable"))
    monkeypatch.setattr(registry, "import_module", import_module)

    assert registry._load_usage_temporal_registry("get_workflows") is None
    assert import_module.call_args_list == [
        call("ee.cloud.temporal"),
        call("ee.usage.temporal"),
    ]
