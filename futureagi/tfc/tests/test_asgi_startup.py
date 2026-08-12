from types import SimpleNamespace

from tfc import asgi_startup


def test_warm_http_urlconf_materializes_every_nested_resolver(monkeypatch):
    class FakeResolver:
        def __init__(self, patterns):
            self._patterns = patterns
            self.read_count = 0

        @property
        def url_patterns(self):
            self.read_count += 1
            return self._patterns

    leaf = SimpleNamespace()
    nested = FakeResolver([leaf])
    root = FakeResolver([leaf, nested])
    monkeypatch.setattr(asgi_startup, "URLResolver", FakeResolver)
    monkeypatch.setattr(asgi_startup, "get_resolver", lambda: root)

    assert asgi_startup.warm_http_urlconf() == 3
    assert root.read_count == 1
    assert nested.read_count == 1
