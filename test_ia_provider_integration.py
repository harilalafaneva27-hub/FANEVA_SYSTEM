import ast
import unittest
from pathlib import Path
from unittest.mock import patch

from source.ai_provider import UnavailableAIProvider


SOURCE = Path("source/main.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)

method_node = None
for node in ast.walk(TREE):
    if isinstance(node, ast.FunctionDef) and node.name == "_ia_provider":
        method_node = node
        break

if method_node is None:
    raise RuntimeError("_ia_provider_NOT_FOUND")

method_code = compile(
    ast.Module(body=[method_node], type_ignores=[]),
    "source/main.py",
    "exec",
)


def make_provider_method(load_config=None, build_provider=None):
    if load_config is None:
        load_config = __import__(
            "source.ai_config", fromlist=["load_config"]
        ).load_config
    if build_provider is None:
        build_provider = __import__(
            "source.ai_provider", fromlist=["build_provider"]
        ).build_provider

    namespace = {
        "HAS_FANEVA_IA": True,
        "build_provider": build_provider,
        "load_config": load_config,
        "UnavailableAIProvider": UnavailableAIProvider,
        "log_error": lambda *args: None,
    }
    exec(method_code, namespace)
    return namespace["_ia_provider"]


class TestIAProviderIntegration(unittest.TestCase):

    def test_default_private_config_returns_disabled_provider(self):
        method = make_provider_method()

        class AppStub:
            _ia_provider = method

        with patch(
            "source.ai_config.load_config",
            return_value={
                "provider": "http",
                "enabled": False,
                "base_url": "https://api.openai.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "",
            },
        ):
            provider = AppStub()._ia_provider()

        self.assertIsNotNone(provider)
        self.assertEqual(provider.name, "http_openai_compatible")
        self.assertFalse(provider.is_available())

    def test_private_config_is_forwarded_to_build_provider(self):
        method = make_provider_method()

        class AppStub:
            _ia_provider = method

        config = {
            "provider": "http",
            "enabled": True,
            "base_url": "https://example.com/v1",
            "model": "test-model",
            "api_key": "TEST-SECRET-KEY",
        }

        with patch(
            "source.main_dummy_never_used",
            create=True,
        ):
            pass

        with patch(
            "source.ai_config.load_config",
            return_value=config,
        ), patch(
            "source.ai_provider.build_provider"
        ) as build_provider:
            build_provider.return_value = object()

            # Rebuild method so its build_provider global is the mocked one.
            namespace = {
                "HAS_FANEVA_IA": True,
                "build_provider": build_provider,
                "load_config": lambda: config,
                "UnavailableAIProvider": UnavailableAIProvider,
                "log_error": lambda *args: None,
            }
            exec(method_code, namespace)

            class AppStub2:
                _ia_provider = namespace["_ia_provider"]

            result = AppStub2()._ia_provider()

        self.assertIs(result, build_provider.return_value)
        build_provider.assert_called_once_with(
            "http",
            api_key="TEST-SECRET-KEY",
            base_url="https://example.com/v1",
            model="test-model",
            enabled=True,
        )

    def test_config_error_returns_safe_unavailable_provider(self):
        method = make_provider_method()

        class AppStub:
            _ia_provider = method

        def failing_load_config():
            raise RuntimeError("config failure")

        method = make_provider_method(load_config=failing_load_config)

        class AppStub2:
            _ia_provider = method

        provider = AppStub2()._ia_provider()

        self.assertIsNotNone(provider)
        self.assertEqual(provider.name, "unavailable")


if __name__ == "__main__":
    unittest.main()
