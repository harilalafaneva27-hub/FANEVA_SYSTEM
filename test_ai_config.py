import json
import os
import tempfile
import unittest
from unittest import mock

from source import ai_config


class AIConfigTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.config_dir = os.path.join(self.root, "FANEVA_IA")
        self.config_path = os.path.join(self.config_dir, "provider.json")

        self.root_patch = mock.patch(
            "source.ai_config._android_private_root",
            return_value=self.root,
        )
        self.root_patch.start()

        ai_config.clear_config()

    def tearDown(self):
        ai_config.clear_config()
        self.root_patch.stop()
        self.tmp.cleanup()

    def test_default_config_is_disabled(self):
        cfg = ai_config.load_config()

        self.assertEqual(cfg["provider"], "http")
        self.assertFalse(cfg["enabled"])
        self.assertEqual(cfg["base_url"], "https://api.openai.com/v1")
        self.assertEqual(cfg["model"], "gpt-4o-mini")
        self.assertEqual(cfg["api_key"], "")

    def test_private_path_resolution(self):
        self.assertEqual(
            ai_config.get_config_dir(),
            self.config_dir,
        )
        self.assertEqual(
            ai_config.get_config_path(),
            self.config_path,
        )

    def test_save_load_roundtrip(self):
        cfg = ai_config.save_config(
            provider="http",
            enabled=True,
            base_url="https://api.openai.com/v1",
            model="gpt-4o-mini",
            api_key="sk-test-config-key",
        )

        self.assertTrue(cfg["enabled"])
        self.assertTrue(os.path.exists(self.config_path))

        loaded = ai_config.load_config()

        self.assertEqual(loaded["provider"], "http")
        self.assertTrue(loaded["enabled"])
        self.assertEqual(
            loaded["base_url"],
            "https://api.openai.com/v1",
        )
        self.assertEqual(
            loaded["model"],
            "gpt-4o-mini",
        )
        self.assertEqual(
            loaded["api_key"],
            "sk-test-config-key",
        )

    def test_no_key_forces_disabled(self):
        cfg = ai_config.save_config(
            enabled=True,
            api_key="",
        )

        self.assertFalse(cfg["enabled"])

        loaded = ai_config.load_config()
        self.assertFalse(loaded["enabled"])
        self.assertEqual(loaded["api_key"], "")

    def test_https_is_accepted(self):
        ok, code, message = ai_config.validate_config(
            {
                "provider": "http",
                "enabled": True,
                "base_url": "https://api.openai.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "test-key",
            }
        )

        self.assertTrue(ok)
        self.assertIsNone(code)
        self.assertIsNone(message)

    def test_remote_http_is_rejected(self):
        ok, code, message = ai_config.validate_config(
            {
                "provider": "http",
                "enabled": True,
                "base_url": "http://evil.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "test-key",
            }
        )

        self.assertFalse(ok)
        self.assertEqual(code, "INSECURE_BASE_URL")

    def test_localhost_http_is_accepted(self):
        ok, code, message = ai_config.validate_config(
            {
                "provider": "http",
                "enabled": True,
                "base_url": "http://127.0.0.1:8080/v1",
                "model": "test-model",
                "api_key": "test-key",
            }
        )

        self.assertTrue(ok)
        self.assertIsNone(code)
        self.assertIsNone(message)

    def test_invalid_provider_is_rejected(self):
        ok, code, message = ai_config.validate_config(
            {
                "provider": "unknown-provider",
                "enabled": True,
                "base_url": "https://api.openai.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "test-key",
            }
        )

        self.assertFalse(ok)
        self.assertEqual(code, "INVALID_PROVIDER")

    def test_atomic_file_is_final_provider_json(self):
        ai_config.save_config(
            enabled=True,
            api_key="sk-atomic-test",
        )

        self.assertTrue(os.path.isfile(self.config_path))

        leftovers = [
            name
            for name in os.listdir(self.config_dir)
            if name.startswith(".provider.")
            and name.endswith(".tmp")
        ]
        self.assertEqual(leftovers, [])

        with open(self.config_path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)

        self.assertEqual(raw["api_key"], "sk-atomic-test")

    def test_file_permissions_are_private_when_supported(self):
        ai_config.save_config(
            enabled=True,
            api_key="sk-permission-test",
        )

        mode = os.stat(self.config_path).st_mode & 0o777
        self.assertEqual(mode, 0o600)

        dir_mode = os.stat(self.config_dir).st_mode & 0o777
        self.assertEqual(dir_mode, 0o700)

    def test_public_config_never_contains_raw_api_key(self):
        cfg = {
            "provider": "http",
            "enabled": True,
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "api_key": "sk-SUPER-SECRET-123",
        }

        public = ai_config.public_config(cfg)

        self.assertNotIn("api_key", public)
        self.assertNotIn(
            "sk-SUPER-SECRET-123",
            repr(public),
        )
        self.assertTrue(public["has_api_key"])

    def test_clear_config(self):
        ai_config.save_config(
            enabled=True,
            api_key="sk-clear-test",
        )

        self.assertTrue(os.path.exists(self.config_path))

        ai_config.clear_config()

        self.assertFalse(os.path.exists(self.config_path))

        cfg = ai_config.load_config()
        self.assertFalse(cfg["enabled"])
        self.assertEqual(cfg["api_key"], "")

    def test_corrupt_config_falls_back_safely(self):
        os.makedirs(self.config_dir, mode=0o700, exist_ok=True)

        with open(self.config_path, "w", encoding="utf-8") as handle:
            handle.write("{INVALID JSON")

        cfg = ai_config.load_config()

        self.assertFalse(cfg["enabled"])
        self.assertEqual(cfg["api_key"], "")

    def test_no_network_call_from_config_module(self):
        with mock.patch(
            "urllib.request.urlopen",
            side_effect=AssertionError("NETWORK_CALL"),
        ) as urlopen:
            ai_config.validate_config(
                {
                    "provider": "http",
                    "enabled": False,
                    "base_url": "https://api.openai.com/v1",
                    "model": "gpt-4o-mini",
                    "api_key": "",
                }
            )
            ai_config.save_config(
                enabled=False,
                api_key="",
            )
            ai_config.load_config()

        urlopen.assert_not_called()

    def test_no_sqlite_reference_in_source(self):
        source_path = os.path.join(
            os.path.dirname(ai_config.__file__),
            "ai_config.py",
        )

        with open(source_path, "r", encoding="utf-8") as handle:
            source = handle.read().lower()

        self.assertNotIn("sqlite3", source)
        self.assertNotIn("config_hybrid", source)
        self.assertNotIn("sync_server_api_key", source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
