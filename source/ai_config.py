"""FANEVA IA — configuration privée du provider.

Aucune donnée IA n'est stockée dans SQLite métier.
La configuration est conservée dans le stockage privé de l'application Android.
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict, Optional


CONFIG_DIR_NAME = "FANEVA_IA"
CONFIG_FILE_NAME = "provider.json"

DEFAULT_CONFIG: Dict[str, Any] = {
    "provider": "http",
    "enabled": False,
    "base_url": "https://api.openai.com/v1",
    "model": "gpt-4o-mini",
    "api_key": "",
}


def _android_private_root() -> str:
    """Retourne le stockage privé de l'application Android."""
    try:
        from jnius import autoclass

        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        activity = PythonActivity.mActivity
        return activity.getFilesDir().getAbsolutePath()
    except Exception:
        # Fallback hôte uniquement pour les tests.
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            ".faneva_ia_private_test",
        )


def get_config_dir() -> str:
    """Dossier privé de configuration FANEVA IA."""
    return os.path.join(_android_private_root(), CONFIG_DIR_NAME)


def get_config_path() -> str:
    """Chemin complet du fichier de configuration."""
    return os.path.join(get_config_dir(), CONFIG_FILE_NAME)


def _normalize(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalise une configuration sans exposer ni modifier la clé."""
    src = dict(DEFAULT_CONFIG)
    if isinstance(config, dict):
        for key in ("provider", "enabled", "base_url", "model", "api_key"):
            if key in config:
                src[key] = config[key]

    src["provider"] = str(src.get("provider") or "http").strip().lower()
    src["enabled"] = bool(src.get("enabled"))
    src["base_url"] = str(
        src.get("base_url") or DEFAULT_CONFIG["base_url"]
    ).strip().rstrip("/")
    src["model"] = str(
        src.get("model") or DEFAULT_CONFIG["model"]
    ).strip()
    src["api_key"] = str(src.get("api_key") or "").strip()

    # Sans clé, le provider reste désactivé.
    if not src["api_key"]:
        src["enabled"] = False

    return src


def validate_config(config: Dict[str, Any]) -> tuple:
    """Valide la configuration sans effectuer aucun appel réseau."""
    cfg = _normalize(config)

    if cfg["provider"] not in {
        "http",
        "openai",
        "grok",
        "openai_compatible",
    }:
        return False, "INVALID_PROVIDER", "Fournisseur IA non autorisé"

    if not cfg["base_url"]:
        return False, "INVALID_BASE_URL", "base_url vide"

    if not cfg["model"]:
        return False, "INVALID_MODEL", "modèle vide"

    # Réutilise la règle de sécurité du provider HTTP.
    try:
        try:
            from ai_provider import validate_remote_base_url
        except ImportError:
            from .ai_provider import validate_remote_base_url

        ok, code, message = validate_remote_base_url(cfg["base_url"])
        if not ok:
            return False, code or "INSECURE_BASE_URL", message or "base_url non sécurisée"
    except Exception:
        return False, "INVALID_BASE_URL", "base_url invalide"

    return True, None, None


def load_config() -> Dict[str, Any]:
    """Charge la configuration privée.

    En cas d'absence, corruption ou erreur de lecture, retourne une
    configuration sûre avec le provider désactivé.
    """
    path = get_config_path()

    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)

        cfg = _normalize(raw)
        ok, _, _ = validate_config(cfg)
        if not ok:
            cfg["enabled"] = False
        return cfg
    except Exception:
        return dict(DEFAULT_CONFIG)


def save_config(
    *,
    provider: str = "http",
    enabled: bool = False,
    base_url: str = "https://api.openai.com/v1",
    model: str = "gpt-4o-mini",
    api_key: str = "",
) -> Dict[str, Any]:
    """Valide puis sauvegarde la configuration dans le stockage privé.

    L'écriture est atomique pour éviter un fichier partiellement écrit.
    """
    cfg = _normalize(
        {
            "provider": provider,
            "enabled": enabled,
            "base_url": base_url,
            "model": model,
            "api_key": api_key,
        }
    )

    ok, code, message = validate_config(cfg)
    if not ok:
        raise ValueError(message or code or "Configuration IA invalide")

    directory = get_config_dir()
    os.makedirs(directory, mode=0o700, exist_ok=True)

    path = get_config_path()
    fd, tmp_path = tempfile.mkstemp(
        prefix=".provider.",
        suffix=".tmp",
        dir=directory,
        text=True,
    )

    try:
        os.chmod(directory, 0o700)

        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                cfg,
                handle,
                ensure_ascii=False,
                indent=2,
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)

        return dict(cfg)

    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise


def clear_config() -> None:
    """Supprime la configuration IA privée, sans toucher à SQLite."""
    path = get_config_path()
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def public_config(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Retourne une représentation sûre pour l'UI/logs.

    La clé API n'est jamais retournée.
    """
    cfg = _normalize(config if config is not None else load_config())

    return {
        "provider": cfg["provider"],
        "enabled": bool(cfg["enabled"]),
        "base_url": cfg["base_url"],
        "model": cfg["model"],
        "has_api_key": bool(cfg["api_key"]),
    }
