"""
FANEVA SYSTEM — AI Provider layer (PHASE 2)
===========================================
Couche d'abstraction indépendante du fournisseur IA.

Permet de basculer entre Mock / Unavailable / HTTP (Gemini, Grok, OpenAI, local)
sans modifier le cœur métier ni AIDataService.

Règles :
- Aucune écriture SQLite
- Aucune action métier
- Timeout et erreurs gérés explicitement
- Absence de clé API → mode indisponible (pas d'appel réseau)
"""

from __future__ import annotations

import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


@dataclass
class AIResponse:
    """Réponse normalisée d'un fournisseur IA."""
    ok: bool
    text: str
    provider: str
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    latency_ms: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None
    analytical: bool = True  # Phase 2 : réponses purement informatives

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AIProviderError(Exception):
    """Erreur contrôlée d'un fournisseur IA."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class BaseAIProvider(ABC):
    """Interface commune à tous les fournisseurs."""

    name: str = "base"

    @abstractmethod
    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        timeout_sec: float = 20.0,
        max_tokens: int = 1024,
    ) -> AIResponse:
        """Produit une réponse textuelle. Ne doit jamais écrire en base."""
        raise NotImplementedError

    def is_available(self) -> bool:
        return True


class UnavailableAIProvider(BaseAIProvider):
    """Fournisseur factice : IA désactivée (pas de clé, offline, etc.)."""

    name = "unavailable"

    def __init__(self, reason: str = "IA indisponible"):
        self.reason = reason

    def is_available(self) -> bool:
        return False

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        timeout_sec: float = 20.0,
        max_tokens: int = 1024,
    ) -> AIResponse:
        return AIResponse(
            ok=False,
            text="",
            provider=self.name,
            error_code="PROVIDER_UNAVAILABLE",
            error_message=self.reason,
            analytical=True,
        )


class MissingAPIKeyProvider(UnavailableAIProvider):
    """Spécialisation : clé API absente."""

    name = "missing_api_key"

    def __init__(self):
        super().__init__(reason="Clé API absente — aucun appel réseau effectué")


class MockAIProvider(BaseAIProvider):
    """Fournisseur déterministe pour tests (aucun réseau)."""

    name = "mock"

    def __init__(
        self,
        fixed_text: Optional[str] = None,
        fail_code: Optional[str] = None,
        fail_message: Optional[str] = None,
        delay_sec: float = 0.0,
        invalid_payload: bool = False,
    ):
        self.fixed_text = fixed_text
        self.fail_code = fail_code
        self.fail_message = fail_message
        self.delay_sec = delay_sec
        self.invalid_payload = invalid_payload

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        timeout_sec: float = 20.0,
        max_tokens: int = 1024,
    ) -> AIResponse:
        started = time.monotonic()
        if self.delay_sec > 0:
            # Simule un délai ; si > timeout → TIMEOUT
            if self.delay_sec > timeout_sec:
                time.sleep(min(self.delay_sec, 0.05))  # court en tests
                return AIResponse(
                    ok=False,
                    text="",
                    provider=self.name,
                    error_code="TIMEOUT",
                    error_message=f"Délai dépassé ({timeout_sec}s)",
                    latency_ms=int((time.monotonic() - started) * 1000),
                )
            time.sleep(min(self.delay_sec, 0.05))

        if self.fail_code:
            return AIResponse(
                ok=False,
                text="",
                provider=self.name,
                error_code=self.fail_code,
                error_message=self.fail_message or self.fail_code,
                latency_ms=int((time.monotonic() - started) * 1000),
            )

        if self.invalid_payload:
            return AIResponse(
                ok=False,
                text="",
                provider=self.name,
                error_code="INVALID_RESPONSE",
                error_message="Réponse fournisseur invalide ou vide",
                latency_ms=int((time.monotonic() - started) * 1000),
            )

        # Réponse analytique déterministe basée sur le prompt (sans action métier)
        text = self.fixed_text
        if text is None:
            text = (
                "[FANEVA IA — analyse informative]\n"
                "Cette réponse est purement descriptive. "
                "Aucune action métier (stock, vente, dette, paiement) n'a été effectuée.\n"
                f"Question reçue (extrait) : {user_prompt[:240]}"
            )
        return AIResponse(
            ok=True,
            text=text,
            provider=self.name,
            latency_ms=int((time.monotonic() - started) * 1000),
            analytical=True,
            raw={"mock": True},
        )


def _redact_secrets(text: str, api_key: Optional[str] = None) -> str:
    """Masque clés API et patterns sensibles dans les messages d'erreur."""
    if not text:
        return text
    out = str(text)
    if api_key and len(api_key) >= 4:
        out = out.replace(api_key, "***REDACTED***")
    # Patterns courants (OpenAI sk-..., Bearer, etc.)
    import re
    out = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+", r"\1***REDACTED***", out)
    out = re.sub(r"(?i)(api[_-]?key\s*[:=]\s*)\S+", r"\1***REDACTED***", out)
    out = re.sub(r"\bsk-[A-Za-z0-9]{8,}\b", "***REDACTED***", out)
    out = re.sub(r"\bAIza[A-Za-z0-9_\-]{10,}\b", "***REDACTED***", out)
    return out


def validate_remote_base_url(base_url: str) -> tuple:
    """Valide l'URL du fournisseur distant.

    Retourne (ok: bool, error_code: Optional[str], message: Optional[str]).
    Règle Phase 2.1 : endpoints distants obligatoirement HTTPS.
    localhost / 127.0.0.1 en HTTP restent autorisés pour un serveur local de dev.
    """
    from urllib.parse import urlparse
    raw = (base_url or "").strip()
    if not raw:
        return False, "INSECURE_BASE_URL", "base_url vide"
    try:
        parsed = urlparse(raw)
    except Exception:
        return False, "INSECURE_BASE_URL", "base_url invalide"
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()
    if not scheme or not host:
        return False, "INSECURE_BASE_URL", "base_url sans schéma ou hôte"
    local_hosts = {"localhost", "127.0.0.1", "::1", "localhost.localdomain"}
    if scheme == "https":
        return True, None, None
    if scheme == "http" and host in local_hosts:
        return True, None, None
    if scheme == "http":
        return False, "INSECURE_BASE_URL", (
            f"Endpoint distant non sécurisé refusé (http://{host}). "
            "Utilisez HTTPS pour tout hôte non local."
        )
    return False, "INSECURE_BASE_URL", f"Schéma non autorisé: {scheme}"


class HTTPOpenAICompatibleProvider(BaseAIProvider):
    """Fournisseur HTTP compatible OpenAI / Grok / endpoints locaux.

    Sécurité Phase 2.1 :
    - enabled=False par défaut → aucun réseau
    - sans api_key → aucun réseau
    - base_url distant doit être HTTPS (http local autorisé uniquement)
    - la clé API n'apparaît jamais dans error_message / raw
    - la clé n'est jamais écrite en SQLite (mémoire process uniquement)
    """

    name = "http_openai_compatible"

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini",
        enabled: bool = False,
    ):
        self.api_key = (api_key or "").strip() or None
        self.base_url = (base_url or "").rstrip("/")
        self.model = model
        # Sécurité : désactivé par défaut — l'appelant doit activer explicitement
        self.enabled = bool(enabled) and bool(self.api_key)
        self._url_ok, self._url_error_code, self._url_error_message = validate_remote_base_url(
            self.base_url
        )

    def is_available(self) -> bool:
        return self.enabled and bool(self.api_key) and bool(self._url_ok)

    def __repr__(self) -> str:
        # Ne jamais afficher la clé
        return (
            f"HTTPOpenAICompatibleProvider(enabled={self.enabled}, "
            f"has_key={bool(self.api_key)}, base_url={self.base_url!r}, model={self.model!r})"
        )

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        timeout_sec: float = 20.0,
        max_tokens: int = 1024,
    ) -> AIResponse:
        if not self.api_key:
            return AIResponse(
                ok=False,
                text="",
                provider=self.name,
                error_code="MISSING_API_KEY",
                error_message="Clé API absente — aucun appel réseau effectué",
            )
        if not self.enabled:
            return AIResponse(
                ok=False,
                text="",
                provider=self.name,
                error_code="PROVIDER_DISABLED",
                error_message="Fournisseur HTTP désactivé (enabled=False)",
            )
        if not self._url_ok:
            return AIResponse(
                ok=False,
                text="",
                provider=self.name,
                error_code=self._url_error_code or "INSECURE_BASE_URL",
                error_message=self._url_error_message or "base_url non sécurisée",
            )

        # Appel HTTP réel uniquement si explicitement activé + HTTPS (ou local).
        started = time.monotonic()
        try:
            import urllib.request

            payload = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": int(max_tokens),
                "temperature": 0.2,
            }
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                f"{self.base_url}/chat/completions",
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(body)
            choices = parsed.get("choices") or []
            if not choices:
                return AIResponse(
                    ok=False,
                    text="",
                    provider=self.name,
                    error_code="INVALID_RESPONSE",
                    error_message="Réponse sans choices",
                    latency_ms=int((time.monotonic() - started) * 1000),
                    raw={"model": self.model, "note": "no_choices"},
                )
            text = (choices[0].get("message") or {}).get("content") or ""
            if not str(text).strip():
                return AIResponse(
                    ok=False,
                    text="",
                    provider=self.name,
                    error_code="INVALID_RESPONSE",
                    error_message="Contenu vide",
                    latency_ms=int((time.monotonic() - started) * 1000),
                    raw={"model": self.model, "note": "empty_content"},
                )
            return AIResponse(
                ok=True,
                text=str(text).strip(),
                provider=self.name,
                latency_ms=int((time.monotonic() - started) * 1000),
                analytical=True,
                raw={"model": self.model},  # jamais la clé, jamais le body brut complet
            )
        except Exception as exc:
            msg = _redact_secrets(str(exc)[:500], self.api_key)
            low = msg.lower()
            code = "TIMEOUT" if "timed out" in low or "timeout" in low else "PROVIDER_ERROR"
            if "name or service not known" in low or "network" in low or "connection" in low:
                code = "NETWORK_ERROR"
            return AIResponse(
                ok=False,
                text="",
                provider=self.name,
                error_code=code,
                error_message=msg,
                latency_ms=int((time.monotonic() - started) * 1000),
            )


def build_provider(
    kind: str = "mock",
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    enabled: bool = False,
    **kwargs: Any,
) -> BaseAIProvider:
    """Factory simple pour choisir un fournisseur sans toucher au métier."""
    k = (kind or "mock").lower().strip()
    if k in ("unavailable", "off", "disabled"):
        return UnavailableAIProvider(kwargs.get("reason", "IA indisponible"))
    if k in ("missing_api_key", "no_key"):
        return MissingAPIKeyProvider()
    if k == "mock":
        return MockAIProvider(**{kk: vv for kk, vv in kwargs.items() if kk in (
            "fixed_text", "fail_code", "fail_message", "delay_sec", "invalid_payload"
        )})
    if k in ("http", "openai", "grok", "openai_compatible"):
        return HTTPOpenAICompatibleProvider(
            api_key=api_key,
            base_url=base_url or "https://api.openai.com/v1",
            model=model or "gpt-4o-mini",
            enabled=enabled,
        )
    return UnavailableAIProvider(f"Fournisseur inconnu: {kind}")
