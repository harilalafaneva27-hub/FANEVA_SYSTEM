#!/usr/bin/env python3
"""
Tests PHASE 2 — FANEVA IA
=========================
1. contexte IA admin
2. contexte IA vendeur
3. confidentialité
4. isolation magasin
5. provider indisponible
6. réponse invalide
7. timeout
8. absence de clé API
9. IA ne peut pas écrire SQLite
10. non-régression (suites existantes via sous-process)
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(ROOT, "source")
sys.path.insert(0, SOURCE)

import faneva_hybrid as hybrid
from ai_data_service import AIDataService
from ai_provider import (
    MockAIProvider,
    UnavailableAIProvider,
    MissingAPIKeyProvider,
    HTTPOpenAICompatibleProvider,
    build_provider,
)
from faneva_ia import FanevaIA, run_faneva_ia_question
from ai_provider import BaseAIProvider, AIResponse


def _open_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    hybrid.run_migration(conn)
    hybrid.initialize_device_identity(conn)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS produits(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT UNIQUE NOT NULL,
            prix_achat INTEGER DEFAULT 0,
            prix_vente INTEGER DEFAULT 0,
            stock INTEGER DEFAULT 0,
            categorie TEXT DEFAULT 'General',
            actif INTEGER DEFAULT 1,
            hybrid_id TEXT,
            magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS ventes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            produit_id INTEGER, produit_nom TEXT, quantite INTEGER DEFAULT 0,
            prix_achat INTEGER DEFAULT 0, prix_vente INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0, benefice INTEGER DEFAULT 0,
            date_vente TEXT DEFAULT CURRENT_TIMESTAMP, sale_mode TEXT,
            transaction_id TEXT, magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS dettes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL, telephone TEXT,
            total INTEGER DEFAULT 0, paye INTEGER DEFAULT 0, reste INTEGER DEFAULT 0,
            statut TEXT DEFAULT 'ACTIF', magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS paiements_dettes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dette_id INTEGER, montant INTEGER,
            date_paiement TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO magasins (id, cle, nom, categorie) VALUES (1, 'quincaillerie', 'Quincaillerie', 'Quincaillerie')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO magasins (id, cle, nom, categorie) VALUES (2, 'cosmetiques', 'Cosmetiques', 'Cosmetiques')"
    )
    conn.execute(
        "INSERT INTO produits (nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id, actif) "
        "VALUES ('Produit Test', 100, 200, 10, 'prod-ia-001', 1, 1)"
    )
    hybrid._apply_stock_movement(
        conn, "prod-ia-001", 1, 10, "MIGRATION_INITIAL_STOCK",
        admin_id="t", admin_username="t",
    )
    conn.commit()
    return conn, path


def _fingerprint(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    parts = []
    for (name,) in cur.fetchall():
        try:
            n = cur.execute(f"SELECT COUNT(*) FROM [{name}]").fetchone()[0]
            parts.append(f"{name}:{n}")
        except Exception:
            parts.append(f"{name}:?")
    return "|".join(parts)


class FanevaIAPhase2Tests(unittest.TestCase):
    def setUp(self):
        self.conn, self.path = _open_db()

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass
        if os.path.exists(self.path):
            os.unlink(self.path)

    # 1. Contexte IA admin
    def test_01_admin_context(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(fixed_text="Analyse admin OK"),
        )
        ctx = ia.build_authorized_context()
        self.assertTrue(ctx["context"]["is_admin"])
        self.assertIsNotNone(ctx["stock"].get("total_value_achat"))
        self.assertEqual(ctx["sales"]["access"], "full")
        res = ia.ask("Quel est le stock total ?")
        self.assertTrue(res["ok"])
        self.assertIn("Analyse admin OK", res["answer"])
        self.assertEqual(res["actions_performed"], [])
        self.assertFalse(res["can_write_sqlite"])

    # 2. Contexte IA vendeur
    def test_02_vendeur_context(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="VENDEUR",
            provider=MockAIProvider(fixed_text="Analyse vendeur"),
        )
        ctx = ia.build_authorized_context()
        self.assertFalse(ctx["context"]["is_admin"])
        self.assertIsNone(ctx["stock"].get("total_value_achat"))
        self.assertEqual(ctx["sales"]["access"], "restricted")
        res = ia.ask("Résume le stock")
        self.assertTrue(res["ok"])
        self.assertEqual(res["context_meta"]["is_admin"], False)

    # 3. Confidentialité
    def test_03_confidentiality_no_secrets_no_achat_for_vendeur(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="VENDEUR",
            provider=MockAIProvider(),
        )
        ctx = ia.build_authorized_context()
        raw = json.dumps(ctx)
        for bad in ("prix_achat", "valeur_achat", "password", "api_key", "token", "secret"):
            self.assertNotIn(bad, raw)

        # Le prompt utilisateur ne doit pas contenir ces clés non plus
        system = ia._system_prompt()
        user = ia._user_prompt("test", ctx)
        combined = system + user
        for bad in ("password", "api_key", "Bearer", "password_hash"):
            self.assertNotIn(bad, combined)

        res = ia.ask("Donne les marges")
        # même si le mock répond, le contexte reste filtré
        self.assertEqual(res["actions_performed"], [])

    # 4. Isolation magasin
    def test_04_store_isolation(self):
        self.conn.execute(
            "INSERT INTO produits (nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id, actif) "
            "VALUES ('Cosme', 30, 60, 7, 'cosme-ia', 2, 1)"
        )
        hybrid._apply_stock_movement(
            self.conn, "cosme-ia", 2, 7, "MIGRATION_INITIAL_STOCK",
            admin_id="t", admin_username="t",
        )
        self.conn.commit()

        ia1 = FanevaIA(self.conn, magasin_id=1, role="ADMIN", provider=MockAIProvider())
        ia2 = FanevaIA(self.conn, magasin_id=2, role="ADMIN", provider=MockAIProvider())
        c1 = ia1.build_authorized_context()
        c2 = ia2.build_authorized_context()
        self.assertEqual(c1["stock"]["total_products"], 1)
        self.assertEqual(c2["stock"]["total_products"], 1)
        names1 = [p["nom"] for p in c1["stock"]["products"]]
        names2 = [p["nom"] for p in c2["stock"]["products"]]
        self.assertIn("Produit Test", names1)
        self.assertNotIn("Cosme", names1)
        self.assertIn("Cosme", names2)

    # 5. Provider indisponible
    def test_05_provider_unavailable(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=UnavailableAIProvider("maintenance"),
        )
        res = ia.ask("Stock ?")
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_code"], "PROVIDER_UNAVAILABLE")
        self.assertEqual(res["actions_performed"], [])

    # 6. Réponse invalide
    def test_06_invalid_response(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(invalid_payload=True),
        )
        res = ia.ask("Stock ?")
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_code"], "INVALID_RESPONSE")

    # 7. Timeout
    def test_07_timeout(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(delay_sec=5.0),
            timeout_sec=0.01,
        )
        res = ia.ask("Stock ?")
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_code"], "TIMEOUT")

    # 8. Absence de clé API
    def test_08_missing_api_key(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MissingAPIKeyProvider(),
        )
        res = ia.ask("Stock ?")
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_code"], "PROVIDER_UNAVAILABLE")

        http = HTTPOpenAICompatibleProvider(api_key=None, enabled=True)
        ia2 = FanevaIA(self.conn, magasin_id=1, role="ADMIN", provider=http)
        res2 = ia2.ask("Stock ?")
        self.assertFalse(res2["ok"])
        self.assertEqual(res2["error_code"], "MISSING_API_KEY")

    # 9. IA ne peut pas écrire SQLite
    def test_09_ia_cannot_write_sqlite(self):
        before = _fingerprint(self.conn)
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(fixed_text="ok"),
        )
        for _ in range(3):
            ia.ask("Analyse complète du stock et des ventes")
        after = _fingerprint(self.conn)
        self.assertEqual(before, after)
        self.assertFalse(ia.status()["can_write_sqlite"])
        self.assertFalse(ia.status()["can_mutate_business"])
        self.assertEqual(ia.ask("x")["actions_performed"], [])
        # pending_sync inchangé
        n = self.conn.execute("SELECT COUNT(*) FROM pending_sync").fetchone()[0]
        self.assertEqual(n, 0)

    # Status / factory
    def test_10_status_and_factory(self):
        ia = FanevaIA(self.conn, magasin_id=1, role="VENDEUR", provider=build_provider("mock"))
        st = ia.status()
        self.assertEqual(st["ia_version"], FanevaIA.VERSION)
        self.assertTrue(st["read_only"])
        self.assertEqual(st["provider"], "mock")

        off = build_provider("unavailable")
        self.assertFalse(off.is_available())

    def test_11_empty_question(self):
        ia = FanevaIA(self.conn, magasin_id=1, role="ADMIN", provider=MockAIProvider())
        res = ia.ask("   ")
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_code"], "EMPTY_QUESTION")

    def test_12_admin_sees_margins_in_context_vendeur_does_not(self):
        admin = FanevaIA(self.conn, magasin_id=1, role="ADMIN", provider=MockAIProvider())
        vend = FanevaIA(self.conn, magasin_id=1, role="VENDEUR", provider=MockAIProvider())
        ca = admin.build_authorized_context()
        cv = vend.build_authorized_context()
        self.assertEqual(ca["margins"]["access"], "full")
        self.assertEqual(cv["margins"]["access"], "restricted")


class FanevaIALifecycleTests(unittest.TestCase):
    """Lifecycle SQLite : prepare → CLOSE → provider.complete (finalize)."""

    def setUp(self):
        self.conn, self.path = _open_db()

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_sqlite_closed_before_provider_complete(self):
        """Échoue si provider.complete() est appelé alors que la connexion est encore ouverte."""

        class TrackingConnection:
            def __init__(self, real):
                self._real = real
                self.connection_closed = False

            def close(self):
                self.connection_closed = True
                return self._real.close()

            def cursor(self, *a, **k):
                return self._real.cursor(*a, **k)

            def execute(self, *a, **k):
                return self._real.execute(*a, **k)

            def executemany(self, *a, **k):
                return self._real.executemany(*a, **k)

            def commit(self, *a, **k):
                return self._real.commit(*a, **k)

            def rollback(self, *a, **k):
                return self._real.rollback(*a, **k)

            def __getattr__(self, name):
                return getattr(self._real, name)

        class TrackingCM:
            def __init__(self, real_conn):
                self.tracked = TrackingConnection(real_conn)
                self.entered = False

            def __enter__(self):
                self.entered = True
                self.tracked.connection_closed = False
                return self.tracked

            def __exit__(self, *exc):
                self.tracked.close()
                return False

        class AssertClosedProvider(BaseAIProvider):
            name = "assert_closed"

            def __init__(self, tracked_holder):
                self.tracked_holder = tracked_holder
                self.complete_called = False

            def is_available(self):
                return True

            def complete(self, system_prompt, user_prompt, *, timeout_sec=20.0, max_tokens=1024):
                self.complete_called = True
                tracked = self.tracked_holder["cm"].tracked
                if not tracked.connection_closed:
                    raise AssertionError(
                        "provider.complete() appelé AVANT fermeture SQLite "
                        "(lifecycle invalide: prepare→complete→close)"
                    )
                return AIResponse(
                    ok=True,
                    text="lifecycle OK — connexion déjà fermée",
                    provider=self.name,
                    analytical=True,
                )

        holder = {}
        cm = TrackingCM(self.conn)
        holder["cm"] = cm
        provider = AssertClosedProvider(holder)

        def factory():
            return cm

        result = run_faneva_ia_question(
            "Analyse du stock",
            factory,
            provider=provider,
            magasin_id=1,
            role="ADMIN",
        )
        self.assertTrue(provider.complete_called, "provider.complete() doit être appelé")
        self.assertTrue(cm.tracked.connection_closed, "connexion doit être fermée")
        self.assertTrue(result.get("ok"), msg=str(result))
        self.assertIn("lifecycle OK", result.get("answer", ""))

    def test_prepare_has_no_provider_complete(self):
        calls = []

        class SpyProvider(BaseAIProvider):
            name = "spy"

            def is_available(self):
                return True

            def complete(self, *a, **k):
                calls.append("complete")
                return AIResponse(ok=True, text="should not run in prepare", provider=self.name)

        ia = FanevaIA(self.conn, magasin_id=1, role="ADMIN", provider=SpyProvider())
        prepared = ia.prepare("Stock ?")
        self.assertTrue(prepared.get("_prepared"))
        self.assertEqual(calls, [], "prepare() ne doit pas appeler provider.complete()")
        result = ia.finalize(prepared)
        self.assertEqual(calls, ["complete"])
        self.assertTrue(result["ok"])


class FanevaIANonRegressionTests(unittest.TestCase):
    """Non-régression des suites Phase 1 / 1.1."""

    def test_existing_suites_still_pass(self):
        import subprocess
        env = os.environ.copy()
        env["PYTHONPATH"] = SOURCE + os.pathsep + env.get("PYTHONPATH", "")
        for script, expected in (
            ("test_canonical_stock_views.py", "Ran 54 tests"),
            ("test_ai_data_service.py", "Ran 20 tests"),
        ):
            result = subprocess.run(
                [sys.executable, "-u", os.path.join(ROOT, script)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=90,
                env=env,
            )
            combined = (result.stdout or "") + "\n" + (result.stderr or "")
            self.assertEqual(result.returncode, 0, msg=f"{script}:\n{combined}")
            self.assertIn(expected, combined)
            self.assertIn("OK", combined)


if __name__ == "__main__":
    unittest.main(verbosity=2)
