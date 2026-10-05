#!/usr/bin/env python3
"""
PHASE 2.2 — Tests intégration UI FANEVA IA (sans runtime Kivy)
==============================================================
1. ouverture écran (méthodes présentes)
2. question française
3. question malagasy
4. réponse IA
5. provider indisponible
6. vendeur
7. admin
8. isolation magasin
9. aucune modification SQLite
10. aucun INSERT/UPDATE/DELETE dans le flux IA UI
11. application fonctionnelle sans module IA
"""

import ast
import os
import sqlite3
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(ROOT, "source")
sys.path.insert(0, SOURCE)

import faneva_hybrid as hybrid
from faneva_ia import FanevaIA
from faneva_ia_lang import detect_language, ui_text, suggestion_pairs, map_error_message
from ai_provider import MockAIProvider, UnavailableAIProvider


MAIN_PATH = os.path.join(SOURCE, "main.py")


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
            prix_achat INTEGER DEFAULT 0, prix_vente INTEGER DEFAULT 0,
            stock INTEGER DEFAULT 0, categorie TEXT DEFAULT 'General',
            actif INTEGER DEFAULT 1, hybrid_id TEXT, magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS ventes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total INTEGER DEFAULT 0, benefice INTEGER DEFAULT 0,
            date_vente TEXT DEFAULT CURRENT_TIMESTAMP, sale_mode TEXT
        );
        CREATE TABLE IF NOT EXISTS dettes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL, telephone TEXT,
            total INTEGER DEFAULT 0, paye INTEGER DEFAULT 0,
            reste INTEGER DEFAULT 0, statut TEXT DEFAULT 'ACTIF', magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS paiements_dettes(
            id INTEGER PRIMARY KEY AUTOINCREMENT, dette_id INTEGER, montant INTEGER,
            date_paiement TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute("INSERT OR IGNORE INTO magasins (id, cle, nom) VALUES (1, 'quincaillerie', 'Quincaillerie')")
    conn.execute("INSERT OR IGNORE INTO magasins (id, cle, nom) VALUES (2, 'cosmetiques', 'Cosmetiques')")
    conn.execute(
        "INSERT INTO produits (nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id, actif) "
        "VALUES ('P1', 100, 200, 8, 'ui-p1', 1, 1)"
    )
    hybrid._apply_stock_movement(
        conn, "ui-p1", 1, 8, "MIGRATION_INITIAL_STOCK", admin_id="t", admin_username="t"
    )
    conn.commit()
    return conn, path


def _fp(conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    parts = []
    for (n,) in cur.fetchall():
        try:
            c = cur.execute(f"SELECT COUNT(*) FROM [{n}]").fetchone()[0]
            parts.append(f"{n}:{c}")
        except Exception:
            parts.append(f"{n}:?")
    return "|".join(parts)


class FanevaIAUIIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(MAIN_PATH, encoding="utf-8") as f:
            cls.main_src = f.read()
        cls.tree = ast.parse(cls.main_src)
        cls.methods = {}
        for node in ast.walk(cls.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                cls.methods[node.name] = ast.get_source_segment(cls.main_src, node) or ""

    def setUp(self):
        self.conn, self.path = _open_db()

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass
        if os.path.exists(self.path):
            os.unlink(self.path)

    # 1. Ouverture écran — méthodes UI présentes
    def test_01_screen_methods_present(self):
        for name in ("show_faneva_ia", "_ia_run_question", "_ia_on_send", "_ia_apply_result", "_ia_provider"):
            self.assertIn(name, self.methods, f"méthode UI manquante: {name}")
        self.assertIn("🤖 FANEVA IA", self.main_src)
        self.assertIn("self.show_faneva_ia", self.main_src)
        # Menu dans show_dashboard
        self.assertIn("FANEVA IA", self.methods.get("show_dashboard", ""))

    # 2. Question française
    def test_02_french_question(self):
        self.assertEqual(detect_language("Analyse les ventes du jour"), "fr")
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(fixed_text="Résumé des ventes OK"),
        )
        res = ia.ask("Analyse les ventes du jour")
        self.assertTrue(res["ok"])
        self.assertIn("ventes", res["answer"].lower() + "ok")

    # 3. Question malagasy
    def test_03_malagasy_question(self):
        self.assertEqual(detect_language("Inona ny stoka sy ny varotra anio"), "mg")
        sys_p = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN", provider=MockAIProvider()
        )._system_prompt(lang="mg")
        self.assertIn("malagasy", sys_p.lower())

    # 4. Réponse IA
    def test_04_ia_response(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(fixed_text="Réponse structurée stock=8"),
        )
        res = ia.ask("Analyse du stock")
        self.assertTrue(res["ok"])
        self.assertIn("stock", res["answer"].lower())
        self.assertEqual(res["actions_performed"], [])

    # 5. Provider indisponible
    def test_05_provider_unavailable(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=UnavailableAIProvider("off"),
        )
        res = ia.ask("Stock ?")
        self.assertFalse(res["ok"])
        msg = map_error_message(res["error_code"], "fr")
        self.assertTrue(len(msg) > 5)

    # 6. Vendeur
    def test_06_vendeur(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="VENDEUR",
            provider=MockAIProvider(fixed_text="ok vendeur"),
        )
        ctx = ia.build_authorized_context()
        self.assertFalse(ctx["context"]["is_admin"])
        self.assertNotIn("prix_achat", str(ctx["stock"].get("products")))

    # 7. Admin
    def test_07_admin(self):
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(fixed_text="ok admin"),
        )
        ctx = ia.build_authorized_context()
        self.assertTrue(ctx["context"]["is_admin"])
        self.assertIsNotNone(ctx["stock"].get("total_value_achat"))

    # 8. Isolation magasin
    def test_08_store_isolation(self):
        self.conn.execute(
            "INSERT INTO produits (nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id, actif) "
            "VALUES ('Cos', 10, 20, 2, 'ui-c2', 2, 1)"
        )
        hybrid._apply_stock_movement(
            self.conn, "ui-c2", 2, 2, "MIGRATION_INITIAL_STOCK", admin_id="t", admin_username="t"
        )
        self.conn.commit()
        ia1 = FanevaIA(self.conn, magasin_id=1, role="ADMIN", provider=MockAIProvider())
        ia2 = FanevaIA(self.conn, magasin_id=2, role="ADMIN", provider=MockAIProvider())
        n1 = [p["nom"] for p in ia1.build_authorized_context()["stock"]["products"]]
        n2 = [p["nom"] for p in ia2.build_authorized_context()["stock"]["products"]]
        self.assertEqual(n1, ["P1"])
        self.assertEqual(n2, ["Cos"])

    # 9 + 10. Pas de modification SQLite / pas d'écriture
    def test_09_10_no_sqlite_mutation(self):
        before = _fp(self.conn)
        ia = FanevaIA(
            self.conn, magasin_id=1, role="ADMIN",
            provider=MockAIProvider(fixed_text="analyse"),
        )
        for q in ("Analyse stock", "Inona ny stoka", "crée une vente"):
            ia.ask(q)
        self.assertEqual(_fp(self.conn), before)
        # UI helpers ne contiennent pas de SQL d'écriture
        for banned in ("INSERT INTO", "UPDATE ", "DELETE FROM", "DROP TABLE", "ALTER TABLE"):
            self.assertNotIn(banned, open(os.path.join(SOURCE, "faneva_ia_lang.py"), encoding="utf-8").read())
        # show_faneva_ia body: pas d'écriture métier
        body = self.methods.get("show_faneva_ia", "")
        for banned in ("record_sale", "record_debt", "record_payment", "INSERT INTO", "UPDATE produits"):
            self.assertNotIn(banned, body)

    # 11. App fonctionnelle sans module IA (import optionnel)
    def test_11_app_works_without_ia_module(self):
        self.assertIn("HAS_FANEVA_IA", self.main_src)
        self.assertIn("except Exception", self.main_src)
        # show_faneva_ia gère l'absence du module
        body = self.methods.get("show_faneva_ia", "")
        self.assertIn("HAS_FANEVA_IA", body)
        self.assertIn("show_dashboard", body)

    def test_ui_strings_bilingual(self):
        self.assertIn("Assistant", ui_text("title", "fr"))
        self.assertIn("Mpanampy", ui_text("title", "mg"))
        self.assertEqual(len(suggestion_pairs("fr")), 5)
        self.assertEqual(len(suggestion_pairs("mg")), 5)

    def test_worker_uses_faneva_ia_not_raw_sql(self):
        body = self.methods.get("_ia_run_question", "")
        # Architecture cible : UI délègue au service, n'ouvre pas SQLite
        self.assertIn("run_faneva_ia_question", body)
        self.assertNotIn("with get_db_connection", body)
        self.assertNotIn("get_db_connection()", body)
        self.assertNotIn("SELECT * FROM", body)
        self.assertNotIn("FanevaIA(", body)

    def test_ui_does_not_open_sqlite_for_ia(self):
        body = self.methods.get("_ia_run_question", "")
        self.assertIn("run_faneva_ia_question", body)
        self.assertIn("_ia_db_factory", body)
        self.assertIn("_ia_db_factory", self.methods)
        # Ouverture SQLite uniquement dans la factory dédiée service
        factory_body = self.methods.get("_ia_db_factory", "")
        self.assertIn("get_db_connection", factory_body)

    def test_service_entry_run_faneva_ia_question(self):
        from faneva_ia import run_faneva_ia_question
        import contextlib

        @contextlib.contextmanager
        def factory():
            yield self.conn

        res = run_faneva_ia_question(
            "Analyse du stock",
            factory,
            provider=UnavailableAIProvider("off"),
            magasin_id=1,
            role="ADMIN",
        )
        self.assertFalse(res["ok"])
        self.assertEqual(res["error_code"], "PROVIDER_UNAVAILABLE")
        self.assertEqual(res["actions_performed"], [])
        self.assertFalse(res["can_write_sqlite"])

        res2 = run_faneva_ia_question(
            "Analyse du stock",
            factory,
            provider=MockAIProvider(fixed_text="stock ok"),
            magasin_id=1,
            role="ADMIN",
        )
        self.assertTrue(res2["ok"])
        self.assertIn("stock", res2["answer"].lower())


class FanevaIAUINonRegression(unittest.TestCase):
    def test_prior_suites_still_pass(self):
        import subprocess
        env = os.environ.copy()
        env["PYTHONPATH"] = SOURCE + os.pathsep + env.get("PYTHONPATH", "")
        for script, expected in (
            ("test_canonical_stock_views.py", "Ran 54 tests"),
            ("test_ai_data_service.py", "Ran 20 tests"),
            ("test_faneva_ia.py", "Ran 15 tests"),
            ("test_faneva_ia_security.py", "Ran 14 tests"),
        ):
            r = subprocess.run(
                [sys.executable, "-u", os.path.join(ROOT, script)],
                cwd=ROOT, capture_output=True, text=True, timeout=90, env=env,
            )
            combined = (r.stdout or "") + "\n" + (r.stderr or "")
            self.assertEqual(r.returncode, 0, msg=f"{script}\n{combined}")
            self.assertIn(expected, combined)


if __name__ == "__main__":
    unittest.main(verbosity=2)
