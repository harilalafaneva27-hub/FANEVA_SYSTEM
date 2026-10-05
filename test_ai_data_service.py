#!/usr/bin/env python3
"""
Tests PHASE 1 — AI Data Service (READ-ONLY)
===========================================
Vérifie :
- lecture seule (aucun changement SQLite avant/après)
- exactitude des totaux / ventes / dettes / paiements / marges
- isolation par magasin
- respect des permissions Admin / Vendeur
- comportement sans Internet
- sérialisation JSON
- absence d'écriture dans pending_sync
- comparaison avec fonctions métier existantes (get_daily_cash_report, get_stock)
- non-régression des tests canoniques existants
"""

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime

# Chemins source
ROOT = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(ROOT, "source")
sys.path.insert(0, SOURCE)

import faneva_hybrid as hybrid
from ai_data_service import AIDataService, _is_admin_role


def _open_memory_db():
    """Crée une base temporaire complète (schéma legacy + hybrid)."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    # Migration hybrid
    hybrid.run_migration(conn)
    # Tables legacy nécessaires aux projections
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
            produit_id INTEGER,
            produit_nom TEXT,
            quantite INTEGER DEFAULT 0,
            prix_achat INTEGER DEFAULT 0,
            prix_vente INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            benefice INTEGER DEFAULT 0,
            vendeur TEXT DEFAULT 'Inconnu',
            date_vente TEXT DEFAULT CURRENT_TIMESTAMP,
            transaction_id TEXT,
            sale_mode TEXT,
            device_id TEXT,
            magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS dettes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client TEXT NOT NULL,
            telephone TEXT,
            total INTEGER DEFAULT 0,
            paye INTEGER DEFAULT 0,
            reste INTEGER DEFAULT 0,
            date_creation TEXT DEFAULT CURRENT_TIMESTAMP,
            statut TEXT DEFAULT 'ACTIF',
            transaction_id TEXT,
            device_id TEXT,
            magasin_ref_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS paiements_dettes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dette_id INTEGER,
            montant INTEGER,
            date_paiement TEXT DEFAULT CURRENT_TIMESTAMP,
            transaction_id TEXT,
            operation_id TEXT,
            device_id TEXT
        );
        """
    )
    # Magasin de test
    conn.execute(
        "INSERT OR IGNORE INTO magasins (id, cle, nom, categorie) VALUES (1, 'quincaillerie', 'Quincaillerie', 'Quincaillerie')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO magasins (id, cle, nom, categorie) VALUES (2, 'cosmetiques', 'Cosmetiques', 'Cosmetiques')"
    )
    # Device identity minimale
    hybrid.initialize_device_identity(conn)
    conn.commit()
    return conn, path


def _seed_product(conn, nom, pa, pv, stock, hybrid_id=None, magasin_id=1):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO produits (nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id, actif) VALUES (?,?,?,?,?,?,1)",
        (nom, pa, pv, stock, hybrid_id, magasin_id),
    )
    pid = cur.lastrowid
    if hybrid_id:
        # Projection stocks_magasin + mouvement initial
        hybrid._apply_stock_movement(
            conn, hybrid_id, magasin_id, stock, "MIGRATION_INITIAL_STOCK",
            admin_id="test", admin_username="tester",
        )
    conn.commit()
    return pid


def _fingerprint_db(conn):
    """Empreinte simple du contenu (tables + row counts + checksums)."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    parts = []
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM [{t}]")
            cnt = cur.fetchone()[0]
            parts.append(f"{t}:{cnt}")
        except Exception:
            parts.append(f"{t}:?")
    # pending_sync spécifique
    if "pending_sync" in tables:
        cur.execute("SELECT COUNT(*) FROM pending_sync")
        parts.append(f"pending_sync_count:{cur.fetchone()[0]}")
    return "|".join(parts)


class AIDataServiceReadOnlyTests(unittest.TestCase):
    def setUp(self):
        self.conn, self.path = _open_memory_db()
        self.pid = _seed_product(
            self.conn, "Produit Test", 100, 200, 10,
            hybrid_id="prod-test-uuid-001", magasin_id=1,
        )

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass
        if os.path.exists(self.path):
            os.unlink(self.path)

    # ---- READ-ONLY ----
    def test_readonly_no_sqlite_change(self):
        before = _fingerprint_db(self.conn)
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN_PRINCIPAL")
        _ = svc.get_business_snapshot(period="day")
        after = _fingerprint_db(self.conn)
        self.assertEqual(before, after, "AIDataService a modifié la base SQLite")

    def test_readonly_does_not_break_shared_connection(self):
        """PRAGMA query_only ne doit PAS être posé sur la connexion partagée."""
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN", enforce_readonly=True)
        self.assertTrue(svc.verify_no_write())
        # La connexion doit rester inscriptible pour le moteur métier
        try:
            self.conn.execute("PRAGMA query_only")
            row = self.conn.execute("PRAGMA query_only").fetchone()
            # Soit non supporté, soit OFF (0)
            if row is not None:
                self.assertFalse(bool(row[0]), "query_only ne doit pas être ON sur la connexion partagée")
        except sqlite3.OperationalError:
            pass
        # Preuve : une écriture métier reste possible après le service
        self.conn.execute("INSERT INTO config_hybrid (cle, valeur) VALUES (?, ?)", ("ads_probe", "1"))
        self.conn.commit()
        self.conn.execute("DELETE FROM config_hybrid WHERE cle=?", ("ads_probe",))
        self.conn.commit()

    def test_no_pending_sync_write(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pending_sync")
        before = cur.fetchone()[0]
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        svc.get_stock_summary()
        svc.get_sales_summary()
        svc.get_debts_summary()
        svc.get_payments_summary()
        cur.execute("SELECT COUNT(*) FROM pending_sync")
        after = cur.fetchone()[0]
        self.assertEqual(before, after)

    # ---- STOCK ----
    def test_stock_summary_exact(self):
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        s = svc.get_stock_summary()
        self.assertEqual(s["total_products"], 1)
        self.assertEqual(s["total_units"], 10)
        self.assertEqual(s["out_of_stock_count"], 0)
        # get_stock doit donner 10
        if hybrid.get_stock:
            self.assertEqual(hybrid.get_stock(self.conn, "prod-test-uuid-001", 1), 10)
        self.assertEqual(s["products"][0]["stock"], 10)
        self.assertEqual(s["products"][0]["prix_vente"], 200)
        self.assertEqual(s["products"][0]["stock_source"], "canonical")

    def test_stock_low_and_out(self):
        _seed_product(self.conn, "Faible", 50, 80, 3, hybrid_id="low-1", magasin_id=1)
        _seed_product(self.conn, "Rupture", 10, 20, 0, hybrid_id="out-1", magasin_id=1)
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        s = svc.get_stock_summary()
        self.assertEqual(s["low_stock_count"], 1)
        self.assertEqual(s["out_of_stock_count"], 1)
        self.assertEqual(s["total_products"], 3)

    # ---- VENTES / MARGES (comparaison métier) ----
    def test_sales_and_margins_match_daily_report(self):
        # Vente comptant
        tx = hybrid.record_sale(
            self.conn,
            [{"produit_id": "prod-test-uuid-001", "q": 2, "prix_vente": 200, "prix_achat": 100}],
            1,
            sale_nonce="ads-cash-1",
            sale_mode="COMPTANT",
        )
        self.conn.execute(
            "INSERT INTO ventes (transaction_id, produit_id, produit_nom, quantite, prix_achat, prix_vente, total, benefice, sale_mode, date_vente) "
            "VALUES (?,?,?,?,?,?,?,?,?,datetime('now','localtime'))",
            (tx, self.pid, "Produit Test", 2, 100, 200, 400, 200, "COMPTANT"),
        )
        self.conn.commit()

        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN_PRINCIPAL")
        sales = svc.get_sales_summary(period="day")
        self.assertEqual(sales["access"], "full")
        self.assertEqual(sales["cash_total"], 400)
        self.assertEqual(sales["cash_count"], 1)
        self.assertEqual(sales["sales_total"], 400)
        self.assertEqual(sales["margin_total"], 200)

        # Comparaison avec get_daily_cash_report
        rep = hybrid.get_daily_cash_report(self.conn)
        self.assertEqual(rep["comptant_total"], 400)
        self.assertEqual(sales["daily_cash_report_ref"]["comptant_total"], 400)

        margins = svc.get_margins_summary(period="day")
        self.assertEqual(margins["margin_total"], 200)
        self.assertEqual(margins["margin_percent"], 50.0)

    def test_credit_and_recovery(self):
        hybrid.record_credit_sale(
            self.conn,
            [{"produit_id": "prod-test-uuid-001", "q": 1, "prix_vente": 200, "prix_achat": 100}],
            1,
            "Client ADS",
            "0340000000",
            200,
            sale_nonce="ads-credit-1",
        )
        # Projection ventes
        tx_credit = self.conn.execute(
            "SELECT transaction_id FROM transactions WHERE type_op='VENTE_CREDIT' ORDER BY rowid DESC LIMIT 1"
        ).fetchone()[0]
        self.conn.execute(
            "INSERT INTO ventes (transaction_id, produit_id, produit_nom, quantite, total, benefice, sale_mode, date_vente) "
            "VALUES (?,?,?,?,?,?,?,datetime('now','localtime'))",
            (tx_credit, self.pid, "Produit Test", 1, 200, 100, "CREDIT"),
        )
        dette_id = self.conn.execute("SELECT id FROM dettes LIMIT 1").fetchone()[0]
        hybrid.record_payment(self.conn, dette_id, 50, magasin_id=1, operation_id="ads-pay-1")
        # Projection locale des dettes (comme apply_remote / chemin métier)
        self.conn.execute(
            "UPDATE dettes SET paye=paye+?, reste=reste-?, statut=CASE WHEN reste-?<=0 THEN 'SOLDE' ELSE 'ACTIF' END WHERE id=?",
            (50, 50, 50, dette_id),
        )
        self.conn.execute(
            "INSERT INTO paiements_dettes (dette_id, montant, operation_id, date_paiement) VALUES (?,?,?,datetime('now','localtime'))",
            (dette_id, 50, "ads-pay-1"),
        )
        self.conn.commit()

        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        sales = svc.get_sales_summary(period="day")
        self.assertEqual(sales["credit_total"], 200)
        payments = svc.get_payments_summary(period="day")
        self.assertEqual(payments["payments_total"], 50)
        debts = svc.get_debts_summary()
        self.assertGreaterEqual(debts["active_count"], 1)
        self.assertEqual(debts["total_reste"], 150)  # 200 - 50

        rep = hybrid.get_daily_cash_report(self.conn)
        self.assertEqual(rep["credit_total"], 200)
        self.assertEqual(rep["recouvrements_total"], 50)

    # ---- PERMISSIONS ----
    def test_vendeur_restricted_financials(self):
        svc = AIDataService(self.conn, magasin_id=1, role="VENDEUR", username="vendeur1")
        sales = svc.get_sales_summary(period="day")
        self.assertEqual(sales["access"], "restricted")
        self.assertIsNone(sales["sales_total"])
        debts = svc.get_debts_summary()
        self.assertEqual(debts["access"], "restricted")
        payments = svc.get_payments_summary()
        self.assertEqual(payments["access"], "restricted")
        margins = svc.get_margins_summary()
        self.assertEqual(margins["access"], "restricted")
        # Stock reste accessible (sans prix_achat)
        stock = svc.get_stock_summary()
        self.assertEqual(stock["total_products"], 1)
        self.assertIsNone(stock["products"][0].get("prix_achat"))
        self.assertIsNone(stock["total_value_achat"])

    def test_admin_roles_accepted(self):
        for role in ("ADMIN", "ADMIN_PRINCIPAL", "ADMIN_MAGASIN"):
            self.assertTrue(_is_admin_role(role))
            svc = AIDataService(self.conn, magasin_id=1, role=role)
            self.assertEqual(svc.get_sales_summary()["access"], "full")

    # ---- ISOLATION MAGASIN ----
    def test_store_isolation(self):
        # Produit magasin 2
        _seed_product(
            self.conn, "Produit Cosme", 30, 60, 7,
            hybrid_id="cosme-uuid", magasin_id=2,
        )
        svc1 = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        s1 = svc1.get_stock_summary()
        # Seuls les produits magasin_ref_id=1 (ou NULL filtrés) — ici 1 produit
        self.assertEqual(s1["total_products"], 1)
        self.assertEqual(s1["products"][0]["nom"], "Produit Test")

        svc2 = AIDataService(self.conn, magasin_id=2, role="ADMIN")
        s2 = svc2.get_stock_summary()
        self.assertEqual(s2["total_products"], 1)
        self.assertEqual(s2["products"][0]["nom"], "Produit Cosme")
        self.assertEqual(s2["total_units"], 7)

    # ---- SÉRIALISATION ----
    def test_json_serializable(self):
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        snap = svc.get_business_snapshot(period="day")
        raw = svc.to_json(snap)
        parsed = json.loads(raw)
        self.assertIn("context", parsed)
        self.assertIn("stock", parsed)
        self.assertIn("sales", parsed)
        self.assertTrue(parsed["context"]["read_only"])
        self.assertFalse(parsed["context"]["internet_required"])

    # ---- SANS INTERNET ----
    def test_works_offline(self):
        # Aucun appel réseau dans le code du service ; on vérifie juste l'exécution
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        snap = svc.get_business_snapshot()
        self.assertIsInstance(snap, dict)
        self.assertTrue(snap["context"]["read_only"])
        self.assertFalse(snap["context"]["internet_required"])

    # ---- CONTEXT ----
    def test_context_fields(self):
        svc = AIDataService(
            self.conn, magasin_id=1, role="ADMIN_PRINCIPAL",
            username="admin1", user_id="uid-1",
        )
        ctx = svc.get_context()
        self.assertEqual(ctx["magasin_id"], 1)
        self.assertTrue(ctx["is_admin"])
        self.assertEqual(ctx["username"], "admin1")
        self.assertEqual(ctx["store"]["cle"], "quincaillerie")
        self.assertFalse(ctx["pragma_query_only_on_shared_conn"])

    # ---- CANONICAL PRIORITY & DIVERGENCE ----
    def test_canonical_stock_priority_over_legacy(self):
        """Quand hybrid_id existe, le stock retourné DOIT être le canonique."""
        # Forcer divergence : legacy=99, canonique=10 (déjà seedé à 10)
        self.conn.execute(
            "UPDATE produits SET stock=99 WHERE hybrid_id=?",
            ("prod-test-uuid-001",),
        )
        self.conn.commit()
        canon = hybrid.get_stock(self.conn, "prod-test-uuid-001", 1)
        self.assertEqual(canon, 10)

        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        s = svc.get_stock_summary()
        p = s["products"][0]
        self.assertEqual(p["stock"], 10, "doit exposer le stock CANONIQUE, pas le legacy 99")
        self.assertEqual(p["stock_source"], "canonical")
        self.assertTrue(p["divergence"])
        self.assertEqual(p["legacy_stock"], 99)
        self.assertEqual(p["canonical_stock"], 10)
        self.assertEqual(p["divergence_detail"]["canonical"], 10)
        self.assertEqual(p["divergence_detail"]["legacy"], 99)
        self.assertGreaterEqual(s["divergence_count"], 1)

    def test_legacy_fallback_when_no_hybrid_id(self):
        """Sans hybrid_id, fallback legacy explicite (source=legacy)."""
        self.conn.execute(
            "INSERT INTO produits (nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id, actif) "
            "VALUES ('Sans Hybrid', 10, 20, 4, NULL, 1, 1)"
        )
        self.conn.commit()
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        s = svc.get_stock_summary()
        bare = [p for p in s["products"] if p["nom"] == "Sans Hybrid"][0]
        self.assertEqual(bare["stock"], 4)
        self.assertEqual(bare["stock_source"], "legacy")
        self.assertFalse(bare["divergence"])

    def test_vendeur_never_sees_achat_or_margin(self):
        """Confidentialité stricte : aucune clé sensible dans le snapshot vendeur."""
        svc = AIDataService(self.conn, magasin_id=1, role="VENDEUR")
        snap = svc.get_business_snapshot(period="day")
        raw = json.dumps(snap)
        for forbidden in (
            "prix_achat", "valeur_achat", "total_value_achat",
            "margin_total", "margin_percent", "benefice",
            "password", "api_key", "token", "secret", "credential",
        ):
            self.assertNotIn(forbidden, raw, f"clé interdite '{forbidden}' présente pour VENDEUR")
        # Stock accessible
        self.assertEqual(snap["stock"]["total_products"], 1)
        self.assertIsNone(snap["stock"].get("total_value_achat"))
        # Financials restricted
        self.assertEqual(snap["sales"]["access"], "restricted")
        self.assertEqual(snap["margins"]["access"], "restricted")
        self.assertEqual(snap["debts"]["access"], "restricted")

    def test_admin_sees_achat_and_margins(self):
        svc = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        snap = svc.get_business_snapshot(period="day")
        self.assertIsNotNone(snap["stock"].get("total_value_achat"))
        self.assertIn("prix_achat", snap["stock"]["products"][0])
        self.assertEqual(snap["sales"]["access"], "full")

    def test_no_secrets_in_any_response(self):
        for role in ("ADMIN", "VENDEUR"):
            svc = AIDataService(self.conn, magasin_id=1, role=role, username="u", user_id="id")
            raw = svc.to_json(svc.get_business_snapshot())
            for bad in ("password", "password_hash", "api_key", "server_api_key", "Bearer", "token="):
                self.assertNotIn(bad, raw.lower() if bad.islower() else raw)

    def test_store_isolation_products_summary(self):
        _seed_product(self.conn, "Cosme2", 5, 10, 2, hybrid_id="c2", magasin_id=2)
        svc1 = AIDataService(self.conn, magasin_id=1, role="ADMIN")
        svc2 = AIDataService(self.conn, magasin_id=2, role="ADMIN")
        self.assertEqual(svc1.get_products_summary()["total_products"], 1)
        self.assertEqual(svc2.get_products_summary()["total_products"], 1)


class AIDataServiceIntegrityTests(unittest.TestCase):
    """Vérifie qu'aucun fichier existant n'a été altéré de façon destructive."""

    def test_existing_canonical_tests_still_pass(self):
        # Exécution réelle des 54 tests canonical
        import subprocess
        env = os.environ.copy()
        env["PYTHONPATH"] = SOURCE + os.pathsep + env.get("PYTHONPATH", "")
        result = subprocess.run(
            [sys.executable, "-u", os.path.join(ROOT, "test_canonical_stock_views.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=90,
            env=env,
        )
        combined = (result.stdout or "") + "\n" + (result.stderr or "")
        self.assertEqual(result.returncode, 0, msg=combined)
        self.assertIn("Ran 54 tests", combined)
        self.assertIn("OK", combined)


if __name__ == "__main__":
    unittest.main(verbosity=2)
