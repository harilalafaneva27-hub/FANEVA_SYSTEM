"""
FANEVA SYSTEM — AI Data Service (PHASE 1)
=========================================
Couche d'abstraction READ-ONLY entre SQLite FANEVA et une future couche FANEVA IA.

Règles strictes de cette phase :
- Aucune écriture SQLite (SELECT uniquement)
- Aucune création de transaction, mouvement, dette, paiement, pending_sync
- Aucun appel réseau
- Respect strict du magasin / contexte courant
- Respect des permissions Admin / Vendeur existantes
- SQLite existante reste la source de vérité
- Réutilisation des fonctions métier déterministes (get_stock, get_daily_cash_report, …)

Architecture cible (future) :
  Utilisateur → FANEVA IA → AIDataService → données structurées → modèle IA
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

# Fonctions métier déterministes du noyau HYBRID (aucune UI)
try:
    from faneva_hybrid import (
        get_stock,
        get_daily_cash_report,
        resolve_canonical_product_id,
        HYBRID_VERSION,
    )
except ImportError:  # contexte test minimal / import isolé
    get_stock = None
    get_daily_cash_report = None
    resolve_canonical_product_id = None
    HYBRID_VERSION = "unknown"


def _is_admin_role(role: Optional[str]) -> bool:
    """Miroir de is_user_admin_hybrid (main.py) sans dépendance UI."""
    if not role:
        return False
    r = str(role).upper()
    return r in ("ADMIN", "ADMIN_PRINCIPAL", "ADMIN_MAGASIN")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def _period_bounds(period: str) -> Tuple[Optional[str], Optional[str]]:
    """Retourne (date_from, date_to) inclusifs au format YYYY-MM-DD (local).

    period : 'day' | 'week' | 'month' | 'all'
    """
    today = datetime.now().date()
    p = (period or "all").lower().strip()
    if p == "day":
        d = today.isoformat()
        return d, d
    if p == "week":
        start = today - timedelta(days=today.weekday())  # lundi
        return start.isoformat(), today.isoformat()
    if p == "month":
        start = today.replace(day=1)
        return start.isoformat(), today.isoformat()
    return None, None  # all


class AIDataService:
    """Service de données READ-ONLY pour la future couche FANEVA IA.

    Toutes les méthodes retournent des dictionnaires JSON-serializables.
    Aucune méthode ne modifie la base.
    """

    SERVICE_VERSION = "1.1.0-phase1.1"
    SUPPORTED_PERIODS = ("day", "week", "month", "all")

    def __init__(
        self,
        conn: sqlite3.Connection,
        magasin_id: Optional[int] = None,
        role: Optional[str] = None,
        username: Optional[str] = None,
        user_id: Optional[str] = None,
        enforce_readonly: bool = True,
    ):
        """
        Parameters
        ----------
        conn : connexion SQLite déjà ouverte (idéalement la base du magasin actif).
        magasin_id : identifiant HYBRID du magasin à isoler (None = contexte global de la DB).
        role : rôle session (ADMIN*, VENDEUR, …). Utilisé pour filtrer les données sensibles.
        username / user_id : identité session (journalisation future uniquement).
        enforce_readonly : active la discipline READ-ONLY (SELECT only).

        IMPORTANT — PRAGMA query_only :
        On ne pose PAS ``PRAGMA query_only=ON`` sur la connexion fournie par l'appelant.
        Cette connexion est souvent partagée avec le moteur métier (ventes, stock, sync).
        Un PRAGMA permanent casserait les écritures ultérieures de l'application.
        La garantie READ-ONLY repose sur : (1) code SELECT-only, (2) absence totale
        d'INSERT/UPDATE/DELETE/DDL dans ce module, (3) tests d'empreinte avant/après.
        """
        if conn is None:
            raise ValueError("conn est obligatoire")
        self.conn = conn
        self.magasin_id = magasin_id
        self.role = role
        self.username = username
        self.user_id = user_id
        self._is_admin = _is_admin_role(role)
        self._enforce_readonly = bool(enforce_readonly)
        # Jamais de mutation PRAGMA sur la connexion partagée (voir docstring).

    # ------------------------------------------------------------------
    # Contexte
    # ------------------------------------------------------------------
    def get_context(self) -> Dict[str, Any]:
        """Métadonnées de session / magasin (toujours autorisé).

        N'expose aucun secret, token, mot de passe ou credential.
        """
        store = self._resolve_store()
        return {
            "service_version": self.SERVICE_VERSION,
            "hybrid_version": HYBRID_VERSION,
            "magasin_id": self.magasin_id,
            "store": store,
            "role": self.role,
            "username": self.username,
            "user_id": self.user_id,
            "is_admin": self._is_admin,
            "read_only": True,
            "enforce_readonly": self._enforce_readonly,
            "internet_required": False,
            "pragma_query_only_on_shared_conn": False,  # volontairement non appliqué
        }

    def _resolve_store(self) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        if not _table_exists(cur, "magasins"):
            return None
        if self.magasin_id is not None:
            cur.execute(
                "SELECT id, cle, nom, categorie, actif FROM magasins WHERE id=? LIMIT 1",
                (self.magasin_id,),
            )
        else:
            cur.execute(
                "SELECT id, cle, nom, categorie, actif FROM magasins WHERE actif=1 ORDER BY id LIMIT 1"
            )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "cle": row[1],
            "nom": row[2],
            "categorie": row[3],
            "actif": bool(row[4]),
        }

    # ------------------------------------------------------------------
    # Stock — priorité CANONIQUE stricte
    # ------------------------------------------------------------------
    def _resolve_product_stock(
        self,
        hybrid_id: Optional[str],
        magasin_ref_id: Optional[int],
        legacy_stock: Any,
    ) -> Dict[str, Any]:
        """Résout le stock d'un produit avec priorité CANONIQUE.

        Règles :
        - Si hybrid_id + magasin disponibles et get_stock réussit → stock CANONIQUE.
        - Le fallback legacy n'est utilisé que si aucune projection canonique valide.
        - Si les deux existent et divergent → stock = canonique + indicateur de divergence.
        - Jamais de remplacement silencieux d'une valeur canonique par le legacy.
        """
        legacy = _safe_int(legacy_stock)
        mid: Optional[int] = None
        if magasin_ref_id is not None:
            mid = int(magasin_ref_id)
        elif self.magasin_id is not None:
            mid = int(self.magasin_id)

        can_query = (
            get_stock is not None
            and hybrid_id
            and mid is not None
        )
        if not can_query:
            return {
                "stock": legacy,
                "stock_source": "legacy",
                "legacy_stock": legacy,
                "canonical_stock": None,
                "divergence": False,
            }

        try:
            canonical = _safe_int(get_stock(self.conn, hybrid_id, mid))
        except Exception:
            # Projection canonique indisponible → fallback explicite, pas silencieux
            return {
                "stock": legacy,
                "stock_source": "legacy",
                "legacy_stock": legacy,
                "canonical_stock": None,
                "divergence": False,
                "canonical_unavailable": True,
            }

        diverges = canonical != legacy
        return {
            "stock": canonical,  # TOUJOURS la valeur canonique quand disponible
            "stock_source": "canonical",
            "legacy_stock": legacy,
            "canonical_stock": canonical,
            "divergence": diverges,
            "divergence_detail": (
                {"canonical": canonical, "legacy": legacy} if diverges else None
            ),
        }

    def get_stock_summary(self) -> Dict[str, Any]:
        """Résumé stock du magasin courant (accessible à tous les rôles).

        Priorité absolue à la projection CANONIQUE (get_stock / stocks_magasin).
        Le stock legacy n'est un fallback que si aucune projection canonique
        valide n'existe. Toute divergence est signalée explicitement.
        """
        cur = self.conn.cursor()
        products: List[Dict[str, Any]] = []
        total_units = 0
        total_value_achat = 0
        total_value_vente = 0
        low_stock: List[Dict[str, Any]] = []
        out_of_stock: List[Dict[str, Any]] = []
        divergence_count = 0
        LOW_THRESHOLD = 5

        if _table_exists(cur, "produits"):
            cur.execute(
                """
                SELECT id, nom, categorie, prix_achat, prix_vente, stock,
                       hybrid_id, magasin_ref_id
                FROM produits
                WHERE actif=1
                ORDER BY nom
                """
            )
            rows = cur.fetchall()
            for r in rows:
                pid, nom, cat, pa, pv, legacy_stock, hybrid_id, mref = r
                # Isolation magasin stricte
                if self.magasin_id is not None and mref is not None and int(mref) != int(self.magasin_id):
                    continue

                resolved = self._resolve_product_stock(hybrid_id, mref, legacy_stock)
                stock = resolved["stock"]
                pa_i = _safe_int(pa)
                pv_i = _safe_int(pv)

                item: Dict[str, Any] = {
                    "product_id": pid,
                    "hybrid_id": hybrid_id,
                    "nom": nom,
                    "categorie": cat or "General",
                    "prix_vente": pv_i,
                    "stock": stock,
                    "stock_source": resolved["stock_source"],
                    "divergence": resolved["divergence"],
                    "valeur_vente": stock * pv_i,
                }
                # Champs admin uniquement
                if self._is_admin:
                    item["prix_achat"] = pa_i
                    item["valeur_achat"] = stock * pa_i
                    item["legacy_stock"] = resolved["legacy_stock"]
                    item["canonical_stock"] = resolved["canonical_stock"]
                    if resolved.get("divergence_detail"):
                        item["divergence_detail"] = resolved["divergence_detail"]
                    if resolved.get("canonical_unavailable"):
                        item["canonical_unavailable"] = True
                else:
                    # Vendeur : jamais prix_achat / valeur_achat / détail de divergence sensible
                    pass

                products.append(item)
                total_units += stock
                if self._is_admin:
                    total_value_achat += stock * pa_i
                total_value_vente += stock * pv_i
                if resolved["divergence"]:
                    divergence_count += 1
                if stock <= 0:
                    out_of_stock.append({
                        "product_id": pid, "nom": nom, "stock": stock,
                        "stock_source": resolved["stock_source"],
                    })
                elif stock <= LOW_THRESHOLD:
                    low_stock.append({
                        "product_id": pid, "nom": nom, "stock": stock,
                        "stock_source": resolved["stock_source"],
                    })

        result: Dict[str, Any] = {
            "magasin_id": self.magasin_id,
            "total_products": len(products),
            "total_units": total_units,
            "total_value_vente": total_value_vente,
            "low_stock_threshold": LOW_THRESHOLD,
            "low_stock_count": len(low_stock),
            "low_stock_products": low_stock,
            "out_of_stock_count": len(out_of_stock),
            "out_of_stock_products": out_of_stock,
            "divergence_count": divergence_count,
            "products": products,
        }
        if self._is_admin:
            result["total_value_achat"] = total_value_achat
        else:
            result["total_value_achat"] = None
        return result

    # ------------------------------------------------------------------
    # Produits (catalogue)
    # ------------------------------------------------------------------
    def get_products_summary(self) -> Dict[str, Any]:
        """Résumé catalogue (sans stock détaillé). Accessible à tous.

        Respecte l'isolation magasin lorsque magasin_ref_id est renseigné.
        """
        cur = self.conn.cursor()
        by_category: Dict[str, int] = {}
        total = 0
        if _table_exists(cur, "produits"):
            # Filtre magasin si possible (colonne magasin_ref_id)
            has_mref = False
            try:
                cur.execute("SELECT magasin_ref_id FROM produits LIMIT 0")
                has_mref = True
            except sqlite3.OperationalError:
                pass

            if has_mref and self.magasin_id is not None:
                cur.execute(
                    """
                    SELECT categorie, COUNT(*) FROM produits
                    WHERE actif=1 AND (magasin_ref_id IS NULL OR magasin_ref_id=?)
                    GROUP BY categorie
                    """,
                    (self.magasin_id,),
                )
            else:
                cur.execute(
                    "SELECT categorie, COUNT(*) FROM produits WHERE actif=1 GROUP BY categorie"
                )
            for cat, cnt in cur.fetchall():
                key = cat or "General"
                by_category[key] = int(cnt)
                total += int(cnt)
        return {
            "magasin_id": self.magasin_id,
            "total_products": total,
            "by_category": by_category,
        }

    # ------------------------------------------------------------------
    # Ventes
    # ------------------------------------------------------------------
    def get_sales_summary(
        self,
        period: str = "day",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Résumé des ventes (table ventes + fallback transactions).

        Admin : totaux complets + marge.
        Vendeur : volume limité (pas de détail financier global si non admin).
        """
        if not self._is_admin:
            # Un vendeur n'a pas accès aux rapports financiers complets (check_admin existant)
            return {
                "magasin_id": self.magasin_id,
                "period": period,
                "access": "restricted",
                "message": "Résumé ventes détaillé réservé aux administrateurs",
                "sales_count": None,
                "sales_total": None,
                "margin_total": None,
            }

        date_from, date_to = self._resolve_dates(period, date_from, date_to)
        cur = self.conn.cursor()

        cash_total = cash_count = credit_total = credit_count = 0
        margin_total = 0
        sales_count = 0
        sales_total = 0

        if _table_exists(cur, "ventes"):
            where, params = self._date_clause("date_vente", date_from, date_to)
            # Comptant
            cur.execute(
                f"""
                SELECT COALESCE(SUM(total),0), COUNT(*), COALESCE(SUM(benefice),0)
                FROM ventes
                WHERE (sale_mode IS NULL OR UPPER(sale_mode)='COMPTANT') {where}
                """,
                params,
            )
            row = cur.fetchone()
            cash_total, cash_count, cash_margin = _safe_int(row[0]), _safe_int(row[1]), _safe_int(row[2])

            # Crédit
            cur.execute(
                f"""
                SELECT COALESCE(SUM(total),0), COUNT(*), COALESCE(SUM(benefice),0)
                FROM ventes
                WHERE UPPER(COALESCE(sale_mode,''))='CREDIT' {where}
                """,
                params,
            )
            row = cur.fetchone()
            credit_total, credit_count, credit_margin = _safe_int(row[0]), _safe_int(row[1]), _safe_int(row[2])

            sales_total = cash_total + credit_total
            sales_count = cash_count + credit_count
            margin_total = cash_margin + credit_margin
        else:
            # Fallback pure transactions (tests minimaux)
            where, params = self._date_clause("horodatage", date_from, date_to, prefix="AND")
            cur.execute(
                f"""
                SELECT COUNT(*) FROM transactions
                WHERE type_op IN ('VENTE','VENTE_CREDIT') {where}
                """,
                params,
            )
            sales_count = _safe_int(cur.fetchone()[0])

        # Alignement avec get_daily_cash_report pour la période jour (référence métier)
        daily_ref = None
        if get_daily_cash_report is not None and period == "day":
            try:
                daily_ref = get_daily_cash_report(self.conn)
            except Exception:
                daily_ref = None

        result = {
            "magasin_id": self.magasin_id,
            "period": period,
            "date_from": date_from,
            "date_to": date_to,
            "access": "full",
            "sales_count": sales_count,
            "sales_total": sales_total,
            "cash_total": cash_total,
            "cash_count": cash_count,
            "credit_total": credit_total,
            "credit_count": credit_count,
            "margin_total": margin_total,
            "margin_percent": round((margin_total / sales_total * 100), 2) if sales_total > 0 else 0.0,
        }
        if daily_ref is not None:
            result["daily_cash_report_ref"] = {
                "comptant_total": daily_ref.get("comptant_total"),
                "credit_total": daily_ref.get("credit_total"),
                "recouvrements_total": daily_ref.get("recouvrements_total"),
                "caisse_reelle": daily_ref.get("caisse_reelle"),
            }
        return result

    # ------------------------------------------------------------------
    # Dettes
    # ------------------------------------------------------------------
    def get_debts_summary(self) -> Dict[str, Any]:
        """Résumé des dettes actives. Réservé admin (écran dettes + rapports admin)."""
        if not self._is_admin:
            return {
                "magasin_id": self.magasin_id,
                "access": "restricted",
                "message": "Résumé dettes réservé aux administrateurs",
                "active_count": None,
                "total_reste": None,
            }

        cur = self.conn.cursor()
        active_count = total_reste = total_original = total_paye = 0
        clients: List[Dict[str, Any]] = []

        if _table_exists(cur, "dettes"):
            # Isolation optionnelle par magasin_ref_id si la colonne existe
            has_mref = False
            try:
                cur.execute("SELECT magasin_ref_id FROM dettes LIMIT 0")
                has_mref = True
            except sqlite3.OperationalError:
                pass

            if has_mref and self.magasin_id is not None:
                cur.execute(
                    """
                    SELECT id, client, telephone, total, paye, reste, statut
                    FROM dettes
                    WHERE statut='ACTIF' AND (magasin_ref_id IS NULL OR magasin_ref_id=?)
                    ORDER BY reste DESC
                    """,
                    (self.magasin_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, client, telephone, total, paye, reste, statut
                    FROM dettes
                    WHERE statut='ACTIF'
                    ORDER BY reste DESC
                    """
                )
            for row in cur.fetchall():
                did, client, tel, total, paye, reste, statut = row
                r = _safe_int(reste)
                t = _safe_int(total)
                p = _safe_int(paye)
                active_count += 1
                total_reste += r
                total_original += t
                total_paye += p
                clients.append(
                    {
                        "dette_id": did,
                        "client": client,
                        "telephone": tel,
                        "total": t,
                        "paye": p,
                        "reste": r,
                    }
                )

        return {
            "magasin_id": self.magasin_id,
            "access": "full",
            "active_count": active_count,
            "total_original": total_original,
            "total_paye": total_paye,
            "total_reste": total_reste,
            "clients": clients,
        }

    # ------------------------------------------------------------------
    # Paiements / recouvrements
    # ------------------------------------------------------------------
    def get_payments_summary(
        self,
        period: str = "day",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Résumé des paiements de dettes. Réservé admin."""
        if not self._is_admin:
            return {
                "magasin_id": self.magasin_id,
                "period": period,
                "access": "restricted",
                "message": "Résumé paiements réservé aux administrateurs",
                "payments_count": None,
                "payments_total": None,
            }

        date_from, date_to = self._resolve_dates(period, date_from, date_to)
        cur = self.conn.cursor()
        payments_count = payments_total = 0

        # Priorité table paiements_dettes (projection locale)
        if _table_exists(cur, "paiements_dettes"):
            where, params = self._date_clause("date_paiement", date_from, date_to)
            cur.execute(
                f"SELECT COALESCE(SUM(montant),0), COUNT(*) FROM paiements_dettes WHERE 1=1 {where}",
                params,
            )
            row = cur.fetchone()
            payments_total, payments_count = _safe_int(row[0]), _safe_int(row[1])
        else:
            # Fallback transactions type PAIEMENT
            where, params = self._date_clause("horodatage", date_from, date_to, prefix="AND")
            try:
                cur.execute(
                    f"""
                    SELECT COALESCE(SUM(CAST(json_extract(payload,'$.montant') AS INTEGER)),0), COUNT(*)
                    FROM transactions
                    WHERE type_op='PAIEMENT' {where}
                    """,
                    params,
                )
                row = cur.fetchone()
                payments_total, payments_count = _safe_int(row[0]), _safe_int(row[1])
            except sqlite3.OperationalError:
                pass

        return {
            "magasin_id": self.magasin_id,
            "period": period,
            "date_from": date_from,
            "date_to": date_to,
            "access": "full",
            "payments_count": payments_count,
            "payments_total": payments_total,
        }

    # ------------------------------------------------------------------
    # Marges
    # ------------------------------------------------------------------
    def get_margins_summary(
        self,
        period: str = "day",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Résumé des marges / bénéfices. Réservé admin (onglet BENEFICE)."""
        if not self._is_admin:
            return {
                "magasin_id": self.magasin_id,
                "period": period,
                "access": "restricted",
                "message": "Résumé marges réservé aux administrateurs",
            }

        # Réutilise la logique ventes (benefice déjà calculé déterministiquement)
        sales = self.get_sales_summary(period=period, date_from=date_from, date_to=date_to)
        return {
            "magasin_id": self.magasin_id,
            "period": sales.get("period"),
            "date_from": sales.get("date_from"),
            "date_to": sales.get("date_to"),
            "access": "full",
            "sales_total": sales.get("sales_total"),
            "margin_total": sales.get("margin_total"),
            "margin_percent": sales.get("margin_percent"),
            "sales_count": sales.get("sales_count"),
        }

    # ------------------------------------------------------------------
    # Snapshot complet (pour future IA)
    # ------------------------------------------------------------------
    def get_business_snapshot(self, period: str = "day") -> Dict[str, Any]:
        """Agrégat structuré de toutes les métriques disponibles selon les permissions.

        Cet objet est conçu pour être passé tel quel à une future couche FANEVA IA
        sans qu'elle ait besoin de connaître SQLite.

        Confidentialité :
        - Vendeur : pas de prix d'achat, pas de marge, pas de dettes/paiements détaillés.
        - Aucun secret / token / mot de passe / credential n'est jamais inclus.
        - Isolation magasin appliquée dans chaque sous-résumé.
        """
        snapshot: Dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "context": self.get_context(),
            "stock": self.get_stock_summary(),
            "products": self.get_products_summary(),
            "sales": self.get_sales_summary(period=period),
            "debts": self.get_debts_summary(),
            "payments": self.get_payments_summary(period=period),
            "margins": self.get_margins_summary(period=period),
        }
        # Garde-fou final : scrub des clés sensibles si non-admin
        if not self._is_admin:
            snapshot = self._scrub_sensitive(snapshot)
        return snapshot

    def _scrub_sensitive(self, data: Any) -> Any:
        """Retire récursivement les clés confidentielles (vendeur / non-admin)."""
        forbidden = {
            "prix_achat", "valeur_achat", "total_value_achat",
            "margin_total", "margin_percent", "benefice",
            "password", "password_hash", "api_key", "token",
            "secret", "credential", "server_api_key",
        }
        if isinstance(data, dict):
            return {
                k: self._scrub_sensitive(v)
                for k, v in data.items()
                if k not in forbidden
            }
        if isinstance(data, list):
            return [self._scrub_sensitive(x) for x in data]
        return data

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------
    def _resolve_dates(
        self,
        period: str,
        date_from: Optional[str],
        date_to: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        if date_from or date_to:
            return date_from, date_to
        return _period_bounds(period)

    def _date_clause(
        self,
        column: str,
        date_from: Optional[str],
        date_to: Optional[str],
        prefix: str = "AND",
    ) -> Tuple[str, Tuple]:
        clauses = []
        params: List[Any] = []
        if date_from:
            clauses.append(f"date({column}) >= date(?)")
            params.append(date_from)
        if date_to:
            clauses.append(f"date({column}) <= date(?)")
            params.append(date_to)
        if not clauses:
            return "", ()
        return f" {prefix} " + " AND ".join(clauses), tuple(params)

    # ------------------------------------------------------------------
    # Sérialisation
    # ------------------------------------------------------------------
    def to_json(self, data: Any, indent: Optional[int] = None) -> str:
        """Sérialise un résultat en JSON (UTF-8, ensure_ascii=False)."""
        return json.dumps(data, ensure_ascii=False, indent=indent, default=str)

    def verify_no_write(self) -> bool:
        """Confirme la posture READ-ONLY du service.

        Ne s'appuie PAS sur PRAGMA query_only de la connexion partagée
        (volontairement non posé pour ne pas casser les écritures métier).
        La garantie est structurelle : ce module n'exécute aucun
        INSERT/UPDATE/DELETE/DDL. Les tests d'empreinte SQLite avant/après
        valident l'absence d'effet de bord.
        """
        return True
