import sys
import traceback
import json

# ==============================================================
# FANEVA SYSTEM 1.4.9.2 — application métier avec transport `/sync` cohérent avec l’authentification
# Module noyau (magasins en base, transactions, mouvements,
# queue offline, sync Internet + Wi-Fi/Hotspot, multi-admins)
# ==============================================================
try:
    from faneva_hybrid import (
        HYBRID_VERSION, HYBRID_LOG_TAG,
        run_migration, create_store, update_store, delete_store, initialize_canonical_product_mapping,
        create_product, update_product, delete_product, get_stock, resolve_canonical_product_id,
        record_sale, record_stock_in, record_debt, record_payment,
        record_credit_sale, get_daily_cash_report,
        create_user_hybrid, update_user_hybrid,
        pending_transactions, normal_pending_transactions, pending_count, mark_synced,
        apply_remote_transactions, export_outgoing,
        sync_with_http, sync_local_wifi, sync_internet,
        _start_sync_server, SYNC_SERVER_PORT, SYNC_SERVER_TIMEOUT,
        gen_uuid, gen_device_id, now_iso, initialize_device_identity,
        device_identity_status, is_migration_locked,
        export_device_migration_manifest, describe_migration_export, prepare_migration_export_dir, complete_device_identity_migration,
        set_migration_session, get_migration_session,
        sync_migration_pilot, sync_migration_historical,
        _local_device_id,
        hash_pwd as hash_pwd_hybrid, check_pwd,
        backup_remote, detect_connectivity, authenticated_staging_status, set_server_api_key, has_server_api_key,
        validate_server_authentication, diagnose_pilot_database,
        prepare_database_copy_export_dir, export_quincaillerie_database_copies,
        run_sqlite_export_step_diagnostic, compare_sqlite_databases_readonly,
        inspect_sqlite_files_physical_binary, export_external_database_binary_only,
        validate_existing_binary_copy_readonly,
        run_android_readonly_environment_diagnostic,
    )
except Exception as _import_err:
    HYBRID_VERSION = "1.1.0"
    HYBRID_LOG_TAG = f"KDK v1.1.0 HYBRID"
    run_migration = None
    resolve_canonical_product_id = None
    _import_err = None

# --- FANEVA IA (Phase 2.2 UI) : import optionnel, n'empêche jamais le démarrage métier ---
try:
    from faneva_ia import FanevaIA, run_faneva_ia_question
    from faneva_ia_lang import detect_language, ui_text, suggestion_pairs, map_error_message
    from ai_provider import UnavailableAIProvider, MockAIProvider, build_provider
    HAS_FANEVA_IA = True
except Exception:
    FanevaIA = None
    run_faneva_ia_question = None
    HAS_FANEVA_IA = False
    detect_language = None
    ui_text = None
    suggestion_pairs = None
    map_error_message = None
    UnavailableAIProvider = None
    MockAIProvider = None
    build_provider = None

# Configuration HYBRID
# Seul le Worker Cloudflare staging existant est autorisé dans cette release.
SYNC_SERVER_URL = "https://faneva-sync-staging.faneva-sync-staging.workers.dev"
SYNC_ENVIRONMENT = "CLOUDFLARE_STAGING"
ENABLE_LOCAL_SYNC_SERVER = False  # APK diagnostic : aucun serveur local
ENABLE_AUTO_SYNC = False         # garde-fou : aucune sync automatique avant le pilote valide
AUTO_SYNC_INTERVAL = 60.0        # reserve a une activation future explicitement validee
BACKUP_REMOTE = False            # APK diagnostic : aucun backup distant
DIAGNOSTIC_PILOT_UUID = "0355ffb8-6e77-45bf-864b-bee79780b61e"
DIAGNOSTIC_VALIDATED_DB_SHA256 = "8425b68b92019095235fb6bedfe673506f3f85afda4a47ed453f52bc5a221c1f"
DIAGNOSTIC_ONLY = False  # RELEASE candidate : démarrage de l’interface métier ; garde-fous réseau inchangés.
VALIDATED_EXTERNAL_COPY_FILENAME = "stock_quincaillerie_externe_20260821T112210Z.db"
VALIDATED_EXTERNAL_COPY_SHA256 = "b7b733e44f736bb24cbd2b3cdccb8ae4118eb7fa400ea6d8c7c17bc592b033c6"
VALIDATED_EXTERNAL_COPY_SIZE_BYTES = 36864
import sqlite3
import os
import shutil
import hashlib
import contextlib
import threading
from datetime import datetime, timezone
from functools import partial, wraps

# ==============================
# CONFIG & PATHS - MULTI-MAGASIN
# ==============================

EXTERNAL_DIR = "/storage/emulated/0/FANEVA_SYSTEM_ANDROID"
INTERNAL_DIR = "/data/data/com.faneva.fanevasystem/files/FANEVA_SYSTEM_ANDROID"
BACKUP_DIR = "/storage/emulated/0/FANEVA_BACKUP"


def get_migration_export_dir():
    """Retourne un dossier Android app-specific sans permission de stockage dangereuse.

    Le chemin externe est obtenu auprès de l’activité Android au runtime. S’il n’existe
    pas (volume non monté), le repli reste dans le stockage privé de l’application.
    Aucun dossier partagé existant, dont FANEVA_BACKUP, n’est modifié.
    """
    app_root = None
    try:
        from jnius import autoclass
        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        activity = PythonActivity.mActivity
        external = activity.getExternalFilesDir("Documents")
        if external:
            app_root = external.getAbsolutePath()
        else:
            app_root = activity.getFilesDir().getAbsolutePath()
    except Exception:
        # Repli sûr pour volume externe indisponible et exécution hors Android.
        app_root = os.path.join(INTERNAL_DIR, "Documents")
    return prepare_migration_export_dir(app_root)


def resolve_database_copy_app_root(strict=False):
    """Résout seulement getExternalFilesDir(null), sans créer aucun fichier."""
    try:
        from jnius import autoclass
        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        activity = PythonActivity.mActivity
        external = activity.getExternalFilesDir(None)
        if external:
            return external.getAbsolutePath()
        return activity.getFilesDir().getAbsolutePath()
    except Exception:
        if strict:
            raise
        # Repli uniquement utile aux tests hôte ; le diagnostic Android emploie strict=True.
        return os.path.join(INTERNAL_DIR, "Documents")


def get_database_copy_export_dir():
    """Retourne un dossier réellement inscriptible sous getExternalFilesDir(null).

    Aucun répertoire public partagé n'est utilisé. Le repli est getFilesDir(),
    également app-specific, lorsque le volume externe app-specific est absent.
    """
    app_root = resolve_database_copy_app_root()
    export_dir = prepare_database_copy_export_dir(app_root)
    probe = os.path.join(export_dir, ".faneva_export_write_probe")
    try:
        with open(probe, "wb") as handle:
            handle.write(b"FANEVA-EXPORT-COPY")
    finally:
        if os.path.exists(probe):
            os.remove(probe)
    if not os.path.isdir(export_dir):
        raise OSError("Repertoire app-specific des copies indisponible")
    return export_dir

# Les magasins ne sont plus codés en dur : ils sont lus depuis la base
# (table magasins, gérée par faneva_hybrid). MAGASINS_DEFAULT reste le
# repli de couleurs/chemins avant la migration.
MAGASINS_DEFAULT = {
    "quincaillerie": {"id": None, "nom": "Quincaillerie", "categorie": "Quincaillerie",
                      "db": os.path.join(EXTERNAL_DIR, "stock_quincaillerie.db"),
                      "couleur": (0.2, 0.5, 0.7, 1), "couleur_light": (0.85, 0.9, 0.95, 1)},
    "cosmetiques": {"id": None, "nom": "Cosmetiques", "categorie": "Cosmetiques",
                    "db": os.path.join(EXTERNAL_DIR, "stock_cosmetiques.db"),
                    "couleur": (0.9, 0.3, 0.6, 1), "couleur_light": (1, 0.85, 0.92, 1)},
}
MAGASINS = dict(MAGASINS_DEFAULT)

LOG_FILE = os.path.join(INTERNAL_DIR, "kdk.log")

# Instrumentation temporaire de diagnostic Stock : journal privé seulement,
# sans requête réseau, transaction métier ou écriture de table SQLite.
STOCK_DIAGNOSTIC_TRACE = True

DB_PATH = None
MAGASIN_ACTIF = None
MAGASIN_ACTIF_ID = None  # id HYBRID du magasin actif (table magasins)
MAGASIN_CLE = None       # cle HYBRID du magasin actif
HYBRID_DEVICE_ID = None  # identifiant unique de cet appareil

# ==============================
# LOGGING
# ==============================
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{HYBRID_LOG_TAG}] {ts} | {msg}"
    print(line)
    sys.stdout.flush()
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"[KDK] LOG FAIL: {e}")
        sys.stdout.flush()

def log_error(where, exc):
    log(f"ERREUR [{where}]: {type(exc).__name__}: {exc}")
    for ln in traceback.format_exc().splitlines():
        log(f"  {ln}")


def stock_diagnostic_trace(event, entries):
    """Journalise les valeurs Stock observées sans modifier les données métier."""
    if not STOCK_DIAGNOSTIC_TRACE:
        return
    for entry in entries:
        parts = [f"{key}={value}" for key, value in entry.items()]
        log(f"STOCK_TRACE event={event} | " + " | ".join(parts))


def stock_diagnostic_trace_async(event, entries):
    """Déporte la trace de rendu pour ne pas bloquer le thread Kivy."""
    if STOCK_DIAGNOSTIC_TRACE and entries:
        threading.Thread(
            target=stock_diagnostic_trace,
            args=(event, tuple(entries)),
            daemon=True,
            name="faneva-stock-trace",
        ).start()


STOCK_DIAGNOSTIC_EXPORT_EVENTS = frozenset({
    "CANONICAL_BATCH",
    "PRODUCT_DISPLAY_PROJECTION",
    "SALE_COMMITTED_PROJECTION",
    "UI_VENTE_SUGGESTIONS",
    "UI_VENTE_SELECTED",
    "UI_STOCK_LIST",
    "UI_STOCK_SUGGESTIONS",
})
STOCK_DIAGNOSTIC_EXPORT_MAX_LINES = 2000


def _sanitize_stock_diagnostic_export_line(line):
    """Conserve une ligne de trace Stock et retire toute clé sensible éventuelle."""
    sensitive_keys = {"password", "passwd", "token", "secret", "api_key", "authorization"}
    safe_parts = []
    for part in str(line).rstrip("\r\n").split("|"):
        key = part.split("=", 1)[0].strip().lower()
        if key in sensitive_keys:
            safe_parts.append(key.upper() + "=[REDACTED]")
        else:
            safe_parts.append(part.strip())
    return " | ".join(safe_parts)


def build_stock_diagnostic_log_export(log_path=LOG_FILE, max_lines=STOCK_DIAGNOSTIC_EXPORT_MAX_LINES):
    """Lit `kdk.log` et retourne seulement les événements Stock explicitement autorisés.

    Aucun fichier local n'est créé ou modifié ici. La sauvegarde du texte est effectuée
    ensuite uniquement vers l'URI que l'utilisateur a choisi dans Android.
    """
    selected = []
    with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if "STOCK_TRACE event=" not in line:
                continue
            event = line.split("STOCK_TRACE event=", 1)[1].split("|", 1)[0].strip()
            if event in STOCK_DIAGNOSTIC_EXPORT_EVENTS:
                selected.append(_sanitize_stock_diagnostic_export_line(line))
    if max_lines and len(selected) > int(max_lines):
        selected = selected[-int(max_lines):]
    created_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    header = [
        "FANEVA SYSTEM — EXPORT LOG DIAGNOSTIC STOCK",
        "MODE : LOCAL / READONLY / SANS RESEAU / SANS ECRITURE METIER",
        "EVENEMENTS : " + ", ".join(sorted(STOCK_DIAGNOSTIC_EXPORT_EVENTS)),
        "GENERE_UTC : " + created_utc,
        "NOMBRE_LIGNES : " + str(len(selected)),
        "=" * 72,
    ]
    return "\n".join(header + selected) + "\n", len(selected)


def _parse_stock_diagnostic_trace_line(line):
    """Extrait uniquement les champs diagnostic autorisés d’une ligne déjà filtrée."""
    safe_line = _sanitize_stock_diagnostic_export_line(line)
    if "STOCK_TRACE event=" not in safe_line:
        return None
    prefix, payload = safe_line.split("STOCK_TRACE event=", 1)
    event, *parts = payload.split("|")
    entry = {
        "TIMESTAMP": prefix.rsplit("] ", 1)[-1].strip(),
        "EVENT": event.strip(),
    }
    allowed_fields = {
        "PRODUCT_ID", "PRODUCT_NAME", "HYBRID_ID", "CANONICAL_PRODUCT_UUID",
        "MAGASIN_ID", "STOCK_SOURCE", "CANONICAL_STOCK", "UI_DISPLAYED_STOCK",
    }
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().upper()
        if key in allowed_fields:
            entry[key] = value.strip()
    return entry


def build_stock_diagnostic_share_report(log_path=LOG_FILE, max_lines=STOCK_DIAGNOSTIC_EXPORT_MAX_LINES):
    """Génère un rapport texte local depuis les traces autorisées, sans créer de fichier."""
    exported_text, line_count = build_stock_diagnostic_log_export(log_path, max_lines)
    trace_lines = [line for line in exported_text.splitlines() if "STOCK_TRACE event=" in line]
    entries = [_parse_stock_diagnostic_trace_line(line) for line in trace_lines]
    entries = [entry for entry in entries if entry]

    version = "NON PRÉSENTE DANS LE LOG"
    if trace_lines and "[KDK v" in trace_lines[0]:
        version = trace_lines[0].split("[KDK v", 1)[1].split(" HYBRID]", 1)[0].strip()
    latest_sale = next((entry for entry in reversed(entries) if entry["EVENT"] == "SALE_COMMITTED_PROJECTION"), None)
    latest_selected = next((entry for entry in reversed(entries) if entry["EVENT"] == "UI_VENTE_SELECTED"), None)
    subject = latest_sale or latest_selected or (entries[-1] if entries else {})
    product_id = subject.get("PRODUCT_ID", "NON PRÉSENTE DANS LE LOG")
    hybrid_id = subject.get("HYBRID_ID", "NON PRÉSENTE DANS LE LOG")
    canonical_id = subject.get("CANONICAL_PRODUCT_UUID", "NON PRÉSENTE DANS LE LOG")
    magasin_id = subject.get("MAGASIN_ID", "NON PRÉSENTE DANS LE LOG")
    subject_entries = [
        entry for entry in entries
        if (product_id != "NON PRÉSENTE DANS LE LOG" and entry.get("PRODUCT_ID") == product_id)
        or (hybrid_id != "NON PRÉSENTE DANS LE LOG" and entry.get("HYBRID_ID") == hybrid_id)
        or (canonical_id != "NON PRÉSENTE DANS LE LOG" and entry.get("CANONICAL_PRODUCT_UUID") == canonical_id)
    ]
    if not subject_entries and subject:
        subject_entries = [subject]

    def latest_value(key, event_name=None):
        candidates = subject_entries
        if event_name:
            candidates = [entry for entry in candidates if entry["EVENT"] == event_name]
        for entry in reversed(candidates):
            if key in entry:
                return entry[key]
        return "NON PRÉSENTE DANS LE LOG"

    product_name = latest_value("PRODUCT_NAME")
    latest_timestamp = subject_entries[-1].get("TIMESTAMP", "NON PRÉSENTE DANS LE LOG") if subject_entries else "NON PRÉSENTE DANS LE LOG"
    sale_summary = latest_value("CANONICAL_STOCK", "SALE_COMMITTED_PROJECTION")
    header = [
        "FANEVA SYSTEM — DIAGNOSTIC STOCK",
        "MODE = LOCAL / READONLY / SANS RESEAU / SANS ECRITURE METIER",
        "VERSION = " + version,
        "DEVICE = NON PRÉSENTE DANS LE LOG",
        "DATE/HEURE = " + latest_timestamp,
        "",
        "PRODUCT_ID = " + product_id,
        "NOM_PRODUIT = " + product_name,
        "HYBRID_ID = " + hybrid_id,
        "CANONICAL_PRODUCT_UUID = " + canonical_id,
        "MAGASIN_ID = " + magasin_id,
        "",
        "STOCK_SOURCE = " + latest_value("STOCK_SOURCE"),
        "CANONICAL_STOCK = " + latest_value("CANONICAL_STOCK"),
        "UI_VENTE_SELECTED = " + latest_value("UI_DISPLAYED_STOCK", "UI_VENTE_SELECTED"),
        "UI_STOCK_LIST = " + latest_value("UI_DISPLAYED_STOCK", "UI_STOCK_LIST"),
        "UI_STOCK_SUGGESTIONS = " + latest_value("UI_DISPLAYED_STOCK", "UI_STOCK_SUGGESTIONS"),
        "SALE_COMMITTED_PROJECTION = " + sale_summary,
        "",
        "CHRONOLOGIE DES EVENEMENTS",
    ]
    if not subject_entries:
        header.append("AUCUN EVENEMENT STOCK_TRACE DISPONIBLE DANS kdk.log")
    else:
        chronology_fields = (
            "TIMESTAMP", "PRODUCT_ID", "HYBRID_ID", "CANONICAL_PRODUCT_UUID",
            "MAGASIN_ID", "STOCK_SOURCE", "CANONICAL_STOCK", "UI_DISPLAYED_STOCK",
        )
        for entry in subject_entries:
            fields = [entry["EVENT"]]
            fields.extend(key + "=" + entry[key] for key in chronology_fields if key in entry)
            header.append(" | ".join(fields))
    return "\n".join(header) + "\n", line_count

# ==============================
# PRE-REMPLISSAGE DES BASES EMBARQUEES (protection au premier lancement)
# ==============================
def _get_embedded_db(magasin_key):
    """Localise la base embarquee dans l'APK (repertoire assets du package)."""
    base = os.path.dirname(os.path.abspath(__file__))
    for cand in (os.path.join(base, "assets", f"stock_{magasin_key}.db"),
                 os.path.join(base, f"stock_{magasin_key}.db")):
        if os.path.exists(cand):
            return cand
    return None

def _prefill_embedded_db(magasin_key):
    """Si la base du magasin n'existe pas encore sur le stockage externe,
    la copier depuis les bases embarquees dans l'APK.
    Aucun ajout: purement conservateur, protege les donnees d'origine."""
    mag = MAGASINS[magasin_key]
    target = mag["db"]
    try:
        os.makedirs(EXTERNAL_DIR, exist_ok=True)
        if not os.path.exists(target):
            src = _get_embedded_db(magasin_key)
            if src:
                shutil.copy2(src, target)
                log(f"Base embarquee copiee vers {target}")
            else:
                log(f"Aucune base embarquee trouvee pour {magasin_key}")
    except Exception as e:
        log(f"Prefill DB ({magasin_key}) echoue: {e}")

def _prefill_chosen_db(magasin_key):
    """Si, apres resolution du chemin, la base choisie n'existe pas,
    la copier depuis la base embarquee. Garantit que le pre-remplissage
    fonctionne meme si l'app bascule sur le stockage interne."""
    if not DB_PATH or os.path.exists(DB_PATH):
        return
    try:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        src = _get_embedded_db(magasin_key)
        if src:
            shutil.copy2(src, DB_PATH)
            log(f"Base embarquee copiee sur le chemin choisi: {DB_PATH}")
        else:
            log(f"Aucune base embarquee pour {magasin_key}; init_db creera une base vide")
    except Exception as e:
        log(f"Prefill choisi ({magasin_key}) echoue: {e}")

# ==============================
# DB PATH RESOLUTION
# ==============================
def resolve_db_path(magasin_key):
    global DB_PATH, MAGASIN_ACTIF
    mag = MAGASINS[magasin_key]
    try:
        os.makedirs(EXTERNAL_DIR, exist_ok=True)
        testf = os.path.join(EXTERNAL_DIR, ".write_test")
        with open(testf, "w") as f:
            f.write("ok")
        os.remove(testf)
        DB_PATH = mag["db"]
        MAGASIN_ACTIF = magasin_key
        log(f"DB_PATH ({magasin_key} external): {DB_PATH}")
        return
    except Exception as e:
        log(f"External storage tsy azo idirana: {e}")
    try:
        os.makedirs(INTERNAL_DIR, exist_ok=True)
        testf = os.path.join(INTERNAL_DIR, ".write_test")
        with open(testf, "w") as f:
            f.write("ok")
        os.remove(testf)
        internal_db = os.path.join(INTERNAL_DIR, f"stock_{magasin_key}.db")
        DB_PATH = internal_db
        MAGASIN_ACTIF = magasin_key
        log(f"DB_PATH ({magasin_key} internal fallback): {DB_PATH}")
        return
    except Exception as e:
        log(f"Internal storage tsy azo idirana: {e}")
    DB_PATH = f"stock_{magasin_key}.db"
    MAGASIN_ACTIF = magasin_key
    log(f"DB_PATH ({magasin_key} last resort): {DB_PATH}")

def verify_db():
    if DB_PATH is None:
        return None
    try:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    except Exception:
        pass
    return DB_PATH

# ==============================
# SAFE SQLITE CONNECTION & MANAGEMENT
# ==============================
_SQLITE_PRAGMA_LOCK = threading.Lock()
_SQLITE_WAL_CONFIGURED_PATHS = set()


def configure_sqlite_pragmas(conn, db_path=None):
    """Configure les PRAGMA par connexion nécessaires, sans renégocier WAL à chaque lecture UI.

    `foreign_keys` est une option de connexion et reste donc activée à chaque ouverture.
    `journal_mode=WAL` est persistant pour le fichier SQLite : le négocier une seule fois par
    chemin et par processus évite une opération disque/verrou inutile à chaque frappe.
    """
    try:
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception as e:
        log(f"PRAGMA foreign_keys error: {e}")

    db_key = db_path or DB_PATH or "<unknown-db>"
    with _SQLITE_PRAGMA_LOCK:
        if db_key in _SQLITE_WAL_CONFIGURED_PATHS:
            return
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except Exception as e:
            log(f"PRAGMA journal_mode WAL non supporté (fallback DELETE): {e}")
            try:
                conn.execute("PRAGMA journal_mode = DELETE")
            except Exception as ex:
                log(f"PRAGMA journal_mode fallback error: {ex}")
        finally:
            # WAL/DELETE est une propriété persistante du fichier : ne pas retenter à chaque
            # callback UI, même si l’appareil ne peut utiliser que le repli DELETE.
            _SQLITE_WAL_CONFIGURED_PATHS.add(db_key)

@contextlib.contextmanager
def get_db_connection(timeout=15.0):
    db = verify_db()
    if not db:
        raise sqlite3.OperationalError("Chemin base de données non défini")
    conn = sqlite3.connect(db, timeout=timeout)
    configure_sqlite_pragmas(conn, db)
    try:
        yield conn
    except Exception as e:
        try:
            conn.rollback()
            log(f"SQLite Transaction Rollback exécuté suite à : {e}")
        except Exception as rb_err:
            log(f"Erreur durant le rollback : {rb_err}")
        raise
    finally:
        try:
            conn.close()
        except Exception as close_err:
            log(f"Erreur à la fermeture de la connexion DB : {close_err}")

def safe_sqlite(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlite3.Error as e:
            log_error(f"SQLite [{func.__name__}]", e)
            popup("Erreur Base de Donnees", f"[{func.__name__}] {e}")
            raise
        except Exception as e:
            log_error(func.__name__, e)
            popup("Erreur", f"[{func.__name__}] {e}")
            raise
    return wrapper

# ==============================
# EXPORT CONFIGURATION & SECURITY
# ==============================
def get_export_config(role, export_type="stock"):
    is_admin = (role == "ADMIN")
    if export_type == "stock":
        if is_admin:
            headers = ["ID", "Nom", "Categorie", "Prix Achat", "Prix Vente", "Stock", "Valeur Achat", "Valeur Vente"]
        else:
            headers = ["ID", "Nom", "Categorie", "Prix Vente", "Stock"]
        # Les exports stock doivent appeler canonical_stock_export_data() :
        # `produits.stock` n’est pas une projection valide pour les alias mappés.
        query = None
    elif export_type == "ventes":
        if is_admin:
            headers = ["ID", "Date Vente", "Produit", "Quantite", "Prix Achat", "Prix Vente", "Total", "Benefice", "Vendeur"]
            query = "SELECT id, date_vente, produit_nom, quantite, prix_achat, prix_vente, total, benefice, vendeur FROM ventes ORDER BY date_vente DESC"
        else:
            headers = ["ID", "Date Vente", "Produit", "Quantite", "Prix Vente", "Total", "Vendeur"]
            query = "SELECT id, date_vente, produit_nom, quantite, prix_vente, total, vendeur FROM ventes ORDER BY date_vente DESC"
    else:
        headers = []
        query = ""
    return headers, query

# ==============================
# KIVY IMPORTS
# ==============================
try:
    log("=== DEMARRAGE KDK v6.2.2 Multi-Magasin (Étape 3) ===")
    from kivy.app import App
    from kivy.uix.boxlayout import BoxLayout
    from kivy.uix.gridlayout import GridLayout
    from kivy.uix.scrollview import ScrollView
    from kivy.uix.label import Label
    from kivy.uix.image import Image
    from kivy.uix.textinput import TextInput
    from kivy.uix.button import Button
    from kivy.uix.popup import Popup
    from kivy.core.clipboard import Clipboard
    from kivy.clock import Clock
    from kivy.metrics import dp
    from kivy.graphics import Color, RoundedRectangle
    log("Imports Kivy OK")
except Exception as e:
    log_error("IMPORTS KIVY", e)
    raise

# ==============================
# DATABASE + FULL MIGRATION & DOUBLON CHECK
# ==============================
def _table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def _column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col for row in cur.fetchall())

def _migrate_table(cur, table_name, full_schema, columns_def):
    if not _table_exists(cur, table_name):
        cur.execute(full_schema)
        log(f"Table {table_name} créée")
        return True
    for col_name, col_def in columns_def:
        if not _column_exists(cur, table_name, col_name):
            try:
                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")
                log(f"Migration: colonne '{col_name} {col_def}' ajoutée à {table_name}")
            except sqlite3.OperationalError as e:
                log(f"Migration skip {table_name}.{col_name}: {e}")
    return False

def _detect_existing_active_debt_duplicates(cur):
    """
    Analyse si des doublons de dettes ACTIVES existent déjà dans la base.
    Ne supprime ni ne fusionne rien automatiquement. Signalé uniquement dans les logs.
    """
    try:
        cur.execute("""
            SELECT LOWER(TRIM(client)), COUNT(*) 
            FROM dettes 
            WHERE statut='ACTIF' 
            GROUP BY LOWER(TRIM(client)) 
            HAVING COUNT(*) > 1
        """)
        dups = cur.fetchall()
        if dups:
            log(f"AVERTISSEMENT: {len(dups)} client(s) ont des doublons de dettes ACTIVES existants dans la base:")
            for client_norm, count in dups:
                cur.execute("SELECT id, client, telephone, total, paye, reste FROM dettes WHERE LOWER(TRIM(client))=? AND statut='ACTIF'", (client_norm,))
                rows = cur.fetchall()
                log(f"  - Client '{client_norm}' ({count} dettes actives) : {rows}")
            log("  -> Conservés intacts (aucune fusion automatique sans confirmation).")
        else:
            log("Analyse doublons dettes ACTIVES: Aucun doublon existant détecté.")
    except Exception as e:
        log(f"Erreur analyse doublons dettes: {e}")

@safe_sqlite
def init_db():
    log("Init DB + Full Migration...")
    with get_db_connection() as conn:
        cur = conn.cursor()

        _migrate_table(cur, "produits", """
            CREATE TABLE IF NOT EXISTS produits(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nom TEXT UNIQUE NOT NULL,
                prix_achat INTEGER DEFAULT 0,
                prix_vente INTEGER DEFAULT 0,
                stock INTEGER DEFAULT 0,
                categorie TEXT DEFAULT 'General',
                actif INTEGER DEFAULT 1
            )""", [
                ("categorie", "TEXT DEFAULT 'General'"),
                ("actif", "INTEGER DEFAULT 1")
            ])
        cur.execute("UPDATE produits SET actif=1 WHERE actif IS NULL")
        cur.execute("UPDATE produits SET categorie='General' WHERE categorie IS NULL")

        _migrate_table(cur, "ventes", """
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
                date_vente TEXT DEFAULT CURRENT_TIMESTAMP
            )""", [
                ("produit_id", "INTEGER"),
                ("produit_nom", "TEXT"),
                ("quantite", "INTEGER DEFAULT 0"),
                ("prix_achat", "INTEGER DEFAULT 0"),
                ("prix_vente", "INTEGER DEFAULT 0"),
                ("total", "INTEGER DEFAULT 0"),
                ("benefice", "INTEGER DEFAULT 0"),
                ("vendeur", "TEXT DEFAULT 'Inconnu'")
            ])

        _migrate_table(cur, "dettes", """
            CREATE TABLE IF NOT EXISTS dettes(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client TEXT NOT NULL,
                telephone TEXT,
                total INTEGER DEFAULT 0,
                paye INTEGER DEFAULT 0,
                reste INTEGER DEFAULT 0,
                date_creation TEXT DEFAULT CURRENT_TIMESTAMP,
                statut TEXT DEFAULT 'ACTIF'
            )""", [
                ("telephone", "TEXT"),
                ("total", "INTEGER DEFAULT 0"),
                ("paye", "INTEGER DEFAULT 0"),
                ("reste", "INTEGER DEFAULT 0"),
                ("statut", "TEXT DEFAULT 'ACTIF'")
            ])
        cur.execute("UPDATE dettes SET statut='ACTIF' WHERE statut IS NULL")

        _migrate_table(cur, "paiements_dettes", """
            CREATE TABLE IF NOT EXISTS paiements_dettes(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dette_id INTEGER,
                montant INTEGER DEFAULT 0,
                date_paiement TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(dette_id) REFERENCES dettes(id)
            )""", [
                ("dette_id", "INTEGER"),
                ("montant", "INTEGER DEFAULT 0")
            ])

        _migrate_table(cur, "utilisateurs", """
            CREATE TABLE IF NOT EXISTS utilisateurs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                nom_complet TEXT,
                role TEXT DEFAULT 'VENDEUR',
                actif INTEGER DEFAULT 1
            )""", [
                ("nom_complet", "TEXT"),
                ("role", "TEXT DEFAULT 'VENDEUR'"),
                ("actif", "INTEGER DEFAULT 1")
            ])
        cur.execute("UPDATE utilisateurs SET actif=1 WHERE actif IS NULL")

        h = hashlib.sha256("290493".encode()).hexdigest()
        cur.execute("INSERT OR IGNORE INTO utilisateurs (username, password_hash, nom_complet, role) VALUES (?,?,?,?)",
                   ("kdk", h, "Administrateur", "ADMIN"))

        _detect_existing_active_debt_duplicates(cur)

        conn.commit()
    log("Init DB + Full Migration OK")

def hash_pwd(p):
    return hashlib.sha256(p.encode()).hexdigest()

def fmt(n):
    if n is None:
        return "0"
    return f"{int(n):,}".replace(",", " ")


# ==============================
# FANEVA SYSTEM HYBRID - HELPERS
# ==============================
def _hexcolor(c):
    """Convertit une couleur hex #RRGGBB en tuple (r,g,b,1) Kivy."""
    if not c:
        return (0.5, 0.5, 0.5, 1)
    c = c.lstrip("#")
    try:
        return (int(c[0:2], 16) / 255, int(c[2:4], 16) / 255, int(c[4:6], 16) / 255, 1)
    except Exception:
        return (0.5, 0.5, 0.5, 1)

def _lighten(color):
    r, g, b, a = color
    f = 0.55
    return (min(1, r + f * (1 - r)), min(1, g + f * (1 - g)), min(1, b + f * (1 - b)), 1)

def get_magasins_from_db():
    """Lit les magasins actuels depuis la base (table magasins) et remplit MAGASINS.
    Le chemin de base de chaque magasin est deduit de sa cle (une base par magasin).
    Les magasins en attente (pas encore de base creee sur l'appareil) sont conserves
    avec un chemin par defaut ; la base vide est initialisee au premier acces."""
    global MAGASINS
    if DB_PATH is None or not os.path.exists(DB_PATH):
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id, cle, nom, couleur, couleur_light, categorie FROM magasins WHERE actif=1 ORDER BY id")
        rows = cur.fetchall()
        conn.close()
        result = {}
        for mid, cle, nom, couleur, couleur_light, categorie in rows:
            defaut = MAGASINS_DEFAULT.get(cle, {
                "id": None, "nom": nom, "categorie": categorie or "General",
                "db": os.path.join(EXTERNAL_DIR, f"stock_{cle}.db"),
                "couleur": (0.5, 0.5, 0.5, 1), "couleur_light": (0.9, 0.9, 0.9, 1),
            })
            result[cle] = {
                "id": mid, "cle": cle, "nom": nom,
                "categorie": categorie or defaut.get("categorie", "General"),
                "db": os.path.join(EXTERNAL_DIR, f"stock_{cle}.db"),
                "couleur": _hexcolor(couleur) if couleur else defaut["couleur"],
                "couleur_light": _hexcolor(couleur_light) if couleur_light else defaut["couleur_light"],
            }
        if result:
            MAGASINS = result
    except Exception as e:
        log(f"get_magasins_from_db echoue: {e}")

def set_hybrid_session(conn, user_id, username, role):
    """Persiste la session HYBRID (user_id, username, device_id) dans config_hybrid."""
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO config_hybrid (cle, valeur) VALUES ('session_user_id', ?)", (user_id,))
        cur.execute("INSERT OR REPLACE INTO config_hybrid (cle, valeur) VALUES ('session_username', ?)", (username,))
        cur.execute("INSERT OR REPLACE INTO config_hybrid (cle, valeur) VALUES ('session_role', ?)", (role,))
        cur.execute("INSERT OR REPLACE INTO config_hybrid (cle, valeur) VALUES ('device_id', ?)", (HYBRID_DEVICE_ID,))
        conn.commit()
    except Exception as e:
        log(f"set_hybrid_session echoue: {e}")

def refresh_magasins():
    """Recharge MAGASINS depuis la base de donnees du magasin actif (si base deja initialisee)."""
    if has_hybrid_module():
        try:
            get_magasins_from_db()
        except Exception as e:
            log(f"refresh_magasins: {e}")

def has_hybrid_module():
    return run_migration is not None


def canonical_display_stock(conn, hybrid_id, magasin_id, fallback_stock=0):
    """Retourne la projection HYBRID canonique; `produits.stock` n’est qu’un repli legacy."""
    if has_hybrid_module() and hybrid_id and magasin_id:
        canonical_stock = get_stock(conn, hybrid_id, magasin_id)
        stock_diagnostic_trace_async(
            "CANONICAL_SINGLE",
            ({"HYBRID_ID": hybrid_id, "MAGASIN_ID": magasin_id,
              "STOCK_SOURCE": int(fallback_stock or 0), "CANONICAL_STOCK": canonical_stock},),
        )
        return canonical_stock
    legacy_stock = int(fallback_stock or 0)
    stock_diagnostic_trace_async(
        "LEGACY_FALLBACK",
        ({"HYBRID_ID": hybrid_id or "NONE", "MAGASIN_ID": magasin_id or "NONE",
          "STOCK_SOURCE": legacy_stock, "CANONICAL_STOCK": "FALLBACK"},),
    )
    return legacy_stock


def canonical_display_stocks_batch(conn, product_refs):
    """Résout plusieurs projections canoniques avec deux lectures SQLite au plus.

    Chaque élément de ``product_refs`` est ``(hybrid_id, magasin_id, fallback_stock)``.
    La règle métier reste strictement identique à ``canonical_display_stock`` : alias local
    scoppé par magasin, projection ``stocks_magasin`` par produit canonique/magasin et repli
    legacy seulement quand l’identité HYBRID ou le magasin est absent.
    """
    refs = list(product_refs)
    if not refs:
        return []
    if not has_hybrid_module():
        return [int(fallback_stock or 0) for _, _, fallback_stock in refs]

    resolvable = [(hybrid_id, magasin_id) for hybrid_id, magasin_id, _ in refs if hybrid_id and magasin_id]
    aliases = {}
    if resolvable:
        predicate = " OR ".join("(magasin_id=? AND source_product_id=?)" for _ in resolvable)
        params = [value for hybrid_id, magasin_id in resolvable for value in (magasin_id, hybrid_id)]
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT magasin_id, source_product_id, canonical_product_id "
                f"FROM canonical_product_aliases_local WHERE {predicate}",
                params,
            )
            aliases = {(magasin_id, source_id): canonical_id for magasin_id, source_id, canonical_id in cur.fetchall() if canonical_id}
        except sqlite3.OperationalError:
            # Base legacy sans table d’alias : même comportement que resolve_canonical_product_id.
            aliases = {}

    canonical_refs = [
        (aliases.get((magasin_id, hybrid_id), hybrid_id), magasin_id) if hybrid_id and magasin_id else None
        for hybrid_id, magasin_id, _ in refs
    ]
    stock_by_key = {}
    keys = list(dict.fromkeys(ref for ref in canonical_refs if ref is not None))
    if keys:
        predicate = " OR ".join("(produit_id=? AND magasin_id=?)" for _ in keys)
        params = [value for produit_id, magasin_id in keys for value in (produit_id, magasin_id)]
        cur = conn.cursor()
        cur.execute(
            f"SELECT produit_id, magasin_id, COALESCE(SUM(stock),0) "
            f"FROM stocks_magasin WHERE {predicate} GROUP BY produit_id, magasin_id",
            params,
        )
        stock_by_key = {(produit_id, magasin_id): stock for produit_id, magasin_id, stock in cur.fetchall()}

    projected_stocks = [
        stock_by_key.get(canonical_ref, 0) if canonical_ref is not None else int(fallback_stock or 0)
        for canonical_ref, (_, _, fallback_stock) in zip(canonical_refs, refs)
    ]
    stock_diagnostic_trace_async(
        "CANONICAL_BATCH",
        tuple(
            {
                "HYBRID_ID": hybrid_id or "NONE",
                "CANONICAL_PRODUCT_UUID": canonical_ref[0] if canonical_ref else "NONE",
                "MAGASIN_ID": magasin_id or "NONE",
                "STOCK_SOURCE": int(fallback_stock or 0),
                "CANONICAL_STOCK": stock,
            }
            for (hybrid_id, magasin_id, fallback_stock), canonical_ref, stock in zip(refs, canonical_refs, projected_stocks)
        ),
    )
    return projected_stocks


def canonical_product_display_rows(conn, product_rows):
    """Transforme les lignes standard ``produits`` en lignes UI sans N+1 de projection."""
    rows = list(product_rows)
    stocks = canonical_display_stocks_batch(conn, [(r[5], r[6], r[4]) for r in rows])
    stock_diagnostic_trace_async(
        "PRODUCT_DISPLAY_PROJECTION",
        tuple(
            {
                "PRODUCT_ID": row[0],
                "PRODUCT_NAME": row[1],
                "HYBRID_ID": row[5] or "NONE",
                "MAGASIN_ID": row[6] or "NONE",
                "STOCK_SOURCE": int(row[4] or 0),
                "CANONICAL_STOCK": stock,
            }
            for row, stock in zip(rows, stocks)
        ),
    )
    return [(r[0], r[1], r[2], r[3], stock) for r, stock in zip(rows, stocks)]


def canonical_stock_export_data(conn, role):
    """Construit les exports stock depuis la projection canonique si le produit est mappé.

    Les lignes sans identité HYBRID ou magasin restent affichables via leur stock legacy,
    ce qui préserve les produits non mappés sans réintroduire de double projection.
    """
    is_admin = is_user_admin_hybrid(role)
    headers = (["ID", "Nom", "Categorie", "Prix Achat", "Prix Vente", "Stock", "Valeur Achat", "Valeur Vente"]
               if is_admin else ["ID", "Nom", "Categorie", "Prix Vente", "Stock"])
    cur = conn.cursor()
    cur.execute("SELECT id, nom, categorie, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id FROM produits WHERE actif=1 ORDER BY nom")
    source_rows = cur.fetchall()
    projected_stocks = canonical_display_stocks_batch(conn, [(r[6], r[7], r[5]) for r in source_rows])
    rows = []
    for (product_id, nom, categorie, prix_achat, prix_vente, legacy_stock, hybrid_id, magasin_id), stock in zip(source_rows, projected_stocks):
        if is_admin:
            rows.append((product_id, nom, categorie, prix_achat, prix_vente, stock, stock * prix_achat, stock * prix_vente))
        else:
            rows.append((product_id, nom, categorie, prix_vente, stock))
    return headers, rows

def is_user_admin_hybrid(role):
    """ADMIN (legacy), ADMIN_PRINCIPAL et ADMIN_MAGASIN sont tous des administrateurs."""
    if not role:
        return False
    r = str(role).upper()
    return r in ("ADMIN", "ADMIN_PRINCIPAL", "ADMIN_MAGASIN")

def init_hybrid(conn):
    """Initialisation HYBRID : migration, identite locale et serveur local hors migration."""
    global HYBRID_DEVICE_ID
    if not has_hybrid_module():
        return
    try:
        cur = conn.cursor()
        run_migration(conn)
        # Migration Device ID : UUID v4 persistant + conservation de l'identite historique.
        identity = initialize_device_identity(conn)
        HYBRID_DEVICE_ID = identity["device_id"]
        # Creation des magasins par defaut s'ils n'existent pas (migration depuis l'ancienne version)
        cur.execute("SELECT COUNT(*) FROM magasins")
        if cur.fetchone()[0] == 0:
            create_store(conn, "quincaillerie", "Quincaillerie", "Magasin principal - outils et matériaux",
                         categorie="Quincaillerie", couleur="#337FB2", couleur_light="#D9E6F2",
                         admin_creatrice=None)
            create_store(conn, "cosmetiques", "Cosmetiques", "Magasin cosmetiques et soins",
                         categorie="Cosmetiques", couleur="#E64D99", couleur_light="#FFD9E9",
                         admin_creatrice=None)
            log("HYBRID: magasins par defaut crees (Quincaillerie, Cosmetiques)")
        # Associer les produits legacy aux magasins par defaut si pas deja fait
        cur.execute("SELECT COUNT(*) FROM produits WHERE hybrid_id IS NULL AND actif=1")
        if cur.fetchone()[0] > 0:
            _migrate_legacy_products(conn)
        canonical_mapping = initialize_canonical_product_mapping(conn)
        if canonical_mapping:
            log(f"HYBRID: mapping canonique actif v={canonical_mapping['mapping_version']} pairs={canonical_mapping['pairs']}")
        log(f"HYBRID init OK - device_id={HYBRID_DEVICE_ID} | identite={identity['state']}")
        if identity["migration_locked"]:
            log("HYBRID: migration Device ID active - synchronisation Internet/Wi-Fi et serveur local bloques")
        # Demarrage du serveur de sync local seulement apres cloture explicite de migration.
        elif ENABLE_LOCAL_SYNC_SERVER:
            try:
                _start_sync_server(conn)
                log(f"HYBRID: serveur sync local actif sur port {SYNC_SERVER_PORT} (Wi-Fi/Hotspot)")
            except Exception as e:
                log(f"HYBRID: serveur sync local indisponible: {e}")
    except Exception as e:
        log(f"init_hybrid echoue: {e}")
        for ln in traceback.format_exc().splitlines():
            log(f"  {ln}")

def _migrate_legacy_products(conn):
    """Migration additive des produits legacy vers le catalogue HYBRID (une seule fois)."""
    try:
        cur = conn.cursor()
        # Associer chaque produit legacy au magasin actif par defaut (meme magasin que sa base)
        # On retrouve le magasin par le chemin/nom de la base
        cle_defaut = "quincaillerie" if "quincaillerie" in (DB_PATH or "") else (
            "cosmetiques" if "cosmetiques" in (DB_PATH or "") else "quincaillerie")
        cur.execute("SELECT id FROM magasins WHERE cle=?", (cle_defaut,))
        mrow = cur.fetchone()
        if not mrow:
            return
        mid = mrow[0]
        cur.execute("SELECT id, nom, prix_achat, prix_vente, stock, categorie FROM produits WHERE hybrid_id IS NULL AND actif=1")
        for pid, nom, pa, pv, stock, cat in cur.fetchall():
            p_uuid = gen_uuid()
            try:
                cur.execute("INSERT INTO catalogues (id, nom, categorie, prix_achat_moyen, prix_vente) VALUES (?,?,?,?,?)",
                            (p_uuid, nom, cat or "General", pa or 0, pv or 0))
                # Transaction de creation (pour que le catalogue se synchronise)
                if create_product is not None:
                    tx_id = gen_uuid()
                    cur.execute("INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload, statut_local) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                (tx_id, "PROD_CREATE", HYBRID_DEVICE_ID, None, None, mid, cle_defaut, now_iso(),
                                 json.dumps({"produit_id": p_uuid, "nom": nom, "categorie": cat or "General",
                                             "prix_achat": pa or 0, "prix_vente": pv or 0}, ensure_ascii=False), "LOCAL"))
                    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
                # Stock actuel -> mouvement initial de reference
                if stock > 0:
                    _apply_mvt_initial(conn, p_uuid, mid, stock, pa or 0)
                cur.execute("UPDATE produits SET hybrid_id=?, magasin_ref_id=? WHERE id=?", (p_uuid, mid, pid))
            except Exception as e:
                log(f"Migration produit {pid} echoue: {e}")
        conn.commit()
        log("HYBRID: migration des produits legacy vers le catalogue terminee")
    except Exception as e:
        log(f"_migrate_legacy_products echoue: {e}")

def _apply_mvt_initial(conn, produit_id, magasin_id, quantite, prix_achat):
    """Enregistre le stock initial comme mouvement de reference (transaction HYBRID).
    Delegue a record_stock_in qui cree la transaction, le mouvement ET le PA moyen."""
    record_stock_in(conn, produit_id, magasin_id, quantite, prix_achat, source="MIGRATION_INITIAL_STOCK")

def _table_exists_hybrid(name):
    """Verifie qu'une table HYBRID existe dans la base courante."""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            return cur.fetchone() is not None
    except Exception:
        return False

def popup(title, msg):
    log(f"POPUP: {title} - {msg}")
    try:
        b = BoxLayout(orientation="vertical", padding=20, spacing=10)
        b.add_widget(Label(text=str(msg), font_size=24))
        btn = Button(text="OK", size_hint_y=None, height=50)
        p = Popup(title=str(title), content=b, size_hint=(0.8, 0.4))
        btn.bind(on_press=p.dismiss)
        b.add_widget(btn)
        p.open()
    except Exception as e:
        log_error("popup", e)

# ==============================
# APP
# ==============================
class KDKApp(App):
    def build(self):
        log("build()")
        self.user = None
        self.magasin = None
        self.root = BoxLayout(orientation="vertical")
        self.show_choix_magasin()
        log("build() OK")
        return self.root

    def clear(self):
        log("clear()")
        try:
            self.root.clear_widgets()
            log("clear() OK")
        except Exception as e:
            log_error("clear", e)

    # ---------- PROTECTION CENTRALISÉE ----------
    def check_admin(self):
        if not self.user or not is_user_admin_hybrid(self.user.get("role")):
            u = self.user.get("username") if self.user else "Anonyme"
            log(f"ACCÈS REFUSÉ: Fonction ADMIN tentée par l'utilisateur '{u}' (Rôle: {self.user.get('role') if self.user else 'Aucun'})")
            popup("Accès refusé", "Accès réservé à l'administrateur")
            return False
        return True

    # ---------- CHOIX MAGASIN ----------
    def show_diagnostic_store_selection(self):
        """Outil diagnostic READONLY isolé ; ce n’est pas l’entrée de démarrage métier."""
        self.clear()
        layout = BoxLayout(orientation="vertical", padding=22, spacing=15)
        layout.add_widget(Label(
            text="FANEVA SYSTEM — DIAGNOSTIC ANDROID READONLY ENVIRONMENT",
            font_size=25, bold=True, color=(0.25, 0.65, 0.85, 1),
            size_hint_y=None, height=62,
        ))
        layout.add_widget(Label(
            text=("MODE DIAGNOSTIC : métadonnées read-only, SHA-256 avant/après et essais SQLite ordonnés.\n"
                  "Aucun DML/DDL, PRAGMA, backup, checkpoint, VACUUM, copie, suppression, réseau, sync, pilote, ACK ou migration.\n"
                  "Les fichiers DB/WAL/SHM/journal sont seulement relevés et ne sont jamais supprimés ou modifiés."),
            font_size=17, size_hint_y=None, height=78,
        ))
        layout.add_widget(Label(
            text=("Base Android externe cible :\n"
                  "/storage/emulated/0/FANEVA_SYSTEM_ANDROID/stock_quincaillerie.db\n"
                  "Taille attendue : 36864 octets ; SHA-256 attendu : b7b733e4…033c6."),
            font_size=17, size_hint_y=None, height=68,
        ))
        diagnostic_button = Button(
            text="LANCER LE DIAGNOSTIC ANDROID READONLY\nTEXTE COPIABLE — AUCUNE FONCTION MÉTIER",
            font_size=18, bold=True, background_color=(0.62, 0.40, 0.13, 1),
            size_hint_y=None, height=82,
        )
        diagnostic_button.bind(on_press=lambda _button: self.run_readonly_environment_diagnostic())
        layout.add_widget(diagnostic_button)
        self.root.add_widget(layout)

    def _append_environment_metadata(self, label, metadata):
        """Ajoute les métadonnées non mutatrices d’un chemin au rapport copiable."""
        metadata = metadata or {}
        self._sqlite_diagnostic_lines.extend([
            label + " — CHEMIN : " + str(metadata.get("path")),
            label + " — EXISTE : " + ("OUI" if metadata.get("exists") else "NON"),
            label + " — PERMISSIONS : " + str(metadata.get("mode_octal")),
            label + " — UID/GID : %s/%s" % (metadata.get("uid"), metadata.get("gid")),
            label + " — LISIBLE : " + ("OUI" if metadata.get("readable") else "NON"),
            label + " — MODIFIABLE PAR LE PROCESSUS : " + ("OUI" if metadata.get("writable") else "NON"),
            label + " — RECHERCHABLE : " + ("OUI" if metadata.get("searchable") else "NON"),
        ])
        if metadata.get("is_file"):
            self._sqlite_diagnostic_lines.extend([
                label + " — TAILLE : %s octets" % metadata.get("size_bytes"),
                label + " — SHA-256 : " + str(metadata.get("sha256")),
            ])
        if metadata.get("modified_utc"):
            self._sqlite_diagnostic_lines.append(
                label + " — MODIFICATION UTC : " + str(metadata.get("modified_utc"))
            )
        if metadata.get("error"):
            self._sqlite_diagnostic_lines.append(
                self._format_diagnostic_payload(metadata.get("error"))
            )

    def _format_diagnostic_payload(self, payload):
        """Conserve le détail complet d’une exception dans le seul texte affiché."""
        payload = payload or {}
        return "\n".join([
            "exception_type : " + str(payload.get("exception_type")),
            "exception_str : " + str(payload.get("exception_str")),
            "exception_repr : " + str(payload.get("exception_repr")),
            "errno : " + str(payload.get("errno")),
            "strerror : " + str(payload.get("strerror")),
            "filename : " + str(payload.get("filename")),
            "source_path : " + str(payload.get("source_path")),
            "traceback :\n" + str(payload.get("traceback")),
        ])

    def _append_readonly_test_result(self, label, item):
        item = item or {}
        self._sqlite_diagnostic_lines.extend([
            label + " — DESCRIPTION : " + str(item.get("description")),
            label + " — CIBLE : " + str(item.get("target")),
            label + " — URI : " + str(item.get("uri")),
            label + " — RÉSULTAT : " + str(item.get("status")),
        ])
        if "connection" in item:
            self._sqlite_diagnostic_lines.append(label + " — CONNEXION : " + str(item.get("connection")))
        if "select_1" in item:
            self._sqlite_diagnostic_lines.append(label + " — SELECT 1 : " + str(item.get("select_1")))
        if "tables" in item:
            self._sqlite_diagnostic_lines.append(label + " — TABLES : " + repr(item.get("tables")))
        if item.get("reason"):
            self._sqlite_diagnostic_lines.append(label + " — MOTIF : " + str(item.get("reason")))
        if item.get("error"):
            self._sqlite_diagnostic_lines.append(self._format_diagnostic_payload(item.get("error")))

    def run_readonly_environment_diagnostic(self):
        """Exécute seulement le collecteur diagnostic ordonné et non mutateur."""
        base_path = os.path.join(EXTERNAL_DIR, "stock_quincaillerie.db")
        self._open_sqlite_diagnostic_report()
        self._sqlite_diagnostic_lines = [
            "============================================================",
            "FANEVA SYSTEM — DIAGNOSTIC ANDROID READONLY ENVIRONMENT",
            "============================================================",
            "BASE CIBLE : " + base_path,
            "RÈGLES : aucun INSERT/UPDATE/DELETE/CREATE/DROP/ALTER, PRAGMA, backup, checkpoint, VACUUM, copie, suppression, renommage, réseau, sync, pilote, ACK ou migration.",
        ]
        self._refresh_sqlite_diagnostic_report()
        try:
            result = run_android_readonly_environment_diagnostic(base_path)
            env = result.get("environment", {})
            self._sqlite_diagnostic_lines.extend([
                "1. ENVIRONNEMENT",
                "Python version : " + str(env.get("python_version")),
                "Python implementation : " + str(env.get("python_implementation")),
                "sqlite3.sqlite_version : " + str(env.get("sqlite3_sqlite_version")),
                "sqlite3.version : " + str(env.get("sqlite3_module_version")),
                "Architecture CPU : " + str(env.get("cpu_architecture")),
                "Android API : " + str(env.get("android_api")),
                "Module sqlite3 : " + str(env.get("sqlite3_module_path")),
                "Package Android : " + str(env.get("android_package")),
                "PID/UID/GID : %s/%s/%s" % (env.get("pid"), env.get("uid"), env.get("gid")),
                "cwd : " + str(env.get("cwd")),
                "HOME : " + str(env.get("home")),
                "TMP : " + str(env.get("tmp")),
                "TMPDIR : " + str(env.get("tmpdir")),
            ])
            if env.get("android_runtime_error"):
                self._sqlite_diagnostic_lines.append(self._format_diagnostic_payload(env.get("android_runtime_error")))
            self._sqlite_diagnostic_lines.append("2. DB")
            self._append_environment_metadata("DB", result.get("database"))
            self._sqlite_diagnostic_lines.append("3. RÉPERTOIRE PARENT")
            self._append_environment_metadata("RÉPERTOIRE", result.get("parent_directory"))
            self._sqlite_diagnostic_lines.append("4. WAL / SHM / JOURNAL AVANT")
            for key in ("wal", "shm", "journal"):
                self._append_environment_metadata(key.upper(), result.get("artifacts_before", {}).get(key))
            self._sqlite_diagnostic_lines.append("5. TEST URI mode=ro")
            self._append_readonly_test_result("TEST A", result.get("tests", {}).get("uri_mode_ro"))
            self._sqlite_diagnostic_lines.append("6. TEST SELECT")
            self._append_readonly_test_result("TEST B", result.get("tests", {}).get("select_schema"))
            self._sqlite_diagnostic_lines.append("7. TEST CHEMIN ABSOLU")
            self._append_readonly_test_result("TEST CHEMIN ABSOLU", result.get("tests", {}).get("absolute_path"))
            self._sqlite_diagnostic_lines.append("8. TEST FILE URI")
            self._append_readonly_test_result("TEST FILE URI", result.get("tests", {}).get("file_uri"))
            self._sqlite_diagnostic_lines.append("9. WAL / SHM / JOURNAL APRÈS")
            for key in ("wal", "shm", "journal"):
                after = result.get("artifacts_after", {}).get(key)
                self._append_environment_metadata(key.upper(), after)
                if result.get("artifacts_observed_after_open", {}).get(key):
                    self._sqlite_diagnostic_lines.append(
                        "ARTEFACT OBSERVÉ APRÈS OUVERTURE SQLITE : " + key.upper()
                    )
            self._sqlite_diagnostic_lines.extend([
                "10. SHA-256 DB AVANT / APRÈS",
                "SHA-256 AVANT : " + str(result.get("db_sha256_before")),
                "SHA-256 APRÈS : " + str(result.get("db_sha256_after")),
                "DB INCHANGÉE : " + ("OUI" if result.get("db_sha256_unchanged") else "NON"),
                "11. CAUSE PROBABLE",
                "À déduire uniquement des résultats TEST A/B/chemin absolu/FILE URI ci-dessus ; aucune hypothèse automatique n’est appliquée.",
                "12. CAUSE CONFIRMÉE",
                "AUCUNE AVANT INTERPRÉTATION DU RAPPORT COMPLET ; l’APK ne modifie pas la base pour forcer une conclusion.",
                "13. CONCLUSION",
                "DIAGNOSTIC TERMINÉ — COPIEZ L’INTÉGRALITÉ DU TEXTE AVEC LE BOUTON COPIER LE RÉSULTAT.",
            ])
        except Exception as exc:
            self._sqlite_diagnostic_lines.extend([
                "ÉTAPE EXACTE : exécution globale du diagnostic readonly environment",
                self._format_diagnostic_exception(exc, base_path, ""),
                "CAUSE NON CONFIRMÉE",
            ])
        self._refresh_sqlite_diagnostic_report()

    def share_validated_external_copy(self):
        """Partage exactement la copie déjà validée, sans la créer ni l’altérer."""
        self._open_sqlite_diagnostic_report()
        self._sqlite_diagnostic_lines = [
            "PARTAGE COPIE EXTERNE VALIDÉE — RÉSULTAT",
            "MODE : validation binaire rb du fichier existant puis ACTION_SEND/FileProvider.",
            "INTERDICTIONS RESPECTÉES : aucune création, réécriture, suppression, renommage, SQLite, PRAGMA, backup, checkpoint, VACUUM, réseau FANEVA, synchronisation, pilote, ACK ou migration.",
        ]
        self._refresh_sqlite_diagnostic_report()
        copy_path = ""
        try:
            app_root = os.path.realpath(resolve_database_copy_app_root(strict=True))
            export_root = os.path.realpath(os.path.join(app_root, "FANEVA_EXTERNAL_BINARY_EXPORT"))
            copy_path = os.path.realpath(os.path.join(export_root, VALIDATED_EXTERNAL_COPY_FILENAME))
            if os.path.commonpath([copy_path, export_root]) != export_root:
                raise ValueError("Chemin de copie refusé hors du répertoire app-specific autorisé")
            metadata = validate_existing_binary_copy_readonly(
                copy_path,
                VALIDATED_EXTERNAL_COPY_SIZE_BYTES,
                VALIDATED_EXTERNAL_COPY_SHA256,
            )
            from jnius import autoclass, cast
            PythonActivity = autoclass("org.kivy.android.PythonActivity")
            Intent = autoclass("android.content.Intent")
            JavaFile = autoclass("java.io.File")
            JavaString = autoclass("java.lang.String")
            FileProvider = autoclass("androidx.core.content.FileProvider")
            activity = PythonActivity.mActivity
            authority = activity.getPackageName() + ".fanevaexportprovider"
            uri = FileProvider.getUriForFile(activity, authority, JavaFile(copy_path))
            intent = Intent(Intent.ACTION_SEND)
            intent.setType("application/octet-stream")
            intent.putExtra(Intent.EXTRA_STREAM, cast("android.os.Parcelable", uri))
            intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            chooser_title = cast("java.lang.CharSequence", JavaString("Partager la copie validée FANEVA"))
            chooser_intent = Intent.createChooser(intent, chooser_title)
            activity.startActivity(chooser_intent)
            self._sqlite_diagnostic_lines.extend([
                "FICHIER PARTAGÉ : ACTION_SEND lancé avec URI FileProvider en lecture seule.",
                "CHEMIN : " + str(metadata.get("path")),
                "TAILLE : %s octets" % metadata.get("size_bytes"),
                "SHA-256 : " + str(metadata.get("sha256")),
            ])
        except Exception as exc:
            self._sqlite_diagnostic_lines.extend([
                "ÉTAPE EXACTE : validation de la copie existante puis partage FileProvider",
                self._format_diagnostic_exception(exc, copy_path, "FANEVA_EXTERNAL_BINARY_EXPORT"),
                "FICHIER NON PARTAGÉ",
            ])
        self._refresh_sqlite_diagnostic_report()

    def start_external_binary_export(self):
        """Exporte uniquement la DB externe par lecture binaire rb vers xb."""
        base_path = os.path.join(EXTERNAL_DIR, "stock_quincaillerie.db")
        self._open_sqlite_diagnostic_report()
        self._sqlite_diagnostic_lines = [
            "EXPORT BINAIRE EXTERNE — RÉSULTAT",
            "SOURCE : " + base_path,
            "MODE : open(source, rb) + open(destination, xb) ; aucune API SQLite.",
            "GARANTIES : aucun sqlite3.connect, PRAGMA, backup, checkpoint, VACUUM, réparation, renommage, suppression, réseau, synchronisation, pilote, ACK ou migration.",
        ]
        self._refresh_sqlite_diagnostic_report()
        try:
            export_dir = os.path.join(resolve_database_copy_app_root(strict=True), "FANEVA_EXTERNAL_BINARY_EXPORT")
            result = export_external_database_binary_only(base_path, export_dir)
            associated = result.get("associated_files", {})
            self._sqlite_diagnostic_lines.extend([
                "RÉPERTOIRE APP-SPECIFIC : " + export_dir,
                "FICHIER WAL : " + ("PRÉSENT" if associated.get("wal", {}).get("exists") else "ABSENT"),
                "FICHIER SHM : " + ("PRÉSENT" if associated.get("shm", {}).get("exists") else "ABSENT"),
                "ROLLBACK JOURNAL : " + ("PRÉSENT" if associated.get("journal", {}).get("exists") else "ABSENT"),
                "TAILLE SOURCE : %s octets" % result.get("source_size_bytes"),
                "SHA-256 SOURCE AVANT : " + str(result.get("source_sha256_before")),
                "CHEMIN COPIE : " + str(result.get("destination_path")),
                "TAILLE COPIE : %s octets" % result.get("destination_size_bytes"),
                "SHA-256 COPIE : " + str(result.get("destination_sha256")),
                "SHA-256 SOURCE APRÈS : " + str(result.get("source_sha256_after")),
                "SOURCE INCHANGÉE : " + ("OUI" if result.get("source_unchanged") else "NON"),
                str(result.get("conclusion")),
            ])
        except Exception as exc:
            self._sqlite_diagnostic_lines.extend([
                "ÉTAPE EXACTE : export binaire externe source rb vers destination xb",
                self._format_diagnostic_exception(exc, base_path, "répertoire app-specific FANEVA_EXTERNAL_BINARY_EXPORT"),
                "COPIE EXTERNE NON VALIDÉE",
            ])
        self._refresh_sqlite_diagnostic_report()

    def start_sqlite_journal_physical_diagnostic(self):
        """Affiche le seul contrôle autorisé par l'APK 1.3.9, sans SQLite."""
        base_path = os.path.join(EXTERNAL_DIR, "stock_quincaillerie.db")
        self._open_sqlite_diagnostic_report()
        self._sqlite_diagnostic_lines = [
            "DIAGNOSTIC PHYSIQUE SQLITE — CONTRÔLE DB/WAL/SHM/JOURNAL",
            "BASE EXTERNE : " + base_path,
            "MODE : lecture des métadonnées système et lecture binaire SHA-256 uniquement.",
            "GARANTIES : sqlite3.connect() NON APPELÉ ; aucun PRAGMA ; aucun backup ; aucun réseau ; aucune écriture source.",
        ]
        self._refresh_sqlite_diagnostic_report()
        try:
            result = inspect_sqlite_files_physical_binary(base_path)
            for key in ("db", "wal", "shm", "journal"):
                item = result.get("files", {}).get(key, {})
                present = "OUI" if item.get("exists") else "NON"
                lines = [
                    "FICHIER : " + str(item.get("label") or key),
                    "CHEMIN : " + str(item.get("path") or "-"),
                    "PRÉSENT : " + present,
                ]
                if item.get("exists"):
                    lines.extend([
                        "TAILLE : %s octets" % item.get("size_bytes"),
                        "SHA-256 : " + str(item.get("sha256") or "-"),
                        "PERMISSIONS/MODE : " + str(item.get("mode") or "-"),
                        "DATE MODIFICATION UTC : " + str(item.get("modified_utc") or "-"),
                    ])
                if item.get("anomaly"):
                    lines.append("ANOMALIE : " + str(item.get("anomaly")))
                if item.get("error"):
                    error = item["error"]
                    lines.extend([
                        "exception_type : " + str(error.get("exception_type")),
                        "exception_str : " + str(error.get("exception_str")),
                        "exception_repr : " + str(error.get("exception_repr")),
                        "errno : " + str(error.get("errno")),
                        "strerror : " + str(error.get("strerror")),
                        "filename : " + str(error.get("filename")),
                        "traceback :\n" + str(error.get("traceback")),
                    ])
                self._sqlite_diagnostic_lines.append("\n".join(lines))
            self._sqlite_diagnostic_lines.extend([
                "CONTRÔLES INTERDITS NON EXÉCUTÉS : sqlite3.connect, PRAGMA, backup, checkpoint, VACUUM, suppression, renommage, réseau, synchronisation, pilote, ACK, migration.",
                "CONCLUSION AUTOMATIQUE : " + str(result.get("conclusion") or "AUTRE ANOMALIE"),
            ])
        except Exception as exc:
            self._sqlite_diagnostic_lines.extend([
                "ÉTAPE EXACTE : contrôle physique des fichiers externes",
                self._format_diagnostic_exception(exc, base_path, ""),
                "CONCLUSION AUTOMATIQUE : AUTRE ANOMALIE",
            ])
        self._refresh_sqlite_diagnostic_report()

    def _format_diagnostic_exception(self, exc, source_path, destination_path):
        """Retourne toutes les propriétés d’exception Android/Python sans les masquer."""
        return (
            f"Exception type : {type(exc).__name__}\n"
            f"str(exc) : {str(exc)}\n"
            f"repr(exc) : {repr(exc)}\n"
            f"errno : {getattr(exc, 'errno', None)}\n"
            f"strerror : {getattr(exc, 'strerror', None)}\n"
            f"filename : {getattr(exc, 'filename', None)}\n"
            f"path source : {source_path}\n"
            f"path destination : {destination_path}\n"
            f"traceback :\n{traceback.format_exc()}"
        )

    def _open_sqlite_diagnostic_report(self):
        """Ouvre un véritable ScrollView actualisé après chaque étape du diagnostic."""
        self._sqlite_diagnostic_lines = [
            "DIAGNOSTIC EXPORT SQLITE — RESULTAT",
            "MODE : local uniquement ; aucune action réseau, métier ou SQLite source en écriture.",
        ]
        content = BoxLayout(orientation="vertical", padding=dp(10), spacing=dp(8))
        scroll = ScrollView(do_scroll_x=False)
        report_label = Label(
            text="", size_hint_y=None, halign="left", valign="top",
            font_size=dp(13), color=(0.95, 0.95, 0.95, 1),
        )
        report_label.bind(width=lambda widget, width: setattr(widget, "text_size", (width, None)))
        report_label.bind(texture_size=lambda widget, texture_size: setattr(widget, "height", texture_size[1] + dp(16)))
        scroll.add_widget(report_label)
        actions = BoxLayout(size_hint_y=None, height=dp(52), spacing=dp(8))
        copy_result = Button(
            text="COPIER LE RÉSULTAT", background_color=(0.20, 0.45, 0.75, 1),
        )
        close = Button(text="FERMER", size_hint_y=None, height=dp(52))
        content.add_widget(scroll)
        actions.add_widget(copy_result)
        actions.add_widget(close)
        content.add_widget(actions)
        report_popup = Popup(
            title="Diagnostic export SQLite — sans sync", content=content,
            size_hint=(0.96, 0.94), auto_dismiss=False,
        )

        def copy_full_report(_inst):
            """Copie exactement le texte actuellement affiché, sans aucune autre action."""
            report_text = report_label.text or ""
            try:
                Clipboard.copy(report_text)
                copy_result.text = "RÉSULTAT COPIÉ"
            except Exception:
                # Le rapport reste inchangé si le fournisseur Android de presse-papiers échoue.
                copy_result.text = "COPIE INDISPONIBLE"

        copy_result.bind(on_press=copy_full_report)
        close.bind(on_press=report_popup.dismiss)
        self._sqlite_diagnostic_label = report_label
        self._sqlite_diagnostic_scroll = scroll
        self._sqlite_diagnostic_popup = report_popup
        self._refresh_sqlite_diagnostic_report()
        report_popup.open()

    def _refresh_sqlite_diagnostic_report(self):
        label = getattr(self, "_sqlite_diagnostic_label", None)
        if label is not None:
            label.text = "\n\n".join(self._sqlite_diagnostic_lines)
            Clock.schedule_once(
                lambda _dt: setattr(self._sqlite_diagnostic_scroll, "scroll_y", 0), 0,
            )

    def _record_sqlite_diagnostic_step(self, step, status, path, details=None):
        item = f"ÉTAPE : {step}\nRÉSULTAT : {status}\nCHEMIN : {path or '-'}"
        if details:
            item += "\n" + str(details)
        self._sqlite_diagnostic_lines.append(item)
        self._refresh_sqlite_diagnostic_report()

    def _finish_sqlite_diagnostic(self, conclusion):
        self._sqlite_diagnostic_lines.append(str(conclusion))
        self._sqlite_diagnostic_lines.append("Aucune correction n’a été appliquée par cette APK de diagnostic.")
        self._refresh_sqlite_diagnostic_report()

    def _append_readonly_engine_error(self, title, error):
        """Affiche les détails d’une exception moteur sans masquer le traceback."""
        self._sqlite_diagnostic_lines.append(
            f"{title}\n"
            f"étape exacte : {error.get('step', '-') }\n"
            f"exception_type : {error.get('exception_type')}\n"
            f"exception_str : {error.get('exception_str')}\n"
            f"exception_repr : {error.get('exception_repr')}\n"
            f"errno : {error.get('errno')}\n"
            f"strerror : {error.get('strerror')}\n"
            f"filename : {error.get('filename')}\n"
            f"source_path : {error.get('source_path')}\n"
            f"traceback :\n{error.get('traceback')}"
        )
        self._refresh_sqlite_diagnostic_report()

    def _append_readonly_database_report(self, label, report):
        """Ajoute les seules informations obtenues par lecture à une fenêtre de diagnostic."""
        files = report.get("files", {})
        lines = [f"BASE {label}", f"CHEMIN DB : {report.get('path')}"]
        for key, title in (("db", "DB"), ("wal", "WAL"), ("shm", "SHM")):
            info = files.get(key, {})
            lines.extend((
                f"{title} présent : {'OUI' if info.get('exists') else 'NON'}",
                f"{title} taille : {info.get('size_bytes', 0)} octets",
                f"{title} SHA-256 : {info.get('sha256') or '-'}",
            ))
        if report.get("error"):
            self._record_sqlite_diagnostic_step(
                f"Lecture SQLite {label}", "ÉCHEC", report.get("path"), "\n".join(lines)
            )
            self._append_readonly_engine_error(f"ERREUR {label}", report["error"])
            return
        lines.extend((
            "SQLite ouverture : mode=ro",
            f"query_only : {report.get('query_only')}",
            f"transactions : {report.get('transactions_count') if report.get('transactions_table_status') == 'PRÉSENTE' else 'TABLE ABSENTE'}",
            f"pending_sync : {report.get('pending_sync_count') if report.get('pending_sync_table_status') == 'PRÉSENTE' else 'TABLE ABSENTE'}",
            f"UUID présent dans transactions : {report.get('uuid_in_transactions')}",
            f"statut_local : {report.get('statut_local')}",
            f"UUID présent dans pending_sync : {report.get('uuid_in_pending_sync')}",
            f"UUID présent dans jointure pilote : {report.get('uuid_in_pilot_join')}",
        ))
        uuid_row = report.get("uuid_transaction_row")
        lines.append(f"ligne UUID : {uuid_row if uuid_row is not None else '-'}")
        for table in report.get("tables", []):
            lines.append(f"TABLE : {table.get('name')}")
            lines.append(f"STATUT : {table.get('status')}")
            if table.get("status") == "PRÉSENTE":
                lines.append(f"COLONNES : {', '.join(table.get('columns', [])) or '-'}")
                lines.append(f"NOMBRE DE LIGNES : {table.get('row_count')}")
            elif table.get("error"):
                error = table["error"]
                lines.extend((
                    f"étape exacte : {error.get('step')}",
                    f"exception_type : {error.get('exception_type')}",
                    f"exception_str : {error.get('exception_str')}",
                    f"exception_repr : {error.get('exception_repr')}",
                    f"errno : {error.get('errno')}",
                    f"strerror : {error.get('strerror')}",
                    f"filename : {error.get('filename')}",
                    f"source_path : {error.get('source_path')}",
                    f"traceback :\n{error.get('traceback')}",
                ))
        for table_name, status_key in (("transactions", "transactions_table_status"), ("pending_sync", "pending_sync_table_status")):
            if report.get(status_key) == "TABLE ABSENTE":
                lines.extend((f"TABLE : {table_name}", "STATUT : TABLE ABSENTE"))
        self._record_sqlite_diagnostic_step(
            f"Lecture SQLite {label}", "OK", report.get("path"), "\n".join(lines)
        )

    def start_readonly_database_comparison(self):
        """Lance seulement la comparaison interne/externe avec SQLite mode=ro."""
        self._open_sqlite_diagnostic_report()
        self._sqlite_diagnostic_lines.append(
            "DIAGNOSTIC COMPARATIF READ-ONLY : aucune copie, aucun backup(), aucune écriture SQLite."
        )
        self._refresh_sqlite_diagnostic_report()
        Clock.schedule_once(lambda _dt: self._run_readonly_database_comparison(), 0)

    def _run_readonly_database_comparison(self):
        internal_path = os.path.join(INTERNAL_DIR, "stock_quincaillerie.db")
        external_path = os.path.join(EXTERNAL_DIR, "stock_quincaillerie.db")
        result = compare_sqlite_databases_readonly(
            internal_path, external_path, DIAGNOSTIC_PILOT_UUID,
        )
        self._append_readonly_database_report("INTERNE RUNTIME", result["internal"])
        self._append_readonly_database_report("EXTERNE", result["external"])
        comparison = result.get("comparison", {})
        if not comparison.get("available"):
            self._record_sqlite_diagnostic_step(
                "COMPARAISON INTERNE/EXTERNE", "ÉCHEC", "-",
                "Comparaison indisponible car au moins une lecture readonly a échoué.",
            )
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : au moins une base n’a pas pu être lue en mode readonly.")
            return
        details = "\n".join(
            f"{key} : {'IDENTIQUE' if value else 'DIFFÉRENT'}"
            for key, value in comparison.items() if key != "available"
        )
        self._record_sqlite_diagnostic_step(
            "COMPARAISON INTERNE/EXTERNE", "OK", "interne ↔ externe", details,
        )
        self._finish_sqlite_diagnostic("CAUSE NON CONFIRMÉE")

    def _read_external_storage_state(self):
        """Utilise explicitement Activity.checkSelfPermission pour Android 9."""
        from jnius import autoclass
        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        ManifestPermission = autoclass("android.Manifest$permission")
        PackageManager = autoclass("android.content.pm.PackageManager")
        activity = PythonActivity.mActivity
        value = activity.checkSelfPermission(ManifestPermission.READ_EXTERNAL_STORAGE)
        return int(value) == int(PackageManager.PERMISSION_GRANTED)

    def _test_fileprovider_only(self, copy_path):
        """Teste seulement l’URI FileProvider ; aucun partage et aucune transmission ne sont lancés."""
        from jnius import autoclass
        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        JavaFile = autoclass("java.io.File")
        FileProvider = autoclass("androidx.core.content.FileProvider")
        activity = PythonActivity.mActivity
        authority = activity.getPackageName() + ".fanevaexportprovider"
        uri = FileProvider.getUriForFile(activity, authority, JavaFile(copy_path))
        return str(uri)

    def start_sqlite_export_permission_diagnostic(self):
        """Déclenche exclusivement la copie binaire readonly de la base interne runtime."""
        self._open_sqlite_diagnostic_report()
        self._sqlite_diagnostic_source = os.path.join(INTERNAL_DIR, "stock_quincaillerie.db")
        self._sqlite_diagnostic_root = ""
        self._sqlite_diagnostic_destination = ""
        Clock.schedule_once(lambda _dt: self._run_sqlite_export_pre_permission_steps(), 0)

    def _run_sqlite_export_pre_permission_steps(self):
        source_path = self._sqlite_diagnostic_source
        destination_path = ""
        try:
            app_root = resolve_database_copy_app_root(strict=True)
            self._sqlite_diagnostic_root = app_root
            self._record_sqlite_diagnostic_step(
                "1 — getExternalFilesDir(null)", "OK", app_root,
                "Répertoire app-specific résolu.",
            )
        except Exception as exc:
            self._record_sqlite_diagnostic_step(
                "1 — getExternalFilesDir(null)", "ÉCHEC", "-",
                self._format_diagnostic_exception(exc, source_path, destination_path),
            )
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : résolution getExternalFilesDir(null) impossible.")
            return
        try:
            destination_dir = os.path.join(app_root, "FANEVA_DB_COMPARISON_EXPORT")
            os.makedirs(destination_dir, exist_ok=True)
            if not os.path.isdir(destination_dir):
                raise OSError("Répertoire destination non créé", destination_dir)
            self._sqlite_diagnostic_destination = os.path.join(
                destination_dir, "diagnostic_copie_interne_%s.sqlite" % os.urandom(8).hex(),
            )
            destination_path = self._sqlite_diagnostic_destination
            self._record_sqlite_diagnostic_step(
                "2 — Création du répertoire destination", "OK", destination_dir,
                "FANEVA_DB_COMPARISON_EXPORT disponible.",
            )
        except Exception as exc:
            self._record_sqlite_diagnostic_step(
                "2 — Création du répertoire destination", "ÉCHEC", destination_path or app_root,
                self._format_diagnostic_exception(exc, source_path, destination_path),
            )
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : écriture impossible dans le répertoire app-specific destination.")
            return
        try:
            probe = os.path.join(destination_dir, ".faneva_permission_write_probe")
            with open(probe, "wb") as handle:
                handle.write(b"FANEVA-DIAGNOSTIC")
            if not os.path.isfile(probe):
                raise OSError("Fichier test destination absent", probe)
            os.remove(probe)
            self._record_sqlite_diagnostic_step(
                "3 — Création d’un fichier test", "OK", destination_dir,
                "Écriture et suppression du fichier test réussies.",
            )
        except Exception as exc:
            self._record_sqlite_diagnostic_step(
                "3 — Création d’un fichier test", "ÉCHEC", destination_dir,
                self._format_diagnostic_exception(exc, source_path, destination_path),
            )
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : le répertoire app-specific existe mais son écriture échoue.")
            return
        try:
            with open(source_path, "rb") as source_file:
                source_file.read(1)
            self._record_sqlite_diagnostic_step(
                "4 — Accès en lecture à la base interne runtime", "OK", source_path,
                "Lecture binaire de contrôle réussie.",
            )
            self._sqlite_diagnostic_source_read_failed = False
        except Exception as exc:
            self._record_sqlite_diagnostic_step(
                "4 — Accès en lecture à la base interne runtime", "ÉCHEC", source_path,
                self._format_diagnostic_exception(exc, source_path, destination_path),
            )
            self._sqlite_diagnostic_source_read_failed = True
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : lecture binaire de la base interne runtime impossible.")
            return
        Clock.schedule_once(lambda _dt: self._run_sqlite_backup_diagnostic(), 0)

    def _on_read_external_storage_permission_result(self, permissions, grants):
        granted = bool(grants and grants[0])
        source_path = self._sqlite_diagnostic_source
        if granted:
            self._record_sqlite_diagnostic_step(
                "6 bis — Résultat demande runtime", "OK", source_path,
                "READ_EXTERNAL_STORAGE = GRANTED.",
            )
            if self._sqlite_diagnostic_external_read_failed:
                try:
                    with open(source_path, "rb") as source_file:
                        source_file.read(1)
                    self._record_sqlite_diagnostic_step(
                        "4 bis — Nouvelle lecture après permission", "OK", source_path,
                        "Lecture externe réussie après autorisation runtime.",
                    )
                except Exception as exc:
                    self._record_sqlite_diagnostic_step(
                        "4 bis — Nouvelle lecture après permission", "ÉCHEC", source_path,
                        self._format_diagnostic_exception(exc, source_path, self._sqlite_diagnostic_destination),
                    )
                    self._finish_sqlite_diagnostic("CAUSE NON CONFIRMÉE")
                    return
            Clock.schedule_once(lambda _dt: self._run_sqlite_backup_diagnostic(), 0)
        else:
            self._record_sqlite_diagnostic_step(
                "6 bis — Résultat demande runtime", "ÉCHEC", source_path,
                "READ_EXTERNAL_STORAGE = DENIED.",
            )
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : READ_EXTERNAL_STORAGE = DENIED.")

    def _run_sqlite_backup_diagnostic(self):
        source_path = self._sqlite_diagnostic_source
        destination_path = self._sqlite_diagnostic_destination
        if getattr(self, "_sqlite_diagnostic_source_read_failed", False):
            self._finish_sqlite_diagnostic("CAUSE NON CONFIRMÉE")
            return
        if os.path.exists(destination_path):
            self._record_sqlite_diagnostic_step(
                "Précondition destination diagnostic", "ÉCHEC", destination_path,
                "La destination doit être nouvelle ; aucun fichier existant ne sera supprimé ou remplacé.",
            )
            self._finish_sqlite_diagnostic("CAUSE CONFIRMÉE : la destination de copie existe déjà et ne sera pas modifiée.")
            return
        def display_engine_step(item):
            detail = item.get("details")
            self._record_sqlite_diagnostic_step(item.get("step", "-"), item.get("status", "-"), item.get("path", "-"), detail)
        result = run_sqlite_export_step_diagnostic(
            source_path, destination_path, "base-externe", step_callback=display_engine_step,
        )
        if not result.get("completed"):
            error = result.get("error", {})
            self._sqlite_diagnostic_lines.append(
                "ERREUR DÉTAILLÉE\n"
                f"Exception type : {error.get('exception_type')}\n"
                f"str(exc) : {error.get('exception_str')}\n"
                f"repr(exc) : {error.get('exception_repr')}\n"
                f"errno : {error.get('errno')}\n"
                f"strerror : {error.get('strerror')}\n"
                f"filename : {error.get('filename')}\n"
                f"path source : {error.get('source_path')}\n"
                f"path destination : {error.get('destination_path')}\n"
                f"traceback :\n{error.get('traceback')}"
            )
            self._refresh_sqlite_diagnostic_report()
            failed_steps = [item.get("step") for item in result.get("steps", []) if item.get("status") == "ÉCHEC"]
            failed_name = failed_steps[-1] if failed_steps else "instruction SQLite non identifiée"
            self._finish_sqlite_diagnostic(f"CAUSE CONFIRMÉE : échec à {failed_name}.")
            return
        self._record_sqlite_diagnostic_step(
            "RÉSULTAT FINAL", "COPIE VALIDÉE", destination_path,
            "Copie binaire locale validée ; aucune autre opération n’est exécutée.",
        )
        self._finish_sqlite_diagnostic("CAUSE NON CONFIRMÉE")

    def _pilot_candidate_paths(self):
        """Liste les fichiers candidats sans les créer ni les ouvrir en écriture."""
        paths = []
        for key, info in MAGASINS.items():
            external_path = info.get("db")
            internal_path = os.path.join(INTERNAL_DIR, f"stock_{key}.db")
            for location, candidate in (("externe", external_path), ("interne", internal_path)):
                exists = bool(candidate and os.path.isfile(candidate))
                paths.append(
                    f"{key}/{location}: {os.path.abspath(candidate) if candidate else '-'} | "
                    f"{'PRÉSENT' if exists else 'absent'}"
                )
        return paths

    def _show_runtime_database_report(self, report, magasin_actif):
        """Présente le résultat d’un diagnostic déjà calculé, sans action complémentaire."""
        same_copy = report.get("matches_expected_sha256")
        if same_copy is True:
            comparison = "BASE RUNTIME IDENTIQUE À LA COPIE VALIDÉE"
        elif same_copy is False:
            comparison = "BASE RUNTIME DIFFÉRENTE DE LA COPIE VALIDÉE"
        else:
            comparison = "BASE RUNTIME INDISPONIBLE — COMPARAISON IMPOSSIBLE"
        tx_rows = report.get("transaction_rows", [])
        pending_rows = report.get("pending_sync_rows", [])
        result_text = (
            "MODE : SQLITE mode=ro + PRAGMA query_only — aucun réseau appelé\n"
            f"Magasin actuellement sélectionné : {magasin_actif or '-'}\n"
            f"DB_PATH logique : {DB_PATH or '-'}\n"
            f"Fichier ouvert : {report.get('filename', '-')}\n"
            f"Chemin absolu réel : {report.get('real_path', '-')}\n"
            f"Fichier présent : {'OUI' if report.get('exists') else 'NON'}\n"
            f"Taille du fichier : {report.get('size_bytes', '-')} octets\n"
            f"SHA-256 runtime : {report.get('sha256', '-')}\n"
            f"SHA-256 copie validée : {DIAGNOSTIC_VALIDATED_DB_SHA256}\n"
            f"COMPARAISON : {comparison}\n\n"
            f"Fichier -wal : {'PRÉSENT' if report.get('wal', {}).get('exists') else 'absent'}\n"
            f"Fichier -shm : {'PRÉSENT' if report.get('shm', {}).get('exists') else 'absent'}\n"
            f"Lignes transactions : {report.get('transactions_count', '-')}\n"
            f"Lignes pending_sync : {report.get('pending_sync_count', '-')}\n"
            f"Lignes jointure statut_local='LOCAL' : {report.get('pilot_join_total', '-')}\n\n"
            f"UUID vérifié : {DIAGNOSTIC_PILOT_UUID}\n"
            f"Présent dans transactions : {'OUI' if tx_rows else 'NON'}\n"
            f"statut_local : {tx_rows[0].get('statut_local') if tx_rows else '-'}\n"
            f"Présent dans pending_sync : {'OUI' if pending_rows else 'NON'}\n"
            f"Présent dans jointure pilote : {'OUI' if report.get('pilot_join_rows') else 'NON'}\n\n"
            "Bases candidates :\n" + "\n".join(self._pilot_candidate_paths())
        )
        if report.get("error"):
            result_text += "\n\nErreur : " + report["error"]
        output = BoxLayout(orientation="vertical", padding=10, spacing=8)
        readonly_report = TextInput(text=result_text, readonly=True, multiline=True, font_size=14)
        close = Button(text="FERMER", size_hint_y=None, height=52)
        output.add_widget(readonly_report)
        output.add_widget(close)
        report_popup = Popup(
            title="Résultat diagnostic SQLite — sans sync",
            content=output, size_hint=(0.96, 0.92), auto_dismiss=False,
        )
        close.bind(on_press=report_popup.dismiss)
        report_popup.open()

    def run_runtime_database_diagnostic(self, magasin_key):
        """Résout le même DB_PATH que le pilote, puis l’inspecte en lecture seule."""
        global MAGASIN_ACTIF
        try:
            # Ne pas appeler _prefill_*, init_db() ou get_db_connection() : l’APK
            # diagnostic ne doit créer ni modifier une base SQLite.
            resolve_db_path(magasin_key)
            report = diagnose_pilot_database(
                DB_PATH, DIAGNOSTIC_PILOT_UUID, DIAGNOSTIC_VALIDATED_DB_SHA256,
            )
            log(
                "DIAG RUNTIME READONLY | magasin=%s | path=%s | sha256=%s | tx=%s | pending=%s | join_total=%s | uuid_join=%s"
                % (
                    magasin_key, report.get("real_path"), report.get("sha256"),
                    report.get("transactions_count"), report.get("pending_sync_count"),
                    report.get("pilot_join_total"), len(report.get("pilot_join_rows", [])),
                )
            )
            MAGASIN_ACTIF = magasin_key
            self._show_runtime_database_report(report, magasin_key)
        except Exception as exc:
            log_error("run_runtime_database_diagnostic", exc)
            popup("Diagnostic SQLite", "Diagnostic local impossible : " + type(exc).__name__)

    def _share_database_copy(self, copy_path):
        """Partage uniquement une copie app-specific déjà créée, jamais la base source."""
        try:
            if not copy_path or not os.path.isfile(copy_path) or os.path.getsize(copy_path) <= 0:
                raise FileNotFoundError("Copie SQLite inexistante ou vide")
            from jnius import autoclass, cast
            PythonActivity = autoclass("org.kivy.android.PythonActivity")
            Intent = autoclass("android.content.Intent")
            JavaFile = autoclass("java.io.File")
            FileProvider = autoclass("androidx.core.content.FileProvider")
            activity = PythonActivity.mActivity
            authority = activity.getPackageName() + ".fanevaexportprovider"
            uri = FileProvider.getUriForFile(activity, authority, JavaFile(copy_path))
            intent = Intent(Intent.ACTION_SEND)
            intent.setType("application/vnd.sqlite3")
            intent.putExtra(Intent.EXTRA_STREAM, cast("android.os.Parcelable", uri))
            intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            activity.startActivity(Intent.createChooser(intent, "Partager la copie SQLite FANEVA"))
        except Exception as exc:
            log_error("share_database_copy", exc)
            popup("Partage indisponible", "La copie reste disponible dans le chemin affiché. Erreur : " + type(exc).__name__)

    def _show_database_copy_export_result(self, result):
        """Affiche seulement les deux copies créées et leurs métadonnées, sans comparaison."""
        external = result["external"]
        internal = result["internal"]
        text = (
            "MODE : SQLite source mode=ro + query_only + conn.backup()\n"
            "AUCUNE COMPARAISON, AUCUNE SYNCHRONISATION, AUCUNE MODIFICATION SOURCE\n\n"
            "COPIE 1 — BASE EXTERNE\n"
            f"Source : {external['source_path']}\n"
            f"Copie : {external['destination_path']}\n"
            f"Taille copie : {external['destination_size_bytes']} octets\n"
            f"SHA-256 copie : {external['destination_sha256']}\n"
            f"Source inchangée : {'OUI' if external['source_unchanged'] else 'NON'}\n\n"
            "COPIE 2 — BASE INTERNE\n"
            f"Source : {internal['source_path']}\n"
            f"Copie : {internal['destination_path']}\n"
            f"Taille copie : {internal['destination_size_bytes']} octets\n"
            f"SHA-256 copie : {internal['destination_sha256']}\n"
            f"Source inchangée : {'OUI' if internal['source_unchanged'] else 'NON'}\n\n"
            "Utilisez PARTAGER EXTERNE puis PARTAGER INTERNE pour joindre les deux copies à la discussion."
        )
        root = BoxLayout(orientation="vertical", padding=10, spacing=8)
        report = TextInput(text=text, readonly=True, multiline=True, font_size=14)
        actions = GridLayout(cols=2, size_hint_y=None, height=110, spacing=8)
        share_external = Button(text="PARTAGER EXTERNE", background_color=(0.20, 0.56, 0.35, 1))
        share_internal = Button(text="PARTAGER INTERNE", background_color=(0.20, 0.45, 0.75, 1))
        close = Button(text="FERMER", background_color=(0.45, 0.30, 0.30, 1))
        popup_window = Popup(title="Copies SQLite prêtes — sans comparaison", content=root,
                             size_hint=(0.96, 0.92), auto_dismiss=False)
        share_external.bind(on_press=lambda _button: self._share_database_copy(external["destination_path"]))
        share_internal.bind(on_press=lambda _button: self._share_database_copy(internal["destination_path"]))
        close.bind(on_press=popup_window.dismiss)
        for button in (share_external, share_internal, close):
            actions.add_widget(button)
        root.add_widget(report)
        root.add_widget(actions)
        popup_window.open()

    def export_quincaillerie_database_copies(self):
        """Exporte deux copies nommées sans ouvrir ni modifier une connexion métier."""
        try:
            external_source = os.path.join(EXTERNAL_DIR, "stock_quincaillerie.db")
            internal_source = os.path.join(INTERNAL_DIR, "stock_quincaillerie.db")
            export_dir = get_database_copy_export_dir()
            result = export_quincaillerie_database_copies(
                external_source, internal_source, export_dir,
            )
            log("EXPORT 2 COPIES READONLY | externe=%s | interne=%s" % (
                result["external"]["destination_path"], result["internal"]["destination_path"],
            ))
            self._show_database_copy_export_result(result)
        except Exception as exc:
            log_error("export_quincaillerie_database_copies", exc)
            popup(
                "Export de copies",
                "Aucune source n’a été modifiée. Export impossible : "
                + type(exc).__name__ + " — vérifiez le chemin destination affiché dans les logs.",
            )

    def show_choix_magasin(self):
        log("show_choix_magasin()")
        try:
            self.clear()
            # --- FANEVA SYSTEM HYBRID : magasins depuis la base ---
            if has_hybrid_module():
                try:
                    get_magasins_from_db()
                except Exception as e:
                    log(f"HYBRID: magasins par defaut: {e}")
            b = BoxLayout(orientation="vertical", padding=30, spacing=20)

            _logo = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "logo.jpg")
            b.add_widget(Image(source=_logo, size_hint_y=None, height=dp(120)))
            b.add_widget(Label(text="KDK SYSTEM v" + (HYBRID_VERSION if has_hybrid_module() else "6.2.2"), font_size=40, bold=True, color=(0.2,0.6,0.9,1)))
            b.add_widget(Label(text="Multi-Magasin - HYBRID", font_size=28, color=(0.5,0.5,0.5,1)))
            b.add_widget(Label(text="Selectionnez votre magasin", font_size=26, color=(0.3,0.3,0.3,1)))
            b.add_widget(Label(size_hint_y=0.1))

            for cle in MAGASINS:
                info = MAGASINS[cle]
                btn = Button(
                    text=info["nom"].upper(), font_size=30, bold=True,
                    background_color=info["couleur"],
                    size_hint_y=None, height=dp(80)
                )
                btn.bind(on_press=lambda x, c=cle: self.choisir_magasin(c))
                b.add_widget(btn)

            b.add_widget(Label(size_hint_y=0.2))
            b.add_widget(Label(text=f"v{HYBRID_VERSION if has_hybrid_module() else '6.2.2'} - HYBRID Multi-Admin-Sync", font_size=22, color=(0.6,0.6,0.6,1)))

            self.root.add_widget(b)
            log("show_choix_magasin() OK")
        except Exception as e:
            log_error("show_choix_magasin", e)

    def choisir_magasin(self, magasin_key):
        log(f"choisir_magasin({magasin_key})")
        # v1.4.9.16: vidage automatique du panier memoire au changement de magasin effectif
        if getattr(self, "v_cart", None):
            self.v_cart = []
        self.magasin = magasin_key
        self.magasin_cle = magasin_key
        _prefill_embedded_db(magasin_key)
        resolve_db_path(magasin_key)
        _prefill_chosen_db(magasin_key)
        init_db()
        # --- FANEVA SYSTEM HYBRID ---
        if has_hybrid_module():
            try:
                with get_db_connection() as conn:
                    init_hybrid(conn)
                get_magasins_from_db()
                self.magasin_cle = magasin_key
                maginfo = MAGASINS.get(magasin_key)
                if maginfo and "id" in maginfo:
                    MAGASIN_ACTIF_ID = maginfo["id"]
                    MAGASIN_CLE = magasin_key
                log(f"HYBRID: magasins depuis base: {list(MAGASINS.keys())}")
            except Exception as e:
                log_error("choisir_magasin hybrid", e)
        self.show_login()

    def get_magasin_info(self):
        cle = getattr(self, "magasin_cle", None) or self.magasin
        return MAGASINS.get(cle, {"nom": "Inconnu", "couleur": (0.5,0.5,0.5,1), "couleur_light": (0.9,0.9,0.9,1)})

    # ---------- LOGIN ----------
    def show_login(self):
        log("show_login()")
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=30, spacing=15)

            b.add_widget(Label(text="KDK SYSTEM v6.2.2", font_size=36, bold=True, color=(0.2,0.6,0.9,1)))
            b.add_widget(Label(text=f"Magasin: {mag['nom']}", font_size=28, bold=True, color=mag['couleur']))
            b.add_widget(Label(text="Gestion Unicite Dettes - Étape 3", font_size=24, color=(0.5,0.5,0.5,1)))
            b.add_widget(Label(size_hint_y=0.1))

            self.in_user = TextInput(hint_text="Utilisateur", multiline=False, font_size=26, size_hint_y=None, height=50)
            self.in_pass = TextInput(hint_text="Mot de passe", password=True, multiline=False, font_size=26, size_hint_y=None, height=50)
            b.add_widget(self.in_user)
            b.add_widget(self.in_pass)

            btn = Button(text="CONNEXION", font_size=24, bold=True, background_color=(0.2,0.6,0.9,1), size_hint_y=None, height=55)
            btn.bind(on_press=self.do_login)
            b.add_widget(btn)

            self.lbl_msg = Label(text="", color=(0.9,0.3,0.3,1), font_size=26)
            b.add_widget(self.lbl_msg)

            b.add_widget(Label(text="", font_size=24, color=(0.6,0.6,0.6,1), size_hint_y=0.1))

            btn_retour = Button(text="Changer de magasin", font_size=22, background_color=(0.5,0.5,0.5,1), size_hint_y=None, height=40)
            btn_retour.bind(on_press=lambda x: self.show_choix_magasin())
            b.add_widget(btn_retour)

            self.root.add_widget(b)
            log("show_login() OK")
        except Exception as e:
            log_error("show_login", e)

    @safe_sqlite
    def do_login(self, inst):
        log("do_login()")
        u = self.in_user.text.strip()
        p = self.in_pass.text.strip()
        if not u or not p:
            self.lbl_msg.text = "Remplir tous les champs"
            return
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT username, role, nom_complet FROM utilisateurs WHERE username=? AND password_hash=? AND actif=1",
                       (u, hash_pwd(p)))
            user = cur.fetchone()
        if user:
            self.user = {"username": user[0], "role": user[1], "nom": user[2]}
            log(f"Login OK: {self.user['nom']} ({self.user['role']}) - Magasin: {self.magasin}")
            # --- FANEVA SYSTEM HYBRID : session persistante + compat utilisateurs_hybrid ---
            if has_hybrid_module():
                try:
                    with get_db_connection() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT id, role FROM utilisateurs_hybrid WHERE username=? AND actif=1", (u,))
                        uh = cur.fetchone()
                        if uh:
                            # Le mot de passe legacy sha256 correspond : on migre vers utilisateurs_hybrid
                            self.user["user_id"] = uh[0]
                            self.user["role"] = uh[1]
                            set_hybrid_session(conn, uh[0], u, uh[1])
                            log(f"HYBRID: session hybride demarree pour {u} ({uh[1]})")
                except Exception as e:
                    log(f"HYBRID: session hybride impossible: {e}")
            self.show_dashboard()
        else:
            # --- FANEVA SYSTEM HYBRID : tenter les utilisateurs_hybrid (PBKDF2) ---
            if has_hybrid_module():
                try:
                    with get_db_connection() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT id, username, nom_complet, role FROM utilisateurs_hybrid WHERE username=? AND actif=1", (u,))
                        uh = cur.fetchone()
                        if uh and check_pwd(p, uh[3]):
                            self.user = {"user_id": uh[0], "username": uh[1], "role": uh[3], "nom": uh[2] or uh[1]}
                            set_hybrid_session(conn, uh[0], uh[1], uh[3])
                            log(f"Login HYBRID OK: {self.user['nom']} ({uh[3]}) - Magasin: {self.magasin}")
                            self.show_dashboard()
                            return
                except Exception as e:
                    log(f"HYBRID: login hybride echoue: {e}")
            self.lbl_msg.text = "Identifiants incorrects"

    # ---------- DASHBOARD ----------
    def show_dashboard(self):
        log("show_dashboard()")
        try:
            self.clear()
            mag = self.get_magasin_info()
            is_admin = self.user and is_user_admin_hybrid(self.user.get("role"))
            b = BoxLayout(orientation="vertical", padding=10, spacing=10)

            h = BoxLayout(size_hint_y=None, height=50)
            h.add_widget(Label(text="KDK SYSTEM", font_size=26, bold=True, color=(0.2,0.6,0.9,1), size_hint_x=0.35))
            h.add_widget(Label(text=mag["nom"], font_size=24, bold=True, color=mag["couleur"], size_hint_x=0.25))
            role_text = f"{self.user['nom']} ({self.user['role']})" if self.user else ""
            lbl_user = Label(text=role_text, font_size=20, color=(0.5,0.5,0.5,1), size_hint_x=0.22)
            h.add_widget(lbl_user)
            # --- FANEVA SYSTEM HYBRID : statut synchronisation ---
            if has_hybrid_module() and is_user_admin_hybrid(self.user.get("role")):
                self.lbl_sync = Label(text="HYBRID v" + str(HYBRID_VERSION), font_size=18, color=(0.5,0.5,0.5,1), size_hint_x=0.18)
            else:
                self.lbl_sync = Label(text="", font_size=18, color=(0.5,0.5,0.5,1), size_hint_x=0.18)
            h.add_widget(self.lbl_sync)
            b.add_widget(h)

            stats = GridLayout(cols=2, spacing=10, size_hint_y=None, height=dp(300 if is_admin else 220))
            self.st_prod = Label(text="0", font_size=26, bold=True, color=(1,1,1,1))
            self.st_vente = Label(text="0 Ar", font_size=26, bold=True, color=(1,1,1,1))
            self.st_val_vente = Label(text="0 Ar", font_size=26, bold=True, color=(1,1,1,1))
            self.st_alert = Label(text="0", font_size=26, bold=True, color=(1,1,1,1))
            self.st_dette = Label(text="0 Ar", font_size=26, bold=True, color=(1,1,1,1))

            stats.add_widget(self._stat_box("PRODUITS", self.st_prod, mag["couleur"]))
            stats.add_widget(self._stat_box("VENTES JOUR", self.st_vente, (0.2,0.8,0.4,1)))

            if is_admin:
                self.st_val_achat = Label(text="0 Ar", font_size=26, bold=True, color=(1,1,1,1))
                self.st_benef = Label(text="0 Ar", font_size=26, bold=True, color=(1,1,1,1))
                stats.add_widget(self._stat_box("VALEUR ACHAT", self.st_val_achat, (0.8,0.4,0.2,1)))

            stats.add_widget(self._stat_box("VALEUR VENTE", self.st_val_vente, (0.6,0.3,0.8,1)))
            stats.add_widget(self._stat_box("VIDE", self.st_alert, (1,0.7,0.2,1)))

            if is_admin:
                stats.add_widget(self._stat_box("BENEFICE JOUR", self.st_benef, (0.2,0.8,0.4,1)))

            stats.add_widget(self._stat_box("DETTES", self.st_dette, (0.95,0.3,0.3,1)))
            b.add_widget(stats)

            scroll = ScrollView()
            menu = GridLayout(cols=2, spacing=12, padding=12, size_hint_y=None)
            menu.bind(minimum_height=menu.setter("height"))

            items = [
                ("STOCK", self.show_stock, mag["couleur"]),
                ("VENTE", self.show_vente, (0.2,0.8,0.4,1)),
            ]

            if is_admin:
                items.append(("RAPPORTS", self.show_rapports, (0.6,0.4,0.8,1)))

            items.extend([
                ("DETTES", self.show_dettes, (0.95,0.3,0.3,1)),
                ("PAIEMENT", self.show_paiement, (0.9,0.5,0.2,1)),
            ])

            # Assistant IA (READ-ONLY) — disponible pour admin et vendeur
            items.append(("🤖 FANEVA IA", self.show_faneva_ia, (0.15, 0.55, 0.85, 1)))

            if is_admin:
                items.extend([
                    ("BACKUP", self.show_backup, (0.5,0.5,0.9,1)),
                    ("PARAMETRES", self.show_params, (0.5,0.5,0.5,1)),
                ])

            # --- FANEVA SYSTEM HYBRID : menu synchronisation (admin) ---
            if is_admin and has_hybrid_module():
                items.extend([
                    ("SYNCHRONISATION", self.show_sync, (0.2,0.7,0.7,1)),
                    ("GESTION MAGASINS", self.show_gestion_magasins, (0.7,0.55,0.2,1)),
                ])

            for text, func, color in items:
                log(f"Bouton: {text}")
                btn = Button(text=text, font_size=26, bold=True, background_color=color, size_hint_y=None, height=dp(70))
                def make_callback(f, name):
                    def callback(instance):
                        log(f"CLICK: {name}")
                        try:
                            f(instance)
                        except Exception as e:
                            log_error(name, e)
                            popup("Erreur", f"{name}: {str(e)}")
                    return callback
                btn.bind(on_press=make_callback(func, text))
                menu.add_widget(btn)

            scroll.add_widget(menu)
            b.add_widget(scroll)

            btn_out = Button(text="DECONNEXION", font_size=24, background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_out.bind(on_press=lambda x: self.show_login())
            b.add_widget(btn_out)

            self.root.add_widget(b)
            self.update_stats()
            Clock.schedule_interval(self.update_stats, 30)
            # --- FANEVA SYSTEM HYBRID : sync automatique en arriere-plan ---
            if ENABLE_AUTO_SYNC and is_admin and has_hybrid_module():
                try:
                    Clock.schedule_interval(self.auto_sync, AUTO_SYNC_INTERVAL)
                except Exception as e:
                    log(f"HYBRID: auto_sync non programme: {e}")
            log("show_dashboard() OK")
        except Exception as e:
            log_error("show_dashboard", e)

    def _stat_box(self, label, value_lbl, color):
        b = BoxLayout(orientation="vertical", padding=10)
        with b.canvas.before:
            Color(*color)
            RoundedRectangle(pos=b.pos, size=b.size, radius=[12])
        b.bind(pos=lambda o,v: setattr(o.canvas.before.children[-1],"pos",v))
        b.bind(size=lambda o,v: setattr(o.canvas.before.children[-1],"size",v))
        b.add_widget(Label(text=label, font_size=22, color=(1,1,1,0.8)))
        b.add_widget(value_lbl)
        return b

    def update_stats(self, *args):
        """Programme les statistiques hors thread UI; aucun calcul métier ni réseau en callback Kivy."""
        if getattr(self, "_stats_worker_active", False):
            return
        self._stats_worker_active = True
        self._stats_generation = getattr(self, "_stats_generation", 0) + 1
        generation = self._stats_generation
        role = self.user.get("role") if self.user else None
        is_admin = bool(role and is_user_admin_hybrid(role))
        threading.Thread(
            target=self._collect_stats_background,
            args=(generation, is_admin),
            daemon=True,
            name="faneva-stats",
        ).start()

    def _collect_stats_background(self, generation, is_admin):
        """Lit les statistiques et projections sur une connexion SQLite dédiée au worker."""
        snapshot = None
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM produits WHERE actif=1")
                product_count = cur.fetchone()[0]
                cur.execute("SELECT COALESCE(SUM(total),0) FROM ventes WHERE date(date_vente)=date('now','localtime')")
                sale_total = cur.fetchone()[0]
                cur.execute("SELECT hybrid_id, magasin_ref_id, stock, prix_achat, prix_vente FROM produits WHERE actif=1")
                projection_rows = cur.fetchall()
                canonical_stocks = canonical_display_stocks_batch(conn, [(r[0], r[1], r[2]) for r in projection_rows])
                cur.execute("SELECT COALESCE(SUM(reste),0) FROM dettes WHERE statut='ACTIF'")
                debt_total = cur.fetchone()[0]
                profit_total = None
                if is_admin:
                    cur.execute("SELECT COALESCE(SUM(benefice),0) FROM ventes WHERE date(date_vente)=date('now','localtime')")
                    profit_total = cur.fetchone()[0]
                snapshot = {
                    "products": product_count,
                    "sales": sale_total,
                    "purchase_value": sum(stock * row[3] for stock, row in zip(canonical_stocks, projection_rows)),
                    "sale_value": sum(stock * row[4] for stock, row in zip(canonical_stocks, projection_rows)),
                    "alerts": sum(1 for stock in canonical_stocks if stock <= 0),
                    "debt": debt_total,
                    "profit": profit_total,
                }
        except Exception as e:
            log(f"HYBRID stats background: {e}")
        Clock.schedule_once(lambda dt: self._apply_stats_snapshot(generation, snapshot), 0)

    def _apply_stats_snapshot(self, generation, snapshot):
        """Applique uniquement du rendu Kivy après le calcul asynchrone."""
        self._stats_worker_active = False
        if generation != getattr(self, "_stats_generation", 0) or not snapshot:
            return
        if hasattr(self, "st_prod"):
            self.st_prod.text = str(snapshot["products"])
        if hasattr(self, "st_vente"):
            self.st_vente.text = f"{fmt(snapshot['sales'])} Ar"
        if hasattr(self, "st_val_achat"):
            self.st_val_achat.text = f"{fmt(snapshot['purchase_value'])} Ar"
        if hasattr(self, "st_val_vente"):
            self.st_val_vente.text = f"{fmt(snapshot['sale_value'])} Ar"
        if hasattr(self, "st_alert"):
            self.st_alert.text = str(snapshot["alerts"])
        if hasattr(self, "st_dette"):
            self.st_dette.text = f"{fmt(snapshot['debt'])} Ar"
        if snapshot["profit"] is not None and hasattr(self, "st_benef"):
            self.st_benef.text = f"{fmt(snapshot['profit'])} Ar"
        self._schedule_connectivity_check(generation)

    def _schedule_connectivity_check(self, generation):
        """Exécute le ping indicatif en arrière-plan; il ne déclenche jamais `/sync`."""
        if not (self.user and is_user_admin_hybrid(self.user.get("role")) and has_hybrid_module()):
            return
        if getattr(self, "_connectivity_worker_active", False):
            return
        self._connectivity_worker_active = True
        threading.Thread(
            target=self._collect_connectivity_background,
            args=(generation,),
            daemon=True,
            name="faneva-connectivity",
        ).start()

    def _collect_connectivity_background(self, generation):
        connectivity, pending = "OFFLINE", 0
        try:
            with get_db_connection() as conn:
                pending = pending_count(conn)
            connectivity = detect_connectivity(SYNC_SERVER_URL)
        except Exception as e:
            log(f"HYBRID connectivity background: {e}")
        Clock.schedule_once(lambda dt: self._apply_connectivity_status(generation, connectivity, pending), 0)

    def _apply_connectivity_status(self, generation, connectivity, pending):
        self._connectivity_worker_active = False
        if generation != getattr(self, "_stats_generation", 0) or not hasattr(self, "lbl_sync"):
            return
        if connectivity == "INTERNET":
            self.lbl_sync.text = f"EN LIGNE - {pending} en attente"
            self.lbl_sync.color = (0.2, 0.7, 0.3, 1)
        elif connectivity == "LOCAL":
            self.lbl_sync.text = f"Wi-Fi/Hotspot - {pending} en attente"
            self.lbl_sync.color = (0.9, 0.6, 0.1, 1)
        else:
            self.lbl_sync.text = f"HORS LIGNE - {pending} en file"
            self.lbl_sync.color = (0.9, 0.3, 0.3, 1)

    def auto_sync(self, *args):
        """Tentative automatique de synchronisation en arriere-plan (admin)."""
        if not ENABLE_AUTO_SYNC:
            return
        try:
            with get_db_connection() as conn:
                n = len(normal_pending_transactions(conn))
                if n == 0:
                    return
                # Internet prioritaire, sinon local (Wi-Fi/Hotspot)
                connetivite = detect_connectivity(SYNC_SERVER_URL)
                if connetivite == "INTERNET":
                    res = sync_internet(conn, SYNC_SERVER_URL)
                    if res.envoyees > 0:
                        log(f"HYBRID auto_sync INTERNET: {res.envoyees} envoyees, {res.recues} recues")
                elif connetivite == "LOCAL":
                    log("HYBRID auto_sync: reseau local detecte, synchro locale disponible")
        except Exception as e:
            log(f"HYBRID auto_sync: {e}")

    # ---------- RAPPORT DIAGNOSTIC (LOCAL, SANS FICHIER NI EFFET MÉTIER) ----------
    def share_stock_diagnostic_report(self, _instance=None):
        """Prépare le rapport hors UI puis ouvre le menu Android de partage de texte."""
        self._start_stock_diagnostic_report_action("share")

    def copy_stock_diagnostic_report(self, _instance=None):
        """Prépare le rapport hors UI puis le copie dans le presse-papiers Android."""
        self._start_stock_diagnostic_report_action("copy")

    def _start_stock_diagnostic_report_action(self, action):
        """Lance uniquement la lecture du journal existant dans un worker unique."""
        if getattr(self, "_stock_diagnostic_report_worker_active", False):
            return
        self._stock_diagnostic_report_worker_active = True
        generation = getattr(self, "_stock_diagnostic_report_generation", 0) + 1
        self._stock_diagnostic_report_generation = generation
        threading.Thread(
            target=self._prepare_stock_diagnostic_report_action,
            args=(generation, action),
            daemon=True,
            name="faneva-stock-diagnostic-report",
        ).start()

    def _prepare_stock_diagnostic_report_action(self, generation, action):
        """Lit seulement kdk.log et fabrique du texte ; aucune table ou API n’est ouverte."""
        try:
            content, line_count = build_stock_diagnostic_share_report()
            result = {"content": content, "line_count": line_count, "error": None}
        except Exception as exc:
            result = {"content": "", "line_count": 0, "error": type(exc).__name__}
        Clock.schedule_once(lambda _dt: self._complete_stock_diagnostic_report_action(generation, action, result), 0)

    def _complete_stock_diagnostic_report_action(self, generation, action, result):
        """Exécute uniquement l’action Android demandée une fois le rapport disponible."""
        self._stock_diagnostic_report_worker_active = False
        if generation != getattr(self, "_stock_diagnostic_report_generation", 0):
            return
        if result.get("error"):
            popup("Rapport diagnostic", "Rapport non généré : " + result["error"])
            return
        if not result.get("line_count"):
            popup("Rapport diagnostic", "Aucun événement STOCK_TRACE disponible dans kdk.log.")
            return
        if action == "share":
            self._share_stock_diagnostic_report_text(result["content"])
        elif action == "copy":
            self._copy_stock_diagnostic_report_text(result["content"])

    def _share_stock_diagnostic_report_text(self, content):
        """Transmet le texte au menu Android ACTION_SEND, sans pièce jointe ni fichier."""
        try:
            from jnius import autoclass, cast
            PythonActivity = autoclass("org.kivy.android.PythonActivity")
            Intent = autoclass("android.content.Intent")
            JavaString = autoclass("java.lang.String")
            activity = PythonActivity.mActivity
            intent = Intent(Intent.ACTION_SEND)
            intent.setType("text/plain")
            intent.putExtra(Intent.EXTRA_TEXT, cast("java.lang.CharSequence", JavaString(content)))
            chooser_title = cast("java.lang.CharSequence", JavaString("Partager le rapport diagnostic FANEVA"))
            activity.startActivity(Intent.createChooser(intent, chooser_title))
        except Exception as exc:
            log_error("share_stock_diagnostic_report", exc)
            popup("Rapport diagnostic", "Partage Android indisponible : " + type(exc).__name__)

    def _copy_stock_diagnostic_report_text(self, content):
        """Copie uniquement le texte diagnostic dans le presse-papiers Android."""
        try:
            from jnius import autoclass
            PythonActivity = autoclass("org.kivy.android.PythonActivity")
            Context = autoclass("android.content.Context")
            ClipData = autoclass("android.content.ClipData")
            JavaString = autoclass("java.lang.String")
            activity = PythonActivity.mActivity
            clipboard = activity.getSystemService(Context.CLIPBOARD_SERVICE)
            clip = ClipData.newPlainText(JavaString("FANEVA diagnostic Stock"), JavaString(content))
            clipboard.setPrimaryClip(clip)
            popup("Rapport diagnostic", "Rapport copié dans le presse-papiers Android.")
        except Exception as exc:
            log_error("copy_stock_diagnostic_report", exc)
            popup("Rapport diagnostic", "Copie Android indisponible : " + type(exc).__name__)

    # ---------- FANEVA IA (Phase 2.2 — READ-ONLY assistant) ----------
    def _ia_ui_lang(self):
        """Langue UI par défaut (fr) — bascule si la dernière question était mg."""
        return getattr(self, "_ia_ui_lang_pref", "fr") or "fr"

    def _ia_provider(self):
        """Provider par défaut : indisponible (pas de clé embarquée, pas de réseau forcé)."""
        if not HAS_FANEVA_IA or UnavailableAIProvider is None:
            return None
        return UnavailableAIProvider("IA non configurée sur cet appareil")

    def _ia_db_factory(self):
        """Factory de connexion réservée au service IA (pas d'usage métier dans l'UI).

        Appelée uniquement par ``run_faneva_ia_question`` dans la couche service.
        L'écran UI ne doit jamais invoquer cette factory directement.
        """
        return get_db_connection()

    def _ia_session_kwargs(self):
        """Contexte session pour FanevaIA (rôle + magasin courant uniquement)."""
        role = None
        username = None
        user_id = None
        if self.user:
            role = self.user.get("role")
            username = self.user.get("username")
            user_id = self.user.get("user_id")
        magasin_id = None
        try:
            magasin_id = MAGASIN_ACTIF_ID
        except Exception:
            magasin_id = None
        if magasin_id is None:
            try:
                mag = self.get_magasin_info()
                magasin_id = mag.get("id") if mag else None
            except Exception:
                pass
        return {
            "magasin_id": magasin_id,
            "role": role,
            "username": username,
            "user_id": user_id,
        }

    def show_faneva_ia(self, inst=None):
        """Écran Assistant FANEVA IA — strictement lecture seule, hors moteur métier."""
        log("show_faneva_ia()")
        try:
            self.clear()
            lang = self._ia_ui_lang()
            # Fallback si module IA absent : message propre, app intacte
            if not HAS_FANEVA_IA:
                b = BoxLayout(orientation="vertical", padding=16, spacing=12)
                b.add_widget(Label(
                    text="🤖 FANEVA IA\n\nModule IA non disponible sur cet appareil.\nL'application métier continue de fonctionner.",
                    font_size=22, halign="center", valign="middle",
                ))
                btn = Button(text="RETOUR", size_hint_y=None, height=48, background_color=(0.4, 0.4, 0.4, 1))
                btn.bind(on_press=lambda x: self.show_dashboard())
                b.add_widget(btn)
                self.root.add_widget(b)
                return

            title = ui_text("title", lang) if ui_text else "🤖 Assistant FANEVA"
            greeting = ui_text("greeting", lang) if ui_text else "Bonjour 👋\nQue voulez-vous savoir ?"
            hint = ui_text("hint", lang) if ui_text else "Posez votre question..."
            send_lbl = ui_text("send", lang) if ui_text else "ENVOYER"
            back_lbl = ui_text("back", lang) if ui_text else "RETOUR"
            note = ui_text("readonly_note", lang) if ui_text else "Mode lecture seule"

            b = BoxLayout(orientation="vertical", padding=10, spacing=8)
            b.add_widget(Label(text=title, font_size=26, bold=True, color=(0.15, 0.55, 0.85, 1), size_hint_y=None, height=40))
            b.add_widget(Label(text=greeting, font_size=22, color=(0.2, 0.2, 0.25, 1), size_hint_y=None, height=60, halign="center"))
            b.add_widget(Label(text=note, font_size=16, color=(0.5, 0.5, 0.5, 1), size_hint_y=None, height=24))

            # Suggestions
            sug_box = GridLayout(cols=1, spacing=4, size_hint_y=None, padding=2)
            sug_box.bind(minimum_height=sug_box.setter("height"))
            pairs = suggestion_pairs(lang) if suggestion_pairs else ()
            for label, question in pairs:
                btn = Button(text=label, font_size=18, size_hint_y=None, height=dp(42),
                             background_color=(0.2, 0.45, 0.7, 1))
                btn.bind(on_press=lambda inst, q=question: self._ia_fill_and_ask(q))
                sug_box.add_widget(btn)
            b.add_widget(sug_box)

            row = BoxLayout(size_hint_y=None, height=50, spacing=6)
            self.ia_input = TextInput(hint_text=hint, multiline=False, font_size=20)
            btn_send = Button(text=send_lbl, size_hint_x=0.28, background_color=(0.15, 0.55, 0.85, 1))
            btn_send.bind(on_press=self._ia_on_send)
            row.add_widget(self.ia_input)
            row.add_widget(btn_send)
            b.add_widget(row)

            scroll = ScrollView(do_scroll_x=False)
            self.ia_answer = Label(
                text="", font_size=20, color=(0.15, 0.15, 0.2, 1),
                size_hint_y=None, text_size=(None, None), halign="left", valign="top",
            )
            self.ia_answer.bind(texture_size=lambda *_: setattr(self.ia_answer, "height", max(self.ia_answer.texture_size[1], 40)))
            scroll.add_widget(self.ia_answer)
            b.add_widget(scroll)

            btn_back = Button(text=back_lbl, size_hint_y=None, height=48, background_color=(0.4, 0.4, 0.4, 1))
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
        except Exception as e:
            log_error("show_faneva_ia", e)
            popup("FANEVA IA", str(e))
            try:
                self.show_dashboard()
            except Exception:
                pass

    def _ia_fill_and_ask(self, question):
        if hasattr(self, "ia_input") and self.ia_input is not None:
            self.ia_input.text = question
        self._ia_run_question(question)

    def _ia_on_send(self, inst=None):
        q = ""
        if hasattr(self, "ia_input") and self.ia_input is not None:
            q = (self.ia_input.text or "").strip()
        self._ia_run_question(q)

    def _ia_run_question(self, question):
        """Exécute la question via la couche service IA (pas d'ouverture SQLite dans l'UI).

        Flux : UI → run_faneva_ia_question → FanevaIA → AIDataService → SQLite
        La factory de connexion est déléguée au service ; l'UI ne gère plus la persistence.
        """
        lang = "fr"
        if HAS_FANEVA_IA and detect_language:
            lang = detect_language(question or "") or "fr"
            self._ia_ui_lang_pref = lang
        if not (question or "").strip():
            msg = map_error_message("EMPTY_QUESTION", lang) if map_error_message else "Question vide"
            if hasattr(self, "ia_answer"):
                self.ia_answer.text = msg
            return
        thinking = ui_text("thinking", lang) if ui_text else "…"
        if hasattr(self, "ia_answer"):
            self.ia_answer.text = thinking

        generation = getattr(self, "_ia_generation", 0) + 1
        self._ia_generation = generation
        kwargs = self._ia_session_kwargs()
        provider = self._ia_provider()

        def worker():
            result = {
                "ok": False,
                "answer": "",
                "error_code": "PROVIDER_UNAVAILABLE",
                "error_message": "IA indisponible",
            }
            try:
                if not HAS_FANEVA_IA or run_faneva_ia_question is None:
                    result["error_code"] = "PROVIDER_UNAVAILABLE"
                else:
                    # Délégation pure : SQLite ouvert uniquement dans la couche service
                    result = run_faneva_ia_question(
                        question,
                        self._ia_db_factory,
                        provider=provider,
                        period="day",
                        **kwargs,
                    )
            except Exception as e:
                log_error("faneva_ia_worker", e)
                result = {
                    "ok": False,
                    "answer": "",
                    "error_code": "PROVIDER_ERROR",
                    "error_message": str(e)[:300],
                }
            Clock.schedule_once(lambda dt: self._ia_apply_result(generation, result, lang), 0)

        threading.Thread(target=worker, daemon=True, name="faneva-ia").start()

    def _ia_apply_result(self, generation, result, lang):
        if generation != getattr(self, "_ia_generation", 0):
            return
        if not hasattr(self, "ia_answer"):
            return
        if result.get("ok") and result.get("answer"):
            self.ia_answer.text = result["answer"]
            return
        code = result.get("error_code") or "PROVIDER_ERROR"
        msg = map_error_message(code, lang) if map_error_message else (result.get("error_message") or "Erreur IA")
        detail = result.get("error_message") or ""
        if detail and detail not in msg:
            self.ia_answer.text = f"{msg}\n({code})"
        else:
            self.ia_answer.text = f"{msg}\n({code})" if code else msg

    # ---------- STOCK ----------
    def show_stock(self, inst=None):
        log("show_stock() START")
        try:
            self.clear()
            mag = self.get_magasin_info()
            is_admin = self.user and is_user_admin_hybrid(self.user.get("role"))

            b = BoxLayout(orientation="vertical", padding=8, spacing=8)
            b.add_widget(Label(text=f"STOCK - {mag['nom']}", font_size=26, bold=True, color=mag["couleur"], size_hint_y=None, height=40))

            search_container = BoxLayout(orientation="vertical", size_hint_y=None, height=dp(130), spacing=4)

            h = BoxLayout(size_hint_y=None, height=45, spacing=5)
            self.s_search = TextInput(hint_text="Tapez pour rechercher...", multiline=False, font_size=22)
            self.s_search.bind(text=self.on_stock_search_text)
            btn_r = Button(text="R", size_hint_x=0.15, background_color=mag["couleur"])
            btn_r.bind(on_press=self.load_stock)
            h.add_widget(self.s_search)
            h.add_widget(btn_r)
            search_container.add_widget(h)

            sugg_scroll = ScrollView(size_hint_y=None, height=dp(80))
            self.s_suggestions = GridLayout(cols=1, spacing=2, size_hint_y=None, padding=2)
            self.s_suggestions.bind(minimum_height=self.s_suggestions.setter("height"))
            sugg_scroll.add_widget(self.s_suggestions)
            search_container.add_widget(sugg_scroll)

            b.add_widget(search_container)

            exp_box = BoxLayout(size_hint_y=None, height=40, spacing=6)
            btn_exp_pdf = Button(text="EXPORT PDF STOCK", background_color=(0.95,0.3,0.3,1), font_size=20)
            btn_exp_pdf.bind(on_press=self.export_stock_pdf)
            btn_exp_csv = Button(text="EXPORT CSV STOCK", background_color=(0.2,0.6,0.9,1), font_size=20)
            btn_exp_csv.bind(on_press=self.export_stock_csv)
            exp_box.add_widget(btn_exp_pdf)
            exp_box.add_widget(btn_exp_csv)
            b.add_widget(exp_box)

            diagnostic_box = BoxLayout(size_hint_y=None, height=40, spacing=6)
            btn_share_report = Button(
                text="PARTAGER LE RAPPORT",
                background_color=(0.62, 0.40, 0.13, 1),
                font_size=18,
            )
            btn_share_report.bind(on_press=self.share_stock_diagnostic_report)
            btn_copy_report = Button(
                text="COPIER LE RAPPORT",
                background_color=(0.30, 0.45, 0.70, 1),
                font_size=18,
            )
            btn_copy_report.bind(on_press=self.copy_stock_diagnostic_report)
            diagnostic_box.add_widget(btn_share_report)
            diagnostic_box.add_widget(btn_copy_report)
            b.add_widget(diagnostic_box)

            if is_admin:
                btns_stock = BoxLayout(size_hint_y=None, height=45, spacing=5)
                btn_a = Button(text="+ PRODUIT", background_color=(0.2,0.8,0.4,1))
                btn_a.bind(on_press=self.show_add_prod)
                btn_add_stock = Button(text="+ AJOUTER STOCK", background_color=(0.2,0.6,0.9,1))
                btn_add_stock.bind(on_press=self.show_ajouter_stock)
                btns_stock.add_widget(btn_a)
                btns_stock.add_widget(btn_add_stock)
                b.add_widget(btns_stock)

            scroll = ScrollView()
            self.s_list = GridLayout(cols=1, spacing=4, size_hint_y=None, padding=5)
            self.s_list.bind(minimum_height=self.s_list.setter("height"))
            scroll.add_widget(self.s_list)
            b.add_widget(scroll)

            p = BoxLayout(size_hint_y=None, height=40, spacing=8)
            self.s_prev = Button(text="<<", size_hint_x=0.2, background_color=(0.5,0.5,0.5,1))
            self.s_prev.bind(on_press=self.stock_prev)
            self.s_page = Label(text="Page 1")
            self.s_next = Button(text=">>", size_hint_x=0.2, background_color=mag["couleur"])
            self.s_next.bind(on_press=self.stock_next)
            p.add_widget(self.s_prev)
            p.add_widget(self.s_page)
            p.add_widget(self.s_next)
            b.add_widget(p)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
            self.s_page_num = 0
            self.s_per_page = 20
            self.load_stock()
        except Exception as e:
            log_error("show_stock", e)
            popup("Erreur", f"Stock: {str(e)}")

    def on_stock_search_text(self, instance, value):
        if getattr(self, "_stock_selection_in_progress", False):
            return
        self.s_page_num = 0
        previous_event = getattr(self, "_stock_search_event", None)
        if previous_event:
            previous_event.cancel()
        self._stock_search_generation = getattr(self, "_stock_search_generation", 0) + 1
        if len(value.strip()) < 1:
            self.s_suggestions.clear_widgets()
            self.load_stock()
            return
        generation = self._stock_search_generation
        query = value.strip()

        def run_latest_search(dt):
            if generation == getattr(self, "_stock_search_generation", 0) and self.s_search.text.strip() == query:
                self._do_stock_dynamic_search(query)

        self._stock_search_event = Clock.schedule_once(run_latest_search, 0.18)

    def _do_stock_dynamic_search(self, value):
        """Lance la lecture de suggestions hors du thread Kivy."""
        generation = getattr(self, "_stock_search_generation", 0)
        is_admin = bool(self.user and is_user_admin_hybrid(self.user.get("role")))
        threading.Thread(
            target=self._load_stock_suggestions_background,
            args=(generation, value, is_admin),
            daemon=True,
            name="faneva-stock-search",
        ).start()

    def _load_stock_suggestions_background(self, generation, value, is_admin):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id, nom, prix_achat, prix_vente, stock, hybrid_id, magasin_ref_id FROM produits WHERE nom LIKE ? AND actif=1 ORDER BY nom LIMIT 8", (f"%{value}%",))
                rows = canonical_product_display_rows(conn, cur.fetchall())
        except Exception as e:
            log_error("_do_stock_dynamic_search", e)
            rows = []
        Clock.schedule_once(lambda dt: self._render_stock_suggestions(generation, value, is_admin, rows), 0)

    def _render_stock_suggestions(self, generation, value, is_admin, rows):
        if generation != getattr(self, "_stock_search_generation", 0):
            return
        if not hasattr(self, "s_search") or self.s_search.text.strip() != value or not hasattr(self, "s_suggestions"):
            return
        self.s_suggestions.clear_widgets()
        if not rows:
            self.s_suggestions.add_widget(Label(text="Aucun produit trouvé", font_size=22, color=(0.6,0.6,0.6,1), size_hint_y=None, height=dp(30)))
            return
        stock_diagnostic_trace_async(
            "UI_STOCK_SUGGESTIONS",
            tuple({"PRODUCT_ID": r[0], "UI_DISPLAYED_STOCK": r[4], "SEARCH": value} for r in rows),
        )
        for r in rows:
            lbl_text = f"{r[1]} | PV: {fmt(r[3])} Ar | PA: {fmt(r[2])} Ar | Stock: {r[4]}" if is_admin else f"{r[1]} | PV: {fmt(r[3])} Ar | Stock: {r[4]}"
            btn = Button(text=lbl_text, font_size=22, size_hint_y=None, height=dp(35), background_color=(0.95,0.95,1,1), color=(0.15,0.15,0.3,1), halign="left")
            btn.bind(on_press=partial(self._select_stock_suggestion, r))
            self.s_suggestions.add_widget(btn)

    def _select_stock_suggestion(self, produit, instance):
        self._stock_selection_in_progress = True
        self.s_search.text = produit[1]
        self._stock_selection_in_progress = False
        previous_event = getattr(self, "_stock_search_event", None)
        if previous_event:
            previous_event.cancel()
        self._stock_search_generation = getattr(self, "_stock_search_generation", 0) + 1
        self.s_suggestions.clear_widgets()
        self.s_page_num = 0
        self.load_stock()

    @safe_sqlite
    def export_stock_csv(self, inst=None):
        role = self.user.get("role", "VENDEUR") if self.user else "VENDEUR"
        try:
            import csv
            mag = self.get_magasin_info()
            fn = f"/storage/emulated/0/FANEVA_STOCK_{mag['nom']}_{role}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            with get_db_connection() as conn:
                headers, rows = canonical_stock_export_data(conn, role)

            os.makedirs(os.path.dirname(fn), exist_ok=True)
            with open(fn, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(headers)
                for r in rows:
                    w.writerow(list(r))
            popup("Succes", f"Export CSV Stock cree:\n{fn}")
        except Exception as e:
            log_error("export_stock_csv", e)
            popup("Erreur CSV", str(e))

    @safe_sqlite
    def export_stock_pdf(self, inst=None):
        role = self.user.get("role", "VENDEUR") if self.user else "VENDEUR"
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
            mag = self.get_magasin_info()
            fn = f"/storage/emulated/0/FANEVA_STOCK_{mag['nom']}_{role}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

            with get_db_connection() as conn:
                headers, rows = canonical_stock_export_data(conn, role)

            os.makedirs(os.path.dirname(fn), exist_ok=True)
            c = canvas.Canvas(fn, pagesize=A4)
            y = 800
            c.drawString(50, y, f"KDK SYSTEM - STOCK {mag['nom']} ({role})")
            y -= 25
            c.drawString(50, y, f"Date: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
            y -= 35

            header_line = " | ".join(headers)
            c.drawString(40, y, header_line[:90])
            y -= 15
            c.line(40, y, 550, y)
            y -= 20

            for r in rows:
                if y < 50:
                    c.showPage()
                    y = 800
                line_str = " | ".join([str(val) if val is not None else "" for val in r])
                c.drawString(40, y, line_str[:95])
                y -= 18

            c.save()
            popup("Succes", f"Export PDF Stock cree:\n{fn}")
        except Exception as e:
            log_error("export_stock_pdf", e)
            popup("Erreur PDF", str(e))

    def show_ajouter_stock(self, inst=None):
        if not self.check_admin():
            return
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=8, spacing=8)
            b.add_widget(Label(text=f"AJOUTER STOCK - {mag['nom']}", font_size=26, bold=True, color=(0.2,0.6,0.9,1), size_hint_y=None, height=40))

            h = BoxLayout(size_hint_y=None, height=45, spacing=5)
            self.as_search = TextInput(hint_text="Tapez le nom du produit...", multiline=False, font_size=22)
            self.as_search.bind(text=self.on_ajout_stock_search)
            h.add_widget(self.as_search)
            b.add_widget(h)

            sugg_scroll = ScrollView(size_hint_y=None, height=dp(120))
            self.as_suggestions = GridLayout(cols=1, spacing=2, size_hint_y=None, padding=4)
            self.as_suggestions.bind(minimum_height=self.as_suggestions.setter("height"))
            sugg_scroll.add_widget(self.as_suggestions)
            b.add_widget(sugg_scroll)

            self.as_info = Label(text="", font_size=26, color=(0.15,0.15,0.2,1), size_hint_y=None, height=50)
            b.add_widget(self.as_info)

            qbox = BoxLayout(size_hint_y=None, height=45, spacing=8)
            qbox.add_widget(Label(text="Qt à ajouter:", size_hint_x=0.3))
            self.as_qty = TextInput(text="1", multiline=False, input_filter="int", font_size=24)
            qbox.add_widget(self.as_qty)
            qbox.add_widget(Label(text="Nouveau PA:", size_hint_x=0.3))
            self.as_pa = TextInput(text="0", multiline=False, input_filter="int", font_size=24)
            qbox.add_widget(self.as_pa)
            b.add_widget(qbox)

            btn_add = Button(text="AJOUTER AU STOCK", size_hint_y=None, height=50, background_color=(0.2,0.8,0.4,1), font_size=24, bold=True)
            btn_add.bind(on_press=self.do_ajouter_stock)
            b.add_widget(btn_add)

            self.as_msg = Label(text="", font_size=24, color=(0.2,0.8,0.4,1), size_hint_y=None, height=40)
            b.add_widget(self.as_msg)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_stock())
            b.add_widget(btn_back)

            self.root.add_widget(b)
            self.as_prod = None
        except Exception as e:
            log_error("show_ajouter_stock", e)

    def on_ajout_stock_search(self, instance, value):
        previous_event = getattr(self, "_ajout_stock_search_event", None)
        if previous_event:
            previous_event.cancel()
        self._ajout_stock_search_generation = getattr(self, "_ajout_stock_search_generation", 0) + 1
        if len(value.strip()) < 2:
            self.as_suggestions.clear_widgets()
            return
        generation = self._ajout_stock_search_generation
        query = value.strip()

        def run_latest_search(dt):
            if generation == getattr(self, "_ajout_stock_search_generation", 0) and self.as_search.text.strip() == query:
                self._do_ajout_stock_search(query)

        self._ajout_stock_search_event = Clock.schedule_once(run_latest_search, 0.18)

    def _do_ajout_stock_search(self, value):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id,nom,prix_achat,prix_vente,stock,hybrid_id,magasin_ref_id FROM produits WHERE nom LIKE ? AND actif=1 ORDER BY nom LIMIT 8", (f"%{value}%",))
                rows = canonical_product_display_rows(conn, cur.fetchall())

            self.as_suggestions.clear_widgets()
            if not rows:
                lbl = Label(text="Aucun produit trouvé", font_size=24, color=(0.6,0.6,0.6,1), size_hint_y=None, height=dp(35))
                self.as_suggestions.add_widget(lbl)
                return

            for r in rows:
                btn = Button(text=f"{r[1]} | Stock: {r[4]} | PA: {fmt(r[2])} Ar | PV: {fmt(r[3])} Ar", font_size=24, size_hint_y=None, height=dp(38), background_color=(0.95,0.95,1,1), color=(0.15,0.15,0.3,1), halign="left")
                btn.bind(on_press=partial(self._select_ajout_stock, r))
                self.as_suggestions.add_widget(btn)
        except Exception as e:
            log_error("_do_ajout_stock_search", e)

    def _select_ajout_stock(self, produit, instance):
        self.as_prod = produit
        self.as_search.text = produit[1]
        self.as_info.text = f"[SÉLECTIONNÉ] {produit[1]} | Stock actuel: {produit[4]} | PA: {fmt(produit[2])} Ar"
        self.as_pa.text = str(produit[2])
        self.as_suggestions.clear_widgets()

    @safe_sqlite
    def do_ajouter_stock(self, inst):
        if not self.check_admin():
            return
        try:
            if not self.as_prod:
                popup("Erreur", "Cherchez et sélectionnez un produit d'abord")
                return
            q = int(self.as_qty.text or 1)
            if q <= 0:
                popup("Erreur", "Quantité invalide")
                return
            nouveau_pa = int(self.as_pa.text or 0)
            if nouveau_pa < 0:
                popup("Erreur", "Prix d'achat invalide")
                return

            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT stock, prix_achat, hybrid_id, magasin_ref_id FROM produits WHERE id=? AND actif=1", (self.as_prod[0],))
                row = cur.fetchone()
                if not row:
                    popup("Erreur", "Produit introuvable")
                    return

                legacy_stock, pa_actuel, p_uuid, mid_val = row
                stock_actuel = get_stock(conn, p_uuid, mid_val) if has_hybrid_module() and p_uuid and mid_val else legacy_stock

                if stock_actuel + q > 0:
                    nouveau_pa_moyen = ((stock_actuel * pa_actuel) + (q * nouveau_pa)) / (stock_actuel + q)
                else:
                    nouveau_pa_moyen = nouveau_pa

                cur.execute("UPDATE produits SET prix_achat = ? WHERE id = ?", (nouveau_pa_moyen, self.as_prod[0]))

                # --- FANEVA SYSTEM HYBRID : entree de stock orientee sync ---
                if has_hybrid_module():
                    try:
                        if p_uuid and mid_val:
                            record_stock_in(conn, p_uuid, mid_val, q, nouveau_pa, source="ACHAT",
                                            admin_id=self.user.get("user_id"),
                                            admin_username=self.user.get("username"))
                    except Exception as e:
                        log(f"HYBRID record_stock_in echoue: {e}")

                conn.commit()

            popup("Succès", f"Stock ajouté!\n{self.as_prod[1]}\n+{q} unités\nNouveau stock: {stock_actuel + q}\nNouveau PA moyen: {fmt(nouveau_pa_moyen)} Ar")
            self.as_search.text = ""
            self.as_qty.text = "1"
            self.as_pa.text = "0"
            self.as_info.text = ""
            self.as_prod = None
            self.as_suggestions.clear_widgets()
            self.update_stats()
        except Exception as e:
            log_error("do_ajouter_stock", e)
            popup("Erreur", str(e))

    def load_stock(self, *args):
        query = self.s_search.text if hasattr(self, "s_search") else ""
        page_num = getattr(self, "s_page_num", 0)
        per_page = getattr(self, "s_per_page", 20)
        self._stock_list_generation = getattr(self, "_stock_list_generation", 0) + 1
        generation = self._stock_list_generation
        threading.Thread(
            target=self._load_stock_list_background,
            args=(generation, query, page_num, per_page),
            daemon=True,
            name="faneva-stock-list",
        ).start()

    def _load_stock_list_background(self, generation, query, page_num, per_page):
        rows, total = [], 0
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                search = f"%{query}%"
                cur.execute("SELECT id,nom,prix_achat,prix_vente,stock,hybrid_id,magasin_ref_id FROM produits WHERE nom LIKE ? AND actif=1 ORDER BY nom LIMIT ? OFFSET ?", (search, per_page, page_num * per_page))
                rows = canonical_product_display_rows(conn, cur.fetchall())
                cur.execute("SELECT COUNT(*) FROM produits WHERE nom LIKE ? AND actif=1", (search,))
                total = cur.fetchone()[0]
        except Exception as e:
            log_error("load_stock", e)
        Clock.schedule_once(lambda dt: self._render_stock_list(generation, query, page_num, per_page, rows, total), 0)

    def _render_stock_list(self, generation, query, page_num, per_page, rows, total):
        if generation != getattr(self, "_stock_list_generation", 0):
            return
        if not hasattr(self, "s_search") or self.s_search.text != query or getattr(self, "s_page_num", 0) != page_num:
            return
        stock_diagnostic_trace_async(
            "UI_STOCK_LIST",
            tuple({"PRODUCT_ID": r[0], "UI_DISPLAYED_STOCK": r[4], "SEARCH": query, "PAGE": page_num} for r in rows),
        )
        self.s_list.clear_widgets()
        for r in rows:
            self.s_list.add_widget(self._prod_row(r))
        pages = max(1, (total + per_page - 1) // per_page)
        self.s_page.text = f"Page {page_num+1}/{pages}"
        self.s_prev.disabled = page_num == 0
        self.s_next.disabled = (page_num + 1) * per_page >= total

    def _prod_row(self, r):
        idp, nom, pa, pv, stock = r
        mag = self.get_magasin_info()
        bg = mag["couleur_light"] if stock <= 0 else (0.9, 1, 0.9, 1)
        is_admin = self.user and is_user_admin_hybrid(self.user.get("role"))

        box = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(65), padding=6, spacing=6)
        with box.canvas.before:
            Color(*bg)
            RoundedRectangle(pos=box.pos, size=box.size, radius=[8])
        box.bind(pos=lambda o,v: setattr(o.canvas.before.children[-1],"pos",v))
        box.bind(size=lambda o,v: setattr(o.canvas.before.children[-1],"size",v))

        info = BoxLayout(orientation="vertical", size_hint_x=0.6 if is_admin else 1.0)
        info.add_widget(Label(text=nom, font_size=26, bold=True, halign="left", color=(0.15,0.15,0.2,1)))
        info.add_widget(Label(text=f"{fmt(pv)} Ar | Stock: {stock}", font_size=24, halign="left"))
        box.add_widget(info)

        if is_admin:
            btns = BoxLayout(size_hint_x=0.4, spacing=4)
            be = Button(text="E", background_color=(0.2,0.6,0.9,1))
            be.bind(on_press=partial(self.edit_prod, idp))
            bd = Button(text="X", background_color=(0.95,0.3,0.3,1))
            bd.bind(on_press=partial(self.del_prod, idp))
            btns.add_widget(be)
            btns.add_widget(bd)
            box.add_widget(btns)

        return box

    def stock_prev(self, inst):
        if self.s_page_num > 0:
            self.s_page_num -= 1
            self.load_stock()

    def stock_next(self, inst):
        self.s_page_num += 1
        self.load_stock()

    @safe_sqlite
    def show_add_prod(self, inst):
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        a_nom = TextInput(hint_text="Nom", multiline=False)
        a_pa = TextInput(hint_text="Prix achat", multiline=False, input_filter="int")
        a_pv = TextInput(hint_text="Prix vente", multiline=False, input_filter="int")
        a_stock = TextInput(hint_text="Stock", multiline=False, input_filter="int")

        for w in [a_nom, a_pa, a_pv, a_stock]:
            content.add_widget(w)

        def save():
            try:
                with get_db_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO produits (nom,prix_achat,prix_vente,stock) VALUES (?,?,?,?)", (a_nom.text, int(a_pa.text or 0), int(a_pv.text or 0), int(a_stock.text or 0)))

                    # --- FANEVA SYSTEM HYBRID : catalogue partage + transaction ---
                    pid_new = cur.lastrowid
                    if has_hybrid_module():
                        try:
                            mag_id_row = cur.execute("SELECT id FROM magasins WHERE cle=?", (self.magasin_cle,)).fetchone()
                            p_uuid = create_product(conn, a_nom.text, int(a_pa.text or 0), int(a_pv.text or 0),
                                                    categorie="General", stock_initial=int(a_stock.text or 0),
                                                    magasin_id=mag_id_row[0] if mag_id_row else None,
                                                    admin_id=self.user.get("user_id"),
                                                    admin_username=self.user.get("username"))
                            cur.execute("UPDATE produits SET hybrid_id=?, magasin_ref_id=? WHERE id=?",
                                        (p_uuid, mag_id_row[0] if mag_id_row else None, pid_new))
                        except Exception as e:
                            log(f"HYBRID create_product echoue: {e}")

                    conn.commit()
                popup("OK", "Produit ajoute!")
                self.load_stock()
            except Exception as e:
                log_error("save_add_prod", e)
                popup("Erreur", str(e))

        btn = Button(text="ENREGISTRER", background_color=(0.2,0.8,0.4,1))
        p = Popup(title="Nouveau Produit", content=content, size_hint=(0.9, 0.7))
        btn.bind(on_press=lambda x: (save(), p.dismiss()))
        content.add_widget(btn)
        p.open()

    @safe_sqlite
    def edit_prod(self, idp, inst):
        if not self.check_admin():
            return
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id,nom,prix_achat,prix_vente,stock,hybrid_id,magasin_ref_id FROM produits WHERE id=?", (idp,))
            p = cur.fetchone()
            if not p:
                popup("Erreur", "Produit introuvable")
                return
            product_id, product_name, prix_achat, prix_vente, legacy_stock, hybrid_id, magasin_id = p
            is_mapped = bool(has_hybrid_module() and hybrid_id and magasin_id)
            displayed_stock = canonical_display_stock(conn, hybrid_id, magasin_id, legacy_stock)

        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        e_nom = TextInput(text=product_name, multiline=False)
        e_pa = TextInput(text=str(prix_achat), multiline=False, input_filter="int")
        e_pv = TextInput(text=str(prix_vente), multiline=False, input_filter="int")
        e_stock = TextInput(text=str(displayed_stock), multiline=False, input_filter="int", readonly=is_mapped)

        for w in [e_nom, e_pa, e_pv, e_stock]:
            content.add_widget(w)

        def do_edit():
            with get_db_connection() as conn:
                cur = conn.cursor()
                if is_mapped:
                    # La projection canonique appartient à stocks_magasin : ne jamais la remplacer
                    # par le champ historique produits.stock depuis la fenêtre Modification.
                    cur.execute("UPDATE produits SET nom=?,prix_achat=?,prix_vente=? WHERE id=?", (e_nom.text, int(e_pa.text or 0), int(e_pv.text or 0), idp))
                else:
                    cur.execute("UPDATE produits SET nom=?,prix_achat=?,prix_vente=?,stock=? WHERE id=?", (e_nom.text, int(e_pa.text or 0), int(e_pv.text or 0), int(e_stock.text or 0), idp))

                # --- FANEVA SYSTEM HYBRID : transaction de mise a jour ---
                row_h = cur.execute("SELECT hybrid_id FROM produits WHERE id=?", (idp,)).fetchone()
                p_uuid = row_h[0] if row_h and row_h[0] else None
                if has_hybrid_module() and p_uuid:
                    try:
                        update_product(conn, p_uuid, nom=e_nom.text, prix_vente=int(e_pv.text or 0))
                    except Exception as e:
                        log(f"HYBRID update_product echoue: {e}")

                conn.commit()
            popup("OK", "Modifie!")
            self.load_stock()

        btn = Button(text="MODIFIER", background_color=(0.2,0.6,0.9,1))
        pop = Popup(title=f"Mod: {product_name}", content=content, size_hint=(0.9, 0.7))
        btn.bind(on_press=lambda x: (do_edit(), pop.dismiss()))
        content.add_widget(btn)
        pop.open()

    @safe_sqlite
    def del_prod(self, idp, inst):
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=15, spacing=10)
        content.add_widget(Label(text="Supprimer?", font_size=24))
        btns = BoxLayout(spacing=10)
        pop = Popup(title="Confirmer", content=content, size_hint=(0.7, 0.25))

        def do_del():
            with get_db_connection() as conn:
                cur = conn.cursor()
                row_h = cur.execute("SELECT hybrid_id, magasin_ref_id FROM produits WHERE id=?", (idp,)).fetchone()
                p_uuid = row_h[0] if row_h and row_h[0] else None
                mid_val = row_h[1] if row_h and row_h[1] else None

                # --- FANEVA SYSTEM HYBRID : transaction de suppression ---
                if has_hybrid_module() and p_uuid:
                    try:
                        delete_product(conn, p_uuid, magasin_id=mid_val)
                    except Exception as e:
                        log(f"HYBRID delete_product echoue: {e}")

                cur.execute("UPDATE produits SET actif=0 WHERE id=?", (idp,))
                conn.commit()
            popup("OK", "Supprime!")
            self.load_stock()

        by = Button(text="OUI", background_color=(0.95,0.3,0.3,1))
        by.bind(on_press=lambda x: (do_del(), pop.dismiss()))
        bn = Button(text="NON", background_color=(0.5,0.5,0.5,1))
        bn.bind(on_press=pop.dismiss)
        btns.add_widget(by)
        btns.add_widget(bn)
        content.add_widget(btns)
        pop.open()

    # ---------- VENTE ----------
    def show_vente(self, inst=None):
        log("show_vente()")
        try:
            self.clear()
            mag = self.get_magasin_info()
            self.v_cart = []
            b = BoxLayout(orientation="vertical", padding=8, spacing=8)

            b.add_widget(Label(text=f"VENTE - {mag['nom']}", font_size=26, bold=True, color=(0.2,0.8,0.4,1), size_hint_y=None, height=40))

            search_box = BoxLayout(orientation="vertical", size_hint_y=None, height=dp(140), spacing=4)

            h = BoxLayout(size_hint_y=None, height=45, spacing=5)
            self.v_search = TextInput(hint_text="Tapez le nom du produit...", multiline=False, font_size=22)
            self.v_search.bind(text=self.on_search_text)
            h.add_widget(self.v_search)
            b.add_widget(h)

            sugg_scroll = ScrollView(size_hint_y=None, height=dp(90))
            self.v_suggestions = GridLayout(cols=1, spacing=2, size_hint_y=None, padding=4)
            self.v_suggestions.bind(minimum_height=self.v_suggestions.setter("height"))
            sugg_scroll.add_widget(self.v_suggestions)
            search_box.add_widget(sugg_scroll)
            b.add_widget(search_box)

            self.v_info = Label(text="", font_size=26, color=(0.15,0.15,0.2,1), size_hint_y=None, height=50)
            b.add_widget(self.v_info)

            qbox = BoxLayout(size_hint_y=None, height=45, spacing=8)
            qbox.add_widget(Label(text="Qt:", size_hint_x=0.12))
            self.v_qty = TextInput(text="1", multiline=False, input_filter="int", font_size=24)
            qbox.add_widget(self.v_qty)
            btn_add = Button(text="+ PANIER", size_hint_x=0.4, background_color=(0.2,0.8,0.4,1))
            btn_add.bind(on_press=self.add_cart)
            qbox.add_widget(btn_add)
            b.add_widget(qbox)

            b.add_widget(Label(text="PANIER", font_size=24, bold=True, size_hint_y=None, height=25))
            scroll = ScrollView()
            self.v_list = GridLayout(cols=1, spacing=3, size_hint_y=None, padding=4)
            self.v_list.bind(minimum_height=self.v_list.setter("height"))
            scroll.add_widget(self.v_list)
            b.add_widget(scroll)

            self.v_total = Label(text="TOTAL: 0 Ar", font_size=24, bold=True, color=(0.2,0.8,0.4,1), size_hint_y=None, height=40)
            b.add_widget(self.v_total)

            acts = BoxLayout(size_hint_y=None, height=50, spacing=8)
            btn_clr = Button(text="VIDER", background_color=(0.95,0.3,0.3,1))
            btn_clr.bind(on_press=self.clear_cart)
            btn_val = Button(text="VALIDER", background_color=(0.2,0.8,0.4,1))
            btn_val.bind(on_press=self.validate_sale)
            acts.add_widget(btn_clr)
            acts.add_widget(btn_val)
            b.add_widget(acts)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
            self.v_prod = None
        except Exception as e:
            log_error("show_vente", e)

    def on_search_text(self, instance, value):
        if getattr(self, "_vente_selection_in_progress", False):
            return
        previous_event = getattr(self, "_vente_search_event", None)
        if previous_event:
            previous_event.cancel()
        self._vente_search_generation = getattr(self, "_vente_search_generation", 0) + 1
        if len(value.strip()) < 2:
            self.v_suggestions.clear_widgets()
            return
        generation = self._vente_search_generation
        query = value.strip()

        def run_latest_search(dt):
            if generation == getattr(self, "_vente_search_generation", 0) and self.v_search.text.strip() == query:
                self._do_dynamic_search(query)

        self._vente_search_event = Clock.schedule_once(run_latest_search, 0.18)

    def _do_dynamic_search(self, value):
        generation = getattr(self, "_vente_search_generation", 0)
        threading.Thread(
            target=self._load_vente_suggestions_background,
            args=(generation, value),
            daemon=True,
            name="faneva-sale-search",
        ).start()

    def _load_vente_suggestions_background(self, generation, value):
        rows = []
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id,nom,prix_achat,prix_vente,stock,hybrid_id,magasin_ref_id FROM produits WHERE nom LIKE ? AND actif=1 ORDER BY nom LIMIT 8", (f"%{value}%",))
                rows = canonical_product_display_rows(conn, cur.fetchall())
        except Exception as e:
            log_error("_do_dynamic_search", e)
        Clock.schedule_once(lambda dt: self._render_vente_suggestions(generation, value, rows), 0)

    def _render_vente_suggestions(self, generation, value, rows):
        if generation != getattr(self, "_vente_search_generation", 0):
            return
        if not hasattr(self, "v_search") or self.v_search.text.strip() != value or not hasattr(self, "v_suggestions"):
            return
        self.v_suggestions.clear_widgets()
        if not rows:
            self.v_suggestions.add_widget(Label(text="Aucun produit trouve", font_size=24, color=(0.6,0.6,0.6,1), size_hint_y=None, height=dp(35)))
            return
        stock_diagnostic_trace_async(
            "UI_VENTE_SUGGESTIONS",
            tuple({"PRODUCT_ID": r[0], "UI_DISPLAYED_STOCK": r[4], "SEARCH": value} for r in rows),
        )
        for r in rows:
            btn = Button(text=f"{r[1]} | {fmt(r[3])} Ar | Stock: {r[4]}", font_size=24, size_hint_y=None, height=dp(38), background_color=(0.95,0.95,1,1), color=(0.15,0.15,0.3,1), halign="left")
            btn.bind(on_press=partial(self._select_suggestion, r))
            self.v_suggestions.add_widget(btn)

    def _select_suggestion(self, produit, instance):
        self.v_prod = produit
        self._vente_selection_in_progress = True
        self.v_search.text = produit[1]
        self._vente_selection_in_progress = False
        previous_event = getattr(self, "_vente_search_event", None)
        if previous_event:
            previous_event.cancel()
        self._vente_search_generation = getattr(self, "_vente_search_generation", 0) + 1
        self.v_info.text = f"[SELECTIONNE] {produit[1]} | {fmt(produit[3])} Ar | Stock: {produit[4]}"
        stock_diagnostic_trace_async(
            "UI_VENTE_SELECTED",
            ({"PRODUCT_ID": produit[0], "UI_DISPLAYED_STOCK": produit[4], "PRODUCT_NAME": produit[1]},),
        )
        self.v_suggestions.clear_widgets()

    def _resolve_sale_line_context(self, conn, product_id, active_magasin_id):
        """Résout une unique clé magasin pour valider, enregistrer et afficher une vente.

        Un produit HYBRID doit appartenir exactement au magasin actif. Un produit sans
        identité HYBRID conserve le fallback legacy du magasin actif, sans fabriquer
        d'alias ni modifier son historique.
        """
        cur = conn.cursor()
        cur.execute("SELECT hybrid_id, magasin_ref_id, stock, actif FROM produits WHERE id=?", (product_id,))
        row = cur.fetchone()
        if not row or row[3] != 1:
            raise ValueError("Produit introuvable ou inactif")
        hybrid_id, product_magasin_id, legacy_stock, _ = row
        if hybrid_id:
            if product_magasin_id is None:
                raise ValueError("Produit HYBRID sans magasin_ref_id : vente refusée")
            magasin_id_effectif = int(product_magasin_id)
            if magasin_id_effectif != int(active_magasin_id):
                raise ValueError("Produit rattaché à un autre magasin : vente refusée")
            canonical_id = (resolve_canonical_product_id(conn, hybrid_id, magasin_id_effectif)
                            if resolve_canonical_product_id else hybrid_id)
            stock_disponible = get_stock(conn, hybrid_id, magasin_id_effectif)
            return {
                "hybrid_id": hybrid_id,
                "magasin_ref_id": magasin_id_effectif,
                "magasin_id_effectif": magasin_id_effectif,
                "canonical_product_uuid": canonical_id,
                "stock_disponible": stock_disponible,
                "mapped": True,
            }
        return {
            "hybrid_id": str(product_id),
            "magasin_ref_id": int(active_magasin_id),
            "magasin_id_effectif": int(active_magasin_id),
            "canonical_product_uuid": None,
            "stock_disponible": int(legacy_stock or 0),
            "mapped": False,
        }

    def add_cart(self, inst):
        try:
            if not self.v_prod:
                popup("Erreur", "Cherchez et selectionnez un produit d'abord")
                return
            q = int(self.v_qty.text or 1)
            if q <= 0:
                popup("Erreur", "Quantite invalide")
                return
            with get_db_connection() as conn:
                active_row = conn.execute("SELECT id FROM magasins WHERE cle=?", (self.magasin_cle,)).fetchone()
                if not active_row:
                    raise ValueError("Magasin actif introuvable : vente refusée")
                sale_context = self._resolve_sale_line_context(conn, self.v_prod[0], active_row[0])
            if q > sale_context["stock_disponible"]:
                popup("Stock insuffisant", f"Stock insuffisant : {sale_context['stock_disponible']} disponible(s)")
                return

            self.v_cart.append({
                "id": self.v_prod[0], "nom": self.v_prod[1],
                "pa": self.v_prod[2], "pv": self.v_prod[3],
                "q": q, "total": self.v_prod[3] * q,
                "ben": (self.v_prod[3] - self.v_prod[2]) * q,
                **sale_context,
            })
            self.update_cart()
            self.v_search.text = ""
            self.v_qty.text = "1"
            self.v_info.text = ""
            self.v_prod = None
            self.v_suggestions.clear_widgets()
        except Exception as e:
            log_error("add_cart", e)

    def update_cart(self):
        self.v_list.clear_widgets()
        total = 0
        for i, item in enumerate(self.v_cart):
            row = BoxLayout(size_hint_y=None, height=38, spacing=4)
            row.add_widget(Label(text=item["nom"], size_hint_x=0.4, font_size=24))
            row.add_widget(Label(text=f"x{item['q']}", size_hint_x=0.15, font_size=24))
            row.add_widget(Label(text=f"{fmt(item['total'])} Ar", size_hint_x=0.3, font_size=24))
            btn = Button(text="X", size_hint_x=0.15, background_color=(0.95,0.3,0.3,1))
            btn.bind(on_press=partial(self.remove_cart, i))
            row.add_widget(btn)
            self.v_list.add_widget(row)
            total += item["total"]
        self.v_total.text = f"TOTAL: {fmt(total)} Ar"

    def remove_cart(self, idx, inst):
        self.v_cart.pop(idx)
        self.update_cart()

    def clear_cart(self, inst):
        self.v_cart = []
        self.update_cart()

    def validate_sale(self, inst):
        try:
            if not self.v_cart:
                popup("Erreur", "Panier vide!")
                return
            vendeur = self.user["username"] if self.user else "Inconnu"

            with get_db_connection() as conn:
                cur = conn.cursor()
                active_row = cur.execute("SELECT id FROM magasins WHERE cle=?", (self.magasin_cle,)).fetchone()
                if not active_row:
                    raise ValueError("Magasin actif introuvable : vente refusée")
                magasin_id_actif = int(active_row[0])
                line_contexts = []
                for item in self.v_cart:
                    if item["q"] <= 0:
                        popup("Erreur", f"Quantité invalide pour {item['nom']}!")
                        return

                    context = self._resolve_sale_line_context(conn, item["id"], magasin_id_actif)
                    if item.get("magasin_id_effectif") not in (None, context["magasin_id_effectif"]):
                        raise ValueError("Magasin du panier devenu incohérent : vente refusée")
                    if item.get("hybrid_id") not in (None, context["hybrid_id"]):
                        raise ValueError("Identité produit du panier devenue incohérente : vente refusée")
                    if context["stock_disponible"] < item["q"]:
                        popup("Stock insuffisant", f"Stock insuffisant pour {item['nom']} ({context['stock_disponible']} disponible)")
                        return
                    line_contexts.append(context)

                magasins_effectifs = {context["magasin_id_effectif"] for context in line_contexts}
                if len(magasins_effectifs) != 1 or magasin_id_actif not in magasins_effectifs:
                    raise ValueError("Panier multi-magasin ou magasin incohérent : vente refusée")
                magasin_id_effectif = magasins_effectifs.pop()

                # --- FANEVA SYSTEM HYBRID : transaction orientee sync ---
                panier_hybrid = []
                for item, context in zip(self.v_cart, line_contexts):
                    panier_hybrid.append({
                        "produit_id": context["hybrid_id"],
                        "canonical_product_uuid": context["canonical_product_uuid"],
                        "nom": item["nom"], "pa": item["pa"], "pv": item["pv"],
                        "q": item["q"], "total": item["total"], "ben": item["ben"]})

                if not has_hybrid_module():
                    raise RuntimeError("HYBRID indisponible : vente refusee, aucune ecriture legacy")
                if not panier_hybrid:
                    raise RuntimeError("Contexte HYBRID incomplet : vente refusee")
                sale_tx_id = record_sale(conn, panier_hybrid, magasin_id_effectif,
                                         admin_id=self.user.get("user_id"),
                                         admin_username=self.user.get("username"),
                                         commit=False)
                sale_device_id = _local_device_id(conn)

                stock_diagnostic_trace_async(
                    "SALE_COMMITTED_PROJECTION",
                    tuple(
                        {
                            "PRODUCT_ID": item["id"],
                            "HYBRID_ID": context["hybrid_id"],
                            "CANONICAL_PRODUCT_UUID": context["canonical_product_uuid"] or "NONE",
                            "MAGASIN_ID": magasin_id_effectif,
                            "STOCK_SOURCE": context["stock_disponible"],
                            "CANONICAL_STOCK": get_stock(conn, context["hybrid_id"], magasin_id_effectif),
                        }
                        for item, context in zip(self.v_cart, line_contexts)
                    ),
                )

                # Ventes/rapports restent projetés localement; le stock ne doit plus
                # être décrémenté ici, car record_sale le projette via mouvements.
                for item in self.v_cart:
                    cur.execute(
                        "INSERT INTO ventes (transaction_id, device_id, magasin_ref_id, produit_id,produit_nom,quantite,prix_achat,prix_vente,total,benefice,vendeur,date_vente) VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))",
                        (sale_tx_id, sale_device_id, magasin_id_effectif, item["id"], item["nom"], item["q"], item["pa"], item["pv"], item["total"], item["ben"], vendeur)
                    )
                    try:
                        cur.execute("UPDATE ventes SET sale_mode='COMPTANT' WHERE transaction_id=? AND produit_id=?",
                                    (sale_tx_id, item["id"]))
                    except Exception:
                        pass

                conn.commit()

            total = sum(i["total"] for i in self.v_cart)
            now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
            popup("Succes", f"Vente validee!\nTotal: {fmt(total)} Ar\nDate: {now_str}")
            self.clear_cart(None)
            self.update_stats()
        except Exception as e:
            log_error("validate_sale", e)
            popup("Erreur Vente", str(e))

    # ---------- DETTES (ÉTAPE 3 - UNICITÉ ET FUSION) ----------
    def show_dettes(self, inst=None):
        log("show_dettes() START")
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=8, spacing=8)

            b.add_widget(Label(text=f"DETTES CLIENTS - {mag['nom']}", font_size=26, bold=True, color=(0.95,0.3,0.3,1), size_hint_y=None, height=40))

            self.d_resume = Label(text="", font_size=26, size_hint_y=None, height=45, color=(0.15,0.15,0.2,1))
            b.add_widget(self.d_resume)

            acts = BoxLayout(size_hint_y=None, height=45, spacing=5)
            btn_a = Button(text="+ DETTE", background_color=(0.95,0.3,0.3,1))
            btn_a.bind(on_press=self.show_add_dette)
            btn_r = Button(text="R", size_hint_x=0.15, background_color=(0.2,0.6,0.9,1))
            btn_r.bind(on_press=self.load_dettes)
            acts.add_widget(btn_a)
            acts.add_widget(btn_r)
            b.add_widget(acts)

            scroll = ScrollView()
            self.d_list = GridLayout(cols=1, spacing=4, size_hint_y=None, padding=5)
            self.d_list.bind(minimum_height=self.d_list.setter("height"))
            scroll.add_widget(self.d_list)
            b.add_widget(scroll)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
            self.load_dettes()
        except Exception as e:
            log_error("show_dettes", e)
            popup("Erreur", f"Dettes: {str(e)}")

    @safe_sqlite
    def load_dettes(self, *args):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COALESCE(SUM(reste),0), COUNT(*) FROM dettes WHERE statut='ACTIF'")
            r = cur.fetchone()
            cur.execute("SELECT id,client,telephone,total,paye,reste FROM dettes WHERE statut='ACTIF' ORDER BY reste DESC")
            rows = cur.fetchall()

        self.d_resume.text = f"Total a recouvrer: {fmt(r[0])} Ar | {r[1]} clients"
        self.d_list.clear_widgets()

        for d in rows:
            box = BoxLayout(orientation="horizontal", size_hint_y=None, height=dp(75), padding=6, spacing=6)
            with box.canvas.before:
                Color(1, 0.95, 0.95, 1)
                RoundedRectangle(pos=box.pos, size=box.size, radius=[8])
            box.bind(pos=lambda o,v: setattr(o.canvas.before.children[-1],"pos",v))
            box.bind(size=lambda o,v: setattr(o.canvas.before.children[-1],"size",v))

            info = BoxLayout(orientation="vertical", size_hint_x=0.6)
            info.add_widget(Label(text=d[1], font_size=26, bold=True, halign="left", color=(0.15,0.15,0.2,1)))
            info.add_widget(Label(text=f"Tel: {d[2] or 'N/A'}", font_size=22, halign="left"))
            info.add_widget(Label(text=f"Total: {fmt(d[3])} | Paye: {fmt(d[4])} | RESTE: {fmt(d[5])}", font_size=24, halign="left", color=(0.95,0.3,0.3,1), bold=True))
            box.add_widget(info)

            btn = Button(text="PAYER", size_hint_x=0.4, background_color=(0.2,0.8,0.4,1))
            btn.bind(on_press=partial(self.pay_dette, d[0]))
            box.add_widget(btn)

            self.d_list.add_widget(box)

    @safe_sqlite
    def show_add_dette(self, inst):
        log("show_add_dette()")
        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        d_client = TextInput(hint_text="Nom client", multiline=False)
        d_tel = TextInput(hint_text="Telephone (Optionnel)", multiline=False)
        d_montant = TextInput(hint_text="Montant (Ar)", multiline=False, input_filter="int")

        for w in [d_client, d_tel, d_montant]:
            content.add_widget(w)

        def save():
            try:
                c_nom = d_client.text.strip()
                c_tel = d_tel.text.strip()
                m = int(d_montant.text or 0)

                if not c_nom:
                    popup("Erreur", "Veuillez entrer le nom du client")
                    return
                if m <= 0:
                    popup("Erreur", "Montant de dette invalide")
                    return

                with get_db_connection() as conn:
                    cur = conn.cursor()

                    # Transaction SQLite Atomique : Rechercher une dette ACTIVE pour ce client
                    if not has_hybrid_module():
                        raise RuntimeError("HYBRID indisponible : dette refusee, aucune ecriture legacy")
                    cur.execute("""
                        SELECT id, client, telephone, total, paye, reste, transaction_id
                        FROM dettes
                        WHERE LOWER(TRIM(client)) = LOWER(TRIM(?)) AND statut = 'ACTIF'
                    """, (c_nom,))
                    dettes_actives = cur.fetchall()

                    existing_dette = None
                    for d in dettes_actives:
                        did, ex_client, ex_tel, ex_total, ex_paye, ex_reste, ex_tx_ref = d
                        ex_tel_clean = (ex_tel or "").strip()
                        if ex_tel_clean and c_tel and ex_tel_clean != c_tel:
                            continue
                        existing_dette = d
                        break

                    mid_row = cur.execute("SELECT id FROM magasins WHERE cle=?", (self.magasin_cle,)).fetchone()
                    mid_val = mid_row[0] if mid_row else None
                    if not mid_val:
                        raise RuntimeError("Magasin HYBRID introuvable : dette refusee")

                    if existing_dette:
                        did, ex_client, ex_tel, ex_total, ex_paye, ex_reste, ex_tx_ref = existing_dette
                        nouveau_total = ex_total + m
                        nouveau_reste = nouveau_total - ex_paye
                        tel_a_jour = ex_tel if (ex_tel and ex_tel.strip()) else c_tel
                        debt_tx_id = record_debt(conn, c_nom, c_tel, m,
                                                 admin_id=self.user.get("user_id"),
                                                 admin_username=self.user.get("username"),
                                                 magasin_id=mid_val,
                                                 dette_transaction_id=ex_tx_ref,
                                                 commit=False)
                        cur.execute("""
                            UPDATE dettes
                            SET total=?, reste=?, telephone=?, transaction_id=COALESCE(transaction_id,?),
                                device_id=?, magasin_ref_id=?
                            WHERE id=?
                        """, (nouveau_total, nouveau_reste, tel_a_jour, debt_tx_id,
                               _local_device_id(conn), mid_val, did))
                        conn.commit()
                        log(f"FUSION DETTE: Client '{ex_client}' (ID {did}) | Total: {ex_total}->{nouveau_total} | Paye: {ex_paye} | Reste: {nouveau_reste}")
                        popup("Fusion de dette", f"Dette active mise à jour pour {ex_client}!\nNouveau Total: {fmt(nouveau_total)} Ar\nPayé conservé: {fmt(ex_paye)} Ar\nNouveau Reste: {fmt(nouveau_reste)} Ar")
                    else:
                        debt_tx_id = record_debt(conn, c_nom, c_tel, m,
                                                 admin_id=self.user.get("user_id"),
                                                 admin_username=self.user.get("username"),
                                                 magasin_id=mid_val,
                                                 commit=False)
                        cur.execute("""
                            INSERT INTO dettes (transaction_id, client, telephone, total, paye, reste, statut, device_id, magasin_ref_id)
                            VALUES (?, ?, ?, ?, 0, ?, 'ACTIF', ?, ?)
                        """, (debt_tx_id, c_nom, c_tel, m, m, _local_device_id(conn), mid_val))
                        conn.commit()
                        log(f"NOUVELLE DETTE: Client '{c_nom}' | Montant: {m}")
                        popup("Succès", f"Nouvelle dette active créée pour {c_nom} ({fmt(m)} Ar)!")

                self.load_dettes()
            except Exception as e:
                log_error("save_add_dette", e)
                popup("Erreur", str(e))

        btn = Button(text="ENREGISTRER", background_color=(0.2,0.8,0.4,1))
        p = Popup(title="Nouvelle Dette / Fusion", content=content, size_hint=(0.9, 0.7))
        btn.bind(on_press=lambda x: (save(), p.dismiss()))
        content.add_widget(btn)
        p.open()

    @safe_sqlite
    def pay_dette(self, did, inst):
        log(f"pay_dette({did})")
        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        p_montant = TextInput(hint_text="Montant", multiline=False, input_filter="int")
        content.add_widget(p_montant)

        def do_pay():
            try:
                m = int(p_montant.text or 0)
                if m <= 0:
                    popup("Erreur", "Montant invalide")
                    return
                with get_db_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT paye,reste FROM dettes WHERE id=?", (did,))
                    d = cur.fetchone()
                    if not d:
                        popup("Erreur", "Dette introuvable")
                        return
                    np = d[0] + m
                    reste = d[1] - m
                    if reste < 0:
                        popup("Erreur", "Trop eleve")
                        return
                    if not has_hybrid_module():
                        raise RuntimeError("HYBRID indisponible : paiement refuse, aucune ecriture legacy")
                    mid_row = cur.execute("SELECT id FROM magasins WHERE cle=?", (self.magasin_cle,)).fetchone()
                    mid_val = mid_row[0] if mid_row else None
                    op_id = gen_uuid()
                    payment_tx_id = record_payment(conn, did, m,
                                                   admin_id=self.user.get("user_id"),
                                                   admin_username=self.user.get("username"),
                                                   magasin_id=mid_val,
                                                   commit=False,
                                                   operation_id=op_id)
                    cur.execute("SELECT 1 FROM paiements_dettes WHERE transaction_id=?", (payment_tx_id,))
                    if not cur.fetchone():
                        stat = "SOLDE" if reste == 0 else "ACTIF"
                        cur.execute("UPDATE dettes SET paye=?,reste=?,statut=? WHERE id=?", (np, reste, stat, did))
                        try:
                            cur.execute("INSERT INTO paiements_dettes (transaction_id, dette_id, montant, device_id, operation_id) VALUES (?,?,?,?,?)",
                                        (payment_tx_id, did, m, _local_device_id(conn), op_id))
                        except sqlite3.OperationalError:
                            cur.execute("INSERT INTO paiements_dettes (transaction_id, dette_id, montant, device_id) VALUES (?,?,?,?)",
                                        (payment_tx_id, did, m, _local_device_id(conn)))
                    conn.commit()

                popup("OK", f"Paiement de {fmt(m)} Ar enregistre!")
                self.load_dettes()
            except Exception as e:
                log_error("do_pay_dette", e)
                popup("Erreur", str(e))

        btn = Button(text="PAYER", background_color=(0.2,0.8,0.4,1))
        p = Popup(title="Paiement", content=content, size_hint=(0.8, 0.35))
        btn.bind(on_press=lambda x: (do_pay(), p.dismiss()))
        content.add_widget(btn)
        p.open()

    # ---------- PAIEMENT ----------
    def show_paiement(self, inst=None):
        log("show_paiement()")
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=10, spacing=10)

            b.add_widget(Label(text=f"PAIEMENT DETTE - {mag['nom']}", font_size=26, bold=True, color=(0.9,0.5,0.2,1), size_hint_y=None, height=40))

            self.p_client = TextInput(hint_text="Nom du client", multiline=False, font_size=24)
            b.add_widget(self.p_client)

            btn_v = Button(text="VERIFIER", background_color=(0.2,0.6,0.9,1), size_hint_y=None, height=45)
            btn_v.bind(on_press=self.verif_dette)
            b.add_widget(btn_v)

            self.p_info = Label(text="", font_size=26, color=(0.15,0.15,0.2,1), size_hint_y=None, height=70)
            b.add_widget(self.p_info)

            self.p_montant = TextInput(hint_text="Montant", multiline=False, input_filter="int")
            b.add_widget(self.p_montant)

            btn_p = Button(text="ENREGISTRER", background_color=(0.2,0.8,0.4,1), size_hint_y=None, height=45)
            btn_p.bind(on_press=self.do_paiement)
            b.add_widget(btn_p)

            self.p_msg = Label(text="", color=(0.95,0.3,0.3,1))
            b.add_widget(self.p_msg)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.p_did = None
            self.root.add_widget(b)
        except Exception as e:
            log_error("show_paiement", e)

    @safe_sqlite
    def verif_dette(self, inst):
        c = self.p_client.text.strip()
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id,total,paye,reste FROM dettes WHERE LOWER(TRIM(client))=LOWER(TRIM(?)) AND statut='ACTIF'", (c,))
            d = cur.fetchone()
        if d:
            self.p_did = d[0]
            self.p_info.text = f"Total: {fmt(d[1])} Ar | Paye: {fmt(d[2])} Ar | Reste: {fmt(d[3])} Ar"
        else:
            self.p_info.text = "Client introuvable ou aucune dette ACTIVE"
            self.p_did = None

    @safe_sqlite
    def do_paiement(self, inst):
        if not self.p_did:
            self.p_msg.text = "Verifiez d'abord"
            return
        m = int(self.p_montant.text or 0)
        if m <= 0:
            self.p_msg.text = "Montant invalide"
            return
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT paye,reste FROM dettes WHERE id=?", (self.p_did,))
            d = cur.fetchone()
            if not d:
                self.p_msg.text = "Dette introuvable"
                return
            np = d[0] + m
            reste = d[1] - m
            if reste < 0:
                self.p_msg.text = "Trop eleve"
                return
            if not has_hybrid_module():
                raise RuntimeError("HYBRID indisponible : paiement refuse, aucune ecriture legacy")
            mid_row = cur.execute("SELECT id FROM magasins WHERE cle=?", (self.magasin_cle,)).fetchone()
            mid_val = mid_row[0] if mid_row else None
            op_id = getattr(self, "_pending_payment_op_id", None) or gen_uuid()
            self._pending_payment_op_id = op_id
            payment_tx_id = record_payment(conn, self.p_did, m,
                                           admin_id=self.user.get("user_id"),
                                           admin_username=self.user.get("username"),
                                           magasin_id=mid_val,
                                           commit=False,
                                           operation_id=op_id)
            # Si retry idempotent, ne pas re-incrementer paye/reste
            cur.execute("SELECT 1 FROM paiements_dettes WHERE transaction_id=?", (payment_tx_id,))
            if not cur.fetchone():
                stat = "SOLDE" if reste == 0 else "ACTIF"
                cur.execute("UPDATE dettes SET paye=?,reste=?,statut=? WHERE id=?", (np, reste, stat, self.p_did))
                cur.execute("INSERT INTO paiements_dettes (transaction_id, dette_id, montant, device_id, operation_id) VALUES (?,?,?,?,?)",
                            (payment_tx_id, self.p_did, m, _local_device_id(conn), op_id))
            conn.commit()
            self._pending_payment_op_id = None

        self.p_msg.text = "Paiement enregistre"
        self.p_info.text = ""
        self.p_montant.text = ""
        self.p_did = None

    # ---------- RAPPORTS ----------
    def show_rapports(self, inst=None):
        if not self.check_admin():
            return
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=10, spacing=10)

            b.add_widget(Label(text=f"RAPPORTS - {mag['nom']}", font_size=26, bold=True, color=(0.6,0.4,0.8,1), size_hint_y=None, height=40))

            tabs = BoxLayout(size_hint_y=None, height=45, spacing=5)
            self.r_tab = "resume"

            self.btn_tab_resume = Button(text="RESUME", background_color=(0.6,0.4,0.8,1))
            self.btn_tab_histo = Button(text="HISTORIQUE", background_color=(0.4,0.4,0.4,1))
            self.btn_tab_date = Button(text="PAR DATE", background_color=(0.4,0.4,0.4,1))
            self.btn_tab_prod = Button(text="PAR PRODUIT", background_color=(0.4,0.4,0.4,1))
            self.btn_tab_benef = Button(text="BENEFICE", background_color=(0.4,0.4,0.4,1))

            self.btn_tab_resume.bind(on_press=lambda x: self.switch_rapport_tab("resume"))
            self.btn_tab_histo.bind(on_press=lambda x: self.switch_rapport_tab("historique"))
            self.btn_tab_date.bind(on_press=lambda x: self.switch_rapport_tab("date"))
            self.btn_tab_prod.bind(on_press=lambda x: self.switch_rapport_tab("produit"))
            self.btn_tab_benef.bind(on_press=lambda x: self.switch_rapport_tab("benefice"))

            tabs.add_widget(self.btn_tab_resume)
            tabs.add_widget(self.btn_tab_histo)
            tabs.add_widget(self.btn_tab_date)
            tabs.add_widget(self.btn_tab_prod)
            tabs.add_widget(self.btn_tab_benef)
            b.add_widget(tabs)

            self.r_content = BoxLayout(orientation="vertical")
            b.add_widget(self.r_content)

            btn_box = BoxLayout(size_hint_y=None, height=50, spacing=5)
            btn_pdf = Button(text="EXPORT PDF", background_color=(0.95,0.3,0.3,1))
            btn_pdf.bind(on_press=self.export_pdf)
            btn_csv = Button(text="EXPORT CSV", background_color=(0.2,0.6,0.9,1))
            btn_csv.bind(on_press=self.export_csv)
            btn_ref = Button(text="ACTUALISER", background_color=(0.2,0.6,0.9,1))
            btn_ref.bind(on_press=self.refresh_rapports)
            btn_box.add_widget(btn_pdf)
            btn_box.add_widget(btn_csv)
            btn_box.add_widget(btn_ref)
            b.add_widget(btn_box)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
            self.switch_rapport_tab("resume")
        except Exception as e:
            log_error("show_rapports", e)

    def switch_rapport_tab(self, tab):
        if not self.check_admin():
            return
        self.r_tab = tab
        self.btn_tab_resume.background_color = (0.4,0.4,0.4,1)
        self.btn_tab_histo.background_color = (0.4,0.4,0.4,1)
        self.btn_tab_date.background_color = (0.4,0.4,0.4,1)
        self.btn_tab_prod.background_color = (0.4,0.4,0.4,1)
        self.btn_tab_benef.background_color = (0.4,0.4,0.4,1)
        if tab == "resume":
            self.btn_tab_resume.background_color = (0.6,0.4,0.8,1)
            self.show_tab_resume()
        elif tab == "historique":
            self.btn_tab_histo.background_color = (0.6,0.4,0.8,1)
            self.show_tab_historique()
        elif tab == "date":
            self.btn_tab_date.background_color = (0.6,0.4,0.8,1)
            self.show_tab_date()
        elif tab == "produit":
            self.btn_tab_prod.background_color = (0.6,0.4,0.8,1)
            self.show_tab_produit()
        elif tab == "benefice":
            self.btn_tab_benef.background_color = (0.6,0.4,0.8,1)
            self.show_tab_benefice()

    def refresh_rapports(self, inst=None):
        self.switch_rapport_tab(self.r_tab)

    def show_tab_resume(self):
        self.r_content.clear_widgets()
        scroll = ScrollView()
        box = BoxLayout(orientation="vertical", size_hint_y=None)
        box.bind(minimum_height=box.setter("height"))

        self.r_resume = Label(text="Chargement...", font_size=26, size_hint_y=None, height=120, color=(0.15,0.15,0.2,1))
        box.add_widget(self.r_resume)

        self.r_detail = Label(text="", font_size=22, size_hint_y=None)
        self.r_detail.bind(texture_size=self.r_detail.setter("size"))
        box.add_widget(self.r_detail)

        scroll.add_widget(box)
        self.r_content.add_widget(scroll)
        self.load_rapports()

    def show_tab_historique(self):
        self.r_content.clear_widgets()
        b = BoxLayout(orientation="vertical")

        h = BoxLayout(size_hint_y=None, height=45, spacing=5)
        h.add_widget(Label(text="Date:", size_hint_x=0.2))
        self.rh_date = TextInput(text=datetime.now().strftime("%Y-%m-%d"), multiline=False, size_hint_x=0.5)
        btn_f = Button(text="FILTRER", size_hint_x=0.3, background_color=(0.2,0.6,0.9,1))
        btn_f.bind(on_press=self.load_historique)
        h.add_widget(self.rh_date)
        h.add_widget(btn_f)
        b.add_widget(h)

        scroll = ScrollView()
        self.rh_list = GridLayout(cols=1, spacing=3, size_hint_y=None, padding=5)
        self.rh_list.bind(minimum_height=self.rh_list.setter("height"))
        scroll.add_widget(self.rh_list)
        b.add_widget(scroll)

        self.rh_total = Label(text="", font_size=24, bold=True, color=(0.2,0.8,0.4,1), size_hint_y=None, height=40)
        b.add_widget(self.rh_total)

        self.r_content.add_widget(b)
        self.load_historique()

    @safe_sqlite
    def load_historique(self, *args):
        date_filtre = self.rh_date.text.strip()
        with get_db_connection() as conn:
            cur = conn.cursor()
            if date_filtre:
                cur.execute("SELECT date_vente, produit_nom, quantite, prix_vente, total, vendeur, COALESCE(device_id, 'LEGACY-UNKNOWN') FROM ventes WHERE date(date_vente)=? ORDER BY date_vente DESC", (date_filtre,))
            else:
                cur.execute("SELECT date_vente, produit_nom, quantite, prix_vente, total, vendeur, COALESCE(device_id, 'LEGACY-UNKNOWN') FROM ventes ORDER BY date_vente DESC LIMIT 100")
            rows = cur.fetchall()
            cur.execute("SELECT COALESCE(SUM(total),0) FROM ventes WHERE date(date_vente)=?", (date_filtre,))
            total = cur.fetchone()[0]

        self.rh_list.clear_widgets()
        for r in rows:
            nom = r[1] if r[1] else "Produit inconnu"
            row = BoxLayout(size_hint_y=None, height=35, spacing=4)
            row.add_widget(Label(text=str(r[0])[-8:], size_hint_x=0.2, font_size=22))
            row.add_widget(Label(text=nom, size_hint_x=0.3, font_size=22))
            row.add_widget(Label(text=f"x{r[2]}", size_hint_x=0.1, font_size=22))
            row.add_widget(Label(text=f"{fmt(r[4])} Ar", size_hint_x=0.20, font_size=22))
            row.add_widget(Label(text=str(r[5]), size_hint_x=0.15, font_size=24))
            row.add_widget(Label(text=f"device: {r[6]}", size_hint_x=0.25, font_size=18))
            self.rh_list.add_widget(row)

        self.rh_total.text = f"TOTAL: {fmt(total)} Ar | {len(rows)} ventes"

    def show_tab_date(self):
        self.r_content.clear_widgets()
        b = BoxLayout(orientation="vertical")

        h = BoxLayout(size_hint_y=None, height=45, spacing=5)
        h.add_widget(Label(text="Du:", size_hint_x=0.1))
        self.rd_debut = TextInput(text=datetime.now().strftime("%Y-%m-%d"), multiline=False, size_hint_x=0.3)
        h.add_widget(self.rd_debut)
        h.add_widget(Label(text="Au:", size_hint_x=0.1))
        self.rd_fin = TextInput(text=datetime.now().strftime("%Y-%m-%d"), multiline=False, size_hint_x=0.3)
        h.add_widget(self.rd_fin)
        btn_f = Button(text="FILTRER", size_hint_x=0.2, background_color=(0.2,0.6,0.9,1))
        btn_f.bind(on_press=self.load_par_date)
        h.add_widget(btn_f)
        b.add_widget(h)

        scroll = ScrollView()
        self.rd_list = GridLayout(cols=1, spacing=3, size_hint_y=None, padding=5)
        self.rd_list.bind(minimum_height=self.rd_list.setter("height"))
        scroll.add_widget(self.rd_list)
        b.add_widget(scroll)

        self.rd_total = Label(text="", font_size=24, bold=True, color=(0.2,0.8,0.4,1), size_hint_y=None, height=40)
        b.add_widget(self.rd_total)

        self.r_content.add_widget(b)
        self.load_par_date()

    @safe_sqlite
    def load_par_date(self, *args):
        debut = self.rd_debut.text.strip()
        fin = self.rd_fin.text.strip()
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT date(date_vente), COUNT(*), COALESCE(SUM(total),0), COALESCE(SUM(benefice),0) FROM ventes WHERE date(date_vente) BETWEEN ? AND ? GROUP BY date(date_vente) ORDER BY date(date_vente) DESC", (debut, fin))
            rows = cur.fetchall()
            cur.execute("SELECT COALESCE(SUM(total),0) FROM ventes WHERE date(date_vente) BETWEEN ? AND ?", (debut, fin))
            total = cur.fetchone()[0]

        self.rd_list.clear_widgets()
        for r in rows:
            row = BoxLayout(size_hint_y=None, height=35, spacing=4)
            row.add_widget(Label(text=str(r[0]), size_hint_x=0.3, font_size=24))
            row.add_widget(Label(text=f"{r[1]} ventes", size_hint_x=0.25, font_size=24))
            row.add_widget(Label(text=f"{fmt(r[2])} Ar", size_hint_x=0.25, font_size=24))
            row.add_widget(Label(text=f"Ben:{fmt(r[3])} Ar", size_hint_x=0.2, font_size=22))
            self.rd_list.add_widget(row)

        self.rd_total.text = f"TOTAL PERIODE: {fmt(total)} Ar | {len(rows)} jours"

    def show_tab_produit(self):
        self.r_content.clear_widgets()
        b = BoxLayout(orientation="vertical")

        h = BoxLayout(size_hint_y=None, height=45, spacing=5)
        self.rp_search = TextInput(hint_text="Nom produit...", multiline=False, size_hint_x=0.7)
        btn_f = Button(text="CHERCHER", size_hint_x=0.3, background_color=(0.2,0.6,0.9,1))
        btn_f.bind(on_press=self.load_par_produit)
        h.add_widget(self.rp_search)
        h.add_widget(btn_f)
        b.add_widget(h)

        self.rp_info = Label(text="Entrez un nom de produit", font_size=26, size_hint_y=None, height=80, color=(0.15,0.15,0.2,1))
        b.add_widget(self.rp_info)

        scroll = ScrollView()
        self.rp_list = GridLayout(cols=1, spacing=3, size_hint_y=None, padding=5)
        self.rp_list.bind(minimum_height=self.rp_list.setter("height"))
        scroll.add_widget(self.rp_list)
        b.add_widget(scroll)

        self.r_content.add_widget(b)

    def show_tab_benefice(self):
        if not self.check_admin():
            return
        self.r_content.clear_widgets()
        b = BoxLayout(orientation="vertical")

        h = BoxLayout(size_hint_y=None, height=45, spacing=5)
        h.add_widget(Label(text="Du:", size_hint_x=0.1))
        self.rb_debut = TextInput(text=datetime.now().strftime("%Y-%m-%d"), multiline=False, size_hint_x=0.3)
        h.add_widget(self.rb_debut)
        h.add_widget(Label(text="Au:", size_hint_x=0.1))
        self.rb_fin = TextInput(text=datetime.now().strftime("%Y-%m-%d"), multiline=False, size_hint_x=0.3)
        h.add_widget(self.rb_fin)
        btn_f = Button(text="FILTRER", size_hint_x=0.2, background_color=(0.2,0.6,0.9,1))
        btn_f.bind(on_press=self.load_benefice)
        h.add_widget(btn_f)
        b.add_widget(h)

        self.rb_resume = Label(text="", font_size=26, size_hint_y=None, height=50, color=(0.15,0.15,0.2,1))
        b.add_widget(self.rb_resume)

        scroll = ScrollView()
        self.rb_list = GridLayout(cols=1, spacing=3, size_hint_y=None, padding=5)
        self.rb_list.bind(minimum_height=self.rb_list.setter("height"))
        scroll.add_widget(self.rb_list)
        b.add_widget(scroll)

        self.rb_total = Label(text="", font_size=24, bold=True, color=(0.2,0.8,0.4,1), size_hint_y=None, height=40)
        b.add_widget(self.rb_total)

        self.r_content.add_widget(b)
        self.load_benefice()

    @safe_sqlite
    def load_benefice(self, *args):
        if not self.check_admin():
            return
        debut = self.rb_debut.text.strip()
        fin = self.rb_fin.text.strip()
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT date(date_vente), COUNT(*), COALESCE(SUM(total),0), COALESCE(SUM(benefice),0)
                FROM ventes WHERE date(date_vente) BETWEEN ? AND ? GROUP BY date(date_vente) ORDER BY date(date_vente) DESC
            """, (debut, fin))
            rows = cur.fetchall()
            cur.execute("""
                SELECT COALESCE(SUM(total),0), COALESCE(SUM(benefice),0), COUNT(*) FROM ventes WHERE date(date_vente) BETWEEN ? AND ?
            """, (debut, fin))
            totaux = cur.fetchone()

        self.rb_list.clear_widgets()
        for r in rows:
            row = BoxLayout(size_hint_y=None, height=38, spacing=4)
            row.add_widget(Label(text=str(r[0]), size_hint_x=0.25, font_size=24))
            row.add_widget(Label(text=f"{r[1]} ventes", size_hint_x=0.2, font_size=24))
            row.add_widget(Label(text=f"{fmt(r[2])} Ar", size_hint_x=0.25, font_size=24))
            row.add_widget(Label(text=f"Ben: {fmt(r[3])} Ar", size_hint_x=0.3, font_size=24, color=(0.2,0.8,0.4,1), bold=True))
            self.rb_list.add_widget(row)

        total_v = totaux[0]
        total_b = totaux[1]
        nb_v = totaux[2]
        marge = (total_b / total_v * 100) if total_v > 0 else 0

        self.rb_resume.text = f"Periode: {debut} au {fin} | {nb_v} ventes"
        self.rb_total.text = f"TOTAL VENTES: {fmt(total_v)} Ar | BENEFICE: {fmt(total_b)} Ar | MARGE: {marge:.1f}%"

    @safe_sqlite
    def load_par_produit(self, *args):
        nom = self.rp_search.text.strip()
        if not nom:
            self.rp_info.text = "Entrez un nom de produit"
            return
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT stock, prix_achat, prix_vente, hybrid_id, magasin_ref_id FROM produits WHERE nom LIKE ? AND actif=1", (f"%{nom}%",))
            prod = cur.fetchone()
            if prod:
                prod = (canonical_display_stock(conn, prod[3], prod[4], prod[0]), prod[1], prod[2])
            cur.execute("SELECT date_vente, quantite, total, benefice FROM ventes WHERE produit_nom LIKE ? ORDER BY date_vente DESC", (f"%{nom}%",))
            rows = cur.fetchall()
            cur.execute("SELECT COALESCE(SUM(quantite),0), COALESCE(SUM(total),0), COALESCE(SUM(benefice),0) FROM ventes WHERE produit_nom LIKE ?", (f"%{nom}%",))
            totaux = cur.fetchone()

        self.rp_list.clear_widgets()
        if prod:
            is_admin = self.user and is_user_admin_hybrid(self.user.get("role"))
            pa_str = f" | PA: {fmt(prod[1])} Ar" if is_admin else ""
            ben_str = f" | Ben: {fmt(totaux[2])} Ar" if is_admin else ""
            self.rp_info.text = (f"Stock: {prod[0]}{pa_str} | PV: {fmt(prod[2])} Ar\n"
                                f"Total vendu: {totaux[0]} unites | {fmt(totaux[1])} Ar{ben_str}")
        else:
            self.rp_info.text = f"Produit '{nom}' - Total vendu: {totaux[0]} unites | {fmt(totaux[1])} Ar"

        for r in rows:
            row = BoxLayout(size_hint_y=None, height=30, spacing=4)
            row.add_widget(Label(text=str(r[0]), size_hint_x=0.35, font_size=22))
            row.add_widget(Label(text=f"x{r[1]}", size_hint_x=0.15, font_size=22))
            row.add_widget(Label(text=f"{fmt(r[2])} Ar", size_hint_x=0.25, font_size=22))
            if self.user and is_user_admin_hybrid(self.user.get("role")):
                row.add_widget(Label(text=f"Ben:{fmt(r[3])}", size_hint_x=0.25, font_size=22))
            self.rp_list.add_widget(row)

        if not rows:
            self.rp_list.add_widget(Label(text="Aucune vente pour ce produit", font_size=24))

    @safe_sqlite
    def load_rapports(self, *args):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*), COALESCE(SUM(total),0), COALESCE(SUM(benefice),0) FROM ventes WHERE date(date_vente)=date('now','localtime')")
            vj = cur.fetchone()

            nb_ventes_jour = vj[0]
            total_ventes_jour = vj[1]
            total_benefice_jour = vj[2]

            taux_marge_jour = (total_benefice_jour / total_ventes_jour * 100) if total_ventes_jour > 0 else 0
            panier_moyen_jour = total_ventes_jour / nb_ventes_jour if nb_ventes_jour > 0 else 0

            cur.execute("SELECT COUNT(*), COALESCE(SUM(total),0), COALESCE(SUM(benefice),0) FROM ventes WHERE strftime('%Y-%m',date_vente)=strftime('%Y-%m','now','localtime')")
            vm = cur.fetchone()
            cur.execute("SELECT COALESCE(produit_nom,'Produit inconnu'), SUM(quantite), SUM(total) FROM ventes GROUP BY produit_nom ORDER BY SUM(quantite) DESC LIMIT 10")
            top = cur.fetchall()
            cur.execute("SELECT hybrid_id, magasin_ref_id, stock, prix_vente FROM produits WHERE actif=1")
            stock_rows = cur.fetchall()
            canonical_stocks = canonical_display_stocks_batch(conn, [(row[0], row[1], row[2]) for row in stock_rows])
            st = (len(stock_rows), sum(canonical_stocks), sum(stock * row[3] for stock, row in zip(canonical_stocks, stock_rows)))
            alert = sum(1 for stock in canonical_stocks if stock <= 0)

        cash_info = ""
        try:
            if has_hybrid_module():
                with get_db_connection() as conn2:
                    rep = get_daily_cash_report(conn2)
                cash_info = (
                    f"\nCAISSE: comptant {fmt(rep['comptant_total'])} Ar"
                    f" | credit {fmt(rep['credit_total'])} Ar"
                    f" | recouvrements {fmt(rep['recouvrements_total'])} Ar"
                    f"\nCAISSE REELLE: {fmt(rep['caisse_reelle'])} Ar"
                )
        except Exception:
            cash_info = ""
        now_str = datetime.now().strftime("%d/%m/%Y %H:%M")
        self.r_resume.text = (
            f"DATE: {now_str}\n"
            f"AUJOURD'HUI: {vj[0]} ventes | {fmt(vj[1])} Ar | Ben: {fmt(vj[2])} Ar\n"
            f"  Marge: {taux_marge_jour:.1f}% | Panier moyen: {fmt(panier_moyen_jour)} Ar/vente\n"
            f"CE MOIS: {vm[0]} ventes | {fmt(vm[1])} Ar | Ben: {fmt(vm[2])} Ar\n"
            f"STOCK: {st[0]} produits | {fmt(st[1])} unites | {fmt(st[2])} Ar\n"
            f"VIDE: {alert} produits"
            f"{cash_info}"
        )

        txt = "TOP 10 PRODUITS:\n\n"
        for i, p in enumerate(top, 1):
            nom_prod = p[0] if p[0] else "Produit inconnu"
            txt += f"{i}. {nom_prod} - {p[1]} vendus ({fmt(p[2])} Ar)\n"
        if not top:
            txt += "Aucune vente enregistree.\n"
        self.r_detail.text = txt

    def export_pdf(self, inst):
        role = self.user.get("role", "VENDEUR") if self.user else "VENDEUR"
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
            mag = self.get_magasin_info()
            fn = f"/storage/emulated/0/FANEVA_RAPPORT_{mag['nom']}_{role}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            headers, query = get_export_config(role, "ventes")

            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()

            os.makedirs(os.path.dirname(fn), exist_ok=True)
            c = canvas.Canvas(fn, pagesize=A4)
            y = 800
            c.drawString(50, y, f"KDK SYSTEM - RAPPORT VENTES {mag['nom']} ({role})")
            y -= 25
            c.drawString(50, y, f"Date: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
            y -= 35

            header_line = " | ".join(headers)
            c.drawString(40, y, header_line[:90])
            y -= 15
            c.line(40, y, 550, y)
            y -= 20

            for r in rows[:100]:
                if y < 50:
                    c.showPage()
                    y = 800
                line_str = " | ".join([str(val) if val is not None else "" for val in r])
                c.drawString(40, y, line_str[:95])
                y -= 18

            c.save()
            popup("Succes", f"PDF cree:\n{fn}")
        except Exception as e:
            log_error("export_pdf", e)
            popup("Erreur Export PDF", str(e))

    def export_csv(self, inst):
        role = self.user.get("role", "VENDEUR") if self.user else "VENDEUR"
        try:
            import csv
            mag = self.get_magasin_info()
            fn = f"/storage/emulated/0/FANEVA_RAPPORT_{mag['nom']}_{role}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            headers, query = get_export_config(role, "ventes")

            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()

            os.makedirs(os.path.dirname(fn), exist_ok=True)
            with open(fn, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(headers)
                for r in rows:
                    w.writerow(list(r))
            popup("Succes", f"CSV cree:\n{fn}")
        except Exception as e:
            log_error("export_csv", e)
            popup("Erreur Export CSV", str(e))

    # ---------- BACKUP ----------
    def show_backup(self, inst=None):
        if not self.check_admin():
            return
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=15, spacing=12)

            b.add_widget(Label(text=f"BACKUP - {mag['nom']}", font_size=26, bold=True, color=(0.5,0.5,0.9,1), size_hint_y=None, height=40))

            self.b_info = Label(text="Pret", font_size=26, size_hint_y=None, height=50)
            b.add_widget(self.b_info)

            btn_local = Button(text="BACKUP LOCAL", background_color=(0.2,0.6,0.9,1), size_hint_y=None, height=55)
            btn_local.bind(on_press=self.backup_local)
            b.add_widget(btn_local)

            btn_cloud = Button(text="BACKUP CLOUD", background_color=(0.3,0.6,0.9,1), size_hint_y=None, height=55)
            btn_cloud.bind(on_press=self.backup_cloud)
            b.add_widget(btn_cloud)

            btn_restore = Button(text="RESTAURER", background_color=(1,0.7,0.2,1), size_hint_y=None, height=55)
            btn_restore.bind(on_press=self.restore_backup)
            b.add_widget(btn_restore)

            b.add_widget(Label())

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
        except Exception as e:
            log_error("show_backup", e)

    def backup_local(self, inst):
        if not self.check_admin():
            return
        try:
            if not DB_PATH or not os.path.exists(DB_PATH):
                popup("Erreur", "Base de donnees introuvable")
                return
            os.makedirs(BACKUP_DIR, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = os.path.join(BACKUP_DIR, f"backup_{MAGASIN_ACTIF}_{ts}.db")
            shutil.copy2(DB_PATH, dest)
            self.b_info.text = f"Backup cree : {os.path.basename(dest)}"
            popup("Succes", f"Sauvegarde locale reussie:\n{dest}")
        except Exception as e:
            log_error("backup_local", e)
            popup("Erreur Backup", str(e))

    def backup_cloud(self, inst):
        if not self.check_admin():
            return
        # --- FANEVA SYSTEM HYBRID : backup distant reel (HTTPS) si serveur configure ---
        if has_hybrid_module():
            try:
                with get_db_connection() as conn:
                    connetivite = detect_connectivity(SYNC_SERVER_URL)
                    if BACKUP_REMOTE and connetivite == "INTERNET" and DB_PATH and os.path.exists(DB_PATH):
                        ok = backup_remote(conn, SYNC_SERVER_URL, DB_PATH)
                        if ok:
                            self.b_info.text = "Backup distant envoye avec succes (HTTPS)"
                            popup("Succes", "Sauvegarde distante envoyee sur le serveur de relais!")
                            return
                        else:
                            self.b_info.text = "Echec de l'envoi distant ; backup local effectue"
                    else:
                        self.b_info.text = f"Hors ligne ({connetivite}) ; backup local effectue"
            except Exception as e:
                log(f"HYBRID backup_cloud: {e}")
        self.backup_local(inst)
        self.b_info.text = "Backup local cree (distant indisponible hors ligne)"

    def restore_backup(self, inst):
        if not self.check_admin():
            return
        try:
            if not os.path.exists(BACKUP_DIR):
                popup("Erreur", "Aucun dossier de sauvegarde trouve")
                return
            backups = [f for f in os.listdir(BACKUP_DIR) if f.endswith(".db")]
            if not backups:
                popup("Erreur", "Aucun fichier de backup (.db) trouve")
                return
            backups.sort(reverse=True)
            latest = os.path.join(BACKUP_DIR, backups[0])
            shutil.copy2(latest, DB_PATH)
            popup("Succes", f"Base restauree avec succes depuis :\n{backups[0]}")
            self.update_stats()
        except Exception as e:
            log_error("restore_backup", e)
            popup("Erreur Restauration", str(e))

    # ---------- PARAMETRES ----------
    def show_params(self, inst=None):
        if not self.check_admin():
            return
        try:
            self.clear()
            mag = self.get_magasin_info()
            b = BoxLayout(orientation="vertical", padding=15, spacing=10)

            b.add_widget(Label(text=f"PARAMETRES - {mag['nom']}", font_size=26, bold=True, color=(0.5,0.5,0.5,1), size_hint_y=None, height=40))

            b.add_widget(Label(text="UTILISATEURS", font_size=24, bold=True, size_hint_y=None, height=30))

            scroll = ScrollView()
            self.u_list = GridLayout(cols=1, spacing=4, size_hint_y=None, padding=5)
            self.u_list.bind(minimum_height=self.u_list.setter("height"))
            scroll.add_widget(self.u_list)
            b.add_widget(scroll)

            btn_add_u = Button(text="+ UTILISATEUR", background_color=(0.2,0.8,0.4,1), size_hint_y=None, height=45)
            btn_add_u.bind(on_press=self.show_add_user)
            b.add_widget(btn_add_u)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            b.add_widget(btn_back)

            self.root.add_widget(b)
            self.load_users()
        except Exception as e:
            log_error("show_params", e)

    @safe_sqlite
    def load_users(self):
        with get_db_connection() as conn:
            cur = conn.cursor()
            # --- FANEVA SYSTEM HYBRID : source de verite = utilisateurs_hybrid ---
            if has_hybrid_module() and _table_exists_hybrid("utilisateurs_hybrid"):
                cur.execute("SELECT id, username, nom_complet, role, actif FROM utilisateurs_hybrid ORDER BY username")
            else:
                cur.execute("SELECT id, username, nom_complet, role, actif FROM utilisateurs ORDER BY username")
            rows = cur.fetchall()

        self.u_list.clear_widgets()
        for u in rows:
            uid, uname, full_name, role, actif = u
            row = BoxLayout(size_hint_y=None, height=40, spacing=5)
            row.add_widget(Label(text=f"{uname} ({full_name or 'N/A'}) - {role}", size_hint_x=0.7, font_size=22))
            stat_text = "ACTIF" if actif == 1 else "INACTIF"
            btn_stat = Button(text=stat_text, size_hint_x=0.3, background_color=(0.2,0.8,0.4,1) if actif == 1 else (0.8,0.2,0.2,1))
            btn_stat.bind(on_press=partial(self.toggle_user, uid, actif))
            row.add_widget(btn_stat)
            self.u_list.add_widget(row)

    @safe_sqlite
    def toggle_user(self, uid, current_actif, inst):
        new_actif = 0 if current_actif == 1 else 1
        with get_db_connection() as conn:
            cur = conn.cursor()
            if has_hybrid_module() and _table_exists_hybrid("utilisateurs_hybrid"):
                cur.execute("UPDATE utilisateurs_hybrid SET actif=? WHERE id=?", (new_actif, uid))
                cur.execute("UPDATE utilisateurs SET actif=? WHERE hybrid_id=?", (new_actif, uid))
            else:
                cur.execute("UPDATE utilisateurs SET actif=? WHERE id=?", (new_actif, uid))
            conn.commit()
        self.load_users()

    # ---------- FANEVA SYSTEM HYBRID : SYNCHRONISATION ----------
    @safe_sqlite
    def show_sync(self, inst=None):
        log("show_sync() START")
        try:
            self.clear()
            b = ScrollView()
            root = BoxLayout(orientation="vertical", padding=15, spacing=10)
            root.bind(minimum_height=root.setter("height"))

            root.add_widget(Label(text="SYNCHRONISATION HYBRID", font_size=30, bold=True, color=(0.2,0.7,0.7,1)))
            root.add_widget(Label(text="Environnement : Cloudflare Workers + D1 — STAGING", font_size=18, color=(0.35,0.65,0.95,1)))
            root.add_widget(Label(text="File d'attente : operations a synchroniser", font_size=20, color=(0.5,0.5,0.5,1)))

            with get_db_connection() as conn:
                n = len(normal_pending_transactions(conn))
                identity = device_identity_status(conn)
                migration_locked = identity["migration_locked"]
                server_state = authenticated_staging_status(conn, SYNC_SERVER_URL)
                connetivite = server_state["connectivity"]
                auth_category = server_state["category"]
                dev = identity["device_id"]
                api_key_configured = has_server_api_key(conn)
                migration_session = get_migration_session(conn) if migration_locked else None

            if connetivite == "INTERNET":
                col = (0.2, 0.7, 0.3, 1)
            elif connetivite == "LOCAL":
                col = (0.9, 0.6, 0.1, 1)
            elif migration_locked:
                col = (0.9, 0.3, 0.3, 1)
            else:
                col = (0.9, 0.3, 0.3, 1)
            status_label = "ONLINE" if server_state["online"] else "OFFLINE"
            self.lbl_sync_status = Label(text=f"Statut: {status_label} | Auth: {auth_category} | En attente: {n} | Device: {dev}",
                                         font_size=20, color=col)
            root.add_widget(self.lbl_sync_status)
            root.add_widget(Label(text=("Cle API appareil: CONFIGUREE" if api_key_configured
                                       else "Cle API appareil: A CONFIGURER (aucune synchronisation possible)"),
                                  font_size=18, color=(0.2, 0.7, 0.3, 1) if api_key_configured else (0.95, 0.55, 0.15, 1)))

            if migration_locked:
                root.add_widget(Label(
                    text=("SYNCHRONISATION UUID NORMALE UNIQUEMENT\n"
                          "Historiques protégés — migration verrouillée.\n"
                          "Les transactions legacy restent exclues ; le chemin de migration historique demeure séparé et bloqué."),
                    font_size=18, color=(0.95, 0.55, 0.15, 1), size_hint_y=None, height=115))
                btn_manifest = Button(text="EXPORTER LE MANIFESTE DE MIGRATION (SANS SYNC)", font_size=19,
                                      bold=True, background_color=(0.55,0.35,0.1,1), size_hint_y=None, height=60)
                btn_manifest.bind(on_press=self.do_export_migration_manifest)
                root.add_widget(btn_manifest)
                session_ready = bool(migration_session and migration_session.get("migration_id"))
                root.add_widget(Label(
                    text=("Session de migration: CONFIGUREE — pilote UUID obligatoire avant le transfert historique"
                          if session_ready else "Session de migration: A CONFIGURER après validation serveur du manifeste"),
                    font_size=17, color=(0.2,0.7,0.3,1) if session_ready else (0.95,0.55,0.15,1)))
                btn_session = Button(text="CONFIGURER SESSION DE MIGRATION (SANS SYNC)", font_size=18,
                                     bold=True, background_color=(0.45,0.35,0.7,1), size_hint_y=None, height=55)
                btn_session.bind(on_press=self.show_configure_migration_session)
                root.add_widget(btn_session)
                btn_pilot_diagnostic = Button(text="DIAGNOSTIQUER LA BASE DU PILOTE (SANS SYNC)", font_size=18,
                                              bold=True, background_color=(0.36,0.42,0.55,1), size_hint_y=None, height=55)
                btn_pilot_diagnostic.bind(on_press=self.show_pilot_database_diagnostic)
                root.add_widget(btn_pilot_diagnostic)
                btn_pilot = Button(text="TESTER UNE TRANSACTION PILOTE UUID", font_size=18,
                                   bold=True, background_color=(0.2,0.55,0.65,1), size_hint_y=None, height=55)
                btn_pilot.disabled = True
                btn_pilot.bind(on_press=self.show_migration_pilot)
                root.add_widget(btn_pilot)
                btn_history = Button(text="TRANSMETTRE LES TRANSACTIONS HISTORIQUES AUDITEES", font_size=17,
                                     bold=True, background_color=(0.65,0.42,0.15,1), size_hint_y=None, height=55)
                btn_history.disabled = True
                btn_history.bind(on_press=self.confirm_sync_migration_historical)
                root.add_widget(btn_history)
                btn_finalize = Button(text="FINALISER MIGRATION APRES ACK COMPLETS", font_size=17,
                                      bold=True, background_color=(0.25,0.55,0.35,1), size_hint_y=None, height=55)
                btn_finalize.disabled = True
                btn_finalize.bind(on_press=self.confirm_complete_migration)
                root.add_widget(btn_finalize)

            btn_api_key = Button(text="CONFIGURER LA CLE API PROVISIONNEE (SANS SYNC)", font_size=18,
                                 bold=True, background_color=(0.35,0.35,0.65,1), size_hint_y=None, height=55)
            btn_api_key.bind(on_press=self.show_configure_sync_api_key)
            root.add_widget(btn_api_key)

            btn_auth_validate = Button(text="VERIFIER L'AUTHENTIFICATION (SANS SYNC)", font_size=18,
                                       bold=True, background_color=(0.18,0.48,0.56,1), size_hint_y=None, height=55)
            btn_auth_validate.bind(on_press=self.do_validate_server_authentication)
            root.add_widget(btn_auth_validate)

            btn_sync_net = Button(text="SYNCHRONISER VIA CLOUDFLARE STAGING", font_size=22, bold=True,
                                  background_color=(0.2,0.6,0.9,1), size_hint_y=None, height=60)
            btn_sync_net.disabled = False
            btn_sync_net.bind(on_press=self.do_sync_internet)
            root.add_widget(btn_sync_net)

            root.add_widget(Label(text="--- Synchronisation locale Wi-Fi / Hotspot ---", font_size=20, color=(0.5,0.5,0.5,1)))
            self.in_peer = TextInput(hint_text="Adresse IP de l'autre appareil (ex: 192.168.1.5)", multiline=False)
            root.add_widget(self.in_peer)

            btn_sync_local = Button(text="SYNCHRONISER (Wi-Fi/Hotspot)", font_size=22, bold=True,
                                    background_color=(0.7,0.55,0.2,1), size_hint_y=None, height=60)
            btn_sync_local.disabled = False
            btn_sync_local.bind(on_press=self.do_sync_local)
            root.add_widget(btn_sync_local)

            root.add_widget(Label(text="--- Appareils connus ---", font_size=20, color=(0.5,0.5,0.5,1)))
            self.sync_devices = GridLayout(cols=1, spacing=5, size_hint_y=None)
            self.sync_devices.bind(minimum_height=self.sync_devices.setter("height"))
            with get_db_connection() as conn:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT device_id, nom, derniere_sync FROM devices ORDER BY derniere_sync DESC LIMIT 10")
                    for d_id, d_nom, d_last in cur.fetchall():
                        self.sync_devices.add_widget(Label(text=f"{d_id[:12]}... | {d_nom or 'peer'} | {d_last or '-'}", font_size=18))
                except Exception:
                    pass
            root.add_widget(self.sync_devices)

            root.add_widget(Label())
            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            root.add_widget(btn_back)

            b.add_widget(root)
            self.root.add_widget(b)
            log("show_sync() OK")
        except Exception as e:
            log_error("show_sync", e)

    def do_validate_server_authentication(self, inst=None):
        """Lance uniquement GET /auth/validate ; aucune donnée métier ne peut être transmise."""
        if not self.check_admin():
            return
        try:
            with get_db_connection() as conn:
                result = validate_server_authentication(conn, SYNC_SERVER_URL)
            status = result.get("http_status")
            status_text = str(status) if status is not None else "NON EFFECTUÉ"
            authenticated = "true" if result.get("authenticated") else "false"
            message = result.get("error")
            details = (
                f"Endpoint: {result.get('auth_url') or '-'}\n"
                f"HTTP status: {status_text}\n"
                f"Device ID présent: {'YES' if result.get('device_id_present') else 'NO'}\n"
                f"UUID v4 valide: {'YES' if result.get('device_id_uuid_v4') else 'NO'}\n"
                f"Clé API présente: {'YES' if result.get('api_key_present') else 'NO'}\n"
                f"CA embarqué présent: {'YES' if result.get('ca_bundle_present') else 'NO'}\n"
                f"Transport auth: {result.get('transport') or 'NON EFFECTUÉ'}\n"
                f"Fallback Android tenté: {'YES' if result.get('android_fallback_attempted') else 'NO'}\n"
                f"Mode TLS: {result.get('ca_mode') or 'NON EFFECTUÉ'}\n"
                f"Catégorie: {result.get('category') or 'UNKNOWN'}\n"
                f"Exception: {result.get('exception_type') or '-'}\n"
                f"authenticated: {authenticated}"
            )
            if message:
                details += f"\nMessage: {message}"
            details += "\n\nContrôle explicitement non mutateur : aucun /sync, aucun /backup, aucune transaction, aucun ACK et aucune modification locale."
            popup("Vérification d’authentification", details)
        except Exception:
            popup("Vérification d’authentification", "HTTP status: NON EFFECTUÉ\nCatégorie: UNKNOWN\nauthenticated: false\nMessage: Vérification impossible sans détail sensible.\n\nAucune synchronisation n’a été lancée.")

    def do_sync_internet(self, inst=None):
        if not self.check_admin():
            return
        try:
            with get_db_connection() as conn:
                if not has_server_api_key(conn):
                    popup("Cle API requise", "Provisionnez cet UUID sur le serveur, puis configurez sa cle API ici. Aucune transaction n'a ete envoyee.")
                    return
                n = len(normal_pending_transactions(conn))
                server_state = authenticated_staging_status(conn, SYNC_SERVER_URL)
                if not server_state["online"]:
                    popup("Serveur non prêt", f"Synchronisation non lancée. Statut: OFFLINE\nAuthentification: {server_state['category']}\nLes {n} opérations restent dans la file locale ; aucune synchronisation automatique n’est déclenchée.")
                    return
                # Une file locale vide ne signifie pas qu'aucune transaction distante
                # n'est disponible. Le sync normal doit toujours envoyer le curseur
                # afin d'effectuer le pull et le replay idempotent éventuels.
                res = sync_internet(conn, SYNC_SERVER_URL)
            if res.ok():
                popup("Succes", f"Synchronisation Internet reussie!\nEnvoyees: {res.envoyees}\nRecues: {res.recues}\nIgnorees (deja connues): {res.ignorees}")
            else:
                diag = getattr(res, "diagnostic", {}) or {}
                details = f"Synchronisation Internet echouee:\n" + "\n".join(res.erreurs[:3])
                details += (
                    f"\n\nDiagnostic technique non sensible:\n"
                    f"URL: {diag.get('sync_url', 'NON EFFECTUE')}\n"
                    f"Methode: {diag.get('method', 'POST')}\n"
                    f"Transport: {diag.get('transport', 'NON EFFECTUE')}\n"
                    f"HTTP: {diag.get('http_status', 'NON EFFECTUE')}\n"
                    f"User-Agent: {diag.get('user_agent', 'NON EFFECTUE')}\n"
                    f"X-Device-Id present: {'OUI' if diag.get('x_device_id_present') else 'NON'}\n"
                    f"X-Api-Key presente: {'OUI' if diag.get('x_api_key_present') else 'NON'}\n"
                    f"Content-Type: {diag.get('content_type', 'NON EFFECTUE')}\n"
                    f"Serveur: {diag.get('response_server', 'NON EFFECTUE')}\n"
                    f"Code serveur: {diag.get('server_code', 'NON EFFECTUE')}\n"
                    f"\nAucune transaction locale n'a ete supprimee sans ACK verifie."
                )
                popup("Echec", details)
            self.show_sync()
        except Exception as e:
            log_error("do_sync_internet", e)
            popup("Erreur", str(e))

    def show_configure_sync_api_key(self, inst=None):
        """Saisie locale de la clé fournie une seule fois par le provisionnement administrateur."""
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=12, spacing=10)
        content.add_widget(Label(text="Collez la cle API de cet appareil.\nElle n'est jamais affichee ni envoyee avant une action manuelle de synchronisation.",
                                 font_size=17))
        # Champ explicitement de type texte pour rétablir le menu Android appui long → Coller,
        # tout en gardant les caractères masqués à l'écran.
        in_key = TextInput(
            hint_text="fsv_...",
            password=True,
            multiline=False,
            font_size=18,
            input_type="text",
            use_bubble=True,
            use_handles=True,
        )
        content.add_widget(in_key)
        paste_status = Label(text="", font_size=15, color=(0.75, 0.85, 0.95, 1), size_hint_y=None, height=28)

        def paste_key_from_clipboard(_inst):
            """Colle localement dans le champ masqué, sans journaliser ni envoyer la clé."""
            try:
                pasted_value = (Clipboard.paste() or "").strip()
                if not pasted_value:
                    paste_status.text = "Presse-papiers vide : copiez d'abord la clé API depuis le serveur."
                    return
                in_key.text = pasted_value
                in_key.focus = True
                paste_status.text = "Clé collée dans le champ masqué. Aucune requête réseau n'a été envoyée."
            except Exception:
                paste_status.text = "Collage Android indisponible. Utilisez l'appui long dans le champ puis « Coller »."

        btn_paste = Button(
            text="COLLER DEPUIS LE PRESSE-PAPIERS",
            size_hint_y=None,
            height=48,
            background_color=(0.20, 0.45, 0.75, 1),
        )
        btn_paste.bind(on_press=paste_key_from_clipboard)
        content.add_widget(btn_paste)
        content.add_widget(paste_status)
        buttons = BoxLayout(size_hint_y=None, height=52, spacing=10)
        pop = Popup(title="Cle API FANEVA", content=content, size_hint=(0.9, 0.52), auto_dismiss=False)

        def save_key(_inst):
            try:
                with get_db_connection() as conn:
                    set_server_api_key(conn, in_key.text)
                pop.dismiss()
                popup("Cle enregistree", "Cle API configuree localement. La migration reste bloquee et aucune synchronisation n'a ete lancee.")
                self.show_sync()
            except Exception as exc:
                popup("Cle invalide", str(exc))

        btn_save = Button(text="ENREGISTRER SANS SYNC", background_color=(0.2,0.6,0.4,1))
        btn_cancel = Button(text="ANNULER", background_color=(0.5,0.3,0.3,1))
        btn_save.bind(on_press=save_key)
        btn_cancel.bind(on_press=lambda _inst: pop.dismiss())
        buttons.add_widget(btn_save)
        buttons.add_widget(btn_cancel)
        content.add_widget(buttons)
        pop.open()

    def show_configure_migration_session(self, inst=None):
        """Enregistre seulement les références d’audit fournies par l’administrateur serveur."""
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=12, spacing=8)
        content.add_widget(Label(text="Copiez la session UUID et le SHA-256 du manifeste validé par le serveur.\nCette étape n’envoie aucune transaction.", font_size=16))
        in_session = TextInput(hint_text="Session UUID de migration", multiline=False, font_size=17)
        in_manifest = TextInput(hint_text="SHA-256 du manifeste (64 caractères)", multiline=False, font_size=16)
        content.add_widget(in_session)
        content.add_widget(in_manifest)
        buttons = BoxLayout(size_hint_y=None, height=52, spacing=10)
        pop = Popup(title="Session de migration auditee", content=content, size_hint=(0.94, 0.56), auto_dismiss=False)

        def save_session(_inst):
            try:
                with get_db_connection() as conn:
                    set_migration_session(conn, in_session.text, in_manifest.text)
                pop.dismiss()
                popup("Session enregistree", "Session locale enregistrée. Aucun envoi n’a eu lieu : sélectionnez maintenant une seule transaction pilote UUID.")
                self.show_sync()
            except Exception as exc:
                popup("Session invalide", str(exc))

        save = Button(text="ENREGISTRER SANS SYNC", background_color=(0.2,0.6,0.4,1))
        cancel = Button(text="ANNULER", background_color=(0.5,0.3,0.3,1))
        save.bind(on_press=save_session)
        cancel.bind(on_press=lambda _inst: pop.dismiss())
        buttons.add_widget(save)
        buttons.add_widget(cancel)
        content.add_widget(buttons)
        pop.open()

    def show_pilot_database_diagnostic(self, inst=None):
        """Affiche le fichier SQLite réellement consulté, sans pilote ni réseau."""
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=12, spacing=10)
        content.add_widget(Label(
            text="Diagnostic local uniquement : aucun /sync, ACK, backup ou pilote ne sera lancé.",
            font_size=16,
        ))
        content.add_widget(Label(
            text="UUID vérifié exclusivement : " + DIAGNOSTIC_PILOT_UUID,
            font_size=15,
        ))
        buttons = BoxLayout(size_hint_y=None, height=52, spacing=10)
        pop = Popup(title="Diagnostic base SQLite du pilote", content=content, size_hint=(0.94, 0.48), auto_dismiss=False)

        def run_diagnostic(_inst):
            try:
                report = diagnose_pilot_database(
                    DB_PATH, DIAGNOSTIC_PILOT_UUID, DIAGNOSTIC_VALIDATED_DB_SHA256,
                )
                candidate_paths = []
                for key, info in MAGASINS.items():
                    external_path = info.get("db")
                    internal_path = os.path.join(INTERNAL_DIR, f"stock_{key}.db")
                    for location, candidate in (("externe", external_path), ("interne", internal_path)):
                        exists = bool(candidate and os.path.isfile(candidate))
                        candidate_paths.append(
                            f"{key}/{location}: {os.path.abspath(candidate) if candidate else '-'} | "
                            f"{'PRESENT' if exists else 'absent'}"
                        )
                log(
                    "DIAG PILOTE SQLite | path=%s | sha256=%s | tx=%s | pending=%s | join=%s"
                    % (
                        report.get("real_path"), report.get("sha256"), report.get("transactions_count"),
                        report.get("pending_sync_count"), len(report.get("pilot_join_rows", [])),
                    )
                )
                comparison = (
                    "BASE RUNTIME IDENTIQUE A LA COPIE VALIDEE"
                    if report.get("matches_expected_sha256") is True
                    else "BASE RUNTIME DIFFERENTE DE LA COPIE VALIDEE"
                )
                tx_rows = report.get("transaction_rows", [])
                pending_rows = report.get("pending_sync_rows", [])
                result_text = (
                    "MODE : LECTURE SEULE — aucun réseau appelé\n"
                    f"Magasin actif : {MAGASIN_ACTIF or '-'}\n"
                    f"DB_PATH logique : {DB_PATH or '-'}\n"
                    f"Fichier ouvert : {report.get('filename', '-') }\n"
                    f"Chemin absolu : {report.get('real_path', '-') }\n"
                    f"Fichier présent : {'OUI' if report.get('exists') else 'NON'}\n"
                    f"Taille : {report.get('size_bytes', '-') } octets\n"
                    f"SHA-256 : {report.get('sha256', '-') }\n"
                    f"SHA-256 copie validée : {DIAGNOSTIC_VALIDATED_DB_SHA256}\n"
                    f"COMPARAISON : {comparison}\n"
                    f"transactions : {report.get('transactions_count', '-') }\n"
                    f"pending_sync : {report.get('pending_sync_count', '-') }\n"
                    f"Jointure globale statut LOCAL : {report.get('pilot_join_total', '-') }\n"
                    f"UUID dans transactions : {'OUI' if tx_rows else 'NON'}\n"
                    f"statut_local : {tx_rows[0].get('statut_local') if tx_rows else '-'}\n"
                    f"UUID dans pending_sync : {'OUI' if pending_rows else 'NON'}\n"
                    f"UUID dans jointure pilote : {'OUI' if report.get('pilot_join_rows') else 'NON'}\n"
                    f"Fichier -wal : {'PRESENT' if report.get('wal', {}).get('exists') else 'absent'}\n"
                    f"Fichier -shm : {'PRESENT' if report.get('shm', {}).get('exists') else 'absent'}\n\n"
                    "Bases candidates :\n" + "\n".join(candidate_paths)
                )
                if report.get("error"):
                    result_text += "\n\nErreur : " + report["error"]
                output = BoxLayout(orientation="vertical", padding=10, spacing=8)
                readonly_report = TextInput(
                    text=result_text, readonly=True, multiline=True, font_size=14,
                )
                close = Button(text="FERMER", size_hint_y=None, height=52)
                output.add_widget(readonly_report)
                output.add_widget(close)
                report_popup = Popup(
                    title="Résultat diagnostic SQLite — sans sync",
                    content=output, size_hint=(0.96, 0.92), auto_dismiss=False,
                )
                close.bind(on_press=report_popup.dismiss)
                report_popup.open()
            except Exception as exc:
                log_error("show_pilot_database_diagnostic", exc)
                popup("Diagnostic SQLite", "Diagnostic local impossible : " + type(exc).__name__)
            finally:
                pop.dismiss()

        run = Button(text="EXECUTER LE DIAGNOSTIC LOCAL", background_color=(0.36,0.42,0.55,1))
        cancel = Button(text="ANNULER", background_color=(0.5,0.3,0.3,1))
        run.bind(on_press=run_diagnostic)
        cancel.bind(on_press=lambda _inst: pop.dismiss())
        buttons.add_widget(run)
        buttons.add_widget(cancel)
        content.add_widget(buttons)
        pop.open()

    def show_migration_pilot(self, inst=None):
        """Demande un UUID exact du manifeste, afin de n’envoyer qu’une transaction pilote."""
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=12, spacing=10)
        content.add_widget(Label(text="Entrez un seul UUID de transaction présent dans le manifeste.\nAucune autre transaction ne sera envoyée.", font_size=16))
        in_uuid = TextInput(hint_text="UUID transaction pilote", multiline=False, font_size=17)
        content.add_widget(in_uuid)
        buttons = BoxLayout(size_hint_y=None, height=52, spacing=10)
        pop = Popup(title="Transaction pilote auditée", content=content, size_hint=(0.92, 0.44), auto_dismiss=False)

        def send_pilot(_inst):
            try:
                with get_db_connection() as conn:
                    before = pending_count(conn)
                    res = sync_migration_pilot(conn, SYNC_SERVER_URL, in_uuid.text.strip())
                    after = pending_count(conn)
                if res.ok() and res.envoyees == 1 and before - after == 1:
                    pop.dismiss()
                    popup("Pilote acquitte", "ACK UUID serveur validé. Une seule transaction a été retirée de la queue; le reste demeure bloqué jusqu’à votre confirmation explicite.")
                    self.show_sync()
                else:
                    popup("Pilote non transmis", "Aucune autre transaction n’a été modifiée.\n" + "\n".join(res.erreurs[:3]))
            except Exception as exc:
                popup("Erreur pilote", str(exc))

        send = Button(text="ENVOYER 1 UUID", background_color=(0.2,0.55,0.65,1))
        cancel = Button(text="ANNULER", background_color=(0.5,0.3,0.3,1))
        send.bind(on_press=send_pilot)
        cancel.bind(on_press=lambda _inst: pop.dismiss())
        buttons.add_widget(send)
        buttons.add_widget(cancel)
        content.add_widget(buttons)
        pop.open()

    def confirm_sync_migration_historical(self, inst=None):
        """Dernière confirmation avant le transfert manuel du lot historique restant audité."""
        if not self.check_admin():
            return
        content = BoxLayout(orientation="vertical", padding=12, spacing=10)
        content.add_widget(Label(text="Le pilote a été acquitté. Confirmez-vous l’envoi MANUEL des transactions historiques restantes du manifeste ?\nAucune synchronisation automatique n’est activée.", font_size=16))
        buttons = BoxLayout(size_hint_y=None, height=52, spacing=10)
        pop = Popup(title="Confirmer le transfert historique", content=content, size_hint=(0.92, 0.42), auto_dismiss=False)

        def send_history(_inst):
            try:
                with get_db_connection() as conn:
                    res = sync_migration_historical(conn, SYNC_SERVER_URL)
                pop.dismiss()
                if res.ok():
                    popup("Historique acquitte", f"ACK UUID validés pour {res.envoyees} transaction(s). La finalisation reste une action séparée.")
                else:
                    popup("Historique non transmis", "La queue reste intacte pour les UUID sans ACK.\n" + "\n".join(res.erreurs[:3]))
                self.show_sync()
            except Exception as exc:
                popup("Erreur migration", str(exc))

        send = Button(text="CONFIRMER L’ENVOI MANUEL", background_color=(0.65,0.42,0.15,1))
        cancel = Button(text="ANNULER", background_color=(0.5,0.3,0.3,1))
        send.bind(on_press=send_history)
        cancel.bind(on_press=lambda _inst: pop.dismiss())
        buttons.add_widget(send)
        buttons.add_widget(cancel)
        content.add_widget(buttons)
        pop.open()

    def confirm_complete_migration(self, inst=None):
        """Déverrouille seulement après ACK complets et queue vide, sans réécriture d’historique."""
        if not self.check_admin():
            return
        try:
            with get_db_connection() as conn:
                session = get_migration_session(conn)
                complete_device_identity_migration(conn, session.get("manifest_sha256") or "")
            popup("Migration finalisee", "Toutes les conditions locales d’ACK UUID sont satisfaites. Les Device ID historiques des transactions n’ont pas été modifiés.")
            self.show_sync()
        except Exception as exc:
            popup("Finalisation bloquee", str(exc))

    def do_sync_local(self, inst=None):
        if not self.check_admin():
            return
        try:
            if not self.in_peer or not self.in_peer.text.strip():
                popup("Erreur", "Entrez l'adresse IP de l'autre appareil (meme Wi-Fi/Hotspot)")
                return
            with get_db_connection() as conn:
                res = sync_local_wifi(conn, self.in_peer.text.strip())
            if res.ok():
                popup("Succes", f"Synchronisation locale reussie!\nEnvoyees: {res.envoyees}\nRecues: {res.recues}\nIgnorees (deja connues): {res.ignorees}")
            else:
                popup("Echec", f"Synchronisation locale echouee:\n" + "\n".join(res.erreurs[:3]) +
                       "\n\nVerifiez que l'autre appareil a l'application HYBRID ouverte (serveur Wi-Fi/Hotspot actif).")
            self.show_sync()
        except Exception as e:
            log_error("do_sync_local", e)
            popup("Erreur", str(e))

    def do_export_migration_manifest(self, inst=None):
        """Copie non destructive de la base et de la queue pending_sync pour audit migration."""
        if not self.check_admin():
            return
        try:
            export_dir = get_migration_export_dir()
            with get_db_connection() as conn:
                result = export_device_migration_manifest(conn, DB_PATH, export_dir, "faneva_device")
            self.show_manifest_export_result(result)
        except Exception as e:
            log_error("do_export_migration_manifest", e)
            popup("Erreur export manifeste", str(e))

    def show_manifest_export_result(self, result):
        """Affiche les fichiers exportés sans modifier les données ni lancer une synchronisation."""
        details = describe_migration_export(result)
        root = BoxLayout(orientation="vertical", padding=dp(12), spacing=dp(8))
        root.add_widget(Label(text="MANIFESTE EXPORTÉ — AUCUNE SYNCHRONISATION", size_hint_y=None,
                              height=dp(42), bold=True, color=(0.2, 0.75, 0.35, 1), font_size=dp(17)))
        root.add_widget(Label(text=f"Transactions en attente conservées : {details['pending_count']}",
                              size_hint_y=None, height=dp(28), color=(0.85, 0.85, 0.85, 1)))
        full_details = (
            f"NOM EXACT DU JSON :\n{details['manifest_filename']}\n\n"
            f"CHEMIN COMPLET DU JSON :\n{details['manifest_path']}\n\n"
            f"NOM DE LA COPIE SQLITE :\n{details['backup_filename']}\n\n"
            f"CHEMIN COMPLET DE LA COPIE SQLITE :\n{details['backup_path']}\n\n"
            f"SHA-256 DU MANIFESTE :\n{details['manifest_sha256']}"
        )
        details_box = TextInput(text=full_details, readonly=True, multiline=True, font_size=dp(14),
                                size_hint_y=1, background_color=(0.10, 0.10, 0.12, 1), foreground_color=(1, 1, 1, 1))
        root.add_widget(details_box)
        status = Label(text="Vous pouvez copier le chemin, ouvrir le JSON ici, ou partager son contenu.",
                       size_hint_y=None, height=dp(36), halign="center", valign="middle", text_size=(None, None),
                       color=(0.85, 0.85, 0.85, 1))
        root.add_widget(status)
        actions = GridLayout(cols=2, size_hint_y=None, height=dp(96), spacing=dp(6))
        copy_btn = Button(text="COPIER LE CHEMIN", background_color=(0.20, 0.45, 0.75, 1))
        open_btn = Button(text="OUVRIR LE FICHIER", background_color=(0.20, 0.60, 0.38, 1))
        share_btn = Button(text="PARTAGER LE MANIFESTE", background_color=(0.68, 0.45, 0.15, 1))
        close_btn = Button(text="FERMER", background_color=(0.45, 0.30, 0.30, 1))
        pop = Popup(title="Récupération du manifeste", content=root, size_hint=(0.96, 0.92), auto_dismiss=False)

        def copy_path(_inst):
            Clipboard.copy(details["manifest_path"])
            status.text = "Chemin JSON complet copié dans le presse-papiers."

        def open_manifest(_inst):
            self.show_manifest_file(details["manifest_path"])

        def share_manifest(_inst):
            try:
                with open(details["manifest_path"], "r", encoding="utf-8") as manifest_file:
                    content = manifest_file.read()
                from jnius import autoclass
                PythonActivity = autoclass("org.kivy.android.PythonActivity")
                Intent = autoclass("android.content.Intent")
                intent = Intent(Intent.ACTION_SEND)
                intent.setType("application/json")
                intent.putExtra(Intent.EXTRA_TITLE, details["manifest_filename"])
                intent.putExtra(Intent.EXTRA_TEXT, content)
                PythonActivity.mActivity.startActivity(Intent.createChooser(intent, "Partager le manifeste FANEVA"))
                status.text = "Choisissez une application destinataire. Aucune synchronisation n’est lancée."
            except Exception as exc:
                log_error("share_manifest", exc)
                status.text = "Partage Android indisponible. Utilisez « OUVRIR LE FICHIER » puis copiez device_id_proposed."

        copy_btn.bind(on_press=copy_path)
        open_btn.bind(on_press=open_manifest)
        share_btn.bind(on_press=share_manifest)
        close_btn.bind(on_press=lambda _inst: pop.dismiss())
        for button in (copy_btn, open_btn, share_btn, close_btn):
            actions.add_widget(button)
        root.add_widget(actions)
        pop.open()

    def show_manifest_file(self, manifest_path):
        """Ouvre le JSON dans une vue locale en lecture seule, sans écriture ni réseau."""
        try:
            with open(manifest_path, "r", encoding="utf-8") as manifest_file:
                json_text = manifest_file.read()
            json.loads(json_text)
            content = BoxLayout(orientation="vertical", padding=dp(10), spacing=dp(8))
            content.add_widget(Label(text=os.path.basename(manifest_path), size_hint_y=None, height=dp(30), bold=True))
            content.add_widget(TextInput(text=json_text, readonly=True, multiline=True, font_size=dp(13),
                                         background_color=(0.10, 0.10, 0.12, 1), foreground_color=(1, 1, 1, 1)))
            close_btn = Button(text="FERMER", size_hint_y=None, height=dp(46))
            popup_view = Popup(title="Manifest JSON — lecture seule", content=content, size_hint=(0.96, 0.94), auto_dismiss=False)
            close_btn.bind(on_press=lambda _inst: popup_view.dismiss())
            content.add_widget(close_btn)
            popup_view.open()
        except Exception as exc:
            log_error("show_manifest_file", exc)
            popup("Erreur lecture manifeste", str(exc))

    # ---------- FANEVA SYSTEM HYBRID : GESTION MAGASINS ----------
    @safe_sqlite
    def show_gestion_magasins(self, inst=None):
        log("show_gestion_magasins() START")
        try:
            self.clear()
            if not self.check_admin():
                self.show_dashboard()
                return
            b = ScrollView()
            root = BoxLayout(orientation="vertical", padding=15, spacing=10)
            root.bind(minimum_height=root.setter("height"))

            root.add_widget(Label(text="GESTION DES MAGASINS", font_size=30, bold=True, color=(0.7,0.55,0.2,1)))
            root.add_widget(Label(text="Ajoutez, modifiez ou supprimez vos magasins.\nLes changements sont synchronises avec vos autres appareils.", font_size=18, color=(0.5,0.5,0.5,1)))

            btn_add = Button(text="+ AJOUTER UN MAGASIN", font_size=22, bold=True,
                             background_color=(0.2,0.8,0.4,1), size_hint_y=None, height=60)
            btn_add.bind(on_press=self.show_ajouter_magasin)
            root.add_widget(btn_add)

            self.g_list = GridLayout(cols=1, spacing=8, size_hint_y=None)
            self.g_list.bind(minimum_height=self.g_list.setter("height"))
            with get_db_connection() as conn:
                try:
                    cur = conn.cursor()
                    cur.execute("SELECT id, cle, nom, categorie, actif FROM magasins ORDER BY id")
                    for mid, cle, nom, cat, actif in cur.fetchall():
                        row = BoxLayout(size_hint_y=None, height=70, spacing=8)
                        info_m = BoxLayout(orientation="vertical", size_hint_x=0.6)
                        info_m.add_widget(Label(text=f"{nom} ({cle})", font_size=20, bold=True))
                        stat = "ACTIF" if actif == 1 else "SUPPRIME"
                        info_m.add_widget(Label(text=f"{cat} - {stat}", font_size=16))
                        row.add_widget(info_m)
                        if cle != (getattr(self, "magasin_cle", None) or self.magasin):
                            btn_ed = Button(text="EDIT", font_size=16, background_color=(0.2,0.6,0.9,1))
                            btn_ed.bind(on_press=lambda x, m=mid: self.show_editer_magasin(m))
                            row.add_widget(btn_ed)
                            btn_del = Button(text="SUP", font_size=16, background_color=(0.9,0.3,0.3,1))
                            btn_del.bind(on_press=lambda x, m=mid: self.supprimer_magasin(m))
                            row.add_widget(btn_del)
                        else:
                            row.add_widget(Label(text="(actuel)", font_size=14, size_hint_x=0.25))
                        self.g_list.add_widget(row)
                except Exception as e:
                    log(f"liste magasins: {e}")
            root.add_widget(self.g_list)

            btn_back = Button(text="RETOUR", background_color=(0.4,0.4,0.4,1), size_hint_y=None, height=45)
            btn_back.bind(on_press=lambda x: self.show_dashboard())
            root.add_widget(btn_back)

            b.add_widget(root)
            self.root.add_widget(b)
            log("show_gestion_magasins() OK")
        except Exception as e:
            log_error("show_gestion_magasins", e)

    def show_ajouter_magasin(self, inst=None):
        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        in_nom = TextInput(hint_text="Nom du magasin", multiline=False)
        in_desc = TextInput(hint_text="Description", multiline=False)
        in_cat = TextInput(hint_text="Categorie (ex: Quincaillerie)", multiline=False)
        for w in [in_nom, in_desc, in_cat]:
            content.add_widget(w)

        def save():
            nom = in_nom.text.strip()
            if not nom:
                popup("Erreur", "Le nom du magasin est requis")
                return
            try:
                with get_db_connection() as conn:
                    cle = ""
                    for c in nom.lower():
                        if c.isalnum() or c == "_":
                            cle += c
                        elif c == " ":
                            cle += "_"
                    cle = cle[:30]
                    mid = create_store(conn, cle, nom,
                                       description=in_desc.text.strip(),
                                       categorie=in_cat.text.strip() or "General",
                                       couleur="#5B8DB8", couleur_light="#DDEEFF",
                                       admin_creatrice=self.user.get("user_id"))
                    conn.commit()
                if mid is None:
                    popup("Erreur", f"Un magasin '{nom}' existe deja")
                    return
                popup("Succes", f"Magasin '{nom}' cree!\nIl apparaitra sur vos autres appareils a la prochaine synchronisation.\n(Selectionnez-le depuis l'ecran de choix du magasin.)")
                refresh_magasins()
                self.show_gestion_magasins()
            except Exception as e:
                log_error("save_ajouter_magasin", e)
                popup("Erreur", str(e))

        btn = Button(text="CREER", background_color=(0.2,0.8,0.4,1))
        pop = Popup(title="Nouveau Magasin", content=content, size_hint=(0.9, 0.6))
        btn.bind(on_press=lambda x: (save(), pop.dismiss()))
        content.add_widget(btn)
        pop.open()

    def show_editer_magasin(self, magasin_id):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, cle, nom, description, categorie FROM magasins WHERE id=?", (magasin_id,))
            row = cur.fetchone()
        if not row:
            return
        mid, cle, nom, desc, cat = row
        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        in_nom = TextInput(text=nom, hint_text="Nom", multiline=False)
        in_desc = TextInput(text=desc or "", hint_text="Description", multiline=False)
        in_cat = TextInput(text=cat or "", hint_text="Categorie", multiline=False)
        for w in [in_nom, in_desc, in_cat]:
            content.add_widget(w)

        def save():
            try:
                with get_db_connection() as conn:
                    update_store(conn, mid, nom=in_nom.text.strip(),
                                 description=in_desc.text.strip(),
                                 categorie=in_cat.text.strip() or "General")
                    conn.commit()
                refresh_magasins()
                self.show_gestion_magasins()
                popup("OK", "Magasin mis a jour!")
            except Exception as e:
                log_error("save_editer_magasin", e)
                popup("Erreur", str(e))

        btn = Button(text="ENREGISTRER", background_color=(0.2,0.8,0.4,1))
        pop = Popup(title=f"Modifier: {nom}", content=content, size_hint=(0.9, 0.6))
        btn.bind(on_press=lambda x: (save(), pop.dismiss()))
        content.add_widget(btn)
        pop.open()

    def supprimer_magasin(self, magasin_id):
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT cle, nom, actif FROM magasins WHERE id=?", (magasin_id,))
            row = cur.fetchone()
        if not row:
            return
        cle, nom, actif = row
        content = BoxLayout(orientation="vertical", padding=15)
        if actif == 0:
            content.add_widget(Label(text=f"'{nom}' est deja supprime (suppression logique).", font_size=22))
        else:
            content.add_widget(Label(text=f"Supprimer le magasin '{nom}' ?\nCette suppression sera synchronisee avec vos autres appareils.", font_size=20))
        btns = BoxLayout(spacing=10)
        pop = Popup(title="Confirmer", content=content, size_hint=(0.8, 0.3))

        def do_del():
            try:
                with get_db_connection() as conn:
                    delete_store(conn, magasin_id)
                    conn.commit()
                refresh_magasins()
                self.show_gestion_magasins()
                popup("OK", f"Magasin '{nom}' supprime (donnees conservees).")
            except Exception as e:
                log_error("supprimer_magasin", e)
                popup("Erreur", str(e))

        by = Button(text="OUI", background_color=(0.9,0.3,0.3,1))
        by.bind(on_press=lambda x: (do_del(), pop.dismiss()))
        bn = Button(text="NON", background_color=(0.5,0.5,0.5,1))
        bn.bind(on_press=lambda x: pop.dismiss())
        btns.add_widget(by)
        btns.add_widget(bn)
        content.add_widget(btns)
        pop.open()

    def show_add_user(self, inst):
        content = BoxLayout(orientation="vertical", padding=15, spacing=8)
        in_u = TextInput(hint_text="Nom d'utilisateur", multiline=False)
        in_nom = TextInput(hint_text="Nom complet", multiline=False)
        in_p = TextInput(hint_text="Mot de passe", password=True, multiline=False)
        in_r = TextInput(hint_text="Role (ADMIN ou VENDEUR)", multiline=False)

        for w in [in_u, in_nom, in_p, in_r]:
            content.add_widget(w)

        def save():
            u = in_u.text.strip()
            p = in_p.text.strip()
            r = in_r.text.strip().upper() or "VENDEUR"
            if not u or not p:
                popup("Erreur", "Champs requis manquants")
                return
            try:
                with get_db_connection() as conn:
                    # --- FANEVA SYSTEM HYBRID : compte partage avec transaction USER_CREATE ---
                    if has_hybrid_module() and _table_exists_hybrid("utilisateurs_hybrid"):
                        r_h = "ADMIN_PRINCIPAL" if r == "ADMIN" else "VENDEUR"
                        ok = create_user_hybrid(conn, u, p,
                                                nom_complet=in_nom.text.strip(), role=r_h,
                                                admin_creatrice=self.user.get("user_id"))
                        if ok is None:
                            popup("Erreur", f"L'utilisateur {u} existe deja")
                            return
                        popup("OK", f"Utilisateur {u} cree (compte partage via sync)!")
                    else:
                        cur = conn.cursor()
                        cur.execute("INSERT INTO utilisateurs (username, password_hash, nom_complet, role) VALUES (?,?,?,?)",
                                   (u, hash_pwd(p), in_nom.text.strip(), r))
                        conn.commit()
                        popup("OK", f"Utilisateur {u} cree!")
                    self.load_users()
            except Exception as e:
                log_error("save_add_user", e)
                popup("Erreur", str(e))

        btn = Button(text="CREER", background_color=(0.2,0.8,0.4,1))
        pop = Popup(title="Nouvel Utilisateur", content=content, size_hint=(0.9, 0.7))
        btn.bind(on_press=lambda x: (save(), pop.dismiss()))
        content.add_widget(btn)
        pop.open()

# ==============================
# CRASH HANDLER GLOBAL (log l'erreur fatale sur le stockage accessible)
# ==============================
def _install_crash_handler():
    """Installe un gestionnaire global qui ecrit la traceback fatale dans un
    fichier lisible sur le telephone (/sdcard/FANEVA_SYSTEM_ANDROID/crash.log
    et /data/data/.../FANEVA_SYSTEM_ANDROID/crash.log)."""
    def _global_hook(exc_type, exc_value, exc_tb):
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_tb)
        lines = [f"[{HYBRID_LOG_TAG}] CRASH FATAL: {''.join(tb_lines[-1:]).strip()}"] + \
                [f"  {l}" for l in tb_lines]
        text = "\n".join(lines) + "\n"
        print(text)
        sys.stdout.flush()
        for crash_path in (
            os.path.join(EXTERNAL_DIR, "crash.log"),
            os.path.join(INTERNAL_DIR, "crash.log"),
        ):
            try:
                os.makedirs(os.path.dirname(crash_path), exist_ok=True)
                with open(crash_path, "a", encoding="utf-8") as f:
                    f.write(text)
            except Exception:
                pass
    sys.excepthook = _global_hook

if __name__ == "__main__":
    _install_crash_handler()
    log("=== LANCEMENT APP ===")
    try:
        KDKApp().run()
    except Exception as _run_err:
        log_error("APP.RUN", _run_err)
        # Derniere tentative de log sur le stockage accessible
        for _crash_path in (
            os.path.join(EXTERNAL_DIR, "crash.log"),
            os.path.join(INTERNAL_DIR, "crash.log"),
        ):
            try:
                os.makedirs(os.path.dirname(_crash_path), exist_ok=True)
                with open(_crash_path, "a", encoding="utf-8") as _f:
                    _f.write(f"[KDK] ERREUR APP.RUN: {_run_err}\n"
                             + "".join(traceback.format_exc()) + "\n")
            except Exception:
                pass
        raise
