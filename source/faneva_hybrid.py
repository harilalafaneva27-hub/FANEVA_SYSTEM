"""
FANEVA SYSTEM HYBRID - Module noyau (sans UI)
=============================================
Contient toute la logique HYBRID decouplee de l'interface Kivy :

- DeviceID unique par appareil
- Transactions (UUID) : vente, achat, ajustement, dette, paiement, magasin, utilisateur
- Journal de mouvements (mouvements) => stock calcule par aggregation (pas de valeur finale sync)
- Queue pending_sync (EN ATTENTE -> SYNCHRONISATION -> CONFIRMÉ)
- Synchronisation Internet (HTTP/HTTPS vers un serveur de relais) et locale Wi-Fi/Hotspot
  (un appareil fait office de serveur HTTP local pour les autres)
- Gestion des conflits : union des transactions par UUID (idempotente, LWW sur les meta,
  stock toujours reconstruit depuis les mouvements -> impossible d'obtenir 90 ou 95 a la
  place de 85)
- Magasins en base de donnees (CRUD dynamique, admin autorises)
- Multi-admin : utilisateurs partages/syncronises avec roles ADMIN_PRINCIPAL / ADMIN_MAGASIN
  et permissions par module
- Backup local (conserve) + distant (HTTPS quand Internet dispo)

Compatibilite : Python 3.11, bibliotheque standard uniquement (json, uuid, sqlite3, hashlib,
http.server, urllib) pour garantir le build p4a sans dependance supplementaire.
"""

import json
import os
import sqlite3
import hashlib
import uuid
import contextlib
import threading
import time
import traceback
import shutil
import socket
import ssl
import sys
import platform
import ipaddress
from datetime import datetime, timezone

# ============================================================
# CONFIGURATION
# ============================================================
HYBRID_VERSION = "1.4.9.16"
HYBRID_LOG_TAG = f"KDK v{HYBRID_VERSION} HYBRID"
DEVICE_IDENTITY_STATE_READY = "READY"
DEVICE_IDENTITY_STATE_MIGRATION_REQUIRED = "MIGRATION_REQUIRED"

# Serveur sync local (Wi-Fi/Hotspot) : port ecoute sur l'appareil
SYNC_SERVER_PORT = 7721
SYNC_SERVER_TIMEOUT = 15.0  # secondes
PRODUCTION_SYNC_DISABLED = True  # Les endpoints de production restent interdits.
CLOUDFLARE_STAGING_HOST = "faneva-sync-staging.faneva-sync-staging.workers.dev"
CANONICAL_STAGING_MAPPING_FILE = "canonical_mapping_staging_v1.json"
CANONICAL_MAPPING_CONFIG_KEY = "canonical_mapping_version"


def _is_cloudflare_staging_endpoint(endpoint):
    """Retourne vrai uniquement pour la base HTTPS du Worker FANEVA staging existant."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse((endpoint or '').strip())
        return (
            parsed.scheme.lower() == 'https'
            and parsed.hostname and parsed.hostname.lower().rstrip('.') == CLOUDFLARE_STAGING_HOST
            and parsed.port in (None, 443)
            and parsed.path in ('', '/')
            and not parsed.params and not parsed.query and not parsed.fragment
        )
    except Exception:
        return False


def _debug_network_endpoint_allowed(endpoint):
    """Autorise le staging Cloudflare exact et les pairs HTTP locaux.

    Tant que le verrou de production est actif, aucun autre nom d’hôte HTTPS public
    ni endpoint de production ne peut être joint. Les adresses HTTP locales restent
    disponibles pour les tests Wi-Fi/Hotspot séparés.
    """
    if not PRODUCTION_SYNC_DISABLED:
        return True
    if _is_cloudflare_staging_endpoint(endpoint):
        return True
    try:
        from urllib.parse import urlparse
        parsed = urlparse((endpoint or '').strip())
        if parsed.scheme.lower() != 'http' or not parsed.hostname:
            return False
        host = parsed.hostname.lower().rstrip('.')
        if host in {'localhost', 'localhost.localdomain'}:
            return True
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            return False
        return bool(address.is_loopback or address.is_private or address.is_link_local)
    except Exception:
        return False


# ============================================================
# UTILITAIRES
# ============================================================
def now_iso():
    """Timestamp ISO UTC stable pour toutes les transactions."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def gen_uuid():
    return str(uuid.uuid4())

def gen_device_id(db_path):
    """Ancien algorithme deterministe, conserve uniquement pour diagnostic/migration."""
    raw = f"faneva-hybrid:{db_path}:{os.uname().nodename if hasattr(os, 'uname') else 'android'}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def gen_persistent_device_id():
    """UUID v4 local : independant du compte, magasin, reseau, chemin et hostname."""
    return str(uuid.uuid4())


def _config_get(cur, key, default=None):
    cur.execute("SELECT valeur FROM config_hybrid WHERE cle=?", (key,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else default


def _config_set(cur, key, value):
    cur.execute("INSERT OR REPLACE INTO config_hybrid (cle, valeur) VALUES (?, ?)", (key, str(value)))


def set_server_api_key(conn, api_key):
    """Enregistre localement la clé remise lors du provisionnement, sans jamais la journaliser."""
    value = (api_key or "").strip()
    if not value.startswith("fsv_") or len(value) < 20:
        raise ValueError("Clé API FANEVA invalide")
    cur = conn.cursor()
    _config_set(cur, "sync_server_api_key", value)
    conn.commit()


def get_server_api_key(conn):
    return _config_get(conn.cursor(), "sync_server_api_key")


def has_server_api_key(conn):
    return bool(get_server_api_key(conn))


def _auth_runtime_trace(event, **fields):
    """Émet un diagnostic auth strictement non sensible pour logcat.

    La clé, les headers, le payload et les valeurs de configuration ne sont jamais
    ajoutés au message. Les champs booléens et catégories servent uniquement à
    distinguer précondition, DNS, TLS, délai, HTTP et JSON au runtime.
    """
    safe = {"event": str(event)}
    for key, value in fields.items():
        if key in {"api_key", "authorization", "headers", "payload", "device_id"}:
            continue
        if isinstance(value, (bool, int)) or value is None:
            safe[key] = value
        elif isinstance(value, str):
            safe[key] = value[:80]
    try:
        print("FANEVA_AUTH " + json.dumps(safe, ensure_ascii=False, sort_keys=True))
    except Exception:
        print("FANEVA_AUTH {\"event\":\"TRACE_UNAVAILABLE\"}")


def set_migration_session(conn, migration_id, manifest_sha256):
    """Enregistre la session auditée créée par l’administrateur, sans transmettre de transaction."""
    if not is_migration_locked(conn):
        raise ValueError("Aucune migration Device ID verrouillee sur cet appareil")
    try:
        normalized_id = str(uuid.UUID((migration_id or "").strip()))
    except Exception as exc:
        raise ValueError("Identifiant de session de migration UUID invalide") from exc
    normalized_hash = (manifest_sha256 or "").strip().lower()
    if len(normalized_hash) != 64 or any(char not in "0123456789abcdef" for char in normalized_hash):
        raise ValueError("SHA-256 du manifeste invalide")
    cur = conn.cursor()
    _config_set(cur, "migration_session_id", normalized_id)
    _config_set(cur, "migration_manifest_sha256", normalized_hash)
    _config_set(cur, "migration_pilot_uuid", "")
    _config_set(cur, "migration_pilot_acknowledged", "0")
    _config_set(cur, "migration_ack_manifest_sha256", "")
    conn.commit()


def get_migration_session(conn):
    cur = conn.cursor()
    return {
        "migration_id": _config_get(cur, "migration_session_id"),
        "manifest_sha256": _config_get(cur, "migration_manifest_sha256"),
        "pilot_uuid": _config_get(cur, "migration_pilot_uuid"),
        "pilot_acknowledged": _config_get(cur, "migration_pilot_acknowledged", "0") == "1",
    }


def _historical_outgoing(conn, only_transaction_id=None):
    """Retourne seulement les évènements legacy en attente autorisables par le manifeste serveur."""
    status = device_identity_status(conn)
    if not status["migration_locked"] or not status["device_id_legacy"]:
        raise ValueError("Migration Device ID historique indisponible")
    outgoing = export_outgoing(conn, pending_transactions(conn))
    if only_transaction_id:
        outgoing = [tx for tx in outgoing if tx["transaction_id"] == only_transaction_id]
        if not outgoing:
            raise ValueError("Transaction pilote absente de pending_sync")
    invalid = [tx["transaction_id"] for tx in outgoing if tx.get("device_id") != status["device_id_legacy"]]
    if invalid:
        raise ValueError("La migration ne peut transmettre que les transactions historiques manifestees")
    return outgoing


def _get_sync_cursor(conn):
    raw = _config_get(conn.cursor(), "sync_cursor")
    if not raw:
        return None
    try:
        cursor = json.loads(raw)
        if isinstance(cursor, dict) and cursor.get("recu_le") and cursor.get("transaction_id"):
            return cursor
    except Exception:
        pass
    return None


def _set_sync_cursor(conn, cursor):
    if cursor is None:
        return
    if not isinstance(cursor, dict) or not cursor.get("recu_le") or not cursor.get("transaction_id"):
        raise ValueError("Curseur de synchronisation serveur invalide")
    _config_set(conn.cursor(), "sync_cursor", json.dumps(cursor, sort_keys=True, separators=(",", ":")))
    conn.commit()


def _payload_sha256(payload):
    raw = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _manifest_canonical_payload(payload):
    """Reproduit sans mutation la représentation figée dans le manifeste.

    Les transactions historiques sont conservées telles quelles dans SQLite. Pour
    un relais de migration seulement, le serveur compare leur empreinte au JSON
    canonique utilisé par l’export de manifeste (clés triées, séparateurs
    compacts). Cette fonction prépare uniquement la copie envoyée sur le réseau.
    """
    value = json.loads(payload) if isinstance(payload, str) else payload
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_legacy_deterministic_device_id(value):
    """Identifie le format 16-hexa emis par v1.1.0-GZFIX sans toucher aux transactions."""
    if not isinstance(value, str) or len(value) != 16:
        return False
    return all(char in "0123456789abcdef" for char in value.lower())


def initialize_device_identity(conn):
    """
    Initialise l'identite locale sans jamais modifier les transactions existantes.

    Une installation neuve recoit un UUID v4 READY. Une installation v1.1.0-GZFIX
    conserve son identite historique, cree un UUID v4 actif et reste verrouillee
    jusqu'a une migration manuelle et auditee.
    """
    run_migration(conn)
    cur = conn.cursor()
    current = _config_get(cur, "device_id")
    state = _config_get(cur, "device_identity_state")
    legacy = _config_get(cur, "device_id_legacy")

    if not current:
        current = gen_persistent_device_id()
        _config_set(cur, "device_id", current)
        _config_set(cur, "device_identity_state", DEVICE_IDENTITY_STATE_READY)
        _config_set(cur, "migration_lock", "0")
        _config_set(cur, "device_identity_created_at", now_iso())
        conn.commit()
        return device_identity_status(conn)

    if state == DEVICE_IDENTITY_STATE_MIGRATION_REQUIRED:
        return device_identity_status(conn)

    if not legacy and _is_legacy_deterministic_device_id(current):
        proposed = gen_persistent_device_id()
        _config_set(cur, "device_id_legacy", current)
        _config_set(cur, "device_id_proposed", proposed)
        _config_set(cur, "device_id", proposed)
        _config_set(cur, "device_identity_state", DEVICE_IDENTITY_STATE_MIGRATION_REQUIRED)
        _config_set(cur, "migration_lock", "1")
        _config_set(cur, "migration_created_at", now_iso())
        conn.commit()
        return device_identity_status(conn)

    if not state:
        _config_set(cur, "device_identity_state", DEVICE_IDENTITY_STATE_READY)
        _config_set(cur, "migration_lock", "0")
        conn.commit()
    return device_identity_status(conn)


def device_identity_status(conn):
    """Retourne l'etat lisible de l'identite sans modifier les donnees metier."""
    cur = conn.cursor()
    state = _config_get(cur, "device_identity_state", DEVICE_IDENTITY_STATE_READY)
    lock = _config_get(cur, "migration_lock", "0") == "1"
    return {
        "device_id": _config_get(cur, "device_id"),
        "device_id_legacy": _config_get(cur, "device_id_legacy"),
        "device_id_proposed": _config_get(cur, "device_id_proposed"),
        "state": state,
        "migration_locked": lock,
    }


def is_migration_locked(conn):
    return device_identity_status(conn)["migration_locked"]


def complete_device_identity_migration(conn, manifest_sha256):
    """
    Deverrouillage volontaire reserve a la fin des ACK UUID et de l'audit serveur.
    Cette fonction ne modifie ni les transactions ni pending_sync.
    """
    if not manifest_sha256:
        raise ValueError("Le hash du manifeste valide est obligatoire")
    cur = conn.cursor()
    manifest_sha256 = manifest_sha256.strip().lower()
    configured_manifest = _config_get(cur, "migration_manifest_sha256")
    acknowledged_manifest = _config_get(cur, "migration_ack_manifest_sha256")
    if configured_manifest != manifest_sha256 or acknowledged_manifest != manifest_sha256:
        raise ValueError("Les ACK UUID complets du manifeste valide sont obligatoires avant deverrouillage")
    if pending_count(conn) != 0:
        raise ValueError("La queue pending_sync doit etre entierement acquittee avant deverrouillage")
    _config_set(cur, "migration_manifest_sha256", manifest_sha256)
    _config_set(cur, "migration_lock", "0")
    _config_set(cur, "device_identity_state", DEVICE_IDENTITY_STATE_READY)
    _config_set(cur, "migration_completed_at", now_iso())
    conn.commit()


def prepare_migration_export_dir(app_specific_root):
    """Crée exclusivement le dossier d’export sous une racine app-specific fournie."""
    if not app_specific_root:
        raise ValueError("Repertoire app-specific indisponible")
    export_dir = os.path.join(app_specific_root, "FANEVA_MIGRATION_DEVICE_ID")
    os.makedirs(export_dir, exist_ok=True)
    if not os.path.isdir(export_dir):
        raise OSError("Creation du repertoire de migration impossible")
    return export_dir


def prepare_database_copy_export_dir(app_specific_root):
    """Crée seulement le dossier app-specific destiné aux copies comparatives."""
    if not app_specific_root:
        raise ValueError("Repertoire app-specific indisponible")
    export_dir = os.path.join(app_specific_root, "FANEVA_DB_COMPARISON_EXPORT")
    os.makedirs(export_dir, exist_ok=True)
    if not os.path.isdir(export_dir):
        raise OSError("Creation du repertoire de copies impossible")
    return export_dir


def _file_sha256(path):
    """Calcule une empreinte de fichier sans modifier son contenu."""
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inspect_sqlite_files_physical_binary(db_path):
    """Contrôle DB/WAL/SHM/journal par métadonnées et lecture binaire seulement.

    Aucun appel SQLite n'est réalisé. La fonction ne crée, ne renomme, ne supprime
    et ne modifie aucun fichier. Les seuls accès au contenu sont des ouvertures
    binaires en lecture pour calculer SHA-256 des fichiers déjà présents.
    """
    real_path = os.path.realpath(os.path.abspath(str(db_path or "")))
    result = {
        "mode": "BINARY_METADATA_AND_READ_ONLY_SHA256_NO_SQLITE_CONNECTION",
        "base_path": real_path,
        "sqlite_connect_called": False,
        "sqlite_pragma_called": False,
        "sqlite_backup_called": False,
        "sqlite_write_called": False,
        "network_called": False,
        "files": {},
    }
    file_specs = (
        ("db", "", "BASE DB"),
        ("wal", "-wal", "FICHIER WAL"),
        ("shm", "-shm", "FICHIER SHM"),
        ("journal", "-journal", "ROLLBACK JOURNAL"),
    )
    has_anomaly = False
    for key, suffix, label in file_specs:
        file_path = real_path + suffix
        entry = {
            "label": label,
            "path": file_path,
            "exists": False,
            "size_bytes": None,
            "sha256": None,
            "mode": None,
            "modified_utc": None,
        }
        try:
            entry["exists"] = os.path.isfile(file_path)
            if entry["exists"]:
                metadata = os.stat(file_path)
                entry["size_bytes"] = int(metadata.st_size)
                entry["sha256"] = _file_sha256(file_path)
                entry["mode"] = oct(metadata.st_mode & 0o777)
                entry["modified_utc"] = datetime.fromtimestamp(
                    metadata.st_mtime, timezone.utc
                ).isoformat()
            elif os.path.exists(file_path):
                entry["anomaly"] = "CHEMIN PRÉSENT MAIS PAS UN FICHIER RÉGULIER"
                has_anomaly = True
        except Exception as exc:
            entry["error"] = _diagnostic_exception_payload(exc, file_path, "")
            has_anomaly = True
        result["files"][key] = entry

    database = result["files"]["db"]
    journal = result["files"]["journal"]
    if has_anomaly or not database.get("exists"):
        result["conclusion"] = "AUTRE ANOMALIE"
    elif not journal.get("exists"):
        result["conclusion"] = "JOURNAL ABSENT"
    elif journal.get("size_bytes") == 0:
        result["conclusion"] = "JOURNAL PRÉSENT ET VIDE"
    elif isinstance(journal.get("size_bytes"), int) and journal["size_bytes"] > 0:
        result["conclusion"] = "JOURNAL PRÉSENT ET NON VIDE"
    else:
        result["conclusion"] = "AUTRE ANOMALIE"
    return result


def export_external_database_binary_only(source_path, export_dir, destination_filename=None):
    """Exporte le seul fichier DB externe par octets, sans aucune API SQLite.

    La source est ouverte exclusivement avec ``rb`` et la destination nouvelle
    exclusivement avec ``xb``. Les fichiers WAL, SHM et rollback journal sont
    seulement observés par métadonnées/lecture binaire ; ils ne sont jamais
    ouverts par SQLite, copiés, supprimés, renommés ni modifiés.
    """
    source_real = os.path.realpath(os.path.abspath(str(source_path or "")))
    export_real = os.path.realpath(os.path.abspath(str(export_dir or "")))
    if not source_real or not os.path.isfile(source_real):
        raise FileNotFoundError("Base externe source introuvable : " + source_real)
    if not export_real:
        raise ValueError("Répertoire app-specific de destination invalide")
    os.makedirs(export_real, exist_ok=True)
    if not os.path.isdir(export_real):
        raise OSError("Répertoire app-specific de destination indisponible")

    filename = destination_filename or (
        "stock_quincaillerie_externe_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + ".db"
    )
    if os.path.basename(filename) != filename:
        raise ValueError("Nom de destination invalide")
    destination_real = os.path.join(export_real, filename)
    if source_real == destination_real:
        raise ValueError("Destination interdite : identique à la source")
    if os.path.exists(destination_real):
        raise FileExistsError("Destination déjà présente : aucune écriture de remplacement autorisée")

    physical_before = inspect_sqlite_files_physical_binary(source_real)
    source_info = physical_before["files"]["db"]
    if not source_info.get("exists"):
        raise FileNotFoundError("Base externe source introuvable après contrôle physique")
    source_sha_before = source_info["sha256"]
    source_size = source_info["size_bytes"]

    with open(source_real, "rb") as source_handle, open(destination_real, "xb") as destination_handle:
        for block in iter(lambda: source_handle.read(1024 * 1024), b""):
            destination_handle.write(block)

    source_sha_after = _file_sha256(source_real)
    destination_sha256 = _file_sha256(destination_real)
    destination_size = os.path.getsize(destination_real)
    source_unchanged = source_sha_before == source_sha_after
    validated = (
        source_unchanged
        and source_size == destination_size
        and source_sha_before == destination_sha256
    )
    return {
        "mode": "EXTERNAL_BINARY_COPY_RB_XB_ONLY_NO_SQLITE",
        "sqlite_connect_called": False,
        "sqlite_pragma_called": False,
        "sqlite_backup_called": False,
        "sqlite_write_called": False,
        "network_called": False,
        "source_path": source_real,
        "source_size_bytes": source_size,
        "source_sha256_before": source_sha_before,
        "source_sha256_after": source_sha_after,
        "source_unchanged": source_unchanged,
        "destination_path": destination_real,
        "destination_filename": filename,
        "destination_size_bytes": destination_size,
        "destination_sha256": destination_sha256,
        "associated_files": physical_before["files"],
        "validated": validated,
        "conclusion": "COPIE EXTERNE VALIDÉE" if validated else "COPIE EXTERNE NON VALIDÉE",
    }


def validate_existing_binary_copy_readonly(copy_path, expected_size_bytes, expected_sha256):
    """Contrôle un fichier existant par ``rb`` sans aucune réécriture ni SQLite."""
    copy_real = os.path.realpath(os.path.abspath(str(copy_path or "")))
    if not copy_real or not os.path.isfile(copy_real):
        raise FileNotFoundError("Copie validée introuvable : " + copy_real)
    actual_size = os.path.getsize(copy_real)
    if int(actual_size) != int(expected_size_bytes):
        raise ValueError(
            "Taille inattendue : %s octets, attendu : %s octets"
            % (actual_size, expected_size_bytes)
        )
    actual_sha256 = _file_sha256(copy_real)
    if actual_sha256.lower() != str(expected_sha256).lower():
        raise ValueError("SHA-256 inattendu : le fichier existant ne sera pas partagé")
    return {
        "mode": "EXISTING_BINARY_FILE_READ_ONLY_VALIDATION_NO_SQLITE",
        "sqlite_connect_called": False,
        "sqlite_write_called": False,
        "network_called": False,
        "path": copy_real,
        "size_bytes": actual_size,
        "sha256": actual_sha256,
        "validated": True,
    }


def _readonly_environment_metadata(path):
    """Relève les métadonnées d’un chemin sans le créer, l’ouvrir en écriture ni le modifier."""
    requested_path = str(path or "")
    result = {
        "path": requested_path,
        "exists": os.path.exists(requested_path),
        "is_file": os.path.isfile(requested_path),
        "is_directory": os.path.isdir(requested_path),
        "readable": os.access(requested_path, os.R_OK),
        "writable": os.access(requested_path, os.W_OK),
        "searchable": os.access(requested_path, os.X_OK),
    }
    if not result["exists"]:
        return result
    try:
        metadata = os.stat(requested_path)
        result.update({
            "size_bytes": int(metadata.st_size),
            "mode_octal": oct(metadata.st_mode & 0o777),
            "uid": getattr(metadata, "st_uid", None),
            "gid": getattr(metadata, "st_gid", None),
            "modified_utc": datetime.fromtimestamp(
                metadata.st_mtime, timezone.utc
            ).isoformat(),
        })
        if result["is_file"]:
            result["sha256"] = _file_sha256(requested_path)
    except Exception as exc:
        result["error"] = _diagnostic_exception_payload(exc, requested_path, "")
    return result


def _android_runtime_environment_metadata():
    """Collecte les informations Python/Android disponibles, sans aucun accès SQLite."""
    environment = {
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "sqlite3_sqlite_version": getattr(sqlite3, "sqlite_version", None),
        "sqlite3_module_version": getattr(sqlite3, "version", None),
        "cpu_architecture": platform.machine(),
        "sqlite3_module_path": getattr(sqlite3, "__file__", None),
        "pid": os.getpid(),
        "uid": os.getuid() if hasattr(os, "getuid") else None,
        "gid": os.getgid() if hasattr(os, "getgid") else None,
        "cwd": os.getcwd(),
        "home": os.environ.get("HOME"),
        "tmp": os.environ.get("TMP"),
        "tmpdir": os.environ.get("TMPDIR"),
        "android_api": None,
        "android_package": None,
    }
    try:
        from jnius import autoclass
        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        BuildVersion = autoclass("android.os.Build$VERSION")
        activity = PythonActivity.mActivity
        environment["android_api"] = int(BuildVersion.SDK_INT)
        environment["android_package"] = str(activity.getPackageName())
    except Exception as exc:
        environment["android_runtime_error"] = _diagnostic_exception_payload(exc, "Android runtime", "")
    return environment


def run_android_readonly_environment_diagnostic(db_path):
    """Diagnostique un échec SQLite Android avec la séquence explicitement autorisée.

    Le contrôle ne crée volontairement aucun fichier et ne modifie aucun fichier
    DB/WAL/SHM/journal. Il relève les métadonnées avant/après, puis ouvre au plus
    une connexion par essai selon l’ordre strict : URI ``mode=ro`` + ``SELECT 1`` ;
    lecture de schéma seulement si ce premier test réussit ; chemin absolu normal
    seulement si l’URI échoue ; FILE URI seulement si les deux premiers échouent.
    Aucun PRAGMA, DDL/DML, checkpoint, backup, VACUUM, copie ou réseau n’est appelé.
    """
    requested_path = str(db_path or "")
    parent_path = os.path.dirname(requested_path)
    artifact_paths = {
        "wal": requested_path + "-wal",
        "shm": requested_path + "-shm",
        "journal": requested_path + "-journal",
    }
    result = {
        "mode": "ANDROID_READONLY_ENVIRONMENT_DIAGNOSTIC_NO_PRAGMA_NO_DML",
        "source_path": requested_path,
        "environment": _android_runtime_environment_metadata(),
        "database": _readonly_environment_metadata(requested_path),
        "parent_directory": _readonly_environment_metadata(parent_path),
        "artifacts_before": {},
        "tests": {},
        "artifacts_after": {},
        "sqlite_write_called": False,
        "sqlite_pragma_called": False,
        "sqlite_backup_called": False,
        "sqlite_checkpoint_called": False,
        "sqlite_vacuum_called": False,
        "network_called": False,
    }

    for key, artifact_path in artifact_paths.items():
        result["artifacts_before"][key] = _readonly_environment_metadata(artifact_path)
    result["db_sha256_before"] = result["database"].get("sha256")

    def run_connection_test(test_key, description, connection_target, use_uri, run_select=False, run_schema=False):
        item = {
            "description": description,
            "target": connection_target,
            "uri": bool(use_uri),
            "status": "NON EXÉCUTÉ",
        }
        connection = None
        try:
            connection = sqlite3.connect(connection_target, uri=bool(use_uri), timeout=5.0)
            item["connection"] = "OUVERTE"
            if run_select:
                row = connection.execute("SELECT 1").fetchone()
                item["select_1"] = row[0] if row else None
            if run_schema:
                rows = connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                    "ORDER BY name"
                ).fetchall()
                item["tables"] = [str(row[0]) for row in rows]
            item["status"] = "OK"
        except Exception as exc:
            item["status"] = "ÉCHEC"
            item["error"] = _diagnostic_exception_payload(exc, requested_path, "")
        finally:
            if connection is not None:
                try:
                    connection.close()
                    item["closed"] = True
                except Exception as exc:
                    item["close_error"] = _diagnostic_exception_payload(exc, requested_path, "")
        result["tests"][test_key] = item
        return item

    if not result["database"].get("is_file"):
        missing = FileNotFoundError(2, "Base SQLite externe introuvable", requested_path)
        result["tests"]["uri_mode_ro"] = {
            "description": "TEST URI mode=ro",
            "status": "ÉCHEC",
            "error": _diagnostic_exception_payload(missing, requested_path, ""),
        }
    else:
        mode_ro_uri = "file:" + requested_path + "?mode=ro"
        first_test = run_connection_test(
            "uri_mode_ro", "TEST A — URI mode=ro puis SELECT 1", mode_ro_uri, True, run_select=True
        )
        if first_test.get("status") == "OK":
            run_connection_test(
                "select_schema", "TEST B — SELECT sqlite_master", mode_ro_uri, True, run_schema=True
            )
            result["tests"]["absolute_path"] = {
                "description": "TEST CHEMIN ABSOLU uri=False",
                "status": "NON EXÉCUTÉ",
                "reason": "TEST A a réussi ; ouverture normale non nécessaire.",
            }
            result["tests"]["file_uri"] = {
                "description": "TEST FILE URI file:///...?...",
                "status": "NON EXÉCUTÉ",
                "reason": "TEST A a réussi ; essai alternatif non nécessaire.",
            }
        else:
            absolute_test = run_connection_test(
                "absolute_path", "TEST CHEMIN ABSOLU uri=False sans requête", requested_path, False
            )
            result["tests"]["select_schema"] = {
                "description": "TEST B — SELECT sqlite_master",
                "status": "NON EXÉCUTÉ",
                "reason": "TEST A a échoué ; SELECT de schéma interdit par la séquence demandée.",
            }
            if absolute_test.get("status") == "ÉCHEC":
                file_uri = "file:///" + requested_path.lstrip("/") + "?mode=ro"
                run_connection_test(
                    "file_uri", "TEST FILE URI mode=ro puis SELECT 1", file_uri, True, run_select=True
                )
            else:
                result["tests"]["file_uri"] = {
                    "description": "TEST FILE URI file:///...?...",
                    "status": "NON EXÉCUTÉ",
                    "reason": "Le test de chemin absolu a ouvert la connexion ; essai alternatif non nécessaire.",
                }

    result["database_after"] = _readonly_environment_metadata(requested_path)
    result["db_sha256_after"] = result["database_after"].get("sha256")
    result["db_sha256_unchanged"] = (
        result["db_sha256_before"] is not None
        and result["db_sha256_before"] == result["db_sha256_after"]
    )
    for key, artifact_path in artifact_paths.items():
        result["artifacts_after"][key] = _readonly_environment_metadata(artifact_path)
    result["artifacts_observed_after_open"] = {
        key: (
            not result["artifacts_before"].get(key, {}).get("exists", False)
            and result["artifacts_after"].get(key, {}).get("exists", False)
        )
        for key in artifact_paths
    }
    return result


def _binary_sqlite_file_copy(source_path, destination_path):
    """Copie binaire d’un fichier SQLite, refusée dès qu’un état WAL/SHM est présent."""
    source_real = os.path.realpath(os.path.abspath(str(source_path or "")))
    destination_real = os.path.realpath(os.path.abspath(str(destination_path or "")))
    if not source_real or not os.path.isfile(source_real):
        raise ValueError("Base SQLite source introuvable")
    if source_real == destination_real:
        raise ValueError("La destination ne peut pas etre la base source")
    destination_parent = os.path.dirname(destination_real)
    if not destination_parent:
        raise ValueError("Repertoire de destination SQLite invalide")
    os.makedirs(destination_parent, exist_ok=True)
    if not os.path.isdir(destination_parent):
        raise OSError("Repertoire de destination SQLite indisponible")
    destination_probe = os.path.join(destination_parent, ".faneva_copy_write_probe")
    try:
        with open(destination_probe, "wb") as probe:
            probe.write(b"FANEVA")
    finally:
        if os.path.exists(destination_probe):
            os.remove(destination_probe)
    if os.path.exists(destination_real):
        raise FileExistsError("Copie de destination deja presente : aucune ecriture de remplacement autorisee")

    wal_path = source_real + "-wal"
    shm_path = source_real + "-shm"
    wal_present = os.path.exists(wal_path)
    shm_present = os.path.exists(shm_path)
    if wal_present or shm_present:
        raise RuntimeError(
            "Copie binaire interrompue : état WAL/SHM détecté (WAL=%s, SHM=%s). "
            "Une copie cohérente requiert le traitement explicite de cet état."
            % ("PRÉSENT" if wal_present else "ABSENT", "PRÉSENT" if shm_present else "ABSENT")
        )
    source_sha_before = _file_sha256(source_real)
    with open(source_real, "rb") as source_handle, open(destination_real, "xb") as destination_handle:
        while True:
            block = source_handle.read(1024 * 1024)
            if not block:
                break
            destination_handle.write(block)
    source_sha_after = _file_sha256(source_real)
    if source_sha_after != source_sha_before:
        raise RuntimeError("Invariance de la base source non verifiee")
    if not os.path.isfile(destination_real) or os.path.getsize(destination_real) <= 0:
        raise OSError("Copie binaire destination absente ou vide")
    destination_sha256 = _file_sha256(destination_real)
    destination_size = os.path.getsize(destination_real)
    source_size = os.path.getsize(source_real)
    if source_size != destination_size:
        raise RuntimeError("Taille source et destination différentes")
    if source_sha_before != destination_sha256:
        raise RuntimeError("SHA-256 source et destination différents")
    return {
        "source_path": source_real,
        "source_sha256": source_sha_before,
        "source_size_bytes": source_size,
        "destination_path": destination_real,
        "destination_filename": os.path.basename(destination_real),
        "destination_size_bytes": destination_size,
        "destination_sha256": destination_sha256,
        "wal_present": wal_present,
        "shm_present": shm_present,
        "source_unchanged": True,
        "network_called": False,
        "source_sqlite_write_called": False,
    }


def export_quincaillerie_database_copies(external_source_path, internal_source_path, export_dir):
    """Crée les deux copies nommées demandées, sans comparer ni modifier les sources.

    Aucune connexion SQLite n’est ouverte : les sources sont lues uniquement en binaire.
    Les noms de destination sont fixes, l’état WAL/SHM bloque la copie et tout écrasement est refusé.
    """
    if not export_dir:
        raise ValueError("Repertoire de destination indisponible")
    external_real = os.path.realpath(os.path.abspath(str(external_source_path or "")))
    internal_real = os.path.realpath(os.path.abspath(str(internal_source_path or "")))
    if external_real == internal_real:
        raise ValueError("Les deux sources doivent etre des fichiers SQLite distincts")
    os.makedirs(export_dir, exist_ok=True)
    external_copy = os.path.join(export_dir, "stock_quincaillerie_externe.db")
    internal_copy = os.path.join(export_dir, "stock_quincaillerie_interne.db")
    return {
        "mode": "BINARY_FILE_COPY_ONLY_WAL_SHM_GUARD_NO_SQLITE_CONNECTION",
        "network_called": False,
        "external": _binary_sqlite_file_copy(external_real, external_copy),
        "internal": _binary_sqlite_file_copy(internal_real, internal_copy),
    }


def _diagnostic_exception_payload(exc, source_path, destination_path):
    """Sérialise toute erreur locale sans masquer ses détails techniques."""
    return {
        "exception_type": type(exc).__name__,
        "exception_str": str(exc),
        "exception_repr": repr(exc),
        "errno": getattr(exc, "errno", None),
        "strerror": getattr(exc, "strerror", None),
        "filename": getattr(exc, "filename", None),
        "source_path": source_path,
        "destination_path": destination_path,
        "traceback": traceback.format_exc(),
    }


def run_sqlite_export_step_diagnostic(source_path, destination_path, label, step_callback=None):
    """Teste une copie binaire locale étape par étape, sans connexion SQLite ni écriture source."""
    source_real = os.path.realpath(os.path.abspath(str(source_path or "")))
    destination_real = os.path.realpath(os.path.abspath(str(destination_path or "")))
    result = {
        "label": str(label),
        "source_path": source_real,
        "destination_path": destination_real,
        "steps": [],
        "network_called": False,
        "source_sqlite_write_called": False,
        "completed": False,
    }

    def record(step, status, path="", details=None):
        item = {"step": step, "status": status, "path": path or ""}
        if details is not None:
            item["details"] = details
        result["steps"].append(item)
        if step_callback is not None:
            try:
                step_callback(dict(item))
            except Exception:
                # Le diagnostic ne doit jamais échouer parce que son affichage UI est indisponible.
                pass

    def fail(step, exc):
        result["error"] = _diagnostic_exception_payload(exc, source_real, destination_real)
        record(step, "ÉCHEC", getattr(exc, "filename", None) or source_real, result["error"])
        return result

    try:
        record("ÉTAPE 4 — Accès en lecture à la source", "DÉBUT", source_real)
        if not os.path.isfile(source_real):
            raise FileNotFoundError(2, "Base SQLite source introuvable", source_real)
        source_size = os.path.getsize(source_real)
        source_sha_before = _file_sha256(source_real)
        result["source_size_bytes"] = source_size
        result["source_sha256_before"] = source_sha_before
        wal_path = source_real + "-wal"
        shm_path = source_real + "-shm"
        result["wal_present"] = os.path.exists(wal_path)
        result["shm_present"] = os.path.exists(shm_path)
        record("ÉTAPE 7 — Vérification fichier WAL", "OK", wal_path,
               "PRÉSENT" if result["wal_present"] else "ABSENT")
        record("ÉTAPE 8 — Vérification fichier SHM", "OK", shm_path,
               "PRÉSENT" if result["shm_present"] else "ABSENT")
        if result["wal_present"] or result["shm_present"]:
            raise RuntimeError(
                "Copie binaire interrompue : état WAL/SHM détecté (WAL=%s, SHM=%s). "
                "Une copie cohérente nécessite de traiter l’état WAL."
                % ("PRÉSENT" if result["wal_present"] else "ABSENT",
                   "PRÉSENT" if result["shm_present"] else "ABSENT")
            )
        record("ÉTAPE 9 — SHA-256 source avant copie", "OK", source_real, source_sha_before)
        record("ÉTAPE 10 — Copie binaire 1 MiB", "DÉBUT", destination_real)
        if source_real == destination_real:
            raise ValueError("Destination interdite : identique à la source")
        if os.path.exists(destination_real):
            raise FileExistsError(17, "Destination diagnostic déjà existante", destination_real)
        with open(source_real, "rb") as source_handle, open(destination_real, "xb") as destination_handle:
            while True:
                block = source_handle.read(1024 * 1024)
                if not block:
                    break
                destination_handle.write(block)
        record("ÉTAPE 10 — Copie binaire 1 MiB", "OK", destination_real)

        if not os.path.isfile(destination_real) or os.path.getsize(destination_real) <= 0:
            raise OSError("Copie diagnostic absente ou vide", destination_real)
        result["destination_size_bytes"] = os.path.getsize(destination_real)
        if result["destination_size_bytes"] != source_size:
            raise RuntimeError("Taille source != taille destination")
        record("ÉTAPE 11 — Comparaison tailles", "OK", destination_real,
               "source=%s ; destination=%s" % (source_size, result["destination_size_bytes"]))
        result["destination_sha256"] = _file_sha256(destination_real)
        if result["destination_sha256"] != source_sha_before:
            raise RuntimeError("SHA-256 source != SHA-256 destination")
        record("ÉTAPE 12 — SHA-256 destination", "OK", destination_real,
               result["destination_sha256"])

        source_sha_after = _file_sha256(source_real)
        result["source_sha256_after"] = source_sha_after
        if source_sha_after != source_sha_before:
            raise RuntimeError("SHA-256 source modifié pendant le diagnostic")
        record("ÉTAPE 13 — SHA-256 source après copie", "OK", source_real, source_sha_after)
        result["sha256_source_copy_equal"] = True
        record("CONTRÔLE 13 bis — SHA-256 source/destination", "OK", destination_real, "IDENTIQUES")
        result["source_unchanged"] = True
        result["completed"] = True
        return result
    except Exception as exc:
        return fail("ÉTAPE EN ÉCHEC", exc)


def inspect_sqlite_database_readonly(db_path, transaction_id):
    """Inspecte une base et ses fichiers associés sans aucune écriture.

    Le fichier SQLite est ouvert exclusivement avec ``mode=ro``. Toutes les
    requêtes sont des SELECT, après activation et contrôle de ``query_only``.
    Aucun PRAGMA de journal, d'intégrité ou de checkpoint n'est exécuté. Les
    fichiers DB/WAL/SHM sont uniquement lus en binaire pour calculer leur
    taille et leur SHA-256.
    """
    real_path = os.path.realpath(os.path.abspath(str(db_path or "")))
    result = {
        "mode": "SQLITE_MODE_RO_QUERY_ONLY_SELECT_AND_BINARY_READ",
        "path": real_path,
        "transaction_id": str(transaction_id or "").strip(),
        "network_called": False,
        "sqlite_write_called": False,
        "files": {},
        "table_names": [],
        "tables": [],
    }
    for suffix, key in (("", "db"), ("-wal", "wal"), ("-shm", "shm")):
        file_path = real_path + suffix
        exists = os.path.isfile(file_path)
        result["files"][key] = {
            "path": file_path,
            "exists": exists,
            "size_bytes": os.path.getsize(file_path) if exists else 0,
            "sha256": _file_sha256(file_path) if exists else None,
        }
    if not result["files"]["db"]["exists"]:
        exc = FileNotFoundError(2, "Base SQLite introuvable", real_path)
        result["error"] = _diagnostic_exception_payload(exc, real_path, "")
        result["error"]["step"] = "Vérification du fichier DB"
        return result

    def error_payload(exc, step):
        payload = _diagnostic_exception_payload(exc, real_path, "")
        payload["step"] = step
        return payload

    def quote_identifier(identifier):
        return '"' + str(identifier).replace('"', '""') + '"'

    def safe_value(column_name, value):
        column_lower = str(column_name or "").lower()
        if any(word in column_lower for word in ("api", "key", "secret", "token", "password")):
            return "<VALEUR MASQUÉE>"
        if value is None:
            return None
        if isinstance(value, bytes):
            return f"<BLOB {len(value)} octets>"
        value_text = str(value)
        if column_lower in ("payload", "data", "content"):
            return f"<CONTENU MASQUÉ : {len(value_text)} caractères, SHA-256={hashlib.sha256(value_text.encode('utf-8')).hexdigest()}>"
        return value_text

    try:
        uri = "file:" + real_path + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        try:
            conn.execute("PRAGMA query_only=ON")
            result["query_only"] = int(conn.execute("PRAGMA query_only").fetchone()[0])
            table_rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            result["table_names"] = [str(row[0]) for row in table_rows]
            table_set = set(result["table_names"])
            for table_name in result["table_names"]:
                table_report = {"name": table_name, "status": "PRÉSENTE"}
                try:
                    quoted_name = quote_identifier(table_name)
                    cursor = conn.execute(f"SELECT * FROM {quoted_name} LIMIT 0")
                    table_report["columns"] = [str(column[0]) for column in (cursor.description or [])]
                    table_report["row_count"] = int(
                        conn.execute(f"SELECT COUNT(*) FROM {quoted_name}").fetchone()[0]
                    )
                except Exception as table_exc:
                    table_report["status"] = "ERREUR DE LECTURE"
                    table_report["error"] = error_payload(
                        table_exc, f"Lecture du schéma et comptage de la table {table_name}"
                    )
                result["tables"].append(table_report)

            result["transactions_table_status"] = "PRÉSENTE" if "transactions" in table_set else "TABLE ABSENTE"
            result["pending_sync_table_status"] = "PRÉSENTE" if "pending_sync" in table_set else "TABLE ABSENTE"
            result["transactions_count"] = next(
                (item.get("row_count") for item in result["tables"] if item["name"] == "transactions"), None
            )
            result["pending_sync_count"] = next(
                (item.get("row_count") for item in result["tables"] if item["name"] == "pending_sync"), None
            )
            pilot_id = result["transaction_id"]
            result["uuid_transaction_row"] = None
            if "transactions" in table_set:
                cursor = conn.execute(
                    "SELECT * FROM transactions WHERE transaction_id=? LIMIT 1", (pilot_id,)
                )
                transaction_row = cursor.fetchone()
                column_names = [str(column[0]) for column in (cursor.description or [])]
                result["uuid_in_transactions"] = transaction_row is not None
                if transaction_row is not None:
                    result["uuid_transaction_row"] = {
                        column: safe_value(column, value)
                        for column, value in zip(column_names, transaction_row)
                    }
                    result["statut_local"] = result["uuid_transaction_row"].get("statut_local")
                else:
                    result["statut_local"] = None
            else:
                result["uuid_in_transactions"] = "TABLE ABSENTE"
                result["statut_local"] = "TABLE ABSENTE"
            if "pending_sync" in table_set:
                result["uuid_in_pending_sync"] = conn.execute(
                    "SELECT 1 FROM pending_sync WHERE transaction_id=? LIMIT 1", (pilot_id,)
                ).fetchone() is not None
            else:
                result["uuid_in_pending_sync"] = "TABLE ABSENTE"
            if "transactions" in table_set and "pending_sync" in table_set:
                result["uuid_in_pilot_join"] = conn.execute(
                    """
                    SELECT 1
                    FROM transactions t
                    JOIN pending_sync p ON p.transaction_id=t.transaction_id
                    WHERE t.statut_local='LOCAL' AND t.transaction_id=?
                    LIMIT 1
                    """,
                    (pilot_id,),
                ).fetchone() is not None
            else:
                result["uuid_in_pilot_join"] = "TABLE ABSENTE"
        finally:
            conn.close()
    except Exception as exc:
        result["error"] = error_payload(exc, "Ouverture SQLite readonly ou lecture des tables")
    return result


def compare_sqlite_databases_readonly(internal_path, external_path, transaction_id):
    """Compare deux états SQLite exclusivement par lecture locale."""
    internal = inspect_sqlite_database_readonly(internal_path, transaction_id)
    external = inspect_sqlite_database_readonly(external_path, transaction_id)
    result = {
        "mode": "LOCAL_READ_ONLY_INTERNAL_EXTERNAL_COMPARISON",
        "network_called": False,
        "sqlite_write_called": False,
        "internal": internal,
        "external": external,
    }
    if internal.get("error") or external.get("error"):
        result["comparison"] = {"available": False}
        return result
    result["comparison"] = {
        "available": True,
        "db_sha256_equal": internal["files"]["db"]["sha256"] == external["files"]["db"]["sha256"],
        "db_size_equal": internal["files"]["db"]["size_bytes"] == external["files"]["db"]["size_bytes"],
        "query_only_both_on": internal["query_only"] == 1 and external["query_only"] == 1,
        "table_names_equal": internal["table_names"] == external["table_names"],
        "table_details_equal": internal["tables"] == external["tables"],
        "transactions_count_equal": internal["transactions_count"] == external["transactions_count"],
        "pending_sync_count_equal": internal["pending_sync_count"] == external["pending_sync_count"],
        "uuid_in_transactions_equal": internal["uuid_in_transactions"] == external["uuid_in_transactions"],
        "statut_local_equal": internal["statut_local"] == external["statut_local"],
        "uuid_in_pending_sync_equal": internal["uuid_in_pending_sync"] == external["uuid_in_pending_sync"],
        "uuid_in_pilot_join_equal": internal["uuid_in_pilot_join"] == external["uuid_in_pilot_join"],
        "wal_presence_equal": internal["files"]["wal"]["exists"] == external["files"]["wal"]["exists"],
        "shm_presence_equal": internal["files"]["shm"]["exists"] == external["files"]["shm"]["exists"],
    }
    return result


def export_device_migration_manifest(conn, db_path, export_dir, label="device"):
    """Exporte une copie SQLite et la queue locale sans modifier les tables source."""
    status = device_identity_status(conn)
    if not status["migration_locked"]:
        raise ValueError("Le manifeste est reserve a une migration Device ID verrouillee")
    if not db_path or not os.path.exists(db_path):
        raise ValueError("Base SQLite source introuvable")

    os.makedirs(export_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_label = "".join(c for c in label if c.isalnum() or c in "-_") or "device"
    backup_path = os.path.join(export_dir, f"{safe_label}_{stamp}_source.sqlite")
    target = sqlite3.connect(backup_path)
    try:
        conn.backup(target)
    finally:
        target.close()

    cur = conn.cursor()
    cur.execute("""
        SELECT t.transaction_id, t.type_op, t.device_id, t.horodatage, t.payload
        FROM transactions t JOIN pending_sync p ON p.transaction_id=t.transaction_id
        WHERE t.statut_local='LOCAL' ORDER BY t.transaction_id
    """)
    pending = []
    for tx_id, op, device_id, timestamp, payload in cur.fetchall():
        canonical = json.dumps(json.loads(payload or "{}"), ensure_ascii=False,
                               sort_keys=True, separators=(",", ":"))
        pending.append({
            "transaction_id": tx_id,
            "type_op": op,
            "device_id_historique": device_id,
            "horodatage": timestamp,
            "payload_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        })
    uuid_list = [item["transaction_id"] for item in pending]
    if len(uuid_list) != len(set(uuid_list)):
        raise ValueError("UUID dupliques detectes dans pending_sync")
    with open(backup_path, "rb") as f:
        backup_sha256 = hashlib.sha256(f.read()).hexdigest()
    uuid_sha256 = hashlib.sha256("\n".join(uuid_list).encode("utf-8")).hexdigest()
    manifest = {
        "manifest_version": 1,
        "created_at": now_iso(),
        "label": safe_label,
        "device_id_legacy": status["device_id_legacy"],
        "device_id_proposed": status["device_id"],
        "pending_count": len(pending),
        "pending_uuid_sha256": uuid_sha256,
        "database_backup": {"filename": os.path.basename(backup_path), "sha256": backup_sha256},
        "pending": pending,
    }
    manifest_json = json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2)
    manifest_sha256 = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
    manifest["manifest_sha256"] = manifest_sha256
    manifest_path = os.path.join(export_dir, f"{safe_label}_{stamp}_migration_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, sort_keys=True)
    return {"manifest_path": manifest_path, "backup_path": backup_path,
            "manifest_sha256": manifest_sha256, "pending_count": len(pending)}


def describe_migration_export(result):
    """Retourne les informations de présentation d’un export sans lire ni modifier SQLite."""
    manifest_path = os.path.abspath(str(result.get("manifest_path") or ""))
    backup_path = os.path.abspath(str(result.get("backup_path") or ""))
    if not manifest_path.endswith(".json"):
        raise ValueError("Chemin manifeste JSON invalide")
    if not backup_path.endswith(".sqlite"):
        raise ValueError("Chemin copie SQLite invalide")
    return {
        "manifest_path": manifest_path,
        "manifest_filename": os.path.basename(manifest_path),
        "backup_path": backup_path,
        "backup_filename": os.path.basename(backup_path),
        "manifest_sha256": str(result.get("manifest_sha256") or ""),
        "pending_count": int(result.get("pending_count") or 0),
    }

def hash_pwd(p, salt=None):
    """PBKDF2-SHA256 pour les nouveaux comptes ; compatibilite SHA-256 legacy."""
    if salt:
        dk = hashlib.pbkdf2_hmac("sha256", p.encode("utf-8"), salt.encode("utf-8"), 100_000, dklen=32)
        return f"pbkdf2${salt}${dk.hex()}"
    return f"sha256${hashlib.sha256(p.encode()).hexdigest()}"

def check_pwd(p, stored):
    if stored.startswith("pbkdf2$"):
        _, salt, dk = stored.split("$", 2)
        return hash_pwd(p, salt) == stored
    if stored.startswith("sha256$"):
        return hashlib.sha256(p.encode()).hexdigest() == stored[7:]
    return hashlib.sha256(p.encode()).hexdigest() == stored

def fmt(n):
    if n is None:
        return "0"
    return f"{int(n):,}".replace(",", " ")

# ============================================================
# MIGRATION DE BASE DE DONNEES (additive, sans perte)
# ============================================================
MIGRATION_SQL = """
-- Table des magasins (dynamique, plus de dict en dur)
CREATE TABLE IF NOT EXISTS magasins(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cle TEXT UNIQUE NOT NULL,
    nom TEXT NOT NULL,
    description TEXT DEFAULT '',
    adresse TEXT DEFAULT '',
    categorie TEXT DEFAULT 'General',
    couleur TEXT DEFAULT '#4488BB',
    couleur_light TEXT DEFAULT '#DDEEFF',
    actif INTEGER DEFAULT 1,
    date_creation TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Catalogue produit partages (independant du magasin)
-- L'ancienne table 'produits' reste le catalogue local du magasin pour compatibilite ;
-- les nouvelles operations utilisent catalogues + stocks_magasin.
CREATE TABLE IF NOT EXISTS catalogues(
    id TEXT PRIMARY KEY,            -- UUID produit
    nom TEXT NOT NULL,
    categorie TEXT DEFAULT 'General',
    prix_achat_moyen INTEGER DEFAULT 0,
    prix_vente INTEGER DEFAULT 0,
    actif INTEGER DEFAULT 1,
    date_creation TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Stock par (produit, magasin)
CREATE TABLE IF NOT EXISTS stocks_magasin(
    produit_id TEXT NOT NULL,
    magasin_id INTEGER NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (produit_id, magasin_id)
);

-- Journal de transactions : source de verite pour la synchronisation
CREATE TABLE IF NOT EXISTS transactions(
    transaction_id TEXT PRIMARY KEY,   -- UUID unique (global)
    type_op TEXT NOT NULL,             -- VENTE, ACHAT, AJUST_STOCK, DETTE, PAIEMENT, PROD_CREATE, PROD_UPDATE, PROD_DELETE, MAG_CREATE, MAG_UPDATE, MAG_DELETE, USER_CREATE, USER_UPDATE, USER_DELETE, PERMISSION, MIGRATION_INITIAL_STOCK
    device_id TEXT NOT NULL,
    admin_id TEXT,                     -- UUID utilisateur (nullable pour legacy)
    admin_username TEXT,
    magasin_id INTEGER,                -- nullable (operations globales)
    magasin_cle TEXT,
    horodatage TEXT NOT NULL,          -- ISO UTC
    payload TEXT NOT NULL DEFAULT '{}',
    statut_local TEXT DEFAULT 'LOCAL', -- LOCAL, SYNCED
    serveur_version INTEGER DEFAULT 0, -- pour LWW sur les meta non mouvement
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tx_status ON transactions(statut_local);
CREATE INDEX IF NOT EXISTS idx_tx_horodatage ON transactions(horodatage);
CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(type_op);
CREATE INDEX IF NOT EXISTS idx_tx_magasin ON transactions(magasin_id);

-- Queue de synchronisation (vue logique : transactions non synchronisees)
-- Table de suivi pour affichage du compteur 'en attente'
CREATE TABLE IF NOT EXISTS pending_sync(
    transaction_id TEXT PRIMARY KEY,
    cible TEXT DEFAULT 'INTERNET',     -- INTERNET ou LOCAL
    tentative INTEGER DEFAULT 0,
    dernier_essai TEXT,
    FOREIGN KEY(transaction_id) REFERENCES transactions(transaction_id)
);

-- Appareils connus (recus lors des sync)
CREATE TABLE IF NOT EXISTS devices(
    device_id TEXT PRIMARY KEY,
    nom TEXT,
    derniere_sync TEXT
);

-- Utilisateurs HYBRID (UUID, partages entre appareils via transactions USER_*)
-- La table legacy 'utilisateurs' est conservee ; cette table est alimentee
-- automatiquement et synchronisee.
CREATE TABLE IF NOT EXISTS utilisateurs_hybrid(
    id TEXT PRIMARY KEY,               -- UUID
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    nom_complet TEXT,
    role TEXT DEFAULT 'ADMIN_MAGASIN', -- ADMIN_PRINCIPAL, ADMIN_MAGASIN, VENDEUR
    actif INTEGER DEFAULT 1,
    magasins_autorises TEXT DEFAULT '[]', -- JSON liste d'ids magasins (vide = tous pour ADMIN_PRINCIPAL)
    permissions TEXT DEFAULT '{}',        -- JSON module->0/1
    date_creation TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Permissions magasin <-> utilisateur HYBRID (redundant avec magasins_autorises,
-- mais pratique pour requetes)
CREATE TABLE IF NOT EXISTS permissions_magasin(
    utilisateur_id TEXT NOT NULL,
    magasin_id INTEGER NOT NULL,
    PRIMARY KEY (utilisateur_id, magasin_id)
);

-- Configuration HYBRID locale (device_id, serveur de relais, dernier vecteur de sync)
CREATE TABLE IF NOT EXISTS config_hybrid(
    cle TEXT PRIMARY KEY,
    valeur TEXT
);
"""

MIGRATION_STORES = """
-- Vue du stock legacy par produit (conservee pour l'ancien ecran STOCK)
CREATE TABLE IF NOT EXISTS _v_migration_stores(
    cle TEXT PRIMARY KEY,
    magasin_id INTEGER NOT NULL,
    produits_migres INTEGER DEFAULT 0,
    ventes_reetiquetees INTEGER DEFAULT 0,
    dettes_reetiquetees INTEGER DEFAULT 0,
    termine INTEGER DEFAULT 0
);
"""


def _table_exists(cur, name):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def _create_default_stores(cur):
    """Magasins par defaut de l'histoire FANEVA (Quincaillerie + Cosmetiques)."""
    try:
        cur.execute("INSERT OR IGNORE INTO magasins (cle, nom, description, categorie, couleur, couleur_light) VALUES (?,?,?,?,?,?)",
                    ("quincaillerie", "Quincaillerie", "Magasin principal - outils et materiaux", "Quincaillerie", "#337FB2", "#D9E6F2"))
        cur.execute("INSERT OR IGNORE INTO magasins (cle, nom, description, categorie, couleur, couleur_light) VALUES (?,?,?,?,?,?)",
                    ("cosmetiques", "Cosmetiques", "Magasin cosmetiques et soins", "Cosmetiques", "#E64D99", "#FFD9E9"))
    except Exception:
        pass

def _column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col for row in cur.fetchall())

def add_columns(cur, table, cols):
    for col, cdef in cols:
        if not _column_exists(cur, table, col):
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {cdef}")
            except Exception:
                pass


def run_migration(conn):
    """Applique toutes les migrations HYBRID. Idempotent. Aucune donnee supprimee."""
    cur = conn.cursor()
    cur.executescript(MIGRATION_SQL)
    cur.executescript(MIGRATION_STORES)

    # Magasins par defaut s'ils n'existent pas (repli securise ; le main.py
    # en cree de toute facon deux specifiques a l'histoire FANEVA)
    cur.execute("SELECT COUNT(*) FROM magasins")
    if cur.fetchone()[0] == 0:
        _create_default_stores(cur)

    # Ajouter les colonnes sync aux tables legacy (compatibilite ecrans existants)
    add_columns(cur, "produits", [
        ("hybrid_id", "TEXT"),                    # UUID catalogue associe
        ("magasin_ref_id", "INTEGER"),            # magasin d'origine
        ("date_sync", "TEXT"),
    ])
    add_columns(cur, "ventes", [
        ("transaction_id", "TEXT"),
        ("device_id", "TEXT"),
        ("magasin_ref_id", "INTEGER"),
    ])
    add_columns(cur, "dettes", [
        ("transaction_id", "TEXT"),
        ("device_id", "TEXT"),
        ("magasin_ref_id", "INTEGER"),
    ])
    add_columns(cur, "paiements_dettes", [
        ("transaction_id", "TEXT"),
        ("device_id", "TEXT"),
    ])
    add_columns(cur, "utilisateurs", [
        ("hybrid_id", "TEXT"),                    # UUID utilisateur hybrid
    ])
    # v1.4.9.16 — idempotence métier + anti-double paiement (additif, idempotent)
    add_columns(cur, "transactions", [
        ("sale_nonce", "TEXT"),
        ("business_fingerprint", "TEXT"),
        ("operation_id", "TEXT"),
    ])
    try:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_business_fingerprint
            ON transactions(business_fingerprint)
            WHERE business_fingerprint IS NOT NULL
              AND type_op IN ('VENTE', 'VENTE_CREDIT')
        """)
    except Exception:
        pass
    try:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_operation_id
            ON transactions(operation_id)
            WHERE operation_id IS NOT NULL AND type_op = 'PAIEMENT'
        """)
    except Exception:
        pass
    add_columns(cur, "ventes", [
        ("sale_mode", "TEXT"),  # COMPTANT | CREDIT (NULL = COMPTANT historique)
    ])
    add_columns(cur, "paiements_dettes", [
        ("operation_id", "TEXT"),
    ])
    conn.commit()


def create_store(conn, cle, nom, description="", adresse="", categorie="General",
                 couleur="#4488BB", couleur_light="#DDEEFF", admin_creatrice=None):
    """Creer un nouveau magasin avec une transaction MAG_CREATE et ses permissions."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM magasins WHERE cle=? OR nom=?", (cle, nom))
    if cur.fetchone():
        return None  # deja existant
    cur.execute(
        "INSERT INTO magasins (cle, nom, description, adresse, categorie, couleur, couleur_light) VALUES (?,?,?,?,?,?,?)",
        (cle, nom, description, adresse, categorie, couleur, couleur_light))
    magasin_id = cur.lastrowid
    tx_id = gen_uuid()
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_id, "MAG_CREATE", _local_device_id(conn), admin_creatrice, _local_username(conn),
         magasin_id, cle, now_iso(), json.dumps({
             "magasin_id": magasin_id, "cle": cle, "nom": nom,
             "description": description, "adresse": adresse, "categorie": categorie,
             "couleur": couleur, "couleur_light": couleur_light, "actif": 1}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    # L'administrateur qui cree le magasin y est automatiquement autorise
    if admin_creatrice:
        cur.execute("INSERT OR IGNORE INTO permissions_magasin VALUES (?,?)", (admin_creatrice, magasin_id))
    conn.commit()
    return magasin_id


def update_store(conn, magasin_id, **fields):
    cur = conn.cursor()
    sets, vals = [], []
    allowed = {"nom", "description", "adresse", "categorie", "couleur", "couleur_light", "actif"}
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=?")
            vals.append(v)
    if not sets:
        return False
    cur.execute(f"UPDATE magasins SET {','.join(sets)} WHERE id=?", vals + [magasin_id])
    tx_id = gen_uuid()
    cur.execute("SELECT cle, nom FROM magasins WHERE id=?", (magasin_id,))
    row = cur.fetchone()
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_id, "MAG_UPDATE", _local_device_id(conn), _local_admin_id(conn), _local_username(conn),
         magasin_id, row[0] if row else None, now_iso(),
         json.dumps({"magasin_id": magasin_id, **fields}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    conn.commit()
    return True


def delete_store(conn, magasin_id):
    cur = conn.cursor()
    cur.execute("SELECT id, cle, nom FROM magasins WHERE id=? AND actif=1", (magasin_id,))
    row = cur.fetchone()
    if not row:
        return False
    # Suppression logique : actif=0 (les donnees restent consultables + sync de la suppression)
    cur.execute("UPDATE magasins SET actif=0 WHERE id=?", (magasin_id,))
    tx_id = gen_uuid()
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_id, "MAG_DELETE", _local_device_id(conn), _local_admin_id(conn), _local_username(conn),
         magasin_id, row[1], now_iso(), json.dumps({"magasin_id": magasin_id, "nom": row[2]}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    conn.commit()
    return True


# ============================================================
# CATALOGUE + STOCK PAR MAGASIN (avec journal de mouvements)
# ============================================================
def create_product(conn, nom, prix_achat, prix_vente, categorie="General",
                   stock_initial=0, magasin_id=None, admin_id=None, admin_username=None):
    """Creer un produit partage + stock initial dans le magasin donne via transactions."""
    cur = conn.cursor()
    p_id = gen_uuid()
    cur.execute("INSERT INTO catalogues (id, nom, categorie, prix_achat_moyen, prix_vente) VALUES (?,?,?,?,?)",
                (p_id, nom, categorie, prix_achat, prix_vente))
    tx_prod = gen_uuid()
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_prod, "PROD_CREATE", _local_device_id(conn), admin_id, admin_username, magasin_id,
         _magasin_cle(conn, magasin_id), now_iso(),
         json.dumps({"produit_id": p_id, "nom": nom, "categorie": categorie,
                     "prix_achat": prix_achat, "prix_vente": prix_vente}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_prod,))
    if stock_initial > 0 and magasin_id is not None:
        _apply_stock_movement(conn, p_id, magasin_id, stock_initial, "MIGRATION_INITIAL_STOCK" if stock_initial == "migration" else "ACHAT",
                              admin_id, admin_username)
    conn.commit()
    return p_id


def update_product(conn, produit_id, **fields):
    cur = conn.cursor()
    sets, vals = [], []
    allowed = {"nom", "categorie", "prix_vente"}
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=?")
            vals.append(v)
    if sets:
        cur.execute(f"UPDATE catalogues SET {','.join(sets)} WHERE id=?", vals + [produit_id])
    tx_id = gen_uuid()
    cur.execute(
                "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, horodatage, payload) VALUES (?,?,?,?,?,?,?,?)",
        (tx_id, "PROD_UPDATE", _local_device_id(conn), _local_admin_id(conn), _local_username(conn), _find_magasin_id(conn, produit_id), now_iso(), json.dumps({"produit_id": produit_id, **fields}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    conn.commit()


def delete_product(conn, produit_id, magasin_id=None):
    cur = conn.cursor()
    cur.execute("UPDATE catalogues SET actif=0 WHERE id=?", (produit_id,))
    tx_id = gen_uuid()
    cur.execute(
                "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_id, "PROD_DELETE", _local_device_id(conn), _local_admin_id(conn), _local_username(conn), magasin_id,
         _magasin_cle(conn, magasin_id), now_iso(),
         json.dumps({"produit_id": produit_id}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    conn.commit()


def get_stock(conn, produit_id, magasin_id):
    canonical_id = resolve_canonical_product_id(conn, produit_id, magasin_id)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(stock),0) FROM stocks_magasin WHERE produit_id=? AND magasin_id=?",
                (canonical_id, magasin_id))
    return cur.fetchone()[0]


def recalc_stock(conn, produit_id, magasin_id, commit=True):
    """Recalcule la projection canonique depuis les mouvements, sans double déduction."""
    canonical_id = resolve_canonical_product_id(conn, produit_id, magasin_id)
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(quantite * signe), 0) FROM mouvements
        WHERE COALESCE(canonical_product_id, produit_id)=? AND magasin_id=?
          AND NOT (source='MIGRATION_INITIAL_STOCK' AND canonical_product_id IS NOT NULL AND canonical_seed_key IS NULL)
    """, (canonical_id, magasin_id))
    total = cur.fetchone()[0]
    cur.execute("INSERT OR REPLACE INTO stocks_magasin (produit_id, magasin_id, stock) VALUES (?,?,?)",
                (canonical_id, magasin_id, total))
    if commit:
        conn.commit()
    return total


MVT_SCHEMA = """
CREATE TABLE IF NOT EXISTS mouvements(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT UNIQUE NOT NULL,
    produit_id TEXT NOT NULL,
    canonical_product_id TEXT,
    canonical_seed_key TEXT,
    magasin_id INTEGER NOT NULL,
    quantite INTEGER NOT NULL,     -- + entree, - sortie
    signe INTEGER NOT NULL,        -- +1 / -1
    source TEXT NOT NULL,          -- VENTE, ACHAT, AJUST_STOCK, MIGRATION_INITIAL_STOCK
    horodatage TEXT NOT NULL,
    admin_id TEXT,
    admin_username TEXT
);
CREATE INDEX IF NOT EXISTS idx_mvt_product_store ON mouvements(produit_id, magasin_id);
CREATE INDEX IF NOT EXISTS idx_mvt_canonical_store ON mouvements(canonical_product_id, magasin_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mvt_canonical_seed_once
    ON mouvements(canonical_seed_key) WHERE canonical_seed_key IS NOT NULL;
"""

def ensure_mvt_table(conn):
    """Garantit le journal sans commit implicite, y compris dans un savepoint."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mouvements(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT UNIQUE NOT NULL,
            produit_id TEXT NOT NULL,
            canonical_product_id TEXT,
            canonical_seed_key TEXT,
            magasin_id INTEGER NOT NULL,
            quantite INTEGER NOT NULL,
            signe INTEGER NOT NULL,
            source TEXT NOT NULL,
            horodatage TEXT NOT NULL,
            admin_id TEXT,
            admin_username TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mvt_product_store ON mouvements(produit_id, magasin_id)")
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(mouvements)").fetchall()}
        if "canonical_product_id" not in columns:
            conn.execute("ALTER TABLE mouvements ADD COLUMN canonical_product_id TEXT")
        if "canonical_seed_key" not in columns:
            conn.execute("ALTER TABLE mouvements ADD COLUMN canonical_seed_key TEXT")
    except Exception:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mvt_canonical_store ON mouvements(canonical_product_id, magasin_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_mvt_canonical_seed_once ON mouvements(canonical_seed_key) WHERE canonical_seed_key IS NOT NULL")


def _canonical_manifest_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), CANONICAL_STAGING_MAPPING_FILE)


def _canonical_manifest_payload():
    """Lit et vérifie le manifeste embarqué; aucun mapping automatique par nom."""
    path = _canonical_manifest_path()
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    supplied_hash = manifest.get("manifest_sha256")
    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    calculated_hash = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if supplied_hash != calculated_hash:
        raise ValueError("Manifeste canonique staging invalide")
    if manifest.get("environment") != "STAGING_ONLY" or manifest.get("scope", {}).get("magasin_id") != 1:
        raise ValueError("Périmètre du manifeste canonique invalide")
    if manifest.get("summary", {}).get("total") != 47 or len(manifest.get("pairs", [])) != 47:
        raise ValueError("Manifeste canonique incomplet")
    return manifest


def initialize_canonical_product_mapping(conn):
    """Installe uniquement les alias explicitement approuvés, scopés magasin=1.

    Les UUID source et les transactions existantes restent intacts. Les semences
    initiales de la paire A1/A2 sont dédupliquées par clé canonique, alors que les
    ventes et entrées conservent leurs mouvements et transaction_id propres.
    """
    ensure_mvt_table(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS canonical_product_aliases_local(
            magasin_id INTEGER NOT NULL,
            source_product_id TEXT NOT NULL,
            canonical_product_id TEXT NOT NULL,
            mapping_version TEXT NOT NULL,
            manifest_sha256 TEXT NOT NULL,
            evidence_status TEXT NOT NULL,
            PRIMARY KEY(magasin_id, source_product_id)
        )
    """)
    manifest = _canonical_manifest_payload()
    if not manifest:
        return None
    cur = conn.cursor()
    for pair in manifest["pairs"]:
        evidence = pair.get("evidence_status", "USER_CONFIRMED")
        for source_id in (pair["a1_uuid"], pair["a2_uuid"]):
            cur.execute("""
                INSERT INTO canonical_product_aliases_local
                (magasin_id, source_product_id, canonical_product_id, mapping_version, manifest_sha256, evidence_status)
                VALUES (1,?,?,?,?,?)
                ON CONFLICT(magasin_id, source_product_id) DO UPDATE SET
                    canonical_product_id=excluded.canonical_product_id,
                    mapping_version=excluded.mapping_version,
                    manifest_sha256=excluded.manifest_sha256,
                    evidence_status=excluded.evidence_status
            """, (source_id, pair["canonical_proposed"], manifest["mapping_version"], manifest["manifest_sha256"], evidence))
    _config_set(cur, CANONICAL_MAPPING_CONFIG_KEY, manifest["mapping_version"])
    # Les mouvements historiques locaux conservent produit_id; seule leur clé de
    # projection est enrichie. Les produits hors manifeste restent eux-mêmes.
    rows = cur.execute("""
        SELECT id, produit_id, magasin_id, source FROM mouvements
        WHERE canonical_product_id IS NULL OR canonical_product_id=produit_id
    """).fetchall()
    seen_seeds = set()
    for row_id, source_id, magasin_id, source in rows:
        canonical_id = resolve_canonical_product_id(conn, source_id, magasin_id)
        candidate_seed = f"seed:{magasin_id}:{canonical_id}" if source == "MIGRATION_INITIAL_STOCK" else None
        seed_key = candidate_seed if candidate_seed and candidate_seed not in seen_seeds else None
        if candidate_seed:
            seen_seeds.add(candidate_seed)
        cur.execute("UPDATE mouvements SET canonical_product_id=?, canonical_seed_key=? WHERE id=?", (canonical_id, seed_key, row_id))
    cur.execute("SELECT DISTINCT COALESCE(canonical_product_id, produit_id), magasin_id FROM mouvements")
    touched = cur.fetchall()
    for canonical_id, magasin_id in touched:
        recalc_stock(conn, canonical_id, magasin_id, commit=False)
    conn.commit()
    return {"mapping_version": manifest["mapping_version"], "manifest_sha256": manifest["manifest_sha256"], "pairs": len(manifest["pairs"])}


def resolve_canonical_product_id(conn, produit_id, magasin_id):
    """Résout un alias explicite, sans fusion par nom ni entre magasins."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT canonical_product_id FROM canonical_product_aliases_local WHERE magasin_id=? AND source_product_id=?", (magasin_id, produit_id))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    except sqlite3.OperationalError:
        pass
    return produit_id


def _apply_stock_movement(conn, produit_id, magasin_id, quantite, source,
                          admin_id=None, admin_username=None, transaction_id=None, occurred_at=None):
    """Conserve le produit source et projette le stock par produit canonique/magasin."""
    ensure_mvt_table(conn)
    cur = conn.cursor()
    tx_id = transaction_id or gen_uuid()
    canonical_id = resolve_canonical_product_id(conn, produit_id, magasin_id)
    # Le stock est SUM(quantite*signe) : quantite est toujours positive (valeur absolue),
    # signe porte la direction (entree +1, sortie -1)
    signe = 1 if quantite >= 0 else -1
    q_abs = abs(quantite)
    seed_key = f"seed:{magasin_id}:{canonical_id}" if source == "MIGRATION_INITIAL_STOCK" else None
    cur.execute("""
        INSERT OR IGNORE INTO mouvements
        (transaction_id, produit_id, canonical_product_id, canonical_seed_key, magasin_id, quantite, signe, source, horodatage, admin_id, admin_username)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (tx_id, produit_id, canonical_id, seed_key, magasin_id, q_abs, signe, source, occurred_at or now_iso(), admin_id, admin_username))
    recalc_stock(conn, canonical_id, magasin_id, commit=False)
    # Si c'est un ACHAT/MIGRATION (entree), maj prix moyen pondere
    if source in ("ACHAT", "MIGRATION_INITIAL_STOCK") and signe > 0:
        # payload transmis via transaction ; le prix est mis a jour par l'appelant
        pass
    return tx_id


def _local_device_id(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT valeur FROM config_hybrid WHERE cle='device_id'")
        r = cur.fetchone()
        if r:
            return r[0]
    except Exception:
        pass
    return "device-inconnu"


def _require_local_device_id(conn):
    """Refuse toute nouvelle operation si l'identite persistante est absente."""
    device_id = _local_device_id(conn)
    if not isinstance(device_id, str) or not device_id.strip() or device_id == "device-inconnu":
        raise ValueError("device_id local obligatoire : operation refusee")
    return device_id.strip()


def _local_username(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT valeur FROM config_hybrid WHERE cle='session_username'")
        r = cur.fetchone()
        if r:
            return r[0]
    except Exception:
        pass
    return None


def _local_admin_id(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT valeur FROM config_hybrid WHERE cle='session_user_id'")
        r = cur.fetchone()
        if r:
            return r[0]
    except Exception:
        pass
    return None



def _sale_business_fingerprint(magasin_id, canonical_lines, sale_nonce):
    """Empreinte déterministe d'un événement de vente métier.

    Inclut magasin + lignes normalisées (canonical_id + quantité) + sale_nonce.
    Deux opérations distinctes (nonces différents) produisent des empreintes distinctes.
    Ne dépend pas de transaction_id ni d'un timestamp.
    """
    lines = []
    for ligne in canonical_lines:
        cid = str(ligne.get("canonical_product_uuid") or ligne.get("produit_id") or "")
        qty = int(ligne.get("q") or 0)
        lines.append(f"{cid}:{qty}")
    lines.sort()
    raw = f"m={int(magasin_id)}|lines={';'.join(lines)}|nonce={str(sale_nonce or '').strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _ensure_tx_idempotence_schema(conn):
    """Garantit colonnes/index d'idempotence sans commit implicite."""
    cur = conn.cursor()
    add_columns(cur, "transactions", [
        ("sale_nonce", "TEXT"),
        ("business_fingerprint", "TEXT"),
        ("operation_id", "TEXT"),
    ])
    try:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_business_fingerprint
            ON transactions(business_fingerprint)
            WHERE business_fingerprint IS NOT NULL
              AND type_op IN ('VENTE', 'VENTE_CREDIT')
        """)
    except Exception:
        pass
    try:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_operation_id
            ON transactions(operation_id)
            WHERE operation_id IS NOT NULL AND type_op = 'PAIEMENT'
        """)
    except Exception:
        pass


# ============================================================
# VENTE / ACHAT / DETTE / PAIEMENT ORIENTES TRANSACTIONS
# ============================================================
def record_sale(conn, panier, magasin_id, admin_id=None, admin_username=None,
                transaction_id=None, commit=True, sale_nonce=None, sale_mode="COMPTANT"):
    """
    Cree une transaction VENTE et ses mouvements sous verrou exclusif.

    D1: BEGIN IMMEDIATE + re-check stock sous verrou + debit atomique.
    Idempotence:
      - meme transaction_id -> NO-OP
      - meme business_fingerprint (meme evenement metier + sale_nonce) sous nouvel ID -> NO-OP
    stock final jamais negatif.
    Avec commit=False, l'appelant complete la TX SQLite avant commit.
    """
    ensure_mvt_table(conn)
    _ensure_tx_idempotence_schema(conn)
    cur = conn.cursor()
    device_id = _require_local_device_id(conn)
    tx_id = str(transaction_id).strip() if transaction_id else gen_uuid()
    if not tx_id:
        raise ValueError("transaction_id obligatoire")
    nonce = str(sale_nonce).strip() if sale_nonce else gen_uuid()
    if not nonce:
        raise ValueError("sale_nonce obligatoire")
    type_op = "VENTE_CREDIT" if str(sale_mode).upper() == "CREDIT" else "VENTE"

    # Acquerir le verrou exclusif avant check + debit (sauf si deja en transaction)
    started_here = False
    if not conn.in_transaction:
        cur.execute("BEGIN IMMEDIATE")
        started_here = True
    try:
        # 1) Retry technique: meme transaction_id => NO-OP
        cur.execute("SELECT 1 FROM transactions WHERE transaction_id=?", (tx_id,))
        if cur.fetchone():
            if started_here and commit:
                conn.commit()
            return tx_id

        canonical_lines = []
        requested_by_canonical = {}
        for ligne in panier:
            source_id = ligne["produit_id"]
            quantity = int(ligne["q"])
            if quantity <= 0:
                raise ValueError("quantite de vente invalide")
            canonical_id = resolve_canonical_product_id(conn, source_id, magasin_id)
            requested_by_canonical[canonical_id] = requested_by_canonical.get(canonical_id, 0) + quantity
            wire_line = dict(ligne)
            wire_line["canonical_product_uuid"] = canonical_id
            canonical_lines.append(wire_line)

        fingerprint = _sale_business_fingerprint(magasin_id, canonical_lines, nonce)

        # 2) Re-injection metier sous nouvel ID (meme nonce + memes lignes) => NO-OP
        cur.execute(
            "SELECT transaction_id FROM transactions WHERE business_fingerprint=? AND type_op IN ('VENTE','VENTE_CREDIT')",
            (fingerprint,))
        existing = cur.fetchone()
        if existing:
            if started_here and commit:
                conn.commit()
            return existing[0]

        # 3) Re-check stock SOUS VERROU
        for canonical_id, quantity in requested_by_canonical.items():
            available = get_stock(conn, canonical_id, magasin_id)
            if available - quantity < 0:
                raise ValueError("Stock commun insuffisant")

        payload = {
            "magasin_id": magasin_id,
            "lignes": canonical_lines,
            "admin_id": admin_id,
            "admin_username": admin_username,
            "sale_nonce": nonce,
            "sale_mode": "CREDIT" if type_op == "VENTE_CREDIT" else "COMPTANT",
        }
        try:
            cur.execute(
                "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload, sale_nonce, business_fingerprint) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (tx_id, type_op, device_id, admin_id, admin_username, magasin_id,
                 _magasin_cle(conn, magasin_id), now_iso(), json.dumps(payload, ensure_ascii=False),
                 nonce, fingerprint))
        except sqlite3.IntegrityError:
            # Course sur fingerprint unique: traiter comme NO-OP metier
            cur.execute(
                "SELECT transaction_id FROM transactions WHERE business_fingerprint=?",
                (fingerprint,))
            row = cur.fetchone()
            if row:
                if started_here and commit:
                    conn.commit()
                return row[0]
            raise

        cur.execute("INSERT OR IGNORE INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
        movement_count = len(canonical_lines)
        for index, ligne in enumerate(canonical_lines):
            movement_id = tx_id if movement_count == 1 else f"{tx_id}:{index}"
            _apply_stock_movement(conn, ligne["produit_id"], magasin_id, -int(ligne["q"]), "VENTE",
                                  admin_id, admin_username, movement_id)
            # Garde-fou final: projection ne doit jamais valider un stock negatif
            can_id = ligne["canonical_product_uuid"]
            if get_stock(conn, can_id, magasin_id) < 0:
                raise ValueError("Stock commun insuffisant")

        if commit:
            conn.commit()
        return tx_id
    except Exception:
        if started_here:
            try:
                conn.rollback()
            except Exception:
                pass
        raise


def record_stock_in(conn, produit_id, magasin_id, quantite, nouveau_pa, source="ACHAT",
                    admin_id=None, admin_username=None):
    """Entree de stock (achat/reception) : mouvement + MAJ prix moyen pondere."""
    ensure_mvt_table(conn)
    cur = conn.cursor()
    tx_id = gen_uuid()
    canonical_id = resolve_canonical_product_id(conn, produit_id, magasin_id)
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_id, source, _local_device_id(conn), admin_id, admin_username, magasin_id,
         _magasin_cle(conn, magasin_id), now_iso(),
         json.dumps({"produit_id": produit_id, "canonical_product_uuid": canonical_id, "magasin_id": magasin_id, "quantite": quantite,
                     "prix_achat": nouveau_pa, "source": source}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    # Stock AVANT le mouvement pour le prix moyen pondere
    stock_avant = get_stock(conn, canonical_id, magasin_id)
    _apply_stock_movement(conn, produit_id, magasin_id, int(quantite), source, admin_id, admin_username, tx_id)
    stock_actuel = stock_avant + int(quantite)
    cur.execute("SELECT prix_achat_moyen FROM catalogues WHERE id=?", (produit_id,))
    row = cur.fetchone()
    if row:
        pa_actuel = row[0]
        nouveau_pa_moyen = ((stock_actuel * pa_actuel) + (quantite * nouveau_pa)) / (stock_actuel + quantite) \
            if (stock_actuel + quantite) > 0 else nouveau_pa
        cur.execute("UPDATE catalogues SET prix_achat_moyen=? WHERE id=?", (nouveau_pa_moyen, produit_id))
    conn.commit()
    return tx_id


def record_debt(conn, client, telephone, montant, admin_id=None, admin_username=None,
                magasin_id=None, transaction_id=None, dette_transaction_id=None, commit=True):
    cur = conn.cursor()
    device_id = _require_local_device_id(conn)
    tx_id = str(transaction_id).strip() if transaction_id else gen_uuid()
    if not tx_id:
        raise ValueError("transaction_id obligatoire")
    cur.execute("SELECT 1 FROM transactions WHERE transaction_id=?", (tx_id,))
    if cur.fetchone():
        return tx_id
    dette_ref = str(dette_transaction_id).strip() if dette_transaction_id else tx_id
    payload = {"client": client, "telephone": telephone, "montant": int(montant),
               "dette_transaction_id": dette_ref}
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload) VALUES (?,?,?,?,?,?,?,?,?)",
        (tx_id, "DETTE", device_id, admin_id, admin_username, magasin_id,
         _magasin_cle(conn, magasin_id), now_iso(), json.dumps(payload, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    if commit:
        conn.commit()
    return tx_id



def record_credit_sale(conn, panier, magasin_id, client, telephone, montant_dette,
                       admin_id=None, admin_username=None, transaction_id=None,
                       sale_nonce=None, commit=True):
    """Vente a credit atomique: vente + stock + dette dans UNE transaction SQLite.

    Toute exception provoque un ROLLBACK integral (stock restaure, aucune dette).
    """
    ensure_mvt_table(conn)
    _ensure_tx_idempotence_schema(conn)
    tx_id = str(transaction_id).strip() if transaction_id else gen_uuid()
    nonce = str(sale_nonce).strip() if sale_nonce else gen_uuid()
    started_here = False
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
        started_here = True
    try:
        # Vente (mode CREDIT) — debit stock + fingerprint
        sale_id = record_sale(
            conn, panier, magasin_id,
            admin_id=admin_id, admin_username=admin_username,
            transaction_id=tx_id, commit=False,
            sale_nonce=nonce, sale_mode="CREDIT",
        )
        # Dette liee a la meme transaction metier
        debt_tx = record_debt(
            conn, client, telephone, int(montant_dette),
            admin_id=admin_id, admin_username=admin_username,
            magasin_id=magasin_id,
            transaction_id=f"{sale_id}:DETTE",
            dette_transaction_id=sale_id,
            commit=False,
        )
        # Projection legacy dette (table dettes) dans la meme TX
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO dettes (client, telephone, total, paye, reste, statut, transaction_id, device_id, magasin_ref_id) VALUES (?,?,?,?,?,?,?,?,?)",
            (client, telephone or "", int(montant_dette), 0, int(montant_dette), "ACTIF",
             debt_tx, _local_device_id(conn), magasin_id))
        if commit:
            conn.commit()
        return {"sale_transaction_id": sale_id, "debt_transaction_id": debt_tx}
    except Exception:
        if started_here:
            try:
                conn.rollback()
            except Exception:
                pass
        raise


def record_payment(conn, dette_id, montant, admin_id=None, admin_username=None,
                   magasin_id=None, transaction_id=None, commit=True, operation_id=None):
    """Enregistre un paiement de dette. operation_id garantit l'anti-double (retry = NO-OP)."""
    _ensure_tx_idempotence_schema(conn)
    cur = conn.cursor()
    device_id = _require_local_device_id(conn)
    cur.execute("SELECT transaction_id, client, telephone FROM dettes WHERE id=?", (dette_id,))
    debt_row = cur.fetchone()
    if not debt_row:
        raise ValueError("Dette introuvable pour paiement")
    tx_id = str(transaction_id).strip() if transaction_id else gen_uuid()
    if not tx_id:
        raise ValueError("transaction_id obligatoire")
    op_id = str(operation_id).strip() if operation_id else tx_id

    started_here = False
    if not conn.in_transaction:
        cur.execute("BEGIN IMMEDIATE")
        started_here = True
    try:
        cur.execute("SELECT 1 FROM transactions WHERE transaction_id=?", (tx_id,))
        if cur.fetchone():
            if started_here and commit:
                conn.commit()
            return tx_id
        # Anti-double via operation_id
        cur.execute(
            "SELECT transaction_id FROM transactions WHERE operation_id=? AND type_op='PAIEMENT'",
            (op_id,))
        existing = cur.fetchone()
        if existing:
            if started_here and commit:
                conn.commit()
            return existing[0]

        dette_ref = debt_row[0] or ""
        payload = {"dette_id": dette_id, "dette_transaction_id": dette_ref,
                   "client": debt_row[1], "telephone": debt_row[2], "montant": int(montant),
                   "operation_id": op_id}
        try:
            cur.execute(
                "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, horodatage, payload, operation_id) VALUES (?,?,?,?,?,?,?,?,?)",
                (tx_id, "PAIEMENT", device_id, admin_id, admin_username, magasin_id, now_iso(),
                 json.dumps(payload, ensure_ascii=False), op_id))
        except sqlite3.IntegrityError:
            cur.execute(
                "SELECT transaction_id FROM transactions WHERE operation_id=? AND type_op='PAIEMENT'",
                (op_id,))
            row = cur.fetchone()
            if row:
                if started_here and commit:
                    conn.commit()
                return row[0]
            raise
        cur.execute("INSERT OR IGNORE INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
        if commit:
            conn.commit()
        return tx_id
    except Exception:
        if started_here:
            try:
                conn.rollback()
            except Exception:
                pass
        raise



def get_daily_cash_report(conn, day_iso=None):
    """Separe CA comptant, credit et recouvrements. Caisse reelle = comptant + recouvrements."""
    cur = conn.cursor()
    if day_iso is None:
        day_filter_ventes = "date(date_vente)=date('now','localtime')"
        day_filter_tx = "date(horodatage)=date('now')"
        params_v, params_t = (), ()
    else:
        day_filter_ventes = "date(date_vente)=date(?)"
        day_filter_tx = "date(horodatage)=date(?)"
        params_v, params_t = (day_iso,), (day_iso,)

    cash_total = cash_count = credit_total = credit_count = 0
    try:
        # Comptant: ventes sans mode CREDIT (NULL ou COMPTANT)
        cur.execute(f"""
            SELECT COALESCE(SUM(total),0), COUNT(*) FROM ventes
            WHERE {day_filter_ventes}
              AND (sale_mode IS NULL OR UPPER(sale_mode)='COMPTANT')
        """, params_v)
        cash_total, cash_count = cur.fetchone()

        cur.execute(f"""
            SELECT COALESCE(SUM(total),0), COUNT(*) FROM ventes
            WHERE {day_filter_ventes}
              AND UPPER(COALESCE(sale_mode,''))='CREDIT'
        """, params_v)
        credit_total, credit_count = cur.fetchone()
    except sqlite3.OperationalError:
        # Table ventes absente (contexte test minimal) : rester a 0
        pass

    # Recouvrements = paiements de dette du jour
    cur.execute(f"""
        SELECT COALESCE(SUM(CAST(json_extract(payload,'$.montant') AS INTEGER)),0), COUNT(*)
        FROM transactions
        WHERE type_op='PAIEMENT' AND {day_filter_tx}
    """, params_t)
    recovery_total, recovery_count = cur.fetchone()

    # Fallback si json_extract indisponible: table paiements_dettes
    if recovery_total == 0 and recovery_count == 0:
        try:
            cur.execute(f"""
                SELECT COALESCE(SUM(montant),0), COUNT(*) FROM paiements_dettes
                WHERE date(date_paiement)=date('now','localtime')
            """ if day_iso is None else """
                SELECT COALESCE(SUM(montant),0), COUNT(*) FROM paiements_dettes
                WHERE date(date_paiement)=date(?)
            """, params_t if day_iso else ())
            recovery_total, recovery_count = cur.fetchone()
        except Exception:
            pass

    cash_total = int(cash_total or 0)
    credit_total = int(credit_total or 0)
    recovery_total = int(recovery_total or 0)
    return {
        "comptant_total": cash_total,
        "comptant_count": int(cash_count or 0),
        "credit_total": credit_total,
        "credit_count": int(credit_count or 0),
        "recouvrements_total": recovery_total,
        "recouvrements_count": int(recovery_count or 0),
        "caisse_reelle": cash_total + recovery_total,
        "transaction_ids": _report_transaction_ids(conn, day_iso),
    }


def _report_transaction_ids(conn, day_iso=None):
    cur = conn.cursor()
    if day_iso is None:
        cur.execute("""
            SELECT transaction_id, type_op, operation_id, sale_nonce, business_fingerprint
            FROM transactions
            WHERE type_op IN ('VENTE','VENTE_CREDIT','PAIEMENT','DETTE')
              AND date(horodatage)=date('now')
            ORDER BY horodatage
        """)
    else:
        cur.execute("""
            SELECT transaction_id, type_op, operation_id, sale_nonce, business_fingerprint
            FROM transactions
            WHERE type_op IN ('VENTE','VENTE_CREDIT','PAIEMENT','DETTE')
              AND date(horodatage)=date(?)
            ORDER BY horodatage
        """, (day_iso,))
    rows = []
    for r in cur.fetchall():
        rows.append({
            "transaction_id": r[0], "type_op": r[1],
            "operation_id": r[2], "sale_nonce": r[3],
            "business_fingerprint": r[4],
        })
    return rows


# ============================================================
# UTILISATEURS HYBRID
# ============================================================
def create_user_hybrid(conn, username, password, nom_complet="", role="ADMIN_MAGASIN",
                       magasins_autorises=None, admin_creatrice=None):
    cur = conn.cursor()
    cur.execute("SELECT id FROM utilisateurs_hybrid WHERE username=?", (username,))
    if cur.fetchone():
        return None
    u_id = gen_uuid()
    cur.execute(
        "INSERT INTO utilisateurs_hybrid (id, username, password_hash, nom_complet, role, magasins_autorises) VALUES (?,?,?,?,?,?)",
        (u_id, username, hash_pwd(password), nom_complet, role,
         json.dumps(magasins_autorises or [], ensure_ascii=False)))
    # Reflexe legacy : compte aussi dans 'utilisateurs' pour l'ecran PARAMETRES existant
    try:
        cur.execute("INSERT OR IGNORE INTO utilisateurs (username, password_hash, nom_complet, role) VALUES (?,?,?,?)",
                    (username, hash_pwd(password), nom_complet, role))
        cur.execute("UPDATE utilisateurs SET hybrid_id=? WHERE username=?", (u_id, username))
    except Exception:
        pass
    tx_id = gen_uuid()
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, horodatage, payload) VALUES (?,?,?,?,?,?,?,?)",
        (tx_id, "USER_CREATE", _local_device_id(conn), admin_creatrice, _local_username(conn), None, now_iso(),
         json.dumps({"user_id": u_id, "username": username, "nom_complet": nom_complet,
                     "role": role, "magasins_autorises": magasins_autorises or []}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    for m_id in (magasins_autorises or []):
        cur.execute("INSERT OR IGNORE INTO permissions_magasin VALUES (?,?)", (u_id, m_id))
    conn.commit()
    return u_id


def update_user_hybrid(conn, user_id, **fields):
    cur = conn.cursor()
    sets, vals = [], []
    allowed = {"nom_complet", "role", "actif"}
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=?")
            vals.append(v)
    if "magasins_autorises" in fields:
        sets.append("magasins_autorises=?")
        vals.append(json.dumps(fields["magasins_autorises"], ensure_ascii=False))
    if "password" in fields:
        sets.append("password_hash=?")
        vals.append(hash_pwd(fields["password"]))
    if sets:
        cur.execute(f"UPDATE utilisateurs_hybrid SET {','.join(sets)} WHERE id=?", vals + [user_id])
    tx_id = gen_uuid()
    cur.execute(
        "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, horodatage, payload) VALUES (?,?,?,?,?,?,?,?)",
        (tx_id, "USER_UPDATE", _local_device_id(conn), _local_admin_id(conn), _local_username(conn), None, now_iso(),
         json.dumps({"user_id": user_id, **{k: v for k, v in fields.items() if k != "password"}}, ensure_ascii=False)))
    cur.execute("INSERT INTO pending_sync (transaction_id, cible) VALUES (?, 'INTERNET')", (tx_id,))
    conn.commit()


# ============================================================
# SYNC ENGINE (INTERNET + LOCAL) - idempotent, dedup par UUID
# ============================================================
def _magasin_cle(conn, magasin_id):
    try:
        cur = conn.cursor()
        cur.execute("SELECT cle FROM magasins WHERE id=?", (magasin_id,))
        r = cur.fetchone()
        return r[0] if r else None
    except Exception:
        return None


def _find_magasin_id(conn, produit_id):
    """Retrouve le magasin associe a un produit (pour les tx sans magasin explicite)."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT magasin_id FROM stocks_magasin WHERE produit_id=? LIMIT 1", (produit_id,))
        r = cur.fetchone()
        return r[0] if r else None
    except Exception:
        return None


def pending_transactions(conn):
    """Toutes les transactions locales en attente, y compris les historiques.

    Cette fonction reste la source de la migration historique. Les chemins normaux
    doivent utiliser ``normal_pending_transactions`` afin de ne jamais mélanger les
    deux périmètres sous ``migration_lock``.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT t.transaction_id FROM transactions t
        JOIN pending_sync p ON p.transaction_id = t.transaction_id
        WHERE t.statut_local = 'LOCAL'
    """)
    return [r[0] for r in cur.fetchall()]


def _is_uuid_v4(value):
    """Valide strictement un UUID v4 canonique sans modifier la base."""
    if not isinstance(value, str):
        return False
    try:
        parsed = uuid.UUID(value)
        return parsed.version == 4 and str(parsed) == value.lower()
    except (ValueError, AttributeError, TypeError):
        return False


def _normal_sync_metadata_valid(row):
    """Valide uniquement les champs nécessaires au contrat normal du Worker.

    ``admin_id``, ``magasin_id`` et ``magasin_cle`` restent optionnels comme dans
    le contrat Worker staging et le schéma local (les opérations globales/legacy
    peuvent les laisser nuls). Lorsqu’ils sont présents, ils sont transportés
    inchangés par ``export_outgoing``.
    """
    transaction_id, type_op, device_id, admin_id, magasin_id, magasin_cle, horodatage, payload = row
    if not _is_uuid_v4(transaction_id) or not _is_uuid_v4(device_id):
        return False
    if not isinstance(type_op, str) or not type_op.strip() or len(type_op) > 64:
        return False
    if not isinstance(horodatage, str) or not horodatage.strip() or len(horodatage) > 64:
        return False
    if not isinstance(payload, str) or not payload.strip():
        return False
    try:
        json.loads(payload)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if admin_id is not None and (not isinstance(admin_id, str) or len(admin_id) > 128):
        return False
    if magasin_id is not None and not isinstance(magasin_id, int):
        return False
    if magasin_cle is not None and (not isinstance(magasin_cle, str) or len(magasin_cle) > 128):
        return False
    return True


def normal_pending_transactions(conn):
    """Retourne uniquement les transactions normales UUID éligibles.

    Le filtre s’applique sous ou hors migration_lock. Il exige l’UUID v4 de la
    transaction et de l’appareil, l’égalité avec l’appareil courant, la différence
    avec l’appareil legacy, ``LOCAL``, une ligne ``pending_sync`` et les champs
    minimaux du Worker. Il ne supprime, ne marque et ne modifie aucune ligne.
    """
    status = device_identity_status(conn)
    current_device_id = status.get("device_id")
    legacy_device_id = status.get("device_id_legacy")
    if not _is_uuid_v4(current_device_id):
        return []
    cur = conn.cursor()
    cur.execute("""
        SELECT t.transaction_id, t.type_op, t.device_id, t.admin_id,
               t.magasin_id, t.magasin_cle, t.horodatage, t.payload
        FROM transactions t
        JOIN pending_sync p ON p.transaction_id = t.transaction_id
        WHERE t.statut_local = 'LOCAL'
        ORDER BY t.transaction_id
    """)
    eligible = []
    for row in cur.fetchall():
        if row[2] != current_device_id or row[2] == legacy_device_id:
            continue
        if not _normal_sync_metadata_valid(row):
            continue
        eligible.append(row[0])
    return eligible


def diagnose_pilot_database(db_path, transaction_id, expected_sha256=None):
    """Inspecte une base de pilote sans écriture ni accès réseau.

    Cette sonde temporaire ouvre exclusivement le fichier résolu en URI SQLite
    ``mode=ro``. Elle ne modifie ni la base, ni pending_sync, ni config_hybrid ;
    elle sert à prouver le chemin réellement consulté au moment du diagnostic.
    """
    requested_path = (db_path or "").strip()
    absolute_path = os.path.abspath(requested_path) if requested_path else ""
    real_path = os.path.realpath(absolute_path) if absolute_path else ""
    result = {
        "mode": "SQLITE_READ_ONLY_MODE_RO_SELECT_ONLY",
        "requested_path": requested_path,
        "absolute_path": absolute_path,
        "real_path": real_path,
        "filename": os.path.basename(real_path) if real_path else "",
        "exists": bool(real_path and os.path.isfile(real_path)),
        "transaction_id": (transaction_id or "").strip(),
        "expected_sha256": (expected_sha256 or "").strip().lower(),
        "network_called": False,
        "sqlite_write_called": False,
    }
    if not result["exists"]:
        result["error"] = "Base SQLite active introuvable"
        return result

    try:
        result["size_bytes"] = os.path.getsize(real_path)
        digest = hashlib.sha256()
        with open(real_path, "rb") as database_file:
            for block in iter(lambda: database_file.read(1024 * 1024), b""):
                digest.update(block)
        result["sha256"] = digest.hexdigest()
        result["matches_expected_sha256"] = (
            result["sha256"] == result["expected_sha256"]
            if result["expected_sha256"] else None
        )
        result["wal"] = {
            "path": real_path + "-wal",
            "exists": os.path.isfile(real_path + "-wal"),
            "size_bytes": os.path.getsize(real_path + "-wal") if os.path.isfile(real_path + "-wal") else 0,
        }
        result["shm"] = {
            "path": real_path + "-shm",
            "exists": os.path.isfile(real_path + "-shm"),
            "size_bytes": os.path.getsize(real_path + "-shm") if os.path.isfile(real_path + "-shm") else 0,
        }

        # `mode=ro` interdit toute écriture dans le fichier. query_only protège
        # aussi cette connexion contre une instruction d'écriture accidentelle.
        uri = "file:" + real_path + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        try:
            conn.execute("PRAGMA query_only = ON")
            result["transactions_count"] = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            result["pending_sync_count"] = conn.execute("SELECT COUNT(*) FROM pending_sync").fetchone()[0]
            result["pilot_join_total"] = conn.execute(
                """
                SELECT COUNT(*)
                FROM transactions t
                JOIN pending_sync p ON p.transaction_id = t.transaction_id
                WHERE t.statut_local = 'LOCAL'
                """
            ).fetchone()[0]
            pilot_id = result["transaction_id"]
            result["transaction_rows"] = [
                {"transaction_id": row[0], "statut_local": row[1], "type_op": row[2], "device_id": row[3]}
                for row in conn.execute(
                    "SELECT transaction_id, statut_local, type_op, device_id FROM transactions WHERE transaction_id=?",
                    (pilot_id,),
                ).fetchall()
            ]
            result["pending_sync_rows"] = [
                {"transaction_id": row[0], "cible": row[1], "tentative": row[2], "dernier_essai": row[3]}
                for row in conn.execute(
                    "SELECT transaction_id, cible, tentative, dernier_essai FROM pending_sync WHERE transaction_id=?",
                    (pilot_id,),
                ).fetchall()
            ]

            # AUDIT-ONLY: explique pourquoi LOCAL+pending_sync est filtre
            # par normal_pending_transactions(). Aucune ecriture SQLite.
            result["device_identity"] = {
                "device_id": conn.execute(
                    "SELECT valeur FROM config_hybrid WHERE cle='device_id'"
                ).fetchone()[0] if conn.execute(
                    "SELECT COUNT(*) FROM config_hybrid WHERE cle='device_id'"
                ).fetchone()[0] else None,
                "device_id_legacy": conn.execute(
                    "SELECT valeur FROM config_hybrid WHERE cle='device_id_legacy'"
                ).fetchone()[0] if conn.execute(
                    "SELECT COUNT(*) FROM config_hybrid WHERE cle='device_id_legacy'"
                ).fetchone()[0] else None,
                "device_identity_state": conn.execute(
                    "SELECT valeur FROM config_hybrid WHERE cle='device_identity_state'"
                ).fetchone()[0] if conn.execute(
                    "SELECT COUNT(*) FROM config_hybrid WHERE cle='device_identity_state'"
                ).fetchone()[0] else None,
                "migration_lock": conn.execute(
                    "SELECT valeur FROM config_hybrid WHERE cle='migration_lock'"
                ).fetchone()[0] if conn.execute(
                    "SELECT COUNT(*) FROM config_hybrid WHERE cle='migration_lock'"
                ).fetchone()[0] else None,
            }

            result["normal_pending_audit"] = {
                "local_pending_total": conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM transactions t
                    JOIN pending_sync p ON p.transaction_id=t.transaction_id
                    WHERE t.statut_local='LOCAL'
                    """
                ).fetchone()[0],
                "by_device_id": [
                    {"device_id": row[0], "count": row[1]}
                    for row in conn.execute(
                        """
                        SELECT COALESCE(t.device_id, '<NULL>'), COUNT(*)
                        FROM transactions t
                        JOIN pending_sync p ON p.transaction_id=t.transaction_id
                        WHERE t.statut_local='LOCAL'
                        GROUP BY t.device_id
                        ORDER BY COUNT(*) DESC, COALESCE(t.device_id, '<NULL>')
                        """
                    ).fetchall()
                ],
                "current_device_matches": 0,
                "legacy_device_matches": 0,
                "invalid_transaction_uuid": 0,
                "invalid_device_uuid": 0,
            }

            current_device_id = result["device_identity"]["device_id"]
            legacy_device_id = result["device_identity"]["device_id_legacy"]

            rows = conn.execute(
                """
                SELECT t.transaction_id, t.device_id
                FROM transactions t
                JOIN pending_sync p ON p.transaction_id=t.transaction_id
                WHERE t.statut_local='LOCAL'
                """
            ).fetchall()

            for tx_id, device_id in rows:
                if device_id == current_device_id:
                    result["normal_pending_audit"]["current_device_matches"] += 1
                if legacy_device_id and device_id == legacy_device_id:
                    result["normal_pending_audit"]["legacy_device_matches"] += 1
                if not _is_uuid_v4(tx_id):
                    result["normal_pending_audit"]["invalid_transaction_uuid"] += 1
                if not _is_uuid_v4(device_id):
                    result["normal_pending_audit"]["invalid_device_uuid"] += 1

            result["normal_pending_audit"]["metadata_invalid"] = 0
            metadata_rows = conn.execute(
                """
                SELECT t.transaction_id, t.type_op, t.device_id, t.admin_id,
                       t.magasin_id, t.magasin_cle, t.horodatage, t.payload
                FROM transactions t
                JOIN pending_sync p ON p.transaction_id=t.transaction_id
                WHERE t.statut_local='LOCAL'
                """
            ).fetchall()

            for metadata_row in metadata_rows:
                if not _normal_sync_metadata_valid(metadata_row):
                    result["normal_pending_audit"]["metadata_invalid"] += 1

            # AUDIT-ONLY: classification exacte de chaque LOCAL+pending_sync.
            # Aucune ecriture SQLite et aucun changement de comportement sync.
            result["normal_pending_audit"]["eligibility"] = {
                "eligible": 0,
                "device_mismatch": 0,
                "legacy_device": 0,
                "invalid_transaction_uuid": 0,
                "invalid_device_uuid": 0,
                "metadata_invalid": 0,
            }

            metadata_by_tx = {
                row[0]: row
                for row in metadata_rows
            }

            for tx_id, device_id in rows:
                if not _is_uuid_v4(tx_id):
                    result["normal_pending_audit"]["eligibility"]["invalid_transaction_uuid"] += 1
                    continue

                if not _is_uuid_v4(device_id):
                    result["normal_pending_audit"]["eligibility"]["invalid_device_uuid"] += 1
                    continue

                if legacy_device_id and device_id == legacy_device_id:
                    result["normal_pending_audit"]["eligibility"]["legacy_device"] += 1
                    continue

                if device_id != current_device_id:
                    result["normal_pending_audit"]["eligibility"]["device_mismatch"] += 1
                    continue

                metadata_row = metadata_by_tx.get(tx_id)
                if metadata_row is None or not _normal_sync_metadata_valid(metadata_row):
                    result["normal_pending_audit"]["eligibility"]["metadata_invalid"] += 1
                    continue

                result["normal_pending_audit"]["eligibility"]["eligible"] += 1

            result["normal_pending_audit"]["estimated_eligible"] = (
                result["normal_pending_audit"]["eligibility"]["eligible"]
            )

            result["pilot_join_rows"] = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT t.transaction_id
                    FROM transactions t
                    JOIN pending_sync p ON p.transaction_id = t.transaction_id
                    WHERE t.statut_local = 'LOCAL' AND t.transaction_id=?
                    """,
                    (pilot_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
    except Exception as exc:
        result["error"] = f"Diagnostic SQLite impossible: {type(exc).__name__}"
    return result


def pending_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions WHERE statut_local='LOCAL'")
    return cur.fetchone()[0]


def mark_synced(conn, tx_ids):
    cur = conn.cursor()
    for tx_id in tx_ids:
        cur.execute("UPDATE transactions SET statut_local='SYNCED' WHERE transaction_id=?", (tx_id,))
        cur.execute("DELETE FROM pending_sync WHERE transaction_id=?", (tx_id,))
    conn.commit()


def apply_remote_transactions(conn, remote_txs, local_device_id, return_failures=False):
    """
    Applique des transactions recues d'un autre appareil/serveur.
    Idempotent : les doublons de meme transaction_id et meme payload sont ignores.
    Une collision de transaction_id/payload distinct ou un replay en erreur est
    remonte au transport afin que le curseur ne puisse jamais le franchir.
    Les mouvements de stock sont rejoues ; le stock est reconstruit par aggregation.
    Retourne (nb appliquees, nb ignorees), ou avec ``return_failures=True``
    (nb appliquees, nb ignorees, echecs). Le mode detaille est reserve au
    transport HTTP avec curseur ; les appelants legacy gardent deux valeurs.
    """
    ensure_mvt_table(conn)
    cur = conn.cursor()
    appliquees, ignorees = 0, 0
    failures = []
    for tx in sorted(remote_txs, key=lambda t: (t["horodatage"], t["transaction_id"])):
        tx_id = tx["transaction_id"]
        # Un UUID local identique doit aussi conserver le même payload. Une
        # empreinte différente n'est jamais assimilée silencieusement à un doublon.
        cur.execute("SELECT payload FROM transactions WHERE transaction_id=?", (tx_id,))
        existing = cur.fetchone()
        if existing:
            if _payload_sha256(existing[0]) != _payload_sha256(tx.get("payload", {})):
                failures.append({
                    "transaction_id": tx_id,
                    "code": "TRANSACTION_ID_COLLISION_LOCAL",
                    "error_type": "PayloadHashMismatch",
                })
            else:
                ignorees += 1
            continue
        savepoint = "replay_tx"
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            cur.execute(
                "INSERT INTO transactions (transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload, statut_local, serveur_version) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (tx_id, tx["type_op"], tx.get("device_id", ""), tx.get("admin_id"), tx.get("admin_username"),
                 tx.get("magasin_id"), tx.get("magasin_cle"), tx["horodatage"],
                 json.dumps(tx.get("payload", {}), ensure_ascii=False) if not isinstance(tx.get("payload"), str) else tx["payload"],
                 "SYNCED", tx.get("serveur_version", 0)))
            _replay_transaction(conn, tx)
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            appliquees += 1
        except Exception as e:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            print(f"[{HYBRID_LOG_TAG}] Replay echoue {tx_id}: {e}")
            failures.append({
                "transaction_id": tx_id,
                "code": "LOCAL_REPLAY_FAILED",
                "error_type": type(e).__name__,
            })
    conn.commit()
    # Recalculer tous les stocks touches
    cur.execute("SELECT DISTINCT produit_id, magasin_id FROM mouvements")
    for pid, mid in cur.fetchall():
        recalc_stock(conn, pid, mid)
    # Enregistrer les devices connus
    seen = set()
    for tx in remote_txs:
        d = tx.get("device_id")
        if d and d != local_device_id and d not in seen:
            seen.add(d)
            cur.execute("INSERT OR IGNORE INTO devices (device_id, nom, derniere_sync) VALUES (?,?,?)",
                        (d, tx.get("admin_username"), now_iso()))
            cur.execute("UPDATE devices SET derniere_sync=? WHERE device_id=?", (now_iso(), d))
    conn.commit()
    if return_failures:
        return appliquees, ignorees, failures
    return appliquees, ignorees


def _replay_transaction(conn, tx):
    """Rejoue l'effet d'une transaction distante sur les tables locales."""
    cur = conn.cursor()
    payload = tx["payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    op = tx["type_op"]
    admin_id, admin_username = tx.get("admin_id"), tx.get("admin_username")

    if op in ("VENTE", "VENTE_CREDIT"):
        lignes = payload.get("lignes", [])
        for index, ligne in enumerate(lignes):
            movement_id = tx["transaction_id"] if len(lignes) == 1 else f"{tx['transaction_id']}:{index}"
            _apply_stock_movement(conn, ligne["produit_id"], tx.get("magasin_id"),
                                  -int(ligne["q"]), "VENTE", admin_id, admin_username, movement_id,
                                  occurred_at=tx.get("horodatage"))
            try:
                cur.execute(
                    "INSERT INTO ventes (transaction_id, produit_id, produit_nom, quantite, prix_achat, prix_vente, total, benefice, vendeur, device_id, magasin_ref_id, date_vente) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (tx["transaction_id"], ligne.get("produit_id"), ligne["nom"], ligne["q"],
                     ligne["pa"], ligne["pv"], ligne["total"], ligne["ben"],
                     admin_username or "Sync", tx.get("device_id"), tx.get("magasin_id"),
                     tx["horodatage"]))
            except sqlite3.OperationalError:
                cur.execute(
                    "INSERT INTO ventes (produit_id, produit_nom, quantite, prix_achat, prix_vente, total, benefice, vendeur, date_vente) VALUES (?,?,?,?,?,?,?,?,?)",
                    (ligne.get("produit_id"), ligne["nom"], ligne["q"], ligne["pa"], ligne["pv"], ligne["total"], ligne["ben"], admin_username or "Sync", tx["horodatage"]))

    elif op in ("ACHAT", "MIGRATION_INITIAL_STOCK"):
        _apply_stock_movement(conn, payload["produit_id"], tx.get("magasin_id"),
                              int(payload["quantite"]), op, admin_id, admin_username, tx["transaction_id"],
                              occurred_at=tx.get("horodatage"))
        # PA moyen pondere sur le catalogue (stock depuis le journal de mouvements)
        if payload.get("prix_achat") is not None:
            stock_actuel = get_stock(conn, payload["produit_id"], tx.get("magasin_id"))
            cur.execute("SELECT prix_achat_moyen FROM catalogues WHERE id=?", (payload["produit_id"],))
            row = cur.fetchone()
            if row:
                pa_actuel = row[0]
                q = int(payload["quantite"])
                npa = ((stock_actuel * pa_actuel) + (q * payload["prix_achat"])) / (stock_actuel + q) \
                    if (stock_actuel + q) > 0 else payload["prix_achat"]
                cur.execute("UPDATE catalogues SET prix_achat_moyen=? WHERE id=?", (npa, payload["produit_id"]))

    elif op == "AJUST_STOCK":
        _apply_stock_movement(conn, payload["produit_id"], tx.get("magasin_id"),
                              int(payload["quantite"]), "AJUST_STOCK", admin_id, admin_username, tx["transaction_id"],
                              occurred_at=tx.get("horodatage"))

    elif op == "PROD_CREATE":
        cur.execute("INSERT OR IGNORE INTO catalogues (id, nom, categorie, prix_achat_moyen, prix_vente) VALUES (?,?,?,?,?)",
                    (payload["produit_id"], payload["nom"], payload.get("categorie", "General"),
                     payload.get("prix_achat", 0), payload.get("prix_vente", 0)))

    elif op == "PROD_UPDATE":
        sets, vals = [], []
        for k in ("nom", "categorie", "prix_vente"):
            if k in payload:
                sets.append(f"{k}=?"); vals.append(payload[k])
        if sets:
            cur.execute(f"UPDATE catalogues SET {','.join(sets)} WHERE id=?", vals + [payload["produit_id"]])

    elif op == "PROD_DELETE":
        cur.execute("UPDATE catalogues SET actif=0 WHERE id=?", (payload["produit_id"],))

    elif op == "MAG_CREATE":
        cur.execute("INSERT OR IGNORE INTO magasins (id, cle, nom, description, adresse, categorie, couleur, couleur_light, actif) VALUES (?,?,?,?,?,?,?,?,?)",
                    (payload["magasin_id"], payload["cle"], payload["nom"],
                     payload.get("description", ""), payload.get("adresse", ""),
                     payload.get("categorie", "General"), payload.get("couleur", "#4488BB"),
                     payload.get("couleur_light", "#DDEEFF"), payload.get("actif", 1)))
        perm = payload.get("admin_autorises", [])
        for a in perm:
            cur.execute("INSERT OR IGNORE INTO permissions_magasin VALUES (?,?)", (a, payload["magasin_id"]))

    elif op == "MAG_UPDATE":
        sets, vals = [], []
        for k in ("nom", "description", "adresse", "categorie", "couleur", "couleur_light", "actif"):
            if k in payload:
                sets.append(f"{k}=?"); vals.append(payload[k])
        if sets:
            cur.execute(f"UPDATE magasins SET {','.join(sets)} WHERE id=?", vals + [payload["magasin_id"]])

    elif op == "MAG_DELETE":
        cur.execute("UPDATE magasins SET actif=0 WHERE id=?", (payload["magasin_id"],))

    elif op == "USER_CREATE":
        cur.execute("INSERT OR IGNORE INTO utilisateurs_hybrid (id, username, password_hash, nom_complet, role, magasins_autorises) VALUES (?,?,?,?,?,?)",
                    (payload["user_id"], payload["username"], payload.get("password_hash", hash_pwd("")),
                     payload.get("nom_complet", ""), payload.get("role", "ADMIN_MAGASIN"),
                     json.dumps(payload.get("magasins_autorises", []), ensure_ascii=False)))
        try:
            cur.execute("INSERT OR IGNORE INTO utilisateurs (username, password_hash, nom_complet, role) VALUES (?,?,?,?)",
                        (payload["username"], payload.get("password_hash", hash_pwd("")),
                         payload.get("nom_complet", ""), payload.get("role", "ADMIN_MAGASIN")))
            cur.execute("UPDATE utilisateurs SET hybrid_id=? WHERE username=?", (payload["user_id"], payload["username"]))
        except Exception:
            pass
        for m in payload.get("magasins_autorises", []):
            cur.execute("INSERT OR IGNORE INTO permissions_magasin VALUES (?,?)", (payload["user_id"], m))

    elif op == "USER_UPDATE":
        sets, vals = [], []
        for k in ("nom_complet", "role", "actif"):
            if k in payload:
                sets.append(f"{k}=?"); vals.append(payload[k])
        if "magasins_autorises" in payload:
            sets.append("magasins_autorises=?")
            vals.append(json.dumps(payload["magasins_autorises"], ensure_ascii=False))
        if sets:
            cur.execute(f"UPDATE utilisateurs_hybrid SET {','.join(sets)} WHERE id=?", vals + [payload["user_id"]])

    elif op == "DETTE":
        montant = int(payload["montant"])
        client = payload["client"]
        telephone = (payload.get("telephone") or "").strip()
        dette_ref = (payload.get("dette_transaction_id") or tx["transaction_id"]).strip()
        cur.execute("SELECT id, telephone, total, paye, reste, transaction_id FROM dettes WHERE transaction_id=? AND statut='ACTIF'", (dette_ref,))
        existing = cur.fetchone()
        if not existing:
            cur.execute("SELECT id, telephone, total, paye, reste, transaction_id FROM dettes WHERE LOWER(TRIM(client))=LOWER(TRIM(?)) AND statut='ACTIF' ORDER BY id", (client,))
            for candidate in cur.fetchall():
                candidate_phone = (candidate[1] or '').strip()
                if not candidate_phone or not telephone or candidate_phone == telephone:
                    existing = candidate
                    break
        if existing:
            did, existing_phone, total, paye, reste, existing_ref = existing
            cur.execute("UPDATE dettes SET total=?, reste=?, telephone=?, transaction_id=COALESCE(transaction_id,?), device_id=COALESCE(device_id,?), magasin_ref_id=COALESCE(magasin_ref_id,?) WHERE id=?",
                        (int(total) + montant, int(reste) + montant, existing_phone or telephone,
                         dette_ref, tx.get("device_id"), tx.get("magasin_id"), did))
        else:
            cur.execute("INSERT INTO dettes (transaction_id, client, telephone, total, paye, reste, statut, device_id, magasin_ref_id, date_creation) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (dette_ref, client, telephone, montant, 0, montant, "ACTIF",
                         tx.get("device_id"), tx.get("magasin_id"), tx["horodatage"]))

    elif op == "PAIEMENT":
        montant = int(payload["montant"])
        dette_ref = (payload.get("dette_transaction_id") or "").strip()
        d = None
        if dette_ref:
            cur.execute("SELECT id, paye, reste FROM dettes WHERE transaction_id=?", (dette_ref,))
            d = cur.fetchone()
        if not d and payload.get("client"):
            cur.execute("SELECT id, paye, reste FROM dettes WHERE LOWER(TRIM(client))=LOWER(TRIM(?)) AND statut='ACTIF' ORDER BY id LIMIT 1", (payload["client"],))
            d = cur.fetchone()
        if not d:
            try:
                cur.execute("SELECT id, paye, reste FROM dettes WHERE id=?", (payload.get("dette_id"),))
                d = cur.fetchone()
            except Exception:
                d = None
        if d:
            did, paye, reste = d
            new_paye, new_reste = int(paye) + montant, int(reste) - montant
            if new_reste < 0:
                raise ValueError("Paiement distant superieur au reste")
            stat = "SOLDE" if new_reste == 0 else "ACTIF"
            cur.execute("UPDATE dettes SET paye=?, reste=?, statut=? WHERE id=?", (new_paye, new_reste, stat, did))
            try:
                cur.execute("INSERT INTO paiements_dettes (transaction_id, dette_id, montant, device_id) VALUES (?,?,?,?)",
                            (tx["transaction_id"], did, montant, tx.get("device_id")))
            except sqlite3.OperationalError:
                cur.execute("INSERT INTO paiements_dettes (dette_id, montant) VALUES (?,?)", (did, montant))


# ============================================================
# EXPORT / IMPORT (format de sync JSON)
# ============================================================
def export_outgoing(conn, tx_ids):
    cur = conn.cursor()
    out = []
    for tx_id in tx_ids:
        cur.execute("SELECT transaction_id, type_op, device_id, admin_id, admin_username, magasin_id, magasin_cle, horodatage, payload, serveur_version FROM transactions WHERE transaction_id=?", (tx_id,))
        r = cur.fetchone()
        if r:
            out.append({
                "transaction_id": r[0], "type_op": r[1], "device_id": r[2],
                "admin_id": r[3], "admin_username": r[4], "magasin_id": r[5],
                "magasin_cle": r[6], "horodatage": r[7], "payload": r[8],
                "serveur_version": r[9],
            })
    return out


# ============================================================
# SYNC TRANSPORT
# ============================================================
class SyncResult:
    def __init__(self):
        self.envoyees = 0
        self.recues = 0
        self.ignorees = 0
        self.erreurs = []
        self.replay_failures = []
        self.mode = None  # INTERNET, LOCAL, OFFLINE
        # Métadonnées techniques bornées ; aucune clé, identité brute, payload
        # ou UUID de transaction ne peut être ajouté à ce diagnostic.
        self.diagnostic = {}

    def ok(self):
        return len(self.erreurs) == 0


def _sync_runtime_trace(event, **fields):
    """Trace runtime `/sync` sans secret ni donnée métier."""
    blocked = {"api_key", "authorization", "headers", "payload", "device_id", "transaction_id"}
    safe = {"event": str(event)}
    for key, value in fields.items():
        if key in blocked:
            continue
        if isinstance(value, (bool, int)) or value is None:
            safe[key] = value
        elif isinstance(value, str):
            safe[key] = value[:120]
    try:
        print("FANEVA_SYNC " + json.dumps(safe, ensure_ascii=False, sort_keys=True))
    except Exception:
        print("FANEVA_SYNC {\"event\":\"TRACE_UNAVAILABLE\"}")


def sync_with_http(endpoint, outgoing, local_device_id, conn, timeout=15, api_key=None,
                   sync_cursor=None, migration_id=None, require_verified_ack=False):
    """
    Echange avec un serveur de sync (Internet ou serveur local Wi-Fi/Hotspot).
    POST /sync {"device_id": ..., "transactions": [...]} -> 200 {"transactions": [...], "accepted": [...]}

    Pour HTTPS, le contexte TLS strict est volontairement identique à celui de
    GET /auth/validate : bundle CA embarqué, vérification de chaîne et contrôle
    obligatoire du nom d’hôte. Les pairs Wi-Fi/Hotspot en HTTP local restent
    inchangés et ne reçoivent aucun contexte HTTPS.
    """
    res = SyncResult()
    endpoint = (endpoint or '').strip().rstrip('/')
    if not _debug_network_endpoint_allowed(endpoint):
        res.erreurs.append('Endpoint réseau refusé par le verrou LOT4 DEBUG : aucune requête envoyée')
        return res
    try:
        import urllib.error
        import urllib.request
        # La session de migration vérifie l’empreinte canonique du manifeste.
        # SQLite, pending_sync et la liste `outgoing` d’origine ne sont jamais
        # modifiés : seule une copie de transport est normalisée.
        wire_outgoing = outgoing
        if migration_id:
            wire_outgoing = [dict(tx, payload=_manifest_canonical_payload(tx.get("payload", "{}")))
                             for tx in outgoing]
        request_data = {"device_id": local_device_id, "transactions": wire_outgoing}
        canonical_mapping_version = _config_get(conn.cursor(), CANONICAL_MAPPING_CONFIG_KEY)
        if not migration_id and canonical_mapping_version and _is_cloudflare_staging_endpoint(endpoint):
            request_data["canonical_stock"] = {"mapping_version": canonical_mapping_version}
        if sync_cursor:
            request_data["sync_cursor"] = sync_cursor
        if migration_id:
            request_data["migration_id"] = migration_id
        body = json.dumps(request_data, ensure_ascii=False).encode("utf-8")
        # Le POST urllib peut être filtré à l’edge avant le Worker. Pour HTTPS,
        # la release 1.4.9.3 utilise requests avec le bundle CA applicatif.
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "User-Agent": "FANEVA-SYSTEM-HYBRID/1.4.9.3-STAGING",
            "X-Device-Id": local_device_id,
        }
        if api_key:
            headers["X-Api-Key"] = api_key
        sync_url = endpoint + "/sync"
        res.diagnostic = {
            "sync_url": sync_url,
            "method": "POST",
            "transport": "requests_https" if endpoint.lower().startswith("https://") else "urllib_http",
            "http_status": None,
            "user_agent": headers["User-Agent"],
            "content_type": headers["Content-Type"],
            "x_device_id_present": bool(local_device_id),
            "x_api_key_present": bool(api_key),
            "outgoing_count": len(wire_outgoing),
            "sync_cursor_present": bool(sync_cursor),
            "migration_id_present": bool(migration_id),
            "canonical_stock_present": bool(request_data.get("canonical_stock")),
            "tls": "STRICT_CA_HOSTNAME" if endpoint.lower().startswith("https://") else "NOT_APPLICABLE",
        }
        _sync_runtime_trace(
            "SYNC_REQUEST_PREPARED",
            sync_url=sync_url,
            method="POST",
            transport=res.diagnostic["transport"],
            outgoing_count=len(wire_outgoing),
            sync_cursor_present=bool(sync_cursor),
            migration_id_present=bool(migration_id),
            canonical_stock_present=bool(request_data.get("canonical_stock")),
            x_device_id_present=bool(local_device_id),
            x_api_key_present=bool(api_key),
        )
        if endpoint.lower().startswith("https://"):
            bundle_ca = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "cacert.pem")
            if not os.path.isfile(bundle_ca):
                raise RuntimeError("Bundle CA HTTPS indisponible")
            try:
                import requests
            except ModuleNotFoundError:
                # Fallback réservé à un environnement sans la recette requests.
                # L’APK 1.4.9.3 la déclare explicitement dans Buildozer.
                res.diagnostic["transport"] = "urllib_https_fallback"
                req = urllib.request.Request(sync_url, data=body, headers=headers, method="POST")
                context = _auth_ssl_context()
                with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                    response_headers = getattr(resp, "headers", {}) or {}
                    res.diagnostic.update({
                        "http_status": int(getattr(resp, "status", 200)),
                        "response_content_type": response_headers.get("content-type", "")[:120],
                        "response_server": response_headers.get("server", "")[:80],
                        "response_cf_mitigated": response_headers.get("cf-mitigated", "")[:80],
                    })
                    data = json.loads(resp.read().decode("utf-8"))
            else:
                response = requests.post(sync_url, data=body, headers=headers, timeout=timeout, verify=bundle_ca)
                res.diagnostic.update({
                    "http_status": int(response.status_code),
                    "response_content_type": response.headers.get("content-type", "")[:120],
                    "response_server": response.headers.get("server", "")[:80],
                    "response_cf_mitigated": response.headers.get("cf-mitigated", "")[:80],
                })
                response.raise_for_status()
                data = response.json()
        else:
            req = urllib.request.Request(sync_url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                response_headers = getattr(resp, "headers", {}) or {}
                res.diagnostic.update({
                    "http_status": int(getattr(resp, "status", 200)),
                    "response_content_type": response_headers.get("content-type", "")[:120],
                    "response_server": response_headers.get("server", "")[:80],
                    "response_cf_mitigated": response_headers.get("cf-mitigated", "")[:80],
                })
                data = json.loads(resp.read().decode("utf-8"))
        _sync_runtime_trace("SYNC_HTTP_RESPONSE", http_status=res.diagnostic["http_status"], transport=res.diagnostic["transport"])
        remote = data.get("transactions", [])
        if require_verified_ack:
            expected_hashes = {tx["transaction_id"]: _payload_sha256(tx.get("payload", {}))
                               for tx in wire_outgoing}
            acknowledgements = data.get("acknowledged", [])
            acknowledged_ids = []
            for ack in acknowledgements:
                tx_id = ack.get("transaction_id") if isinstance(ack, dict) else None
                if tx_id in expected_hashes and ack.get("payload_sha256") == expected_hashes[tx_id]:
                    acknowledged_ids.append(tx_id)
            missing = sorted(set(expected_hashes) - set(acknowledged_ids))
            if missing:
                res.erreurs.append("ACK UUID serveur absent ou invalide : " + ", ".join(missing[:3]))
            accepted = acknowledged_ids
        else:
            accepted = [tx_id for tx_id in data.get("accepted", [])
                        if tx_id in {tx["transaction_id"] for tx in wire_outgoing}]
        appliquees, ignorees, replay_failures = apply_remote_transactions(
            conn, remote, local_device_id, return_failures=True
        )
        res.recues = appliquees
        res.ignorees = ignorees
        res.envoyees = len(accepted)
        mark_synced(conn, accepted)
        if replay_failures:
            res.replay_failures = replay_failures
            failed_ids = ", ".join(failure["transaction_id"] for failure in replay_failures[:3])
            res.erreurs.append(
                "Replay distant incomplet : curseur conserve pour retry; transactions a confirmer : " + failed_ids
            )
            return res
        if require_verified_ack:
            _set_sync_cursor(conn, data.get("next_sync_cursor"))
    except Exception as exc:
        # Distinguer un refus protocolaire du Worker/edge de DNS/TLS sans
        # exposer clé, headers complets ou payload métier.
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        response_headers = getattr(response, "headers", {}) if response is not None else {}
        raw_error_bytes = None
        if isinstance(exc, urllib.error.HTTPError):
            status = int(exc.code)
            response_headers = exc.headers or {}
            try:
                raw_error_bytes = exc.read()
            except Exception:
                raw_error_bytes = None
        elif response is not None:
            try:
                raw_error_bytes = response.content
            except Exception:
                raw_error_bytes = None
        if status is None:
            res.erreurs.append(f"{endpoint}: {type(exc).__name__}")
            _sync_runtime_trace("SYNC_TRANSPORT_ERROR", exception_type=type(exc).__name__)
            return res
        res.diagnostic.update({
            "http_status": int(status),
            "response_content_type": str(response_headers.get("content-type", ""))[:120],
            "response_server": str(response_headers.get("server", ""))[:80],
            "response_cf_mitigated": str(response_headers.get("cf-mitigated", ""))[:80],
        })
        safe_code = "HTTP_ERROR"
        safe_message = "Réponse serveur refusée"
        try:
            error_body = json.loads((raw_error_bytes or b"").decode("utf-8"))
            raw_code = error_body.get("error") if isinstance(error_body, dict) else None
            if isinstance(raw_code, str) and raw_code.replace("_", "").isalnum() and len(raw_code) <= 64:
                safe_code = raw_code
            raw_message = error_body.get("message") if isinstance(error_body, dict) else None
            if isinstance(raw_message, str):
                safe_message = raw_message[:160]
        except Exception:
            pass
        res.diagnostic["server_code"] = safe_code
        res.erreurs.append(f"Synchronisation HTTP {int(status)} [{safe_code}] : {safe_message}")
        _sync_runtime_trace("SYNC_HTTP_ERROR", http_status=int(status), server_code=safe_code, transport=res.diagnostic.get("transport"))
    return res


def sync_local_wifi(conn, peer_host, peer_port=SYNC_SERVER_PORT):
    """Synchronisation directe avec un autre appareil sur le meme Wi-Fi/Hotspot."""
    local_device_id = _local_device_id(conn)
    outgoing = export_outgoing(conn, normal_pending_transactions(conn))
    res = sync_with_http(f"http://{peer_host}:{peer_port}", outgoing, local_device_id, conn)
    res.mode = "LOCAL"
    return res


def sync_internet(conn, server_url):
    """Synchronisation HTTPS manuelle vers le seul Worker Cloudflare staging autorisé."""
    if PRODUCTION_SYNC_DISABLED and not _is_cloudflare_staging_endpoint(server_url):
        res = SyncResult()
        res.mode = "INTERNET"
        res.erreurs.append("Endpoint Internet refusé : seul le Worker Cloudflare staging est autorisé")
        return res
    api_key = get_server_api_key(conn)
    if not api_key:
        res = SyncResult()
        res.mode = "INTERNET"
        res.erreurs.append("Cle API de cet appareil non configuree : aucune transaction envoyee")
        return res
    local_device_id = _local_device_id(conn)
    outgoing = export_outgoing(conn, normal_pending_transactions(conn))
    res = sync_with_http(server_url.rstrip("/"), outgoing, local_device_id, conn,
                         api_key=api_key, sync_cursor=_get_sync_cursor(conn),
                         require_verified_ack=True)
    res.mode = "INTERNET"
    return res


def _auth_ssl_context():
    """Construit le contexte TLS strict de la seule sonde d’authentification.

    Le bundle CA est embarqué dans le private.tar et extrait dans les données
    privées de l’application. La validation de chaîne et du nom d’hôte reste
    obligatoire ; aucune option de contournement TLS n’est utilisée.
    """
    import ssl
    bundle_ca = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "cacert.pem")
    if not os.path.isfile(bundle_ca):
        raise RuntimeError("Bundle CA HTTPS indisponible")
    context = ssl.create_default_context(cafile=bundle_ca)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


def _attempt_android_https_auth_fallback(result, auth_url, device_id, api_key, timeout):
    """Essaie GET `/auth/validate` avec HTTPS Android après un échec DNS Python.

    Cette compatibilité est strictement limitée à l’URL staging déjà validée. Elle
    n’appelle ni `/sync` ni `/backup`, ne modifie aucune table, et ne journalise
    jamais la clé ou les valeurs d’identité. HttpsURLConnection conserve la
    validation TLS et le contrôle du nom d’hôte du système Android.
    """
    result["android_fallback_attempted"] = True
    try:
        from jnius import autoclass

        URL = autoclass("java.net.URL")
        Scanner = autoclass("java.util.Scanner")
        connection = URL(auth_url).openConnection()
        connection.setConnectTimeout(int(timeout * 1000))
        connection.setReadTimeout(int(timeout * 1000))
        connection.setRequestMethod("GET")
        connection.setRequestProperty("Accept", "application/json")
        connection.setRequestProperty("User-Agent", "FANEVA-SYSTEM-HYBRID/1.4.9.4-ANDROID-FALLBACK")
        connection.setRequestProperty("X-Device-Id", device_id)
        connection.setRequestProperty("X-Api-Key", api_key)
        status = int(connection.getResponseCode())
        stream = connection.getInputStream() if 200 <= status < 400 else connection.getErrorStream()
        raw_text = ""
        if stream is not None:
            scanner = Scanner(stream, "UTF-8")
            scanner.useDelimiter("\\A")
            raw_text = str(scanner.next()) if scanner.hasNext() else ""
            scanner.close()
        try:
            connection.disconnect()
        except Exception:
            pass
        result["transport"] = "android_https_urlconnection"
        result["ca_mode"] = "ANDROID_SYSTEM_TLS"
        result["http_status"] = status
        result["category"] = "HTTP"
        payload = json.loads(raw_text)
        result["authenticated"] = (
            status == 200
            and payload.get("authenticated") is True
            and payload.get("device_id") == device_id
        )
        if result["authenticated"]:
            result["category"] = "SUCCESS"
            result["error"] = None
            _auth_runtime_trace("AUTH_ANDROID_FALLBACK_SUCCESS", http_status=status)
        elif status == 401:
            result["error"] = "Authentification refusée"
            _auth_runtime_trace("AUTH_ANDROID_FALLBACK_HTTP", http_status=status)
        elif status == 403:
            result["error"] = "Accès à l’authentification interdit"
            _auth_runtime_trace("AUTH_ANDROID_FALLBACK_HTTP", http_status=status)
        elif status == 404:
            result["error"] = "Route d’authentification introuvable"
            _auth_runtime_trace("AUTH_ANDROID_FALLBACK_HTTP", http_status=status)
        else:
            result["error"] = "Réponse d’authentification non valide"
            _auth_runtime_trace("AUTH_ANDROID_FALLBACK_INVALID", http_status=status)
        return True
    except json.JSONDecodeError as exc:
        result["transport"] = "android_https_urlconnection"
        result["category"] = "JSON"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Réponse JSON d’authentification invalide"
        _auth_runtime_trace("AUTH_ANDROID_FALLBACK_JSON")
        return True
    except Exception as exc:
        result["android_fallback_exception_type"] = type(exc).__name__
        _auth_runtime_trace("AUTH_ANDROID_FALLBACK_UNAVAILABLE", exception_type=type(exc).__name__)
        return False


def validate_server_authentication(conn, server_url, timeout=10):
    """
    Contrôle explicite et non mutateur de l’association appareil / clé API.

    Cette fonction appelle exclusivement GET /auth/validate. Elle n’exporte aucune
    transaction, n’appelle ni /sync ni /backup, ne crée aucun ACK et ne modifie
    aucune table SQLite. La clé reste uniquement dans le header HTTPS et n’est
    jamais ajoutée au résultat, à un journal applicatif ou à une erreur affichée.
    """
    device_id = _local_device_id(conn)
    api_key = get_server_api_key(conn)
    endpoint = (server_url or "").strip().rstrip("/")
    result = {
        "http_status": None,
        "device_id": device_id,
        "authenticated": False,
        "error": None,
        "auth_url": endpoint + "/auth/validate" if endpoint else None,
        "category": "PRECHECK",
        "exception_type": None,
        "device_id_present": bool(device_id),
        "device_id_uuid_v4": _is_uuid_v4(device_id),
        "api_key_present": bool(api_key),
        "api_key_valid": bool(isinstance(api_key, str) and api_key.startswith("fsv_") and len(api_key) >= 20),
        "ca_bundle_present": False,
        "transport": "urllib_https",
        "android_fallback_attempted": False,
        "ca_mode": "BUNDLED_CA",
    }
    _auth_runtime_trace(
        "AUTH_START",
        device_id_present=result["device_id_present"],
        device_id_uuid_v4=result["device_id_uuid_v4"],
        api_key_present=result["api_key_present"],
        api_key_valid=result["api_key_valid"],
    )
    if PRODUCTION_SYNC_DISABLED and not _debug_network_endpoint_allowed(endpoint):
        result["error"] = "Endpoint d’authentification refusé par le verrou staging"
        _auth_runtime_trace("AUTH_PRECHECK_ENDPOINT_REJECTED")
        return result
    if not result["device_id_present"]:
        result["error"] = "Device ID local indisponible"
        _auth_runtime_trace("AUTH_PRECHECK_DEVICE_ID_MISSING")
        return result
    if not result["device_id_uuid_v4"]:
        result["error"] = "Device ID local UUID v4 invalide"
        _auth_runtime_trace("AUTH_PRECHECK_DEVICE_ID_INVALID")
        return result
    if not result["api_key_present"]:
        result["error"] = "Clé API locale non configurée : aucune requête envoyée"
        _auth_runtime_trace("AUTH_PRECHECK_API_KEY_MISSING")
        return result
    if not result["api_key_valid"]:
        result["error"] = "Clé API locale invalide : aucune requête envoyée"
        _auth_runtime_trace("AUTH_PRECHECK_API_KEY_INVALID")
        return result
    if not endpoint.startswith("https://"):
        result["error"] = "URL HTTPS du serveur d’authentification invalide"
        _auth_runtime_trace("AUTH_PRECHECK_URL_INVALID")
        return result
    try:
        import urllib.error
        import urllib.request
        bundle_ca = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "cacert.pem")
        result["ca_bundle_present"] = os.path.isfile(bundle_ca)
        request = urllib.request.Request(
            endpoint + "/auth/validate",
            headers={
                "X-Device-Id": device_id,
                "X-Api-Key": api_key,
                "User-Agent": "FANEVA-SYSTEM-HYBRID/1.4.9.4-STAGING",
            },
            method="GET",
        )
        _auth_runtime_trace("AUTH_REQUEST_PREPARED", ca_bundle_present=result["ca_bundle_present"])
        with urllib.request.urlopen(request, timeout=timeout, context=_auth_ssl_context()) as response:
            status = int(getattr(response, "status", response.getcode()))
            payload = json.loads(response.read().decode("utf-8"))
        result["http_status"] = status
        result["category"] = "HTTP"
        result["authenticated"] = (
            status == 200
            and payload.get("authenticated") is True
            and payload.get("device_id") == device_id
        )
        if result["authenticated"]:
            result["category"] = "SUCCESS"
            _auth_runtime_trace("AUTH_RESPONSE_HTTP_200", authenticated=True)
        else:
            result["error"] = "Réponse d’authentification non valide"
            _auth_runtime_trace("AUTH_RESPONSE_INVALID", http_status=status)
    except json.JSONDecodeError as exc:
        result["category"] = "JSON"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Réponse JSON d’authentification invalide"
        _auth_runtime_trace("AUTH_JSON_INVALID")
    except urllib.error.HTTPError as exc:
        result["http_status"] = int(exc.code)
        result["category"] = "HTTP"
        result["exception_type"] = type(exc).__name__
        if exc.code == 401:
            result["error"] = "Authentification refusée"
        elif exc.code == 403:
            result["error"] = "Accès à l’authentification interdit"
        elif exc.code == 404:
            result["error"] = "Route d’authentification introuvable"
        elif exc.code >= 500:
            result["error"] = "Serveur d’authentification indisponible"
        else:
            result["error"] = "Réponse HTTP d’authentification non attendue"
        _auth_runtime_trace("AUTH_HTTP_RESPONSE", http_status=int(exc.code))
    except ssl.SSLCertVerificationError as exc:
        result["category"] = "TLS"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Validation du certificat HTTPS échouée"
        _auth_runtime_trace("AUTH_TLS_CERTIFICATE_ERROR")
    except ssl.SSLError as exc:
        result["category"] = "TLS"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Connexion TLS au serveur d’authentification impossible"
        _auth_runtime_trace("AUTH_TLS_ERROR")
    except socket.gaierror as exc:
        if _attempt_android_https_auth_fallback(result, endpoint + "/auth/validate", device_id, api_key, timeout):
            return result
        result["category"] = "DNS"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Nom du serveur d’authentification introuvable"
        _auth_runtime_trace("AUTH_DNS_ERROR")
    except (socket.timeout, TimeoutError) as exc:
        result["category"] = "TIMEOUT"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Délai de connexion au serveur d’authentification dépassé"
        _auth_runtime_trace("AUTH_TIMEOUT")
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", None)
        result["exception_type"] = type(reason).__name__ if reason is not None else type(exc).__name__
        if isinstance(reason, ssl.SSLCertVerificationError):
            result["category"] = "TLS"
            result["error"] = "Validation du certificat HTTPS échouée"
        elif isinstance(reason, ssl.SSLError):
            result["category"] = "TLS"
            result["error"] = "Connexion TLS au serveur d’authentification impossible"
        elif isinstance(reason, socket.gaierror):
            if _attempt_android_https_auth_fallback(result, endpoint + "/auth/validate", device_id, api_key, timeout):
                return result
            result["category"] = "DNS"
            result["error"] = "Nom du serveur d’authentification introuvable"
        elif isinstance(reason, (socket.timeout, TimeoutError)):
            result["category"] = "TIMEOUT"
            result["error"] = "Délai de connexion au serveur d’authentification dépassé"
        else:
            result["category"] = "NETWORK"
            result["error"] = "Connexion réseau au serveur d’authentification impossible"
        _auth_runtime_trace("AUTH_URL_ERROR", category=result["category"])
    except Exception as exc:
        result["category"] = "UNKNOWN"
        result["exception_type"] = type(exc).__name__
        result["error"] = "Erreur de connexion au serveur d’authentification"
        _auth_runtime_trace("AUTH_UNKNOWN_ERROR", exception_type=result["exception_type"])
    return result


def sync_migration_pilot(conn, server_url, transaction_id):
    """Envoie explicitement une seule transaction historique et exige l’ACK UUID serveur."""
    if _is_cloudflare_staging_endpoint(server_url):
        res = SyncResult()
        res.mode = "MIGRATION_PILOTE"
        res.erreurs.append("Migration historique explicitement interdite vers le staging Cloudflare")
        return res
    session = get_migration_session(conn)
    if not session["migration_id"] or not session["manifest_sha256"]:
        res = SyncResult()
        res.mode = "MIGRATION_PILOTE"
        res.erreurs.append("Session de migration et hash manifeste non configures")
        return res
    api_key = get_server_api_key(conn)
    if not api_key:
        res = SyncResult()
        res.mode = "MIGRATION_PILOTE"
        res.erreurs.append("Cle API de cet appareil non configuree")
        return res
    try:
        outgoing = _historical_outgoing(conn, transaction_id)
    except Exception as exc:
        res = SyncResult()
        res.mode = "MIGRATION_PILOTE"
        res.erreurs.append(str(exc))
        return res
    res = sync_with_http(server_url.rstrip("/"), outgoing, _local_device_id(conn), conn,
                         api_key=api_key, migration_id=session["migration_id"],
                         require_verified_ack=True)
    res.mode = "MIGRATION_PILOTE"
    if res.ok() and res.envoyees == 1:
        cur = conn.cursor()
        _config_set(cur, "migration_pilot_uuid", transaction_id)
        _config_set(cur, "migration_pilot_acknowledged", "1")
        if pending_count(conn) == 0:
            _config_set(cur, "migration_ack_manifest_sha256", session["manifest_sha256"])
        conn.commit()
    return res


def sync_migration_historical(conn, server_url):
    """Envoie manuellement le reste de la queue historique après validation du pilote."""
    if _is_cloudflare_staging_endpoint(server_url):
        res = SyncResult()
        res.mode = "MIGRATION_HISTORIQUE"
        res.erreurs.append("Migration historique explicitement interdite vers le staging Cloudflare")
        return res
    session = get_migration_session(conn)
    if not session["migration_id"] or not session["manifest_sha256"] or not session["pilot_acknowledged"]:
        res = SyncResult()
        res.mode = "MIGRATION_HISTORIQUE"
        res.erreurs.append("Pilote ACK UUID et session de migration valides obligatoires")
        return res
    api_key = get_server_api_key(conn)
    if not api_key:
        res = SyncResult()
        res.mode = "MIGRATION_HISTORIQUE"
        res.erreurs.append("Cle API de cet appareil non configuree")
        return res
    try:
        outgoing = _historical_outgoing(conn)
    except Exception as exc:
        res = SyncResult()
        res.mode = "MIGRATION_HISTORIQUE"
        res.erreurs.append(str(exc))
        return res
    if not outgoing:
        res = SyncResult()
        res.mode = "MIGRATION_HISTORIQUE"
        res.erreurs.append("Aucune transaction historique restante a transmettre")
        return res
    res = sync_with_http(server_url.rstrip("/"), outgoing, _local_device_id(conn), conn,
                         api_key=api_key, migration_id=session["migration_id"],
                         require_verified_ack=True)
    res.mode = "MIGRATION_HISTORIQUE"
    if res.ok() and pending_count(conn) == 0:
        _config_set(conn.cursor(), "migration_ack_manifest_sha256", session["manifest_sha256"])
        conn.commit()
    return res


# ============================================================
# SERVEUR SYNC LOCAL (Wi-Fi/Hotspot) : un appareil ecoute les autres
# ============================================================
_sync_store = {"conn": None, "transactions_out": [], "httpd": None}

def stop_sync_server():
    """Arrete le serveur sync local s'il tourne (tests / reconfiguration)."""
    if _sync_store.get("httpd"):
        try:
            _sync_store["httpd"].shutdown()
        except Exception:
            pass
        _sync_store["httpd"] = None

def _start_sync_server(conn):
    """Demarre un mini serveur HTTP sur le port SYNC_SERVER_PORT (thread demon)."""
    from http.server import HTTPServer, BaseHTTPRequestHandler

    store = {"conn": conn, "lock": threading.Lock()}

    class SyncHandler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"service": "faneva-hybrid-sync",
                                         "port": SYNC_SERVER_PORT}).encode())

        def do_POST(self):
            if self.path != "/sync":
                self.send_response(404)
                self.end_headers()
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                remote_txs = body.get("transactions", [])
                remote_device = body.get("device_id", "")
                with store["lock"]:
                    # Dedup locale puis application
                    appliquees, ignorees = apply_remote_transactions(store["conn"], remote_txs, _local_device_id(store["conn"]))
                    # Renvoyer nos propres transactions non connues du peer
                    local_txs = export_outgoing(store["conn"], pending_transactions(store["conn"]))
                    # Marquer comme envoyees celles que le peer vient de nous renvoyer
                    seen_remote = {t["transaction_id"] for t in remote_txs}
                    acked = [t["transaction_id"] for t in local_txs if t["transaction_id"] in seen_remote]
                    if acked:
                        mark_synced(store["conn"], acked)
                    # Enregistrer device distant
                    if remote_device:
                        cur = store["conn"].cursor()
                        cur.execute("INSERT OR IGNORE INTO devices (device_id, nom, derniere_sync) VALUES (?,?,?)",
                                    (remote_device, "peer", now_iso()))
                        cur.execute("UPDATE devices SET derniere_sync=? WHERE device_id=?", (now_iso(), remote_device))
                        store["conn"].commit()
                resp = {"transactions": local_txs, "accepted": [t["transaction_id"] for t in remote_txs],
                        "appliquées": appliquees, "dupliquées_ignorées": ignorees}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(resp, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())

    def serve():
        try:
            httpd = HTTPServer(("0.0.0.0", SYNC_SERVER_PORT), SyncHandler)
            _sync_store["httpd"] = httpd
            httpd.timeout = SYNC_SERVER_TIMEOUT
            httpd.serve_forever()
        except Exception as e:
            print(f"[{HYBRID_LOG_TAG}] Serveur sync local echoue: {e}")

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    return t


# ============================================================
# DETECTION CONNECTIVITE
# ============================================================
def authenticated_staging_status(conn, server_url, timeout=10):
    """Retourne un état Hybrid Online seulement après `/auth/validate` valide.

    La sonde est explicitement non mutatrice : aucune transaction, aucun `/sync`,
    aucun ACK et aucune modification SQLite ne sont effectués. Les informations
    retournées restent limitées aux catégories non sensibles de l’authentification.
    """
    auth = validate_server_authentication(conn, server_url, timeout=timeout)
    online = bool(auth.get("authenticated"))
    return {
        "online": online,
        "connectivity": "INTERNET" if online else "OFFLINE",
        "category": auth.get("category") or "UNKNOWN",
        "http_status": auth.get("http_status"),
        "exception_type": auth.get("exception_type"),
        "error": auth.get("error"),
    }


def detect_connectivity(server_url=None):
    """Retourne 'INTERNET', 'LOCAL' ou 'OFFLINE' en réutilisant le CA HTTPS embarqué.

    Cette compatibilité de sonde ne détermine pas seule le statut Online de l’UI ;
    le tableau Hybrid exige `authenticated_staging_status`. Aucun `/sync` n’est
    exécuté par cette fonction.
    """
    if server_url and _debug_network_endpoint_allowed(server_url):
        try:
            import urllib.request
            open_kwargs = {"timeout": 5}
            if server_url.lower().startswith("https://"):
                open_kwargs["context"] = _auth_ssl_context()
            with urllib.request.urlopen(server_url.rstrip("/") + "/ping", **open_kwargs) as resp:
                if resp.status == 200:
                    return "INTERNET"
        except Exception:
            pass
    # Test serveur local sur le meme reseau Wi-Fi/Hotspot (broadcast simple : localhost pour test)
    try:
        import urllib.request
        with urllib.request.urlopen(f"http://127.0.0.1:{SYNC_SERVER_PORT}/ping", timeout=2) as resp:
            if resp.status == 200:
                return "LOCAL"
    except Exception:
        pass
    return "OFFLINE"


# ============================================================
# BACKUP DISTANT (HTTPS)
# ============================================================
def backup_remote(conn, server_url, db_path):
    """Envoie la base SQLite en HTTPS multipart (quand Internet dispo)."""
    endpoint = (server_url or '').strip().rstrip('/')
    if not _debug_network_endpoint_allowed(endpoint):
        return False
    try:
        import urllib.request
        api_key = get_server_api_key(conn)
        if not api_key:
            return False
        device_id = _local_device_id(conn)
        boundary = "----FanevaBackupBoundary"
        with open(db_path, "rb") as f:
            data = f.read()
        body = (f"--{boundary}\r\n"
                f"Content-Disposition: form-data; name=\"device_id\"\r\n\r\n{device_id}\r\n"
                f"--{boundary}\r\n"
                f"Content-Disposition: form-data; name=\"dbfile\"; filename=\"{os.path.basename(db_path)}\"\r\n"
                f"Content-Type: application/octet-stream\r\n\r\n").encode() + data + f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(endpoint + "/backup", data=body,
                                     headers={"Content-Type": f"multipart/form-data; boundary={boundary}",
                                              "X-Api-Key": api_key},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[{HYBRID_LOG_TAG}] Backup distant echoue: {e}")
        return False
