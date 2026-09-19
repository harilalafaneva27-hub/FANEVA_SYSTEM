[app]
# FANEVA SYSTEM 1.4.9 — workspace séparé depuis la release 1.4.8.
title = FANEVA SYSTEM 1.4.9.16
package.name = fanevasystem
package.domain = com.faneva
source.dir = source
source.include_exts = py,png,jpg,db,pem,json
version = 1.4.9.16
# reportlab : requis pour export PDF stock/rapports (import runtime dans main.py).
requirements = python3,kivy,pyjnius,requests,reportlab
p4a.local_recipes = ./p4a-recipes
orientation = portrait
fullscreen = 0

# RELEASE build preparation; no production sync is executed during build/preflight.
android.api = 34
android.minapi = 21
android.ndk = 25b
# Release validation architecture retained from LOT5.
android.archs = arm64-v8a
android.accept_sdk_license = True
android.private_storage = True
android.release_artifact = apk
android.numeric_version = 1040916
# Requis pour toute résolution DNS, TLS et requête HTTPS depuis l’APK Android.
# Permissions normales : aucune invite runtime ni accès aux données métier.
# Pas de WRITE/READ_EXTERNAL_STORAGE : résolution DB = external best-effort puis fallback
# stockage privé app (INTERNAL_DIR / getFilesDir). Voir resolve_db_path() dans main.py.
android.permissions = INTERNET, ACCESS_NETWORK_STATE
# p4a.source_dir retiré : chemin absolu spécifique à un environnement de build antérieur.
# Buildozer/python-for-android utilisent leur configuration standard.

[buildozer]
log_level = 2
warn_on_root = 0
