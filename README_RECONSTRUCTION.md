# FANEVA SYSTEM 1.4.9.15 — Backup complet reconstructible

## Référence

Cette archive est la sauvegarde de reconstruction de la release staging **FANEVA SYSTEM 1.4.9.15**. L’APK de référence est `bin/FANEVA_SYSTEM_1.4.9.15_PENDING_ZERO_REMOTE_PULL_STAGING.apk`.

| Élément | Valeur |
|---|---|
| Package | `com.faneva.fanevasystem` |
| VersionName | `1.4.9.15` |
| VersionCode | `102140916` |
| ABI | `arm64-v8a` |
| APK SHA-256 | `f069e00d95a54180d0c2d0a3937abd83d980f5b84bebe279156a58d035072f88` |
| Certificat FANEVA SHA-256 | `f1a82e2d24cff33dcd290b1f6fbf1bbbfd455b3544032efe5d84432ec371859b` |
| CA SHA-256 | `9cc2a774b5198dcff14d9be1e66091f538975d867ce029a96bce15a55dfd730f` |

## Contenu

Le backup contient la source Android effective (`source/`), le moteur `faneva_hybrid.py`, `buildozer.spec`, les assets, le manifeste canonique staging, le bundle `assets/cacert.pem`, la suite de tests `test_canonical_stock_views.py`, la source python-for-android utilisée (`p4a-source/`), les rapports de correctif/build et l’APK staging vérifiée.

Il exclut volontairement les bases SQLite métier, les fichiers `.env`, clés API, mots de passe, le keystore de signature, les bytecodes et caches. Le keystore historique FANEVA doit être fourni de nouveau par un canal sécurisé au moment de signer une reconstruction ; il ne fait pas partie de cette archive.

## Reconstruction

Sur un environnement Linux disposant de Python, Buildozer, JDK 17, Android SDK/NDK r25b et des outils Android Build Tools :

1. Vérifier `MANIFEST_SHA256.txt` et l’intégrité de l’APK avant toute utilisation.
2. Exécuter `python3 test_canonical_stock_views.py` à la racine et obtenir `37/37 PASS`.
3. Définir `JAVA_HOME` vers un **JDK 17 complet** qui fournit `javac`.
4. Exécuter `buildozer android release`.
5. Zipaligner le fichier `*-release-unsigned.apk` puis le signer avec le keystore FANEVA historique, alias `faneva`, en v1/v2/v3.
6. Vérifier package, VersionName, VersionCode, ABI, zipalign et certificat avant installation.

> La reconstruction ne doit pas appeler `/sync`, ne doit pas ouvrir/modifier une base d’appareil et ne doit pas modifier D1. Les tests inclus utilisent des fixtures locales uniquement.

## Correctif inclus

Dans `source/main.py`, la sortie anticipée qui arrêtait la synchronisation lorsque `pending_sync=0` a été retirée. Les contrôles ONLINE/Auth restent applicables et la synchronisation peut appeler `sync_internet()` avec une liste sortante vide et le curseur existant afin de récupérer les transactions distantes.

## Vérification

Le fichier `MANIFEST_SHA256.txt` contient l’empreinte SHA-256 de chaque fichier du backup. Le rapport `FANEVA_1.4.9.15_PENDING_ZERO_REMOTE_PULL_STAGING_BUILD_REPORT.md` fournit la trace de build et de signature.
