# FANEVA SYSTEM 1.4.9.14 — Correctif ciblé du pull distant lorsque pending=0

## Objet et résultat

Le correctif retire uniquement le retour anticipé qui assimilait une file locale `pending_sync=0` à l’absence de transactions distantes. Après correction, le bouton **Synchronisation Internet** conserve le contrôle d’authentification/ONLINE existant, puis appelle systématiquement `sync_internet()`. La requête porte donc un tableau `transactions` vide lorsque rien n’est à envoyer, **et** le curseur existant afin que le Worker renvoie les transactions distantes disponibles.

> **Résultat local : PASS.** La suite disponible s’exécute entièrement avec **37/37 tests passants**. Aucun APK n’a été généré et aucun endpoint réel, appareil, SQLite métier ou D1 n’a été sollicité.

## Diff exact de la logique produit

Fichier modifié : `source/main.py`, fonction `KDKApp.do_sync_internet()`.

```diff
                 if not server_state["online"]:
                     popup("Serveur non prêt", ...)
                     return
-                if n == 0:
-                    popup("OK", "Rien a synchroniser. Toutes les transactions sont a jour.")
-                    return
+                # Une file locale vide ne signifie pas qu'aucune transaction distante
+                # n'est disponible. Le sync normal doit toujours envoyer le curseur
+                # afin d'effectuer le pull et le replay idempotent éventuels.
                 res = sync_internet(conn, SYNC_SERVER_URL)
```

Aucune autre branche de `do_sync_internet()`, aucune constante TLS, URL, API key, mapping, logique de vente, gestion D1 ou logique de projection n’a été modifiée.

## Chemin corrigé

| État avant appui | Avant correctif | Après correctif |
|---|---|---|
| `pending_sync=0`, transactions distantes disponibles | Popup locale `OK`, retour avant réseau ; aucun pull/replay/curseur | `sync_internet()` est appelé avec `outgoing=[]` et le curseur local ; le Worker peut retourner les transactions distantes |
| `pending_sync>0` | Push des transactions locales, pull distant et ACK contrôlés | Inchangé |
| `pending_sync=0`, aucune transaction distante | Aucun appel réseau | Appel normal, réponse vide, aucun mouvement local ni données métier modifiées |
| Échec auth/ONLINE | Sync non lancée | Inchangé : blocage avant `sync_internet()` |

La fonction `sync_internet()` conserve l’appel à `sync_with_http(..., sync_cursor=_get_sync_cursor(conn), require_verified_ack=True)`. Le replay distant reste exécuté avant l’avancement du curseur ; en cas de `replay_failures`, le curseur est conservé.[1]

## Tests ajoutés et exécutés

Les nouveaux tests utilisent uniquement une connexion, un transport HTTPS et des UUID de **fixture** simulés en mémoire. Ils ne font ni requête HTTP ni écriture SQLite hors mémoire.

| Test requis | Test local | Résultat |
|---|---|---|
| 1. `pending=0` avec transaction distante fictive | `test_empty_push_receives_remote_fixture_replays_it_and_advances_cursor` | PASS |
| 2. `pending>0` conserve push/pull | `test_pending_nonzero_keeps_historical_push_and_cursor_contract` | PASS |
| 3. Réponse distante vide sans effet métier | `test_empty_remote_response_changes_no_local_sync_state` | PASS |
| 4. Replay UUID identique idempotent | `test_duplicate_remote_replay_remains_idempotent_with_empty_push` | PASS |
| 5. Auth/TLS/curseur conservés | `test_pull_fix_keeps_tls_auth_and_canonical_sync_contract` | PASS |
| Contrôle direct de l’absence du court-circuit | `test_pending_zero_no_longer_short_circuits_the_normal_remote_pull` | PASS |
| Appel avec `outgoing=[]` + curseur existant | `test_pending_zero_invokes_sync_with_existing_cursor_and_empty_push` | PASS |
| 6. Stock/Vente/non-régressions déjà disponibles | 30 contrôles restants de la suite | PASS |
| 7. Suite locale disponible complète | `python3 test_canonical_stock_views.py` | **37/37 PASS** |

## Fichiers modifiés

| Fichier | Modification | Portée |
|---|---|---|
| `source/main.py` | Suppression du seul `return` déclenché par `n == 0` ; commentaire de l’invariant pull | Correctif produit ciblé |
| `test_canonical_stock_views.py` | Sept tests de transport/pull mocké et harnais de simulation mémoire | Validation locale uniquement |

## Limites et prochaine étape

Le correctif est validé **localement**. Il n’a pas été intégré dans un APK, installé sur A2 ni testé contre D1 réel. L’étape ultérieure devra d’abord autoriser explicitement un build, puis une validation physique A2 à appui unique, avec contrôle `Envoyées`, `Reçues`, `Ignorées`, curseur et Bande collante à 12. Cette étape n’est pas exécutée par le présent travail.

## Invariants de sécurité

| Invariant | Valeur |
|---|---:|
| Sync réelle A1/A2 | 0 |
| Écriture D1 | 0 |
| SQLite métier réel | 0 |
| Reprise des 8 UUID A1 | 0 |
| Migration | 0 |
| Rollback | 0 |
| Production | 0 |
| APK | 0 |

## Référence

[1]: source/faneva_hybrid.py — `sync_internet()` et `sync_with_http()`, lignes 2481–2674 et 2686–2705.
