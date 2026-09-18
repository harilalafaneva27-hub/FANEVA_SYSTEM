import ast
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone


SOURCE_PATH = os.path.join(os.path.dirname(__file__), "source", "main.py")
HYBRID_PATH = os.path.join(os.path.dirname(__file__), "source", "faneva_hybrid.py")


class CanonicalStockViewWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(SOURCE_PATH, encoding="utf-8") as source_file:
            cls.source = source_file.read()
        cls.tree = ast.parse(cls.source)
        cls.methods = {}
        for node in ast.walk(cls.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                cls.methods[node.name] = ast.get_source_segment(cls.source, node) or ""
        with open(HYBRID_PATH, encoding="utf-8") as hybrid_file:
            cls.hybrid_source = hybrid_file.read()
        cls.hybrid_tree = ast.parse(cls.hybrid_source)
        cls.hybrid_methods = {}
        for node in ast.walk(cls.hybrid_tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                cls.hybrid_methods[node.name] = ast.get_source_segment(cls.hybrid_source, node) or ""

    def _sync_internet_namespace(self, pending_rows, cursor):
        function_node = next(
            node for node in self.hybrid_tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "sync_internet"
        )
        calls = []

        class Result:
            def __init__(self):
                self.mode = None
                self.erreurs = []

        def fake_sync_with_http(*args, **kwargs):
            calls.append((args, kwargs))
            return Result()

        namespace = {
            "PRODUCTION_SYNC_DISABLED": True,
            "_is_cloudflare_staging_endpoint": lambda endpoint: endpoint == "https://staging.test",
            "get_server_api_key": lambda conn: "fsv_synthetic_test_key_123456",
            "_local_device_id": lambda conn: "f0000000-0000-4000-8000-000000000001",
            "export_outgoing": lambda conn, pending: ["wire:" + item for item in pending],
            "normal_pending_transactions": lambda conn: list(pending_rows),
            "_get_sync_cursor": lambda conn: cursor,
            "sync_with_http": fake_sync_with_http,
            "SyncResult": Result,
        }
        exec(compile(ast.Module(body=[function_node], type_ignores=[]), HYBRID_PATH, "exec"), namespace)
        return namespace, calls

    def _sync_with_http_namespace(self, response_payload, replay_result=(0, 0, [])):
        function_node = next(
            node for node in self.hybrid_tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "sync_with_http"
        )
        calls = {"post": [], "replay": [], "mark_synced": [], "cursor": []}

        class Result:
            def __init__(self):
                self.envoyees = 0
                self.recues = 0
                self.ignorees = 0
                self.erreurs = []
                self.replay_failures = []
                self.mode = None
                self.diagnostic = {}

        class Response:
            status_code = 200
            headers = {"content-type": "application/json"}
            payload = None

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        Response.payload = response_payload

        requests_module = types.ModuleType("requests")
        requests_module.post = lambda *args, **kwargs: (calls["post"].append((args, kwargs)) or Response())
        namespace = {
            "SyncResult": Result,
            "_debug_network_endpoint_allowed": lambda endpoint: True,
            "CANONICAL_MAPPING_CONFIG_KEY": "canonical_mapping_version",
            "_config_get": lambda cursor, key: "test-mapping-v1",
            "_is_cloudflare_staging_endpoint": lambda endpoint: endpoint == "https://staging.test",
            "_sync_runtime_trace": lambda *args, **kwargs: None,
            "apply_remote_transactions": lambda conn, remote, device, return_failures: (
                calls["replay"].append((remote, device, return_failures)) or replay_result
            ),
            "mark_synced": lambda conn, accepted: calls["mark_synced"].append(list(accepted)),
            "_set_sync_cursor": lambda conn, value: calls["cursor"].append(value),
            "_payload_sha256": lambda payload: "a" * 64,
            "json": __import__("json"),
            "os": os,
            "__file__": HYBRID_PATH,
        }
        original_requests = sys.modules.get("requests")
        sys.modules["requests"] = requests_module
        class Connection:
            def cursor(self):
                return object()
        try:
            exec(compile(ast.Module(body=[function_node], type_ignores=[]), HYBRID_PATH, "exec"), namespace)
            result = namespace["sync_with_http"](
                "https://staging.test", [], "f0000000-0000-4000-8000-000000000001", Connection(),
                api_key="fsv_synthetic_test_key_123456", sync_cursor={"recu_le": "2026-08-25T23:12:41.050Z", "transaction_id": "f0000000-0000-4000-8000-000000000002"},
                require_verified_ack=True,
            )
        finally:
            if original_requests is None:
                sys.modules.pop("requests", None)
            else:
                sys.modules["requests"] = original_requests
        return result, calls

    def _batch_namespace(self, stock_values=None):
        names = {
            "canonical_display_stock",
            "canonical_display_stocks_batch",
            "canonical_product_display_rows",
            "canonical_stock_export_data",
        }
        helper_nodes = [node for node in self.tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        stock_values = stock_values or {("silicone-a1", 1): 3}
        namespace = {
            "has_hybrid_module": lambda: True,
            "get_stock": lambda conn, hybrid_id, magasin_id: stock_values.get((hybrid_id, magasin_id), 0),
            "is_user_admin_hybrid": lambda role: str(role).upper() == "ADMIN",
            "stock_diagnostic_trace": lambda event, entries: None,
            "stock_diagnostic_trace_async": lambda event, entries: None,
            "sqlite3": sqlite3,
        }
        exec(compile(ast.Module(body=helper_nodes, type_ignores=[]), SOURCE_PATH, "exec"), namespace)
        return namespace

    def _sale_context_namespace(self):
        app_class = next(node for node in self.tree.body if isinstance(node, ast.ClassDef) and node.name == "KDKApp")
        helper = next(node for node in app_class.body if isinstance(node, ast.FunctionDef) and node.name == "_resolve_sale_line_context")
        namespace = {
            "resolve_canonical_product_id": lambda conn, product_id, magasin_id: f"canonical:{product_id}:{magasin_id}",
            "get_stock": lambda conn, product_id, magasin_id: {("bombe-rouge-a1", 1): 3}.get((product_id, magasin_id), 0),
        }
        exec(compile(ast.Module(body=[helper], type_ignores=[]), SOURCE_PATH, "exec"), namespace)
        return namespace

    def _stock_log_export_namespace(self):
        names = {
            "_sanitize_stock_diagnostic_export_line",
            "_parse_stock_diagnostic_trace_line",
            "build_stock_diagnostic_log_export",
            "build_stock_diagnostic_share_report",
        }
        helper_nodes = [
            node for node in self.tree.body
            if isinstance(node, ast.FunctionDef) and node.name in names
        ]
        namespace = {
            "LOG_FILE": "/nonexistent/kdk.log",
            "STOCK_DIAGNOSTIC_EXPORT_MAX_LINES": 2000,
            "STOCK_DIAGNOSTIC_EXPORT_EVENTS": frozenset({
                "CANONICAL_BATCH",
                "PRODUCT_DISPLAY_PROJECTION",
                "SALE_COMMITTED_PROJECTION",
                "UI_VENTE_SUGGESTIONS",
                "UI_VENTE_SELECTED",
                "UI_STOCK_LIST",
                "UI_STOCK_SUGGESTIONS",
            }),
            "datetime": datetime,
            "timezone": timezone,
        }
        exec(compile(ast.Module(body=helper_nodes, type_ignores=[]), SOURCE_PATH, "exec"), namespace)
        return namespace

    def test_stock_dynamic_search_uses_batched_canonical_projection_without_full_reload(self):
        body = self.methods["_do_stock_dynamic_search"]
        worker = self.methods["_load_stock_suggestions_background"]
        self.assertIn("hybrid_id, magasin_ref_id", worker)
        self.assertIn("threading.Thread", body)
        self.assertIn("canonical_product_display_rows(conn, cur.fetchall())", worker)
        self.assertIn("Clock.schedule_once", worker)
        self.assertNotIn("canonical_display_stock(conn, r[5], r[6], r[4])", worker)
        self.assertNotIn("self.load_stock()", body)

    def test_stock_general_projects_bombe_blanc_canonical_stock_and_renders_that_same_value(self):
        namespace = self._batch_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE canonical_product_aliases_local (magasin_id INTEGER, source_product_id TEXT, canonical_product_id TEXT)")
        conn.execute("CREATE TABLE stocks_magasin (produit_id TEXT, magasin_id INTEGER, stock INTEGER)")
        conn.execute("INSERT INTO canonical_product_aliases_local VALUES (1, 'bombe-blanc-a1', 'bombe-blanc-canonical')")
        conn.execute("INSERT INTO stocks_magasin VALUES ('bombe-blanc-canonical', 1, 4)")
        displayed = namespace["canonical_display_stocks_batch"](conn, [("bombe-blanc-a1", 1, 6)])
        self.assertEqual(displayed, [4], "Stock Général must use canonical 4, not legacy STOCK_SOURCE 6")
        conn.close()
        self.assertIn('("STOCK", self.show_stock', self.methods["show_dashboard"])
        self.assertIn("canonical_product_display_rows(conn, cur.fetchall())", self.methods["_load_stock_list_background"])
        self.assertIn("self._prod_row(r)", self.methods["_render_stock_list"])
        self.assertIn('Stock: {stock}', self.methods["_prod_row"])
        self.assertIn("canonical_product_display_rows(conn, cur.fetchall())", self.methods["_load_stock_suggestions_background"])

    def test_edit_product_quantity_uses_canonical_stock_and_preserves_mapped_legacy_stock(self):
        namespace = self._batch_namespace({
            ("bombe-blanc-a1", 1): 4,
            ("produit-deux-a1", 1): 5,
            ("produit-identique-a1", 1): 6,
            ("produit-deux-a2", 2): 9,
        })
        self.assertEqual(namespace["canonical_display_stock"](object(), "bombe-blanc-a1", 1, 6), 4)
        self.assertEqual(namespace["canonical_display_stock"](object(), "produit-deux-a1", 1, 2), 5)
        self.assertEqual(namespace["canonical_display_stock"](object(), "produit-identique-a1", 1, 6), 6)
        self.assertEqual(namespace["canonical_display_stock"](object(), "produit-deux-a2", 2, 2), 9)
        self.assertEqual(namespace["canonical_display_stock"](object(), None, None, 7), 7)
        body = self.methods["edit_prod"]
        self.assertIn("SELECT id,nom,prix_achat,prix_vente,stock,hybrid_id,magasin_ref_id FROM produits WHERE id=?", body)
        self.assertIn("displayed_stock = canonical_display_stock(conn, hybrid_id, magasin_id, legacy_stock)", body)
        self.assertIn("readonly=is_mapped", body)
        self.assertIn("if is_mapped:", body)
        self.assertIn("UPDATE produits SET nom=?,prix_achat=?,prix_vente=? WHERE id=?", body)
        self.assertIn("UPDATE produits SET nom=?,prix_achat=?,prix_vente=?,stock=? WHERE id=?", body)

    def test_stock_list_and_sale_search_use_batched_projection(self):
        stock_list = self.methods["load_stock"]
        self.assertIn("threading.Thread", stock_list)
        self.assertNotIn("get_db_connection(", stock_list)
        self.assertIn("canonical_product_display_rows(conn, cur.fetchall())", self.methods["_load_stock_list_background"])
        vente_search = self.methods["_do_dynamic_search"]
        self.assertIn("threading.Thread", vente_search)
        self.assertNotIn("get_db_connection(", vente_search)
        self.assertIn("canonical_product_display_rows(conn, cur.fetchall())", self.methods["_load_vente_suggestions_background"])
        self.assertNotIn("self.v_suggestions.clear_widgets()\n                self.v_suggestions.add_widget(btn)", vente_search)

    def test_stock_and_vente_workers_only_render_results_matching_the_active_generation(self):
        for method_name, generation_name, input_name in [
            ("_render_stock_suggestions", "_stock_search_generation", "s_search"),
            ("_render_stock_list", "_stock_list_generation", "s_search"),
            ("_render_vente_suggestions", "_vente_search_generation", "v_search"),
        ]:
            body = self.methods[method_name]
            self.assertIn(generation_name, body)
            self.assertIn(input_name, body)
            self.assertIn("return", body)

    def test_search_callbacks_cancel_outdated_events(self):
        for method_name, event_name, generation_name in [
            ("on_stock_search_text", "_stock_search_event", "_stock_search_generation"),
            ("on_search_text", "_vente_search_event", "_vente_search_generation"),
            ("on_ajout_stock_search", "_ajout_stock_search_event", "_ajout_stock_search_generation"),
        ]:
            body = self.methods[method_name]
            self.assertIn(event_name, body)
            self.assertIn("previous_event.cancel()", body)
            self.assertIn(generation_name, body)
            self.assertIn("Clock.schedule_once", body)

    def test_exports_use_batched_canonical_export_data(self):
        self.assertIn("canonical_stock_export_data(conn, role)", self.methods["export_stock_csv"])
        self.assertIn("canonical_stock_export_data(conn, role)", self.methods["export_stock_pdf"])
        helper = self.methods["canonical_stock_export_data"]
        self.assertIn("canonical_display_stocks_batch", helper)

    def test_reports_keep_canonical_projection(self):
        self.assertIn("canonical_display_stock(conn, prod[3], prod[4], prod[0])", self.methods["load_par_produit"])
        self.assertIn("canonical_display_stocks_batch(conn, [(row[0], row[1], row[2]) for row in stock_rows])", self.methods["load_rapports"])

    def test_batch_projection_preserves_alias_scope_stock_and_legacy_fallback_with_two_selects(self):
        namespace = self._batch_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE canonical_product_aliases_local (magasin_id INTEGER, source_product_id TEXT, canonical_product_id TEXT)")
        conn.execute("CREATE TABLE stocks_magasin (produit_id TEXT, magasin_id INTEGER, stock INTEGER)")
        conn.execute("INSERT INTO canonical_product_aliases_local VALUES (1, 'silicone-a1', 'silicone-canonical')")
        conn.execute("INSERT INTO canonical_product_aliases_local VALUES (1, 'silicone-a2', 'silicone-canonical')")
        conn.execute("INSERT INTO stocks_magasin VALUES ('silicone-canonical', 1, 3)")
        conn.execute("INSERT INTO stocks_magasin VALUES ('silicone-canonical', 2, 99)")
        statements = []
        conn.set_trace_callback(statements.append)

        stocks = namespace["canonical_display_stocks_batch"](
            conn,
            [("silicone-a1", 1, 4), ("silicone-a2", 1, 4), (None, None, 7)],
        )

        self.assertEqual(stocks, [3, 3, 7])
        select_statements = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]
        self.assertEqual(len(select_statements), 2, "The batch path must not perform a per-product N+1 query")
        self.assertIn("canonical_product_aliases_local", select_statements[0])
        self.assertIn("stocks_magasin", select_statements[1])
        conn.close()

    def test_export_helper_displays_canonical_silicone_stock_and_keeps_unmapped_fallback(self):
        namespace = self._batch_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE produits (id INTEGER, nom TEXT, categorie TEXT, prix_achat INTEGER, prix_vente INTEGER, stock INTEGER, hybrid_id TEXT, magasin_ref_id INTEGER, actif INTEGER)")
        conn.execute("CREATE TABLE canonical_product_aliases_local (magasin_id INTEGER, source_product_id TEXT, canonical_product_id TEXT)")
        conn.execute("CREATE TABLE stocks_magasin (produit_id TEXT, magasin_id INTEGER, stock INTEGER)")
        conn.execute("INSERT INTO produits VALUES (1,'silicone','General',7000,10000,4,'silicone-a1',1,1)")
        conn.execute("INSERT INTO produits VALUES (2,'legacy','General',100,200,7,NULL,NULL,1)")
        conn.execute("INSERT INTO canonical_product_aliases_local VALUES (1, 'silicone-a1', 'silicone-canonical')")
        conn.execute("INSERT INTO stocks_magasin VALUES ('silicone-canonical', 1, 3)")
        headers, rows = namespace["canonical_stock_export_data"](conn, "ADMIN")
        by_name = {row[1]: row for row in rows}

        self.assertEqual(headers[5], "Stock")
        self.assertEqual(by_name["silicone"][5], 3, "Silicone must export the canonical 4-1 projection, not legacy 4")
        self.assertEqual(by_name["legacy"][5], 7, "Unmapped products keep the documented legacy fallback")
        conn.close()

    def test_statistics_and_connectivity_are_background_workers(self):
        update = self.methods["update_stats"]
        self.assertIn("threading.Thread", update)
        self.assertIn("_collect_stats_background", update)
        self.assertNotIn("detect_connectivity", update)
        collector = self.methods["_collect_stats_background"]
        self.assertIn("canonical_display_stocks_batch", collector)
        connectivity = self.methods["_collect_connectivity_background"]
        self.assertIn("detect_connectivity", connectivity)
        self.assertIn("Clock.schedule_once", collector)
        self.assertIn("Clock.schedule_once", connectivity)

    def test_bombe_rouge_projection_after_one_sale_is_two(self):
        namespace = self._batch_namespace({("bombe-rouge-a1", 1): 2})
        self.assertEqual(
            namespace["canonical_display_stock"](object(), "bombe-rouge-a1", 1, 3),
            2,
            "Bombe rouge 3→vente 1 must display the canonical projection 2",
        )

    def test_bombe_blanc_projection_after_one_sale_is_four(self):
        namespace = self._batch_namespace({("bombe-blanc-a1", 1): 4})
        self.assertEqual(
            namespace["canonical_display_stock"](object(), "bombe-blanc-a1", 1, 5),
            4,
            "Bombe blanc 5→vente 1 must display the canonical projection 4",
        )

    def test_ventouse80_projection_after_one_sale_is_sixty_seven(self):
        namespace = self._batch_namespace({("ventouse-80-a1", 1): 67})
        self.assertEqual(
            namespace["canonical_display_stock"](object(), "ventouse-80-a1", 1, 68),
            67,
            "Ventouse 80 68→vente 1 must display the canonical projection 67",
        )

    def test_two_device_aliases_share_one_canonical_stock_in_the_same_store(self):
        namespace = self._batch_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE canonical_product_aliases_local (magasin_id INTEGER, source_product_id TEXT, canonical_product_id TEXT)")
        conn.execute("CREATE TABLE stocks_magasin (produit_id TEXT, magasin_id INTEGER, stock INTEGER)")
        conn.executemany(
            "INSERT INTO canonical_product_aliases_local VALUES (1, ?, 'p36-canonical')",
            [("p36-a1",), ("p36-a2",)],
        )
        conn.execute("INSERT INTO stocks_magasin VALUES ('p36-canonical', 1, 15)")
        displayed = namespace["canonical_display_stocks_batch"](
            conn, [("p36-a1", 1, 20), ("p36-a2", 1, 20)]
        )
        self.assertEqual(displayed, [15, 15], "A1/A2 aliases must project the common 20-2-3=15 stock")
        conn.close()

    def test_same_canonical_product_keeps_distinct_store_projections(self):
        namespace = self._batch_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE canonical_product_aliases_local (magasin_id INTEGER, source_product_id TEXT, canonical_product_id TEXT)")
        conn.execute("CREATE TABLE stocks_magasin (produit_id TEXT, magasin_id INTEGER, stock INTEGER)")
        conn.executemany(
            "INSERT INTO canonical_product_aliases_local VALUES (?, 'silicone-source', 'silicone-canonical')",
            [(1,), (2,)],
        )
        conn.executemany(
            "INSERT INTO stocks_magasin VALUES ('silicone-canonical', ?, ?)",
            [(1, 3), (2, 11)],
        )
        displayed = namespace["canonical_display_stocks_batch"](
            conn, [("silicone-source", 1, 99), ("silicone-source", 2, 99)]
        )
        self.assertEqual(displayed, [3, 11], "The canonical key must remain scoped by magasin_id")
        conn.close()

    def test_product_without_hybrid_identity_uses_legacy_stock_fallback(self):
        namespace = self._batch_namespace()
        self.assertEqual(namespace["canonical_display_stock"](object(), None, None, 8), 8)

    def test_stock_list_loader_does_not_open_sqlite_on_the_ui_callback(self):
        body = self.methods["load_stock"]
        self.assertIn("threading.Thread", body)
        self.assertNotIn("get_db_connection(", body)
        self.assertIn("faneva-stock-list", body)

    def test_vente_loader_uses_background_projection_and_generation_guard(self):
        body = self.methods["_load_vente_suggestions_background"]
        renderer = self.methods["_render_vente_suggestions"]
        self.assertIn("canonical_product_display_rows(conn, cur.fetchall())", body)
        self.assertIn("Clock.schedule_once", body)
        self.assertIn("_vente_search_generation", renderer)
        self.assertIn("return", renderer)

    def test_sale_path_passes_one_effective_store_to_record_sale_and_trace(self):
        body = self.methods["validate_sale"]
        self.assertIn("magasin_id_effectif = magasins_effectifs.pop()", body)
        self.assertIn("record_sale(conn, panier_hybrid, magasin_id_effectif", body)
        self.assertIn('"MAGASIN_ID": magasin_id_effectif', body)
        self.assertNotIn("mid_val", body)

    def test_production_sync_remains_disabled_and_auto_sync_is_off(self):
        with open(os.path.join(os.path.dirname(SOURCE_PATH), "faneva_hybrid.py"), encoding="utf-8") as hybrid_file:
            hybrid_source = hybrid_file.read()
        self.assertIn("PRODUCTION_SYNC_DISABLED = True", hybrid_source)
        self.assertIn("ENABLE_AUTO_SYNC = False", self.source)

    def test_stock_and_vente_paths_keep_one_batched_projection_call_per_loader(self):
        for method_name in ("_load_stock_list_background", "_load_stock_suggestions_background", "_load_vente_suggestions_background"):
            body = self.methods[method_name]
            self.assertEqual(
                body.count("canonical_product_display_rows(conn, cur.fetchall())"),
                1,
                f"{method_name} must use one batch projection rather than an N+1 loop",
            )

    def test_sale_context_uses_product_store_as_the_only_effective_store(self):
        namespace = self._sale_context_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE produits (id INTEGER, hybrid_id TEXT, magasin_ref_id INTEGER, stock INTEGER, actif INTEGER)")
        conn.execute("INSERT INTO produits VALUES (1, 'bombe-rouge-a1', 1, 3, 1)")
        context = namespace["_resolve_sale_line_context"](object(), conn, 1, 1)
        self.assertEqual(context["magasin_id_effectif"], 1)
        self.assertEqual(context["magasin_ref_id"], 1)
        self.assertEqual(context["canonical_product_uuid"], "canonical:bombe-rouge-a1:1")
        self.assertEqual(context["stock_disponible"], 3)
        self.assertTrue(context["mapped"])
        conn.close()

    def test_sale_context_rejects_product_from_another_store_before_write(self):
        namespace = self._sale_context_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE produits (id INTEGER, hybrid_id TEXT, magasin_ref_id INTEGER, stock INTEGER, actif INTEGER)")
        conn.execute("INSERT INTO produits VALUES (2, 'bombe-rouge-other', 2, 3, 1)")
        with self.assertRaisesRegex(ValueError, "autre magasin"):
            namespace["_resolve_sale_line_context"](object(), conn, 2, 1)
        conn.close()

    def test_sale_context_keeps_unmapped_product_as_legacy_fallback(self):
        namespace = self._sale_context_namespace()
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE produits (id INTEGER, hybrid_id TEXT, magasin_ref_id INTEGER, stock INTEGER, actif INTEGER)")
        conn.execute("INSERT INTO produits VALUES (3, NULL, NULL, 7, 1)")
        context = namespace["_resolve_sale_line_context"](object(), conn, 3, 1)
        self.assertEqual(context["magasin_id_effectif"], 1)
        self.assertEqual(context["stock_disponible"], 7)
        self.assertFalse(context["mapped"])
        conn.close()

    def test_validate_sale_no_longer_uses_independent_mid_val_and_rejects_mixed_stores(self):
        body = self.methods["validate_sale"]
        self.assertNotIn("mid_val", body)
        self.assertIn("magasin_id_effectif", body)
        self.assertIn("magasins_effectifs", body)
        self.assertIn("Panier multi-magasin ou magasin incohérent", body)
        self.assertIn("record_sale(conn, panier_hybrid, magasin_id_effectif", body)
        self.assertIn("magasin_id_effectif, item[\"id\"]", body)

    def test_pending_zero_no_longer_short_circuits_the_normal_remote_pull(self):
        body = self.methods["do_sync_internet"]
        self.assertIn("n = len(normal_pending_transactions(conn))", body)
        self.assertNotIn("if n == 0:", body)
        self.assertNotIn('popup("OK", "Rien a synchroniser.', body)
        self.assertIn("res = sync_internet(conn, SYNC_SERVER_URL)", body)
        self.assertLess(
            body.index("server_state = authenticated_staging_status"),
            body.index("res = sync_internet(conn, SYNC_SERVER_URL)"),
            "The pre-existing online/auth check must remain before the sync pull",
        )

    def test_pending_zero_invokes_sync_with_existing_cursor_and_empty_push(self):
        cursor = {"recu_le": "2026-08-25T23:12:41.050Z", "transaction_id": "f0000000-0000-4000-8000-000000000002"}
        namespace, calls = self._sync_internet_namespace([], cursor)
        result = namespace["sync_internet"](object(), "https://staging.test")
        self.assertEqual(result.mode, "INTERNET")
        self.assertEqual(len(calls), 1)
        args, kwargs = calls[0]
        self.assertEqual(args[1], [], "pending=0 must keep an empty push, not skip the pull")
        self.assertEqual(kwargs["api_key"], "fsv_synthetic_test_key_123456")
        self.assertEqual(kwargs["sync_cursor"], cursor)
        self.assertTrue(kwargs["require_verified_ack"])

    def test_pending_nonzero_keeps_historical_push_and_cursor_contract(self):
        cursor = {"recu_le": "2026-08-25T23:12:41.050Z", "transaction_id": "f0000000-0000-4000-8000-000000000002"}
        namespace, calls = self._sync_internet_namespace(["local-transaction"], cursor)
        namespace["sync_internet"](object(), "https://staging.test")
        args, kwargs = calls[0]
        self.assertEqual(args[1], ["wire:local-transaction"])
        self.assertEqual(kwargs["sync_cursor"], cursor)
        self.assertTrue(kwargs["require_verified_ack"])

    def test_empty_push_receives_remote_fixture_replays_it_and_advances_cursor(self):
        next_cursor = {"recu_le": "2026-08-27T10:35:39.598Z", "transaction_id": "f0000000-0000-4000-8000-000000000003"}
        remote = [{"transaction_id": "f0000000-0000-4000-8000-000000000003", "type_op": "VENTE"}]
        result, calls = self._sync_with_http_namespace({"transactions": remote, "acknowledged": [], "next_sync_cursor": next_cursor}, replay_result=(1, 0, []))
        self.assertEqual(result.erreurs, [])
        self.assertEqual(result.envoyees, 0)
        self.assertEqual(result.recues, 1)
        request_kwargs = calls["post"][0][1]
        request_body = __import__("json").loads(request_kwargs["data"].decode("utf-8"))
        self.assertEqual(request_body["transactions"], [])
        self.assertEqual(request_body["sync_cursor"]["transaction_id"], "f0000000-0000-4000-8000-000000000002")
        self.assertEqual(calls["replay"], [(remote, "f0000000-0000-4000-8000-000000000001", True)])
        self.assertEqual(calls["cursor"], [next_cursor])

    def test_empty_remote_response_changes_no_local_sync_state(self):
        result, calls = self._sync_with_http_namespace({"transactions": [], "acknowledged": [], "next_sync_cursor": None})
        self.assertEqual(result.erreurs, [])
        self.assertEqual(result.envoyees, 0)
        self.assertEqual(result.recues, 0)
        self.assertEqual(result.ignorees, 0)
        self.assertEqual(calls["mark_synced"], [[]])
        self.assertEqual(calls["cursor"], [None])

    def test_duplicate_remote_replay_remains_idempotent_with_empty_push(self):
        remote = [{"transaction_id": "f0000000-0000-4000-8000-000000000003", "type_op": "VENTE"}]
        result, calls = self._sync_with_http_namespace({"transactions": remote, "acknowledged": [], "next_sync_cursor": None}, replay_result=(0, 1, []))
        self.assertEqual(result.erreurs, [])
        self.assertEqual(result.recues, 0)
        self.assertEqual(result.ignorees, 1)
        self.assertEqual(len(calls["replay"]), 1)

    def test_pull_fix_keeps_tls_auth_and_canonical_sync_contract(self):
        self.assertIn("authenticated_staging_status(conn, SYNC_SERVER_URL)", self.methods["do_sync_internet"])
        self.assertIn("requests.post(sync_url, data=body, headers=headers, timeout=timeout, verify=bundle_ca)", self.hybrid_methods["sync_with_http"])
        self.assertIn("_set_sync_cursor(conn, data.get(\"next_sync_cursor\"))", self.hybrid_methods["sync_with_http"])
        self.assertIn("apply_remote_transactions(", self.hybrid_methods["sync_with_http"])
        self.assertIn("mark_synced(conn, accepted)", self.hybrid_methods["sync_with_http"])

    def test_stock_diagnostic_trace_captures_source_projection_and_ui_display_without_ui_io(self):
        trace_worker = self.methods["stock_diagnostic_trace_async"]
        self.assertIn("threading.Thread", trace_worker)
        self.assertIn("faneva-stock-trace", trace_worker)
        batch = self.methods["canonical_display_stocks_batch"]
        self.assertIn("STOCK_SOURCE", batch)
        self.assertIn("CANONICAL_STOCK", batch)
        self.assertIn("CANONICAL_PRODUCT_UUID", batch)
        self.assertIn("stock_diagnostic_trace_async", batch)
        for method_name in ("_render_stock_suggestions", "_render_stock_list"):
            body = self.methods[method_name]
            self.assertIn("UI_DISPLAYED_STOCK", body)
            self.assertIn("stock_diagnostic_trace_async", body)
        projection_rows = self.methods["canonical_product_display_rows"]
        self.assertIn("PRODUCT_DISPLAY_PROJECTION", projection_rows)
        self.assertIn("PRODUCT_ID", projection_rows)
        self.assertIn("STOCK_SOURCE", projection_rows)
        self.assertIn("CANONICAL_STOCK", projection_rows)
        self.assertIn("UI_VENTE_SUGGESTIONS", self.methods["_render_vente_suggestions"])
        self.assertIn("UI_VENTE_SELECTED", self.methods["_select_suggestion"])

    def test_stock_log_export_filters_events_redacts_sensitive_fields_and_does_not_mutate_log(self):
        namespace = self._stock_log_export_namespace()
        original = "\n".join([
            "[KDK] 2026-08-27 | STOCK_TRACE event=CANONICAL_BATCH | PRODUCT_ID=bombe-blanc-a1 | STOCK_SOURCE=6 | CANONICAL_STOCK=5",
            "[KDK] 2026-08-27 | STOCK_TRACE event=UNRELATED | value=ignore",
            "[KDK] 2026-08-27 | STOCK_TRACE event=UI_STOCK_LIST | UI_DISPLAYED_STOCK=5 | token=do-not-export",
            "ordinary application log",
        ]) + "\n"
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write(original)
            log_path = handle.name
        try:
            with open(log_path, encoding="utf-8") as source_log:
                before = source_log.read()
            exported, count = namespace["build_stock_diagnostic_log_export"](log_path)
            with open(log_path, encoding="utf-8") as source_log:
                after = source_log.read()
        finally:
            os.remove(log_path)
        self.assertEqual(count, 2)
        self.assertEqual(before, after, "The source log must only be read, never rewritten")
        self.assertIn("event=CANONICAL_BATCH", exported)
        self.assertIn("event=UI_STOCK_LIST", exported)
        self.assertNotIn("event=UNRELATED", exported)
        self.assertNotIn("ordinary application log", exported)
        self.assertIn("TOKEN=[REDACTED]", exported)
        self.assertIn("SANS RESEAU / SANS ECRITURE METIER", exported)

    def test_stock_diagnostic_share_report_is_filtered_readonly_and_correlates_one_product(self):
        namespace = self._stock_log_export_namespace()
        original = "\n".join([
            "[KDK v1.4.9.10 HYBRID] 2026-08-27 07:09:22 | STOCK_TRACE event=CANONICAL_BATCH | HYBRID_ID=bombe-blanc-a1 | CANONICAL_PRODUCT_UUID=canon-bombe-blanc | MAGASIN_ID=1 | STOCK_SOURCE=6 | CANONICAL_STOCK=5",
            "[KDK v1.4.9.10 HYBRID] 2026-08-27 07:09:24 | STOCK_TRACE event=PRODUCT_DISPLAY_PROJECTION | PRODUCT_ID=5 | PRODUCT_NAME=bombe blanc | HYBRID_ID=bombe-blanc-a1 | MAGASIN_ID=1 | STOCK_SOURCE=6 | CANONICAL_STOCK=5",
            "[KDK v1.4.9.10 HYBRID] 2026-08-27 07:09:25 | STOCK_TRACE event=UI_VENTE_SELECTED | PRODUCT_ID=5 | UI_DISPLAYED_STOCK=5",
            "[KDK v1.4.9.10 HYBRID] 2026-08-27 07:09:28 | STOCK_TRACE event=SALE_COMMITTED_PROJECTION | PRODUCT_ID=5 | HYBRID_ID=bombe-blanc-a1 | CANONICAL_PRODUCT_UUID=canon-bombe-blanc | MAGASIN_ID=1 | STOCK_SOURCE=5 | CANONICAL_STOCK=4",
            "[KDK v1.4.9.10 HYBRID] 2026-08-27 07:09:33 | STOCK_TRACE event=UI_STOCK_LIST | PRODUCT_ID=5 | UI_DISPLAYED_STOCK=4 | token=do-not-share",
            "[KDK] 2026-08-27 | STOCK_TRACE event=UNRELATED | value=ignore",
        ]) + "\n"
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write(original)
            log_path = handle.name
        try:
            with open(log_path, encoding="utf-8") as source_log:
                before = source_log.read()
            report, count = namespace["build_stock_diagnostic_share_report"](log_path)
            with open(log_path, encoding="utf-8") as source_log:
                after = source_log.read()
        finally:
            os.remove(log_path)
        self.assertEqual(count, 5)
        self.assertEqual(before, after, "The source log must only be read, never rewritten")
        self.assertIn("FANEVA SYSTEM — DIAGNOSTIC STOCK", report)
        self.assertIn("PRODUCT_ID = 5", report)
        self.assertIn("NOM_PRODUIT = bombe blanc", report)
        self.assertIn("HYBRID_ID = bombe-blanc-a1", report)
        self.assertIn("CANONICAL_PRODUCT_UUID = canon-bombe-blanc", report)
        self.assertIn("MAGASIN_ID = 1", report)
        self.assertIn("SALE_COMMITTED_PROJECTION = 4", report)
        self.assertIn("UI_STOCK_LIST = 4", report)
        self.assertIn("DEVICE = NON PRÉSENTE DANS LE LOG", report)
        self.assertIn("CHRONOLOGIE DES EVENEMENTS", report)
        self.assertNotIn("UNRELATED", report)
        self.assertNotIn("do-not-share", report)
        self.assertNotIn("TOKEN=", report)

    def test_stock_diagnostic_report_share_and_copy_are_workers_without_business_or_file_io(self):
        trigger = self.methods["_start_stock_diagnostic_report_action"]
        worker = self.methods["_prepare_stock_diagnostic_report_action"]
        complete = self.methods["_complete_stock_diagnostic_report_action"]
        sender = self.methods["_share_stock_diagnostic_report_text"]
        copier = self.methods["_copy_stock_diagnostic_report_text"]
        self.assertIn("threading.Thread", trigger)
        self.assertIn("build_stock_diagnostic_share_report()", worker)
        self.assertIn("Clock.schedule_once", worker)
        combined = trigger + worker + complete + sender + copier
        self.assertNotIn("get_db_connection", combined)
        self.assertNotIn("openOutputStream", combined)
        self.assertNotIn("ACTION_CREATE_DOCUMENT", combined)
        self.assertIn("Intent.ACTION_SEND", sender)
        self.assertIn("Intent.EXTRA_TEXT", sender)
        self.assertIn("Intent.createChooser", sender)
        self.assertIn("ClipData.newPlainText", copier)
        self.assertIn("clipboard.setPrimaryClip", copier)
        self.assertIn("PARTAGER LE RAPPORT", self.methods["show_stock"])
        self.assertIn("COPIER LE RAPPORT", self.methods["show_stock"])



class Faneva14916RegressionTests(unittest.TestCase):
    """N1-N16 — v1.4.9.16 (bases temporaires, aucun impact historique)."""

    def _open_hybrid_db(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("faneva_hybrid", HYBRID_PATH)
        hybrid = importlib.util.module_from_spec(spec)
        # Minimal stubs sometimes needed
        spec.loader.exec_module(hybrid)
        fd, path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        conn = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        hybrid.run_migration(conn)
        hybrid.ensure_mvt_table(conn)
        # Device identity required by record_*
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO config_hybrid (cle, valeur) VALUES ('device_id', ?)",
                    ("f0000000-0000-4000-8000-000000000099",))
        # Store + product
        cur.execute("INSERT OR IGNORE INTO magasins (id, cle, nom) VALUES (1, 'testshop', 'Test Shop')")
        pid = "prod-canon-001"
        cur.execute("INSERT OR IGNORE INTO catalogues (id, nom, prix_achat_moyen, prix_vente) VALUES (?,?,?,?)",
                    (pid, "Produit Test", 100, 200))
        cur.execute("INSERT OR REPLACE INTO stocks_magasin (produit_id, magasin_id, stock) VALUES (?,?,?)",
                    (pid, 1, 0))
        conn.commit()
        return hybrid, conn, path, pid

    def _seed_stock(self, hybrid, conn, pid, qty, magasin_id=1):
        # Direct movement seed (not bootstrap) so tests control exact stock
        hybrid._apply_stock_movement(conn, pid, magasin_id, int(qty), "ACHAT",
                                     transaction_id=hybrid.gen_uuid())
        conn.commit()
        self.assertEqual(hybrid.get_stock(conn, pid, magasin_id), int(qty))

    def _line(self, pid, q=1, pa=100, pv=200):
        return {"produit_id": pid, "nom": "Produit Test", "pa": pa, "pv": pv,
                "q": q, "total": pv * q, "ben": (pv - pa) * q}

    # ---- N1 ----
    def test_n1_stock_never_negative_simple(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 5)
            hybrid.record_sale(conn, [self._line(pid, 5)], 1, sale_nonce="n1-a")
            self.assertEqual(hybrid.get_stock(conn, pid, 1), 0)
            with self.assertRaises(ValueError):
                hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="n1-b")
            self.assertEqual(hybrid.get_stock(conn, pid, 1), 0)
        finally:
            conn.close(); os.unlink(path)

    # ---- N2 ----
    def test_n2_recheck_under_lock_sequential_last_unit(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 1)
            hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="n2-a")
            with self.assertRaises(ValueError):
                hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="n2-b")
            self.assertEqual(hybrid.get_stock(conn, pid, 1), 0)
            n = conn.execute("SELECT COUNT(*) FROM mouvements WHERE source='VENTE'").fetchone()[0]
            self.assertEqual(n, 1)
        finally:
            conn.close(); os.unlink(path)

    # ---- N3 ----
    def test_n3_concurrency_two_connections_last_unit(self):
        import threading
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 1)
            conn.close()
            results = []
            errors = []

            def worker(nonce, tx):
                c = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
                c.execute("PRAGMA busy_timeout=5000")
                try:
                    hybrid.record_sale(c, [self._line(pid, 1)], 1,
                                       transaction_id=tx, sale_nonce=nonce)
                    results.append(tx)
                except Exception as e:
                    errors.append(str(e))
                finally:
                    c.close()

            t1 = threading.Thread(target=worker, args=("n3-a", hybrid.gen_uuid()))
            t2 = threading.Thread(target=worker, args=("n3-b", hybrid.gen_uuid()))
            t1.start(); t2.start(); t1.join(); t2.join()
            c = sqlite3.connect(path)
            stock = hybrid.get_stock(c, pid, 1)
            n_mvt = c.execute("SELECT COUNT(*) FROM mouvements WHERE source='VENTE'").fetchone()[0]
            c.close()
            self.assertEqual(stock, 0)
            self.assertEqual(n_mvt, 1)
            self.assertEqual(len(results), 1)
            self.assertTrue(any("insuffisant" in e.lower() or "stock" in e.lower() for e in errors) or len(errors) >= 1)
        finally:
            try:
                os.unlink(path)
            except Exception:
                pass

    # ---- N4 ----
    def test_n4_retry_same_transaction_id_noop(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 3)
            tx = hybrid.gen_uuid()
            r1 = hybrid.record_sale(conn, [self._line(pid, 1)], 1, transaction_id=tx, sale_nonce="n4")
            stock1 = hybrid.get_stock(conn, pid, 1)
            r2 = hybrid.record_sale(conn, [self._line(pid, 1)], 1, transaction_id=tx, sale_nonce="n4")
            stock2 = hybrid.get_stock(conn, pid, 1)
            self.assertEqual(r1, r2)
            self.assertEqual(stock1, stock2)
            self.assertEqual(stock1, 2)
            n = conn.execute("SELECT COUNT(*) FROM mouvements WHERE source='VENTE'").fetchone()[0]
            self.assertEqual(n, 1)
        finally:
            conn.close(); os.unlink(path)

    # ---- N5 ----
    def test_n5_fingerprint_blocks_reinjection_new_tx_same_nonce(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 5)
            t1 = hybrid.gen_uuid()
            r1 = hybrid.record_sale(conn, [self._line(pid, 2)], 1, transaction_id=t1, sale_nonce="nonce-N5")
            stock1 = hybrid.get_stock(conn, pid, 1)
            t2 = hybrid.gen_uuid()
            r2 = hybrid.record_sale(conn, [self._line(pid, 2)], 1, transaction_id=t2, sale_nonce="nonce-N5")
            stock2 = hybrid.get_stock(conn, pid, 1)
            self.assertEqual(r1, t1)
            self.assertEqual(r2, r1)  # returns original
            self.assertEqual(stock1, stock2)
            self.assertEqual(stock1, 3)
            n = conn.execute("SELECT COUNT(*) FROM mouvements WHERE source='VENTE'").fetchone()[0]
            self.assertEqual(n, 1)
        finally:
            conn.close(); os.unlink(path)

    # ---- N6 ----
    def test_n6_distinct_nonces_allow_two_sales(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 5)
            r1 = hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="nonce-A")
            r2 = hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="nonce-B")
            self.assertNotEqual(r1, r2)
            self.assertEqual(hybrid.get_stock(conn, pid, 1), 3)
            n = conn.execute("SELECT COUNT(*) FROM mouvements WHERE source='VENTE'").fetchone()[0]
            self.assertEqual(n, 2)
        finally:
            conn.close(); os.unlink(path)

    # ---- N7 ----
    def test_n7_bootstrap_seed_not_reinjected(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            # First bootstrap seed
            hybrid._apply_stock_movement(conn, pid, 1, 10, "MIGRATION_INITIAL_STOCK",
                                         transaction_id="boot-1")
            conn.commit()
            s1 = hybrid.get_stock(conn, pid, 1)
            # Re-apply same logical seed (different tx id) — unique seed_key must block
            hybrid._apply_stock_movement(conn, pid, 1, 10, "MIGRATION_INITIAL_STOCK",
                                         transaction_id="boot-2")
            conn.commit()
            s2 = hybrid.get_stock(conn, pid, 1)
            # seed_key unique: second insert ignored => stock unchanged
            self.assertEqual(s1, s2)
            self.assertEqual(s1, 10)
        finally:
            conn.close(); os.unlink(path)

    # ---- N8 ----
    def test_n8_sync_normal_does_not_double_bootstrap(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            hybrid._apply_stock_movement(conn, pid, 1, 7, "MIGRATION_INITIAL_STOCK",
                                         transaction_id="seed-local")
            conn.commit()
            s1 = hybrid.get_stock(conn, pid, 1)
            remote_tx = {
                "transaction_id": "seed-local",
                "type_op": "MIGRATION_INITIAL_STOCK",
                "device_id": "remote-device",
                "admin_id": None, "admin_username": None,
                "magasin_id": 1, "horodatage": hybrid.now_iso(),
                "payload": {"produit_id": pid, "quantite": 7, "prix_achat": 100},
            }
            hybrid.apply_remote_transactions(conn, [remote_tx], "f0000000-0000-4000-8000-000000000099")
            s2 = hybrid.get_stock(conn, pid, 1)
            self.assertEqual(s1, s2)
        finally:
            conn.close(); os.unlink(path)

    # ---- N9 ----
    def test_n9_cart_cleared_on_store_change(self):
        body = None
        with open(SOURCE_PATH, encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "choisir_magasin":
                body = ast.get_source_segment(source, node)
                break
        self.assertIsNotNone(body)
        self.assertIn("v_cart", body)
        self.assertIn("[]", body)

    # ---- N10 ----
    def test_n10_add_cart_does_not_debit_stock(self):
        body = None
        with open(SOURCE_PATH, encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "add_cart":
                body = ast.get_source_segment(source, node)
                break
        self.assertIsNotNone(body)
        self.assertNotIn("record_sale", body)
        self.assertNotIn("_apply_stock_movement", body)
        self.assertIn("v_cart.append", body)

    # ---- N11 ----
    def test_n11_credit_sale_atomic_success(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 4)
            # Ensure dettes table exists (legacy)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dettes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client TEXT, telephone TEXT, total INTEGER, paye INTEGER, reste INTEGER,
                    statut TEXT, transaction_id TEXT, device_id TEXT, magasin_ref_id INTEGER
                )
            """)
            conn.commit()
            res = hybrid.record_credit_sale(
                conn, [self._line(pid, 2)], 1, "Client X", "0320000000", 400,
                sale_nonce="credit-n11")
            self.assertIn("sale_transaction_id", res)
            self.assertEqual(hybrid.get_stock(conn, pid, 1), 2)
            n_debt = conn.execute("SELECT COUNT(*) FROM dettes WHERE client='Client X'").fetchone()[0]
            self.assertEqual(n_debt, 1)
            n_tx = conn.execute("SELECT COUNT(*) FROM transactions WHERE type_op='VENTE_CREDIT'").fetchone()[0]
            self.assertEqual(n_tx, 1)
        finally:
            conn.close(); os.unlink(path)

    # ---- N12 ----
    def test_n12_credit_sale_rollback_on_error(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 3)
            # Force failure: dettes table missing AND we monkeypatch record_debt to raise after sale
            original = hybrid.record_debt
            def boom(*a, **k):
                raise RuntimeError("forced debt failure")
            hybrid.record_debt = boom
            try:
                with self.assertRaises(RuntimeError):
                    hybrid.record_credit_sale(
                        conn, [self._line(pid, 1)], 1, "Client Y", "", 200,
                        sale_nonce="credit-n12")
            finally:
                hybrid.record_debt = original
            # Stock restored
            self.assertEqual(hybrid.get_stock(conn, pid, 1), 3)
            n_tx = conn.execute("SELECT COUNT(*) FROM transactions WHERE type_op IN ('VENTE','VENTE_CREDIT')").fetchone()[0]
            self.assertEqual(n_tx, 0)
        finally:
            conn.close(); os.unlink(path)

    # ---- N13 ----
    def test_n13_payment_operation_id_anti_double(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dettes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client TEXT, telephone TEXT, total INTEGER, paye INTEGER, reste INTEGER,
                    statut TEXT, transaction_id TEXT, device_id TEXT, magasin_ref_id INTEGER
                )
            """)
            cur = conn.cursor()
            cur.execute("INSERT INTO dettes (client, telephone, total, paye, reste, statut, transaction_id) VALUES (?,?,?,?,?,?,?)",
                        ("Z", "", 1000, 0, 1000, "ACTIF", "debt-tx-1"))
            dette_id = cur.lastrowid
            conn.commit()
            op = "op-pay-n13"
            t1 = hybrid.record_payment(conn, dette_id, 100, magasin_id=1, operation_id=op)
            t2 = hybrid.record_payment(conn, dette_id, 100, magasin_id=1, operation_id=op,
                                       transaction_id=hybrid.gen_uuid())
            self.assertEqual(t1, t2)
            n = conn.execute("SELECT COUNT(*) FROM transactions WHERE type_op='PAIEMENT'").fetchone()[0]
            self.assertEqual(n, 1)
        finally:
            conn.close(); os.unlink(path)

    # ---- N14 ----
    def test_n14_payment_increments_cash_once(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dettes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client TEXT, telephone TEXT, total INTEGER, paye INTEGER, reste INTEGER,
                    statut TEXT, transaction_id TEXT, device_id TEXT, magasin_ref_id INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paiements_dettes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dette_id INTEGER, montant INTEGER, date_paiement TEXT DEFAULT CURRENT_TIMESTAMP,
                    transaction_id TEXT, device_id TEXT, operation_id TEXT
                )
            """)
            cur = conn.cursor()
            cur.execute("INSERT INTO dettes (client, total, paye, reste, statut) VALUES (?,?,?,?,?)",
                        ("W", 500, 0, 500, "ACTIF"))
            dette_id = cur.lastrowid
            conn.commit()
            op = "op-pay-n14"
            hybrid.record_payment(conn, dette_id, 200, magasin_id=1, operation_id=op)
            hybrid.record_payment(conn, dette_id, 200, magasin_id=1, operation_id=op)
            # Simulate single ledger write
            cur.execute("SELECT transaction_id FROM transactions WHERE type_op='PAIEMENT' AND operation_id=?", (op,))
            tx = cur.fetchone()[0]
            cur.execute("INSERT OR IGNORE INTO paiements_dettes (transaction_id, dette_id, montant, operation_id) VALUES (?,?,?,?)",
                        (tx, dette_id, 200, op))
            conn.commit()
            total = conn.execute("SELECT COALESCE(SUM(montant),0) FROM paiements_dettes WHERE operation_id=?", (op,)).fetchone()[0]
            self.assertEqual(total, 200)
            rep = hybrid.get_daily_cash_report(conn)
            self.assertEqual(rep["recouvrements_total"], 200)
            self.assertEqual(rep["caisse_reelle"], 200)
        finally:
            conn.close(); os.unlink(path)

    # ---- N15 ----
    def test_n15_report_separates_cash_credit_recoveries(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 10)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ventes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT, device_id TEXT, magasin_ref_id INTEGER,
                    produit_id TEXT, produit_nom TEXT, quantite INTEGER,
                    prix_achat INTEGER, prix_vente INTEGER, total INTEGER, benefice INTEGER,
                    vendeur TEXT, date_vente TEXT DEFAULT CURRENT_TIMESTAMP, sale_mode TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dettes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client TEXT, telephone TEXT, total INTEGER, paye INTEGER, reste INTEGER,
                    statut TEXT, transaction_id TEXT, device_id TEXT, magasin_ref_id INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paiements_dettes(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dette_id INTEGER, montant INTEGER, date_paiement TEXT DEFAULT CURRENT_TIMESTAMP,
                    transaction_id TEXT, operation_id TEXT
                )
            """)
            # Cash sale
            t_cash = hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="n15-cash")
            conn.execute("INSERT INTO ventes (transaction_id, produit_id, produit_nom, quantite, total, benefice, sale_mode, date_vente) VALUES (?,?,?,?,?,?,?,datetime('now','localtime'))",
                         (t_cash, pid, "Produit Test", 1, 200, 100, "COMPTANT"))
            # Credit sale
            hybrid.record_credit_sale(conn, [self._line(pid, 1)], 1, "C15", "", 200, sale_nonce="n15-credit")
            conn.execute("INSERT INTO ventes (transaction_id, produit_id, produit_nom, quantite, total, benefice, sale_mode, date_vente) VALUES (?,?,?,?,?,?,?,datetime('now','localtime'))",
                         (conn.execute("SELECT transaction_id FROM transactions WHERE type_op='VENTE_CREDIT'").fetchone()[0],
                          pid, "Produit Test", 1, 200, 100, "CREDIT"))
            # Recovery
            dette_id = conn.execute("SELECT id FROM dettes LIMIT 1").fetchone()[0]
            hybrid.record_payment(conn, dette_id, 50, magasin_id=1, operation_id="n15-pay")
            conn.execute("INSERT INTO paiements_dettes (dette_id, montant, operation_id, date_paiement) VALUES (?,?,?,datetime('now','localtime'))",
                         (dette_id, 50, "n15-pay"))
            conn.commit()
            rep = hybrid.get_daily_cash_report(conn)
            self.assertEqual(rep["comptant_total"], 200)
            self.assertEqual(rep["credit_total"], 200)
            self.assertEqual(rep["recouvrements_total"], 50)
            self.assertEqual(rep["caisse_reelle"], 250)
        finally:
            conn.close(); os.unlink(path)

    # ---- N16 ----
    def test_n16_report_keeps_transaction_references(self):
        hybrid, conn, path, pid = self._open_hybrid_db()
        try:
            self._seed_stock(hybrid, conn, pid, 2)
            tx = hybrid.record_sale(conn, [self._line(pid, 1)], 1, sale_nonce="n16-nonce")
            rep = hybrid.get_daily_cash_report(conn)
            ids = [r["transaction_id"] for r in rep["transaction_ids"]]
            self.assertIn(tx, ids)
            row = next(r for r in rep["transaction_ids"] if r["transaction_id"] == tx)
            self.assertEqual(row["sale_nonce"], "n16-nonce")
            self.assertTrue(row["business_fingerprint"])
        finally:
            conn.close(); os.unlink(path)



if __name__ == "__main__":
    unittest.main(verbosity=2)
