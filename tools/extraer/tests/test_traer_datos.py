"""Tests para traer_datos — conexión única y ejecución secuencial."""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock, call
from extraer.traer_datos import (
    _es_query_datos,
    _crear_conexion,
    procesar_query,
    leer_y_guardar_datos_mysql,
)


# ── _es_query_datos ────────────────────────────────────────────────────────


class TestEsQueryDatos:
    def test_select_simple(self):
        assert _es_query_datos("SELECT * FROM tabla") is True

    def test_select_minusculas(self):
        assert _es_query_datos("select * from tabla") is True

    def test_select_con_espacios(self):
        assert _es_query_datos("  SELECT col FROM t") is True

    def test_select_con_salto_linea(self):
        assert _es_query_datos("\nSELECT col FROM t") is True

    def test_create_temporary_table(self):
        assert _es_query_datos("CREATE TEMPORARY TABLE temp AS SELECT 1") is False

    def test_create_table(self):
        assert _es_query_datos("CREATE TABLE t (id INT)") is False

    def test_insert(self):
        assert _es_query_datos("INSERT INTO t VALUES (1)") is False

    def test_drop(self):
        assert _es_query_datos("DROP TABLE IF EXISTS temp") is False


# ── _crear_conexion ────────────────────────────────────────────────────────


class TestCrearConexion:
    def test_crea_conexion_con_variables_correctas(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "USER_DATABASE=usr\n"
            "PASSWORD_DATABASE=pwd\n"
            "HOST_DATABASE=localhost\n"
            "NAME_DATABASE=mydb\n"
            "PORT_DATABASE=3306\n"
        )
        with patch("extraer.traer_datos.mysql.connector.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            conn = _crear_conexion(env_file)
            mock_connect.assert_called_once_with(
                host="localhost",
                user="usr",
                password="pwd",
                database="mydb",
                port="3306",
            )

    def test_falla_si_falta_variable(self, tmp_path, monkeypatch):
        for var in ("USER_DATABASE", "PASSWORD_DATABASE", "HOST_DATABASE", "NAME_DATABASE", "PORT_DATABASE"):
            monkeypatch.delenv(var, raising=False)
        env_file = tmp_path / ".env"
        env_file.write_text(
            "USER_DATABASE=usr\n"
            "HOST_DATABASE=localhost\n"
        )
        with patch("extraer.traer_datos.mysql.connector.connect"):
            with pytest.raises(ValueError, match="variables de entorno"):
                _crear_conexion(env_file)


# ── procesar_query ─────────────────────────────────────────────────────────


class TestProcesarQuery:
    def test_usa_la_conexion_recibida(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos.guardar_resultados") as mock_guardar:
            procesar_query(
                conexion=mock_conn,
                query="SELECT 1",
                RUTA_backups=tmp_path,
                prefijo_salida="test",
                regex_sufijo="",
                indice=0,
            )
            mock_guardar.assert_called_once()
            assert mock_guardar.call_args.kwargs["cur"] == mock_cursor

    def test_sufijo_con_regex(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos.guardar_resultados") as mock_guardar:
            procesar_query(
                conexion=mock_conn,
                query="SELECT * FROM facturas_2025_03",
                RUTA_backups=tmp_path,
                prefijo_salida="fact",
                regex_sufijo=r"\d{4}_\d{2}",
                indice=0,
            )
            nombre_usado = mock_guardar.call_args.kwargs["nombre"]
            assert nombre_usado == "fact_2025_03"

    def test_sufijo_sin_regex_usa_indice(self, tmp_path):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos.guardar_resultados") as mock_guardar:
            procesar_query(
                conexion=mock_conn,
                query="SELECT 1",
                RUTA_backups=tmp_path,
                prefijo_salida="res",
                regex_sufijo="",
                indice=5,
            )
            nombre_usado = mock_guardar.call_args.kwargs["nombre"]
            assert nombre_usado == "res_5"


# ── leer_y_guardar_datos_mysql (integración) ──────────────────────────────


class TestLeerYGuardarConexionUnica:
    """Verifica que se crea UNA sola conexión y se reutiliza en todas las queries."""

    def _make_env(self, tmp_path):
        env = tmp_path / ".env"
        env.write_text(
            "USER_DATABASE=u\n"
            "PASSWORD_DATABASE=p\n"
            "HOST_DATABASE=h\n"
            "NAME_DATABASE=d\n"
            "PORT_DATABASE=3306\n"
        )
        return env

    def test_una_sola_conexion_para_multiples_queries(self, tmp_path):
        """Se llama a _crear_conexion exactamente 1 vez, sin importar cuántas queries haya."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1; SELECT 2; SELECT 3;")
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos._crear_conexion", return_value=mock_conn) as mock_crear, \
             patch("extraer.traer_datos.guardar_resultados"):
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["1"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )
            mock_crear.assert_called_once()

    def test_ddl_ejecutado_antes_de_selects(self, tmp_path):
        """CREATE TEMPORARY TABLE se ejecuta directo; SELECTs pasan por guardar_resultados."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            "CREATE TEMPORARY TABLE tmp AS SELECT 1;\n"
            "SELECT * FROM tmp;\n"
            "SELECT * FROM tmp;"
        )
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos._crear_conexion", return_value=mock_conn), \
             patch("extraer.traer_datos.guardar_resultados") as mock_guardar:
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["1"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )
            # DDL ejecutada directamente via cursor
            mock_cursor.execute.assert_called_once()
            ddl_call = mock_cursor.execute.call_args[0][0]
            assert ddl_call.startswith("CREATE TEMPORARY TABLE")

            # SELECTs pasaron por guardar_resultados
            assert mock_guardar.call_count == 2

    def test_conexion_se_cierra_al_final(self, tmp_path):
        """La conexión se cierra (via closing) al terminar."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;")
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos._crear_conexion", return_value=mock_conn), \
             patch("extraer.traer_datos.guardar_resultados"):
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["1"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )
        mock_conn.close.assert_called_once()

    def test_misma_conexion_en_ddl_y_select(self, tmp_path):
        """DDL y SELECT usan el mismo objeto conexión."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            "CREATE TEMPORARY TABLE tmp AS SELECT 1;\n"
            "SELECT * FROM tmp;"
        )
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        conexiones_usadas = []

        original_procesar = procesar_query

        def spy_procesar(conexion, **kwargs):
            conexiones_usadas.append(id(conexion))
            return original_procesar(conexion=conexion, **kwargs)

        with patch("extraer.traer_datos._crear_conexion", return_value=mock_conn), \
             patch("extraer.traer_datos.procesar_query", side_effect=spy_procesar), \
             patch("extraer.traer_datos.guardar_resultados"):
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["1"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )

        # El cursor de DDL y la conexión del SELECT son el mismo objeto
        assert all(c == id(mock_conn) for c in conexiones_usadas)

    def test_placeholder_se_reemplaza(self, tmp_path):
        """El placeholder {rucs_a_buscar} se reemplaza con los valores."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT * FROM t WHERE id IN ({rucs_a_buscar});")
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos._crear_conexion", return_value=mock_conn), \
             patch("extraer.traer_datos.guardar_resultados") as mock_guardar:
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["100", "200", "300"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )
            query_ejecutada = mock_guardar.call_args.kwargs["query"]
            assert "100,200,300" in query_ejecutada
            assert "{rucs_a_buscar}" not in query_ejecutada

    def test_no_hay_process_pool_executor(self):
        """Verificar que ProcessPoolExecutor ya no se importa ni usa."""
        import inspect
        from extraer import traer_datos

        source = inspect.getsource(traer_datos)
        assert "ProcessPoolExecutor" not in source

    def test_archivos_previos_se_limpian(self, tmp_path):
        """Los archivos con el prefijo de salida se eliminan antes de empezar."""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;")
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"
        backups.mkdir()
        (backups / "resultado_viejo.parquet").write_text("basura")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("extraer.traer_datos._crear_conexion", return_value=mock_conn), \
             patch("extraer.traer_datos.guardar_resultados"):
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["1"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )
        assert not (backups / "resultado_viejo.parquet").exists()

    def test_credenciales_inexistentes_lanza_error(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("SELECT 1;")
        backups = tmp_path / "backups"

        with pytest.raises(FileNotFoundError):
            leer_y_guardar_datos_mysql(
                ruta_credenciales=tmp_path / "no_existe.env",
                valores_busqueda=["1"],
                ruta_sql=sql_file,
                ruta_backups=backups,
            )

    def test_sql_inexistente_lanza_error(self, tmp_path):
        env = self._make_env(tmp_path)
        backups = tmp_path / "backups"

        with pytest.raises(FileNotFoundError):
            leer_y_guardar_datos_mysql(
                ruta_credenciales=env,
                valores_busqueda=["1"],
                ruta_sql=tmp_path / "no_existe.sql",
                ruta_backups=backups,
            )
