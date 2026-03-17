"""Tests para traer_fact CLI."""
import pytest
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO


# ── parse_args ──────────────────────────────────────────────────────────────


class TestParseArgsQuery:
    """Tests para el subcomando 'query'."""

    def test_query_list(self):
        from traer_fact.cli import parse_args

        with patch("sys.argv", ["traer_fact", "query", "--list"]):
            args = parse_args()
        assert args.comando == "query"
        assert args.list is True
        assert args.editar is None

    def test_query_editar_sin_argumento(self):
        from traer_fact.cli import parse_args

        with patch("sys.argv", ["traer_fact", "query", "--editar"]):
            args = parse_args()
        assert args.comando == "query"
        assert args.editar == ""
        assert args.list is False

    def test_query_editar_con_editor(self):
        from traer_fact.cli import parse_args

        with patch("sys.argv", ["traer_fact", "query", "--editar", "nvim"]):
            args = parse_args()
        assert args.comando == "query"
        assert args.editar == "nvim"
        assert args.list is False

    def test_query_sin_flag_falla(self):
        from traer_fact.cli import parse_args

        with patch("sys.argv", ["traer_fact", "query"]):
            with pytest.raises(SystemExit):
                parse_args()

    def test_query_list_y_editar_mutuamente_exclusivos(self):
        from traer_fact.cli import parse_args

        with patch("sys.argv", ["traer_fact", "query", "--list", "--editar"]):
            with pytest.raises(SystemExit):
                parse_args()


class TestParseArgsTraer:
    """Tests para el subcomando 'traer'."""

    def test_traer_con_args_requeridos(self):
        from traer_fact.cli import parse_args

        with patch(
            "sys.argv",
            [
                "traer_fact",
                "traer",
                "--ruta-credenciales-data-fact",
                ".env",
                "--id-establecimientos-a-buscar",
                "123",
                "456",
            ],
        ):
            args = parse_args()
        assert args.comando == "traer"
        assert args.ruta_credenciales_data_fact == ".env"
        assert args.id_establecimientos_a_buscar == ["123", "456"]
        assert args.verbose is False

    def test_traer_verbose(self):
        from traer_fact.cli import parse_args

        with patch(
            "sys.argv",
            [
                "traer_fact",
                "traer",
                "--verbose",
                "--ruta-credenciales-data-fact",
                ".env",
                "--id-establecimientos-a-buscar",
                "123",
            ],
        ):
            args = parse_args()
        assert args.verbose is True

    def test_traer_sin_credenciales_falla(self):
        from traer_fact.cli import parse_args

        with patch(
            "sys.argv",
            ["traer_fact", "traer", "--id-establecimientos-a-buscar", "123"],
        ):
            with pytest.raises(SystemExit):
                parse_args()

    def test_traer_sin_ids_falla(self):
        from traer_fact.cli import parse_args

        with patch(
            "sys.argv",
            ["traer_fact", "traer", "--ruta-credenciales-data-fact", ".env"],
        ):
            with pytest.raises(SystemExit):
                parse_args()


class TestSinSubcomando:
    """Tests cuando no se pasa subcomando."""

    def test_sin_subcomando_sale_con_error(self):
        from traer_fact.cli import main

        with patch("sys.argv", ["traer_fact"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


# ── cmd_query --list ────────────────────────────────────────────────────────


class TestCmdQueryList:
    """Tests para 'traer_fact query --list'."""

    def test_list_muestra_contenido_sql(self, capsys):
        from traer_fact.cli import cmd_query
        from traer_fact.traer_datos import resolver_rutas

        rutas = resolver_rutas()
        ruta_sql = rutas["RUTA_sql_executables"]

        # Verificar que el archivo existe
        assert ruta_sql.exists(), f"No existe {ruta_sql}"

        # Simular args
        args = MagicMock()
        args.list = True
        args.editar = False

        cmd_query(args)
        # Si llega aqui sin error, el comando funciono

    def test_list_con_sql_temporal(self, tmp_path):
        from traer_fact.cli import cmd_query

        sql_content = "SELECT * FROM tabla WHERE id IN ({rucs_a_buscar});"
        sql_file = tmp_path / "consulta_formato.sql"
        sql_file.write_text(sql_content)

        args = MagicMock()
        args.list = True
        args.editar = False

        with patch("traer_fact.cli.resolver_rutas") as mock_rutas:
            mock_rutas.return_value = {
                "RUTA_sql_executables": sql_file,
                "RUTA_backups": tmp_path / "backups",
            }
            cmd_query(args)


class TestCmdQueryEditar:
    """Tests para 'traer_fact query --editar'."""

    def test_editar_usa_editor_del_env(self, tmp_path):
        from traer_fact.cli import cmd_query

        sql_file = tmp_path / "consulta_formato.sql"
        sql_file.write_text("SELECT 1;")

        args = MagicMock()
        args.list = False
        args.editar = ""

        with patch("traer_fact.cli.resolver_rutas") as mock_rutas, \
             patch.dict(os.environ, {"EDITOR": "vim"}), \
             patch("os.execlp") as mock_exec:
            mock_rutas.return_value = {
                "RUTA_sql_executables": sql_file,
                "RUTA_backups": tmp_path / "backups",
            }
            cmd_query(args)
            mock_exec.assert_called_once_with("vim", "vim", str(sql_file))

    def test_editar_usa_nano_por_defecto(self, tmp_path):
        from traer_fact.cli import cmd_query

        sql_file = tmp_path / "consulta_formato.sql"
        sql_file.write_text("SELECT 1;")

        args = MagicMock()
        args.list = False
        args.editar = ""

        env_sin_editor = os.environ.copy()
        env_sin_editor.pop("EDITOR", None)

        with patch("traer_fact.cli.resolver_rutas") as mock_rutas, \
             patch.dict(os.environ, env_sin_editor, clear=True), \
             patch("os.execlp") as mock_exec:
            mock_rutas.return_value = {
                "RUTA_sql_executables": sql_file,
                "RUTA_backups": tmp_path / "backups",
            }
            cmd_query(args)
            mock_exec.assert_called_once_with("nano", "nano", str(sql_file))

    def test_editar_con_editor_explicito(self, tmp_path):
        from traer_fact.cli import cmd_query

        sql_file = tmp_path / "consulta_formato.sql"
        sql_file.write_text("SELECT 1;")

        args = MagicMock()
        args.list = False
        args.editar = "nvim"

        with patch("traer_fact.cli.resolver_rutas") as mock_rutas, \
             patch("os.execlp") as mock_exec:
            mock_rutas.return_value = {
                "RUTA_sql_executables": sql_file,
                "RUTA_backups": tmp_path / "backups",
            }
            cmd_query(args)
            mock_exec.assert_called_once_with("nvim", "nvim", str(sql_file))


# ── cmd_traer ───────────────────────────────────────────────────────────────


class TestCmdTraer:
    """Tests para 'traer_fact traer'."""

    def test_traer_con_ids_directos(self):
        from traer_fact.cli import cmd_traer

        args = MagicMock()
        args.id_establecimientos_a_buscar = ["111", "222", "333"]
        args.ruta_credenciales_data_fact = "/tmp/fake.env"
        args.verbose = False

        with patch("traer_fact.cli.leer_y_guardar_datos_mysql") as mock_traer:
            cmd_traer(args)
            mock_traer.assert_called_once()
            call_kwargs = mock_traer.call_args
            assert call_kwargs.kwargs["id_establecimientos_a_buscar"] == [
                "111",
                "222",
                "333",
            ]

    def test_traer_con_psv(self, tmp_path):
        import polars as pl
        from traer_fact.cli import cmd_traer

        psv_file = tmp_path / "ids.psv"
        df = pl.DataFrame({"id_establecimiento": [100, 200, 300]})
        df.write_csv(psv_file, separator="|")

        args = MagicMock()
        args.id_establecimientos_a_buscar = [str(psv_file)]
        args.ruta_credenciales_data_fact = "/tmp/fake.env"
        args.verbose = False

        with patch("traer_fact.cli.leer_y_guardar_datos_mysql") as mock_traer:
            cmd_traer(args)
            mock_traer.assert_called_once()
            ids = mock_traer.call_args.kwargs["id_establecimientos_a_buscar"]
            assert len(ids) == 3
            assert "100" in ids
            assert "200" in ids
            assert "300" in ids

    def test_traer_con_psv_inexistente_sale(self):
        from traer_fact.cli import cmd_traer

        args = MagicMock()
        args.id_establecimientos_a_buscar = ["/tmp/no_existe_xyz.psv"]
        args.ruta_credenciales_data_fact = "/tmp/fake.env"
        args.verbose = False

        with pytest.raises(SystemExit):
            cmd_traer(args)


# ── resolver_rutas ──────────────────────────────────────────────────────────


class TestResolverRutas:
    """Tests para resolver_rutas."""

    def test_devuelve_rutas_esperadas(self):
        from traer_fact.traer_datos import resolver_rutas

        rutas = resolver_rutas()
        assert "RUTA_sql_executables" in rutas
        assert "RUTA_backups" in rutas
        assert rutas["RUTA_sql_executables"].name == "consulta_formato.sql"
        assert rutas["RUTA_backups"].name == "backups"

    def test_sql_existe(self):
        from traer_fact.traer_datos import resolver_rutas

        rutas = resolver_rutas()
        assert rutas["RUTA_sql_executables"].exists()


# ── consulta_formato.sql ────────────────────────────────────────────────────


class TestConsultaFormato:
    """Tests sobre el contenido del SQL por defecto."""

    def test_sql_tiene_placeholder(self):
        from traer_fact.traer_datos import resolver_rutas

        rutas = resolver_rutas()
        contenido = rutas["RUTA_sql_executables"].read_text()
        assert "{rucs_a_buscar}" in contenido

    def test_sql_tiene_12_queries(self):
        from traer_fact.traer_datos import resolver_rutas
        import sqlparse

        rutas = resolver_rutas()
        contenido = rutas["RUTA_sql_executables"].read_text()
        contenido = sqlparse.format(contenido, strip_comments=True)
        queries = [q.strip() for q in contenido.split(";") if q.strip()]
        assert len(queries) == 12

    def test_sql_queries_tienen_fecha_valida(self):
        from traer_fact.traer_datos import resolver_rutas
        import sqlparse, re

        rutas = resolver_rutas()
        contenido = rutas["RUTA_sql_executables"].read_text()
        contenido = sqlparse.format(contenido, strip_comments=True)
        queries = [q.strip() for q in contenido.split(";") if q.strip()]
        for q in queries:
            match = re.search(r"[0-9]{4}_[0-9]{2}", q)
            assert match is not None, f"Query sin fecha YYYY_MM: {q[:60]}..."
