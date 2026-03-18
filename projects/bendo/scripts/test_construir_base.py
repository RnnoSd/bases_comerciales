"""
Tests para verificar la integridad de base_procesada y base_filtrada.
"""

import polars as pl
import pytest
from pathlib import Path
from construir_base import (
    calcular_mejor_promedio,
    calcular_precision_establecimiento,
    VARS_FACTURACION,
)

BASES_DIR = Path(__file__).resolve().parent.parent / "bases"


@pytest.fixture(scope="module")
def base_consolidada():
    return pl.read_parquet(BASES_DIR / "base_consolidada.parquet")


@pytest.fixture(scope="module")
def base_procesada():
    return pl.read_parquet(BASES_DIR / "base_procesada.parquet")


@pytest.fixture(scope="module")
def base_filtrada():
    return pl.read_parquet(BASES_DIR / "base_filtrada.parquet")


@pytest.fixture(scope="module")
def actividades():
    return pl.read_csv(BASES_DIR / "actividades_economicas.csv")


# --- Tests de base_procesada ---

class TestBaseProcesada:
    def test_columnas_esperadas(self, base_procesada):
        expected = {
            "id_establecimiento", "numero_ruc", "numero_establecimiento",
            "razon_social", "nombre_fantasia_comercial", "clase_contribuyente",
            "tipo_contribuyente", "actividad_economica",
            "ingreso_reportado", "ingreso_estimado", "precision", "num_periodos",
        }
        assert expected == set(base_procesada.columns)

    def test_sin_duplicados_id_establecimiento(self, base_procesada):
        n_unique = base_procesada["id_establecimiento"].n_unique()
        assert n_unique == base_procesada.height

    def test_precision_valores_validos(self, base_procesada):
        valores = set(base_procesada["precision"].unique().to_list())
        assert valores.issubset({0, 1, 2})

    def test_ingreso_reportado_no_negativo(self, base_procesada):
        negativos = base_procesada.filter(pl.col("ingreso_reportado") < 0).height
        assert negativos == 0, f"Hay {negativos} registros con ingreso_reportado negativo"

    def test_ingreso_estimado_no_negativo(self, base_procesada):
        negativos = base_procesada.filter(pl.col("ingreso_estimado") < 0).height
        assert negativos == 0, f"Hay {negativos} registros con ingreso_estimado negativo"

    def test_consistencia_ingresos_con_consolidada(self, base_consolidada, base_procesada):
        """Verificar que ingreso_reportado == sum(total_facturas) para una muestra."""
        sample_ids = base_procesada.head(100)["id_establecimiento"].to_list()
        df_fact = base_consolidada.filter(
            pl.col("id_establecimiento").is_in(sample_ids)
        )
        sumas = df_fact.group_by("id_establecimiento").agg(
            pl.col("total_facturas").sum().alias("check_reportado"),
            (pl.col("ticket_promedio") * pl.col("numero_facturas")).sum().alias("check_estimado"),
        )
        merged = base_procesada.filter(
            pl.col("id_establecimiento").is_in(sample_ids)
        ).join(sumas, on="id_establecimiento")

        for row in merged.iter_rows(named=True):
            assert abs(row["ingreso_reportado"] - row["check_reportado"]) < 0.01, (
                f"id={row['id_establecimiento']}: reportado {row['ingreso_reportado']} != check {row['check_reportado']}"
            )
            assert abs(row["ingreso_estimado"] - row["check_estimado"]) < 0.01, (
                f"id={row['id_establecimiento']}: estimado {row['ingreso_estimado']} != check {row['check_estimado']}"
            )

    def test_num_periodos_rango(self, base_procesada):
        max_p = base_procesada["num_periodos"].max()
        assert max_p <= 14, f"Más de 14 periodos: {max_p}"
        min_p = base_procesada["num_periodos"].min()
        assert min_p >= 1


# --- Tests de precisión ---

class TestPrecision:
    def test_nivel0_con_cero(self):
        data = [
            {"numero_facturas": 10, "total_facturas": 0, "ticket_promedio": 5},
            {"numero_facturas": 10, "total_facturas": 100, "ticket_promedio": 10},
        ]
        assert calcular_precision_establecimiento(data) == 0

    def test_nivel0_con_nulo(self):
        data = [
            {"numero_facturas": None, "total_facturas": 100, "ticket_promedio": 10},
            {"numero_facturas": 10, "total_facturas": 100, "ticket_promedio": 10},
        ]
        assert calcular_precision_establecimiento(data) == 0

    def test_nivel2_estable(self):
        data = [
            {"numero_facturas": 10, "total_facturas": 100, "ticket_promedio": 10},
            {"numero_facturas": 11, "total_facturas": 105, "ticket_promedio": 9.5},
            {"numero_facturas": 10, "total_facturas": 98, "ticket_promedio": 9.8},
        ]
        assert calcular_precision_establecimiento(data) == 2

    def test_nivel1_inestable(self):
        data = [
            {"numero_facturas": 10, "total_facturas": 100, "ticket_promedio": 10},
            {"numero_facturas": 10, "total_facturas": 100, "ticket_promedio": 10},
            {"numero_facturas": 50, "total_facturas": 500, "ticket_promedio": 10},
        ]
        assert calcular_precision_establecimiento(data) == 1

    def test_mejor_promedio_valores_iguales(self):
        assert calcular_mejor_promedio([10, 10, 10]) == 10.0

    def test_mejor_promedio_con_outlier(self):
        resultado = calcular_mejor_promedio([10, 11, 10, 100])
        # Debería ser cercano al promedio de [10, 10, 11] = 10.33
        assert resultado is not None
        assert 9 <= resultado <= 12

    def test_mejor_promedio_vacio(self):
        assert calcular_mejor_promedio([]) is None

    def test_mejor_promedio_un_valor(self):
        assert calcular_mejor_promedio([42.5]) == 42.5


# --- Tests de base_filtrada ---

class TestBaseFiltrada:
    def test_columnas_tiene_tipo_actividad(self, base_filtrada):
        assert "tipo_actividad" in base_filtrada.columns
        assert "filtro_ingreso_usado" in base_filtrada.columns

    def test_personas_naturales_son_rimpe(self, base_filtrada):
        pn = base_filtrada.filter(pl.col("filtro_ingreso_usado") == "no_aplica")
        assert pn.height > 0, "No hay personas naturales en base_filtrada"
        for row in pn.iter_rows(named=True):
            assert row["tipo_contribuyente"] == "PERSONA NATURAL"
            assert row["clase_contribuyente"] == "RIMPE"

    def test_sociedades_rango_ingresos_reportado(self, base_filtrada):
        soc = base_filtrada.filter(pl.col("filtro_ingreso_usado") == "ingreso_reportado")
        for row in soc.iter_rows(named=True):
            assert row["tipo_contribuyente"] == "SOCIEDAD"
            ing = row["ingreso_reportado"]
            in_range = (500_000 <= ing <= 990_000) or (1_000_000 <= ing <= 5_000_000)
            assert in_range, f"Sociedad con ingreso_reportado={ing} fuera de rango"

    def test_sociedades_rango_ingresos_estimado(self, base_filtrada):
        soc = base_filtrada.filter(pl.col("filtro_ingreso_usado") == "ingreso_estimado")
        for row in soc.iter_rows(named=True):
            assert row["tipo_contribuyente"] == "SOCIEDAD"
            ing = row["ingreso_estimado"]
            in_range = (500_000 <= ing <= 990_000) or (1_000_000 <= ing <= 5_000_000)
            assert in_range, f"Sociedad con ingreso_estimado={ing} fuera de rango"

    def test_no_registros_vacios(self, base_filtrada):
        assert base_filtrada.height > 0


# --- Tests de actividades_economicas ---

class TestActividades:
    def test_sin_sobreposicion(self, actividades):
        """Cada actividad_economica tiene exactamente un tipo_actividad."""
        assert actividades["actividad_economica"].n_unique() == actividades.height

    def test_cubre_todas_actividades(self, actividades, base_procesada):
        """Todas las actividades en base_procesada existen en el CSV."""
        acts_base = set(base_procesada["actividad_economica"].unique().to_list())
        acts_csv = set(actividades["actividad_economica"].to_list())
        faltantes = acts_base - acts_csv
        assert len(faltantes) == 0, f"Actividades sin clasificar: {faltantes}"

    def test_tipo_actividad_no_vacio(self, actividades):
        vacios = actividades.filter(
            pl.col("tipo_actividad").is_null() | (pl.col("tipo_actividad") == "")
        ).height
        assert vacios == 0
