"""
construir_base.py
Genera base_procesada.parquet y base_filtrada.parquet a partir de base_consolidada.parquet.
Python 3.11 / Polars
"""

import polars as pl
from pathlib import Path

BASES_DIR = Path(__file__).resolve().parent.parent / "bases"
BASE_CONSOLIDADA = BASES_DIR / "base_consolidada.parquet"
ACTIVIDADES_CSV = BASES_DIR / "actividades_economicas.csv"
BASE_PROCESADA = BASES_DIR / "base_procesada.parquet"
BASE_FILTRADA = BASES_DIR / "base_filtrada.parquet"

VARS_FACTURACION = ["numero_facturas", "total_facturas", "ticket_promedio"]


def calcular_mejor_promedio(valores: list[float]) -> float | None:
    """
    Calcula el mejor promedio recortado: el subconjunto de periodos cuyo promedio
    minimiza la distancia máxima entre el promedio y cada valor del subconjunto.
    Retorna el promedio del mejor subconjunto, o None si no hay valores.
    """
    if not valores:
        return None
    n = len(valores)
    if n == 1:
        return valores[0]

    vals_sorted = sorted(valores)
    mejor_avg = None
    mejor_max_dist = float("inf")

    # Probar ventanas contiguas de tamaño k (ordenadas) desde n hasta 1
    for k in range(n, 0, -1):
        for start in range(n - k + 1):
            subset = vals_sorted[start : start + k]
            avg = sum(subset) / k
            max_dist = max(abs(v - avg) for v in subset)
            if max_dist < mejor_max_dist:
                mejor_max_dist = max_dist
                mejor_avg = avg
        if mejor_max_dist < 1e-10:
            break
    return mejor_avg


def calcular_precision_establecimiento(
    periodos_data: list[dict],
) -> int:
    """
    Calcula el nivel de precisión para un establecimiento dado sus datos por periodo.
    - Nivel 0: algún periodo tiene 0 o nulo en numero_facturas, total_facturas o ticket_promedio
    - Nivel 1: datos completos pero inestables (algún valor fuera del ±20% del mejor promedio)
    - Nivel 2: datos completos y estables
    """
    for p in periodos_data:
        for var in VARS_FACTURACION:
            v = p.get(var)
            if v is None or v == 0:
                return 0

    # Nivel 1 o 2: verificar estabilidad por variable
    for var in VARS_FACTURACION:
        valores = [p[var] for p in periodos_data]
        mejor_avg = calcular_mejor_promedio(valores)
        if mejor_avg is None or mejor_avg == 0:
            return 0
        rango_inf = mejor_avg * 0.8
        rango_sup = mejor_avg * 1.2
        for v in valores:
            if v < rango_inf or v > rango_sup:
                return 1

    return 2


def construir_base_procesada(df: pl.DataFrame) -> pl.DataFrame:
    """Construye base_procesada con ingreso_reportado, ingreso_estimado y precision."""

    # Filtrar solo filas con datos de facturación
    df_fact = df.filter(pl.col("id_establecimiento").is_not_null()).select(
        ["id_establecimiento", "numero_ruc", "numero_establecimiento",
         "razon_social", "nombre_fantasia_comercial", "clase_contribuyente",
         "tipo_contribuyente", "actividad_economica",
         "periodo", "numero_facturas", "total_facturas", "ticket_promedio"]
    )

    # Calcular ingreso_estimado por periodo
    df_fact = df_fact.with_columns(
        (pl.col("ticket_promedio") * pl.col("numero_facturas")).alias("ingreso_estimado_periodo")
    )

    # Agregar por establecimiento
    df_agg = df_fact.group_by("id_establecimiento").agg(
        pl.col("numero_ruc").first(),
        pl.col("numero_establecimiento").first(),
        pl.col("razon_social").first(),
        pl.col("nombre_fantasia_comercial").first(),
        pl.col("clase_contribuyente").first(),
        pl.col("tipo_contribuyente").first(),
        pl.col("actividad_economica").first(),
        pl.col("total_facturas").sum().alias("ingreso_reportado"),
        pl.col("ingreso_estimado_periodo").sum().alias("ingreso_estimado"),
        # Recopilar datos por periodo para calcular precisión
        pl.struct(VARS_FACTURACION).alias("periodos_data"),
        pl.col("periodo").count().alias("num_periodos"),
    )

    # Calcular precisión usando apply
    precision_values = []
    for row in df_agg.iter_rows(named=True):
        periodos = row["periodos_data"]
        prec = calcular_precision_establecimiento(periodos)
        precision_values.append(prec)

    df_agg = df_agg.with_columns(
        pl.Series("precision", precision_values, dtype=pl.Int8)
    ).drop("periodos_data")

    return df_agg


def construir_base_filtrada(
    df_procesada: pl.DataFrame, df_actividades: pl.DataFrame
) -> pl.DataFrame:
    """
    Aplica filtros y mapea tipo_actividad.
    Filtros:
    - Personas Naturales: clase_contribuyente == 'RIMPE' (RIMPE Negocio Popular)
    - Sociedades: Pequeñas ($500K-$990K) y medianas ($1M-$5M) por ingreso_reportado e ingreso_estimado
    """
    # Mapear tipo_actividad
    df = df_procesada.join(df_actividades, on="actividad_economica", how="left")

    # Filtro Personas Naturales: RIMPE
    personas_naturales = df.filter(
        (pl.col("tipo_contribuyente") == "PERSONA NATURAL")
        & (pl.col("clase_contribuyente") == "RIMPE")
    )

    # Filtro Sociedades por ingreso_reportado
    sociedades_por_reportado = df.filter(
        (pl.col("tipo_contribuyente") == "SOCIEDAD")
        & (
            # Pequeñas empresas
            ((pl.col("ingreso_reportado") >= 500_000) & (pl.col("ingreso_reportado") <= 990_000))
            |
            # Medianas empresas
            ((pl.col("ingreso_reportado") >= 1_000_000) & (pl.col("ingreso_reportado") <= 5_000_000))
        )
    ).with_columns(pl.lit("ingreso_reportado").alias("filtro_ingreso_usado"))

    # Filtro Sociedades por ingreso_estimado
    sociedades_por_estimado = df.filter(
        (pl.col("tipo_contribuyente") == "SOCIEDAD")
        & (
            ((pl.col("ingreso_estimado") >= 500_000) & (pl.col("ingreso_estimado") <= 990_000))
            |
            ((pl.col("ingreso_estimado") >= 1_000_000) & (pl.col("ingreso_estimado") <= 5_000_000))
        )
    ).with_columns(pl.lit("ingreso_estimado").alias("filtro_ingreso_usado"))

    # Personas naturales no tienen filtro de ingreso
    personas_naturales = personas_naturales.with_columns(
        pl.lit("no_aplica").alias("filtro_ingreso_usado")
    )

    # Unir ambos resultados (puede haber sobreposición entre reportado y estimado)
    base_filtrada = pl.concat(
        [personas_naturales, sociedades_por_reportado, sociedades_por_estimado],
        how="diagonal",
    ).unique(subset=["id_establecimiento", "filtro_ingreso_usado"])

    return base_filtrada


def main():
    print("Leyendo base_consolidada.parquet...")
    df = pl.read_parquet(BASE_CONSOLIDADA)
    print(f"  Shape: {df.shape}")

    print("\nConstruyendo base_procesada...")
    df_procesada = construir_base_procesada(df)
    print(f"  Establecimientos: {df_procesada.height}")
    print(f"  Precisión distribución:")
    print(df_procesada.group_by("precision").agg(pl.len().alias("n")).sort("precision"))
    df_procesada.write_parquet(BASE_PROCESADA)
    print(f"  Guardado en {BASE_PROCESADA}")

    print("\nLeyendo actividades_economicas.csv...")
    df_actividades = pl.read_csv(ACTIVIDADES_CSV)
    print(f"  Categorías: {df_actividades['tipo_actividad'].n_unique()}")

    print("\nConstruyendo base_filtrada...")
    df_filtrada = construir_base_filtrada(df_procesada, df_actividades)
    print(f"  Registros filtrados: {df_filtrada.height}")
    print(f"  Por filtro_ingreso_usado:")
    print(df_filtrada.group_by("filtro_ingreso_usado").agg(pl.len().alias("n")).sort("n", descending=True))
    print(f"  Por tipo_actividad:")
    print(
        df_filtrada.group_by("tipo_actividad")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(15)
    )
    df_filtrada.write_parquet(BASE_FILTRADA)
    print(f"  Guardado en {BASE_FILTRADA}")


if __name__ == "__main__":
    main()
