"""
construir_base.py
Genera base_procesada.parquet y base_filtrada.parquet a partir de base_consolidada.parquet.
Usa ingreso_estimado (con imputación de outliers bajos) para filtrar sociedades.
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
TOTAL_PERIODOS = 14
UMBRAL_ESTABILIDAD = 0.2


def calcular_mejor_promedio(valores: list[float]) -> float | None:
    """
    Encuentra el promedio del subconjunto contiguo (ordenado) más grande
    donde todos los valores están dentro de ±UMBRAL_ESTABILIDAD del promedio.
    Itera desde k=n hasta k=2, retorna el primero que cumple.
    Fallback: mediana.
    """
    if not valores:
        return None
    n = len(valores)
    if n == 1:
        return valores[0]

    vals_sorted = sorted(valores)

    for k in range(n, 1, -1):
        for start in range(n - k + 1):
            subset = vals_sorted[start : start + k]
            avg = sum(subset) / k
            if avg == 0:
                continue
            max_dist = max(abs(v - avg) for v in subset)
            if max_dist <= avg * UMBRAL_ESTABILIDAD:
                return avg

    # Fallback: mediana
    return vals_sorted[n // 2]


def calcular_precision_establecimiento(
    periodos_data: list[dict],
) -> int:
    """
    Calcula calidad de datos por periodo (pre-nivel, sin considerar completitud).
    Retorna 0 si algún periodo tiene ceros/nulos, 1 si inestable, 2 si estable.
    El nivel final (0-3) se asigna en construir_base_procesada considerando num_periodos.
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
        rango_inf = mejor_avg * (1 - UMBRAL_ESTABILIDAD)
        rango_sup = mejor_avg * (1 + UMBRAL_ESTABILIDAD)
        for v in valores:
            if v < rango_inf or v > rango_sup:
                return 1

    return 2


def construir_base_procesada(df: pl.DataFrame) -> pl.DataFrame:
    """Construye base_procesada con ingreso_estimado y precision.
    - Precision 1-2: suma de total_facturas por periodo, reemplazando outliers bajos
      (< mejor_promedio * 0.8) con mejor_promedio.
    - Precision 0: null (se resuelve en imputar_ingreso con mediana del grupo).
    """

    df_fact = df.select(
        ["id_establecimiento", "numero_ruc", "numero_establecimiento",
         "razon_social", "nombre_fantasia_comercial", "clase_contribuyente",
         "tipo_contribuyente", "actividad_economica", "direccion_completa",
         "periodo", "numero_facturas", "total_facturas", "ticket_promedio"]
    )

    # Parsear provincia y canton desde direccion_completa (formato: PROV / CANTON / PARR / DIR)
    df_fact = df_fact.with_columns(
        pl.col("direccion_completa").str.split(" / ").list.get(0).str.strip_chars().alias("provincia"),
        pl.col("direccion_completa").str.split(" / ").list.get(1).str.strip_chars().alias("canton"),
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
        pl.col("provincia").first(),
        pl.col("canton").first(),
        # Recopilar datos por periodo para calcular precisión e ingreso_estimado
        pl.struct(VARS_FACTURACION).alias("periodos_data"),
        pl.col("periodo").count().alias("num_periodos"),
        pl.col("ticket_promedio").mean().alias("ticket_promedio"),
    )

    # Calcular precisión e ingreso_estimado
    precision_values = []
    ingreso_estimado_values = []
    for row in df_agg.iter_rows(named=True):
        periodos = row["periodos_data"]
        prec_base = calcular_precision_establecimiento(periodos)
        if prec_base == 0:
            prec = 0   # datos con ceros/nulos
        elif row["num_periodos"] < TOTAL_PERIODOS:
            prec = 1   # incompleto (< 14 periodos)
        elif prec_base == 1:
            prec = 2   # completo pero inestable
        else:
            prec = 3   # completo y estable
        precision_values.append(prec)

        tf_values = [p["total_facturas"] for p in periodos]
        if prec == 0:
            ingreso_estimado_values.append(None)
        elif prec in (1, 2):
            # Imputar outliers bajos para incompletos e inestables
            mejor_avg = calcular_mejor_promedio(tf_values)
            if mejor_avg is not None and mejor_avg > 0:
                rango_inf = mejor_avg * (1 - UMBRAL_ESTABILIDAD)
                adjusted = [mejor_avg if v < rango_inf else v for v in tf_values]
                ingreso_estimado_values.append(sum(adjusted))
            else:
                ingreso_estimado_values.append(sum(tf_values))
        else:
            # Precision 3: completo y estable, sin corrección
            ingreso_estimado_values.append(sum(tf_values))

    df_agg = df_agg.with_columns([
        pl.Series("precision", precision_values, dtype=pl.Int8),
        pl.Series("ingreso_estimado", ingreso_estimado_values, dtype=pl.Float64),
    ]).drop("periodos_data")

    return df_agg


def imputar_ingreso(df: pl.DataFrame) -> pl.DataFrame:
    """
    Resuelve ingreso_estimado null (precision 0) con mediana del grupo.
    Usa la mediana de ingreso_estimado/periodo por actividad_economica
    de establecimientos con 14 periodos y precision > 0.
    No modifica precision.
    """
    referencia = df.filter(
        (pl.col("num_periodos") == TOTAL_PERIODOS)
        & (pl.col("precision") > 0)
    )

    mediana_grupo = referencia.group_by("actividad_economica").agg(
        (pl.col("ingreso_estimado") / TOTAL_PERIODOS).median().alias("mediana_ingreso_periodo")
    )

    mediana_global = referencia["ingreso_estimado"].median() / TOTAL_PERIODOS

    df = df.join(mediana_grupo, on="actividad_economica", how="left")
    df = df.with_columns(
        pl.col("mediana_ingreso_periodo").fill_null(mediana_global)
    )

    df = df.with_columns(
        pl.col("ingreso_estimado")
        .fill_null(pl.col("mediana_ingreso_periodo") * TOTAL_PERIODOS)
    )

    return df.drop("mediana_ingreso_periodo")


def construir_base_filtrada(
    df_procesada: pl.DataFrame, df_actividades: pl.DataFrame
) -> pl.DataFrame:
    """
    Aplica filtros y mapea tipo_actividad.
    Filtros:
    - Personas Naturales: clase_contribuyente == 'RIMPE' (RIMPE Negocio Popular)
    - Sociedades: Pequeñas ($500K-$990K) y medianas ($1M-$5M) por ingreso_estimado
    """
    # Mapear tipo_actividad
    df = df_procesada.join(df_actividades, on="actividad_economica", how="left")

    # Filtro Personas Naturales: RIMPE
    personas_naturales = df.filter(
        (pl.col("tipo_contribuyente") == "PERSONA NATURAL")
        & (pl.col("clase_contribuyente") == "RIMPE")
    )

    # Filtro Sociedades por ingreso_estimado
    sociedades = df.filter(
        (pl.col("tipo_contribuyente") == "SOCIEDAD")
        & (
            # Pequeñas empresas
            ((pl.col("ingreso_estimado") >= 500_000) & (pl.col("ingreso_estimado") <= 990_000))
            |
            # Medianas empresas
            ((pl.col("ingreso_estimado") >= 1_000_000) & (pl.col("ingreso_estimado") <= 5_000_000))
        )
    )

    base_filtrada = pl.concat(
        [personas_naturales, sociedades],
        how="diagonal",
    ).unique(subset=["id_establecimiento"])

    return base_filtrada


def main():
    print("=" * 60)
    print("PIPELINE DE CONSTRUCCIÓN DE BASE COMERCIAL")
    print("=" * 60)

    # ── Lectura ──────────────────────────────────────────────
    print("\nLeyendo base_consolidada.parquet...")
    df = pl.read_parquet(BASE_CONSOLIDADA)
    rucs_total = df["numero_ruc"].n_unique()
    print(f"  Shape: {df.shape}")
    print(f"  RUCs totales: {rucs_total:,}")

    # ── Filtro 1: Establecimientos activos ────────────────────
    print("\n── Filtro 1: Establecimientos activos ──")
    rucs_sin_id = df.filter(pl.col("id_establecimiento").is_null())["numero_ruc"].n_unique()
    df_activos = df.filter(pl.col("id_establecimiento").is_not_null())
    rucs_con_id = df_activos["numero_ruc"].n_unique()
    est_con_id = df_activos["id_establecimiento"].n_unique()
    print(f"  RUCs sin id_establecimiento (sin local activo): {rucs_sin_id:,} ({rucs_sin_id/rucs_total:.1%})")
    print(f"  RUCs con establecimiento activo: {rucs_con_id:,}")
    print(f"  Establecimientos activos: {est_con_id:,}")

    # ── Base procesada ───────────────────────────────────────
    print("\n── Construyendo base_procesada ──")
    df_procesada = construir_base_procesada(df_activos)
    print(f"  Establecimientos: {df_procesada.height:,}")
    print(f"  RUCs: {df_procesada['numero_ruc'].n_unique():,}")
    print(f"  Precisión (pre-imputación):")
    print(df_procesada.group_by("precision").agg(pl.len().alias("n")).sort("precision"))

    # ── Imputación de ingreso (precision 0) ──────────────────
    print("\n── Imputando ingreso (precision 0 → mediana del grupo) ──")
    n_nulls = df_procesada.filter(pl.col("ingreso_estimado").is_null()).height
    df_procesada = imputar_ingreso(df_procesada)
    print(f"  Establecimientos imputados por mediana: {n_nulls:,}")
    df_procesada.write_parquet(BASE_PROCESADA)
    print(f"  Guardado en {BASE_PROCESADA}")

    # ── Filtro 2: Segmento comercial ─────────────────────────
    print("\n── Filtro 2: Segmento comercial ──")
    print("  Criterios: RIMPE (PN) + Sociedades $500K-$5M (ingreso_estimado)")
    df_actividades = pl.read_csv(ACTIVIDADES_CSV)
    df_filtrada = construir_base_filtrada(df_procesada, df_actividades)
    est_filtrada = df_filtrada["id_establecimiento"].n_unique()
    rucs_filtrada = df_filtrada["numero_ruc"].n_unique()
    descartados = df_procesada.height - est_filtrada
    print(f"  Establecimientos que pasan: {est_filtrada:,}")
    print(f"  RUCs que pasan: {rucs_filtrada:,}")
    print(f"  Descartados: {descartados:,}")
    print(f"  Por tipo_actividad (top 10):")
    print(
        df_filtrada.group_by("tipo_actividad")
        .agg(pl.col("id_establecimiento").n_unique().alias("establecimientos"))
        .sort("establecimientos", descending=True)
        .head(10)
    )
    df_filtrada.write_parquet(BASE_FILTRADA)
    print(f"  Guardado en {BASE_FILTRADA}")

    # ── Resumen del embudo ───────────────────────────────────
    print("\n" + "=" * 70)
    print("RESUMEN DEL EMBUDO")
    print("=" * 70)
    print(f"  {'Etapa':<42} {'RUCs':>10}   {'Establ.':>10}   Base generada")
    print(f"  {'-'*42} {'-'*10}   {'-'*10}   {'-'*22}")
    print(f"  {'base_consolidada.parquet':<42} {rucs_total:>10,}   {'—':>10}   base_consolidada")
    print(f"    ↓ Filtro 1: establ. activos")
    print(f"  {'Con id_establecimiento':<42} {rucs_con_id:>10,}   {est_con_id:>10,}")
    print(f"    ↓ Agregación + ingreso + precisión")
    print(f"  {'base_procesada.parquet':<42} {df_procesada['numero_ruc'].n_unique():>10,}   {df_procesada.height:>10,}   base_procesada")
    print(f"    ↓ Filtro 2: RIMPE (PN) + Soc. $500K-$5M")
    print(f"  {'base_filtrada.parquet':<42} {rucs_filtrada:>10,}   {est_filtrada:>10,}   base_filtrada")
    print(f"  {'-'*42} {'-'*10}   {'-'*10}")
    pct_rucs = rucs_filtrada / rucs_total * 100
    pct_est = est_filtrada / est_con_id * 100
    print(f"  {'Tasa de conversion total':<42} {pct_rucs:>9.2f}%   {pct_est:>9.2f}%")
    print("=" * 70)


if __name__ == "__main__":
    main()
