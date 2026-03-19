"""
construir_base.py
Genera base_procesada.parquet y base_filtrada.parquet a partir de base_consolidada.parquet.
Solo usa ingreso_reportado (total_facturas agregado) para filtrar sociedades.
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
    """Construye base_procesada con ingreso_reportado, ingreso_imputado y precision.
    ingreso_imputado = mejor_promedio(total_facturas) × 14 para precision > 0.
    Para precision 0, ingreso_imputado queda null (se resuelve en imputar_ingreso).
    """

    df_fact = df.select(
        ["id_establecimiento", "numero_ruc", "numero_establecimiento",
         "razon_social", "nombre_fantasia_comercial", "clase_contribuyente",
         "tipo_contribuyente", "actividad_economica",
         "periodo", "numero_facturas", "total_facturas", "ticket_promedio"]
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
        # Recopilar datos por periodo para calcular precisión e imputación
        pl.struct(VARS_FACTURACION).alias("periodos_data"),
        pl.col("periodo").count().alias("num_periodos"),
    )

    # Calcular precisión e ingreso_imputado (trimmed average × 14)
    precision_values = []
    ingreso_imputado_values = []
    for row in df_agg.iter_rows(named=True):
        periodos = row["periodos_data"]
        prec = calcular_precision_establecimiento(periodos)
        precision_values.append(prec)

        tf_values = [p["total_facturas"] for p in periodos]
        mejor_avg = calcular_mejor_promedio(tf_values)
        if mejor_avg is not None and prec > 0:
            ingreso_imputado_values.append(mejor_avg * TOTAL_PERIODOS)
        else:
            ingreso_imputado_values.append(None)

    df_agg = df_agg.with_columns([
        pl.Series("precision", precision_values, dtype=pl.Int8),
        pl.Series("ingreso_imputado", ingreso_imputado_values, dtype=pl.Float64),
    ]).drop("periodos_data")

    return df_agg


def imputar_ingreso(df: pl.DataFrame) -> pl.DataFrame:
    """
    Resuelve ingreso_imputado para precision 0 (donde el trimmed average no es confiable).
    Usa la mediana de ingreso/periodo por actividad_economica de establecimientos confiables.
    No modifica precision.
    """
    # Referencia: establecimientos con 14 periodos y datos confiables
    referencia = df.filter(
        (pl.col("num_periodos") == TOTAL_PERIODOS)
        & (pl.col("precision") > 0)
    )

    # Mediana de ingreso/periodo por actividad_economica
    mediana_grupo = referencia.group_by("actividad_economica").agg(
        (pl.col("ingreso_reportado") / TOTAL_PERIODOS).median().alias("mediana_ingreso_periodo")
    )

    # Mediana global como fallback
    mediana_global = referencia["ingreso_reportado"].median() / TOTAL_PERIODOS

    df = df.join(mediana_grupo, on="actividad_economica", how="left")
    df = df.with_columns(
        pl.col("mediana_ingreso_periodo").fill_null(mediana_global)
    )

    # Solo rellenar los nulls (precision 0)
    df = df.with_columns(
        pl.col("ingreso_imputado")
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
    - Sociedades: Pequeñas ($500K-$990K) y medianas ($1M-$5M) por ingreso_imputado
    """
    # Mapear tipo_actividad
    df = df_procesada.join(df_actividades, on="actividad_economica", how="left")

    # Filtro Personas Naturales: RIMPE
    personas_naturales = df.filter(
        (pl.col("tipo_contribuyente") == "PERSONA NATURAL")
        & (pl.col("clase_contribuyente") == "RIMPE")
    )

    # Filtro Sociedades por ingreso_imputado
    sociedades = df.filter(
        (pl.col("tipo_contribuyente") == "SOCIEDAD")
        & (
            # Pequeñas empresas
            ((pl.col("ingreso_imputado") >= 500_000) & (pl.col("ingreso_imputado") <= 990_000))
            |
            # Medianas empresas
            ((pl.col("ingreso_imputado") >= 1_000_000) & (pl.col("ingreso_imputado") <= 5_000_000))
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

    # ── Imputación de ingreso ─────────────────────────────────
    print("\n── Imputando ingreso (establ. con < 14 periodos) ──")
    df_procesada = imputar_ingreso(df_procesada)
    n_imputados = df_procesada.filter(
        pl.col("ingreso_reportado") != pl.col("ingreso_imputado")
    ).height
    print(f"  Establecimientos imputados: {n_imputados:,}")
    print(f"  Precisión post-imputación:")
    print(df_procesada.group_by("precision").agg(pl.len().alias("n")).sort("precision"))
    df_procesada.write_parquet(BASE_PROCESADA)
    print(f"  Guardado en {BASE_PROCESADA}")

    # ── Filtro 2: Segmento comercial ─────────────────────────
    print("\n── Filtro 2: Segmento comercial ──")
    print("  Criterios: RIMPE (PN) + Sociedades $500K-$5M (por ingreso_imputado)")
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
