"""
construir_base.py
Genera base_procesada, base_filtrada y base_presentar a partir de base_consolidada.
Usa ingreso_estimado (con imputación de outliers bajos) para filtrar sociedades.
Python 3.11 / Polars
"""

import re
from collections import Counter

import polars as pl
from pathlib import Path

BASES_DIR = Path(__file__).resolve().parent.parent / "bases"
BASE_CONSOLIDADA = BASES_DIR / "base_consolidada.parquet"
ACTIVIDADES_CSV = BASES_DIR / "actividades_economicas.csv"
BASE_PROCESADA = BASES_DIR / "base_procesada.parquet"
BASE_FILTRADA = BASES_DIR / "base_filtrada.parquet"
BASE_PRESENTAR = BASES_DIR / "base_presentar.parquet"
CONTACTABILIDAD = BASES_DIR / "contactabilidad_recargada.parquet"
BASE_PRESENTAR_CONTACTABILIDAD = BASES_DIR / "base_presentar_contactabilidad.parquet"
PREVIA_CONTACTABILIDAD = BASES_DIR / "previa_contactabilidad.parquet"

TIPO_ACTIVIDAD_EXCLUIR = [
    "CONSTRUCCION",
    "SERVICIOS PROFESIONALES Y CIENTIFICOS",
    "ACTIVIDADES INMOBILIARIAS",
    "ASOCIACIONES Y ORGANIZACIONES",
    "ELECTRICIDAD, GAS Y AGUA",
    "ADMINISTRACION PUBLICA",
    "SERVICIOS ADMINISTRATIVOS Y DE APOYO",
    "ALQUILER DE BIENES",
    "MINERIA Y EXTRACCION",
    "GESTION DE RESIDUOS",
    "SERVICIOS PERSONALES",
    "SERVICIOS HOSPITALARIOS",
]

COLS_PRESENTAR = [
    "numero_ruc",
    "razon_social",
    "nombre_comercial",
    "clase_contribuyente",
    "tipo_contribuyente",
    "actividad_economica",
    "valor_balance_2024",
    "ticket_promedio",
    "ingreso_estimado",
    "tipo_actividad",
    "descripcion_corta",
]

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
        [
            "id_establecimiento",
            "numero_ruc",
            "numero_establecimiento",
            "razon_social",
            "nombre_fantasia_comercial",
            "clase_contribuyente",
            "tipo_contribuyente",
            "actividad_economica",
            "periodo",
            "numero_facturas",
            "total_facturas",
            "ticket_promedio",
            "cedula_representante_legal",
            "valor_balance_2024",
        ]
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
        pl.col("cedula_representante_legal").first(),
        pl.col("valor_balance_2024").first(),
        # Recopilar datos por periodo para calcular precisión e ingreso_estimado
        pl.struct(VARS_FACTURACION).alias("periodos_data"),
        pl.col("periodo").count().alias("num_periodos"),
        pl.col("numero_facturas").sum().alias("numero_facturas"),
        pl.col("ticket_promedio").mean().alias("ticket_promedio"),
    )

    # Calcular precisión e ingreso_estimado
    precision_values = []
    ingreso_estimado_values = []
    for row in df_agg.iter_rows(named=True):
        periodos = row["periodos_data"]
        prec_base = calcular_precision_establecimiento(periodos)
        if prec_base == 0:
            prec = 0  # datos con ceros/nulos
        elif row["num_periodos"] < TOTAL_PERIODOS:
            prec = 1  # incompleto (< 14 periodos)
        elif prec_base == 1:
            prec = 2  # completo pero inestable
        else:
            prec = 3  # completo y estable
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

    df_agg = df_agg.with_columns(
        [
            pl.Series("precision", precision_values, dtype=pl.Int8),
            pl.Series("ingreso_estimado", ingreso_estimado_values, dtype=pl.Float64),
        ]
    ).drop("periodos_data")

    return df_agg


def imputar_ingreso(df: pl.DataFrame) -> pl.DataFrame:
    """
    Resuelve ingreso_estimado null (precision 0) con mediana del grupo.
    Usa la mediana de ingreso_estimado/periodo por actividad_economica
    de establecimientos con 14 periodos y precision > 0.
    No modifica precision.
    """
    referencia = df.filter(
        (pl.col("num_periodos") == TOTAL_PERIODOS) & (pl.col("precision") > 0)
    )

    mediana_grupo = referencia.group_by("actividad_economica").agg(
        (pl.col("ingreso_estimado") / TOTAL_PERIODOS)
        .median()
        .alias("mediana_ingreso_periodo")
    )

    mediana_global = referencia["ingreso_estimado"].median() / TOTAL_PERIODOS

    df = df.join(mediana_grupo, on="actividad_economica", how="left")
    df = df.with_columns(pl.col("mediana_ingreso_periodo").fill_null(mediana_global))

    df = df.with_columns(
        pl.col("ingreso_estimado").fill_null(
            pl.col("mediana_ingreso_periodo") * TOTAL_PERIODOS
        )
    )

    return df.drop("mediana_ingreso_periodo")


_STOPWORDS = {
    "DE", "LA", "EL", "LOS", "LAS", "DEL", "EN", "Y", "A", "SAN", "SANTA",
    "MI", "SA", "S.A.", "CIA", "CIA.", "LTDA", "LTDA.", "C.A.", "II", "III",
    "AV", "AV.", "CALLE", "VIA", "KM", "NO", "NRO", "S/N",
}


def _nombre_comercial_ruc(nombres: list[str], top_n: int = 3) -> str:
    """Top N palabras más frecuentes entre los nombre_fantasia_comercial de un RUC."""
    palabras: Counter[str] = Counter()
    for n in nombres:
        if n:
            for p in n.strip().split():
                if p not in _STOPWORDS and re.match(r"^[A-ZÁÉÍÓÚÑ]{2,}$", p):
                    palabras[p] += 1
    if not palabras:
        return ""
    return " ".join(w for w, _ in palabras.most_common(top_n))


def _extraer_nombre_comercial(df: pl.DataFrame) -> pl.DataFrame:
    """Genera un DataFrame numero_ruc → nombre_comercial."""
    rucs = df.group_by("numero_ruc").agg(
        pl.col("nombre_fantasia_comercial").alias("nombres")
    )
    resultados = [
        _nombre_comercial_ruc(row["nombres"])
        for row in rucs.iter_rows(named=True)
    ]
    return rucs.select("numero_ruc").with_columns(
        pl.Series("nombre_comercial", resultados)
    )


def construir_base_filtrada(
    df_procesada: pl.DataFrame, df_actividades: pl.DataFrame
) -> pl.DataFrame:
    """
    Agrega base_procesada a nivel de numero_ruc (sumando ingreso_estimado)
    y aplica filtros de segmento comercial.
    Filtros:
    - Personas Naturales: clase_contribuyente == 'RIMPE'
    - Sociedades: Pequeñas ($500K-$990K) y medianas ($1M-$5M) por ingreso_estimado
    Entidad resultante: numero_ruc.
    """
    # nombre_comercial: top 3 palabras más frecuentes de nombre_fantasia_comercial
    nombres_top = _extraer_nombre_comercial(df_procesada)

    # Agregar a nivel RUC
    df_ruc = df_procesada.group_by("numero_ruc").agg(
        pl.col("razon_social").first(),
        pl.col("clase_contribuyente").first(),
        pl.col("tipo_contribuyente").first(),
        pl.col("actividad_economica").first(),
        pl.col("cedula_representante_legal").first(),
        pl.col("valor_balance_2024").first(),
        pl.col("precision").mean().alias("precision"),
        pl.col("numero_facturas").sum(),
        pl.col("ingreso_estimado").sum(),
        pl.col("ticket_promedio").mean(),
    ).with_columns(
        pl.col("precision").floor().cast(pl.Int8)
    ).join(nombres_top, on="numero_ruc", how="left")

    # Mapear tipo_actividad
    df = df_ruc.join(df_actividades, on="actividad_economica", how="left")

    # Filtro Personas Naturales: RIMPE
    personas_naturales = df.filter(
        (pl.col("tipo_contribuyente") == "PERSONA NATURAL")
        & (pl.col("clase_contribuyente") == "RIMPE")
    )

    # Filtro Sociedades por ingreso_estimado (suma de todos sus establecimientos)
    sociedades = df.filter(
        (pl.col("tipo_contribuyente") == "SOCIEDAD")
        & (
            # Pequeñas empresas
            (
                (pl.col("ingreso_estimado") >= 500_000)
                & (pl.col("ingreso_estimado") <= 990_000)
            )
            |
            # Medianas empresas
            (
                (pl.col("ingreso_estimado") >= 1_000_000)
                & (pl.col("ingreso_estimado") <= 5_000_000)
            )
        )
    )

    base_filtrada = pl.concat(
        [personas_naturales, sociedades],
        how="diagonal",
    ).unique(subset=["numero_ruc"])

    return base_filtrada


def construir_base_presentar(df_filtrada: pl.DataFrame) -> pl.DataFrame:
    """
    Filtra base_filtrada eliminando todos los RUCs cuyo tipo_actividad
    esté en TIPO_ACTIVIDAD_EXCLUIR y descarta columnas internas (precision).
    """
    rucs_excluir = (
        df_filtrada.filter(pl.col("tipo_actividad").is_in(TIPO_ACTIVIDAD_EXCLUIR))
        .select("numero_ruc")
        .unique()
    )
    df = df_filtrada.join(rucs_excluir, on="numero_ruc", how="anti")
    return df.select(COLS_PRESENTAR).rename({
        "ticket_promedio": "ticket_promedio_2025",
        "ingreso_estimado": "ingreso_estimado_2025",
    })


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
    rucs_sin_id = df.filter(pl.col("id_establecimiento").is_null())[
        "numero_ruc"
    ].n_unique()
    df_activos = df.filter(pl.col("id_establecimiento").is_not_null())
    rucs_con_id = df_activos["numero_ruc"].n_unique()
    est_con_id = df_activos["id_establecimiento"].n_unique()
    print(
        f"  RUCs sin id_establecimiento (sin local activo): {rucs_sin_id:,} ({rucs_sin_id/rucs_total:.1%})"
    )
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
    rucs_filtrada = df_filtrada["numero_ruc"].n_unique()
    rucs_procesada = df_procesada["numero_ruc"].n_unique()
    descartados = rucs_procesada - rucs_filtrada
    print(f"  RUCs que pasan: {rucs_filtrada:,}")
    print(f"  RUCs descartados: {descartados:,}")
    print(f"  Por tipo_actividad (top 10):")
    print(
        df_filtrada.group_by("tipo_actividad")
        .agg(pl.col("numero_ruc").n_unique().alias("rucs"))
        .sort("rucs", descending=True)
        .head(10)
    )
    df_filtrada.write_parquet(BASE_FILTRADA)
    print(f"  Guardado en {BASE_FILTRADA}")

    # ── Filtro 3: Excluir tipo_actividad no relevante ─────────
    print("\n── Filtro 3: Excluir tipo_actividad no relevante ──")
    print(f"  Categorías excluidas: {len(TIPO_ACTIVIDAD_EXCLUIR)}")
    df_presentar = construir_base_presentar(df_filtrada)
    rucs_presentar = df_presentar["numero_ruc"].n_unique()
    print(f"  RUCs eliminados: {rucs_filtrada - rucs_presentar:,}")
    print(f"  RUCs que pasan: {rucs_presentar:,}")
    df_presentar.write_parquet(BASE_PRESENTAR)
    print(f"  Guardado en {BASE_PRESENTAR}")

    # ── Filtro 4: Solo RUCs con contactabilidad ────────────────
    print("\n── Filtro 4: RUCs con contactabilidad ──")
    df_contactabilidad = pl.read_parquet(CONTACTABILIDAD)
    rucs_contactables = df_contactabilidad.select(
        pl.col("numero_ruc").cast(pl.Int64)
    ).unique()
    # Personas naturales pasan directo (cédula derivada del RUC)
    # Sociedades requieren estar en contactabilidad_recargada
    pn = df_presentar.filter(pl.col("tipo_contribuyente") == "PERSONA NATURAL")
    soc = df_presentar.filter(
        pl.col("tipo_contribuyente") == "SOCIEDAD"
    ).join(rucs_contactables, on="numero_ruc", how="semi")
    df_presentar_contactabilidad = pl.concat([pn, soc])
    rucs_contactabilidad = df_presentar_contactabilidad["numero_ruc"].n_unique()
    print(f"  RUCs sin contactabilidad: {rucs_presentar - rucs_contactabilidad:,}")
    print(f"  RUCs que pasan: {rucs_contactabilidad:,}")
    df_presentar_contactabilidad.write_parquet(BASE_PRESENTAR_CONTACTABILIDAD)
    print(f"  Guardado en {BASE_PRESENTAR_CONTACTABILIDAD}")

    # ── Previa contactabilidad (numero_ruc + cedula) ───────────
    print("\n── Generando previa_contactabilidad ──")
    previa = df_filtrada.filter(
        pl.col("numero_ruc").is_in(df_presentar_contactabilidad["numero_ruc"])
    ).select("numero_ruc", "cedula_representante_legal")
    previa.write_parquet(PREVIA_CONTACTABILIDAD)
    print(f"  RUCs: {previa.height:,}")
    print(f"  Guardado en {PREVIA_CONTACTABILIDAD}")

    # ── Resumen del embudo ───────────────────────────────────
    print("\n" + "=" * 70)
    print("RESUMEN DEL EMBUDO")
    print("=" * 70)
    print(f"  {'Etapa':<45} {'RUCs':>10}   {'Entidad':>15}")
    print(f"  {'-'*45} {'-'*10}   {'-'*15}")
    print(
        f"  {'base_consolidada.parquet':<45} {rucs_total:>10,}   {'—':>15}"
    )
    print(f"    ↓ Filtro 1: establ. activos")
    print(f"  {'Con id_establecimiento':<45} {rucs_con_id:>10,}   {'establecimiento':>15}")
    print(f"    ↓ Agregación + ingreso + precisión")
    print(
        f"  {'base_procesada.parquet':<45} {df_procesada['numero_ruc'].n_unique():>10,}   {'establecimiento':>15}"
    )
    print(f"    ↓ Filtro 2: RIMPE (PN) + Soc. $500K-$5M (agregado a RUC)")
    print(
        f"  {'base_filtrada.parquet':<45} {rucs_filtrada:>10,}   {'numero_ruc':>15}"
    )
    print(f"    ↓ Filtro 3: Excluir tipo_actividad")
    print(
        f"  {'base_presentar.parquet':<45} {rucs_presentar:>10,}   {'numero_ruc':>15}"
    )
    print(f"    ↓ Filtro 4: Solo RUCs con contactabilidad")
    print(
        f"  {'base_presentar_contactabilidad.parquet':<45} {rucs_contactabilidad:>10,}   {'numero_ruc':>15}"
    )
    print(f"  {'-'*45} {'-'*10}")
    pct_rucs = rucs_contactabilidad / rucs_total * 100
    print(f"  {'Tasa de conversión total':<45} {pct_rucs:>9.2f}%")
    print("=" * 70)


if __name__ == "__main__":
    main()
