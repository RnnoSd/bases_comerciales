"""
generar_reporte.py
Genera un reporte ejecutivo en Excel mostrando el embudo de datos
del pipeline: base_consolidada → base_procesada → base_filtrada.
Python 3.11 / Polars / openpyxl
"""

import polars as pl
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter

BASES_DIR = Path(__file__).resolve().parent.parent / "bases"
ROOT_DIR = Path(__file__).resolve().parent.parent
BASE_CONSOLIDADA = BASES_DIR / "base_consolidada.parquet"
ACTIVIDADES_CSV = BASES_DIR / "actividades_economicas.csv"
BASE_PROCESADA = BASES_DIR / "base_procesada.parquet"
BASE_FILTRADA = BASES_DIR / "base_filtrada.parquet"
REPORTE_XLSX = ROOT_DIR / "reporte_pipeline.xlsx"

# ── Estilos ──────────────────────────────────────────────────────────
FONT_TITULO = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
FONT_HEADER = Font(name="Calibri", size=10, bold=True, color="FFFFFF")
FONT_NORMAL = Font(name="Calibri", size=10)
FONT_BOLD = Font(name="Calibri", size=10, bold=True)
FONT_SUBTITULO = Font(name="Calibri", size=11, bold=True, color="1F4E79")

FILL_TITULO = PatternFill("solid", fgColor="1F4E79")
FILL_HEADER = PatternFill("solid", fgColor="2E75B6")
FILL_LIGHT = PatternFill("solid", fgColor="D6E4F0")
FILL_WHITE = PatternFill("solid", fgColor="FFFFFF")
FILL_GREEN = PatternFill("solid", fgColor="E2EFDA")
FILL_YELLOW = PatternFill("solid", fgColor="FFF2CC")
FILL_RED = PatternFill("solid", fgColor="FCE4EC")

ALIGN_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
ALIGN_LEFT = Alignment(horizontal="left", vertical="center", wrap_text=True)
ALIGN_RIGHT = Alignment(horizontal="right", vertical="center")

BORDER_THIN = Border(
    left=Side(style="thin", color="B4C6E7"),
    right=Side(style="thin", color="B4C6E7"),
    top=Side(style="thin", color="B4C6E7"),
    bottom=Side(style="thin", color="B4C6E7"),
)


def apply_style(
    cell, font=None, fill=None, alignment=None, border=None, number_format=None
):
    if font:
        cell.font = font
    if fill:
        cell.fill = fill
    if alignment:
        cell.alignment = alignment
    if border:
        cell.border = border
    if number_format:
        cell.number_format = number_format


def write_title(ws, row, col, text, colspan=1):
    """Escribe un título con merge y estilo."""
    cell = ws.cell(row=row, column=col, value=text)
    apply_style(cell, FONT_TITULO, FILL_TITULO, ALIGN_CENTER, BORDER_THIN)
    if colspan > 1:
        ws.merge_cells(
            start_row=row,
            start_column=col,
            end_row=row,
            end_column=col + colspan - 1,
        )
        for c in range(col + 1, col + colspan):
            apply_style(
                ws.cell(row=row, column=c), fill=FILL_TITULO, border=BORDER_THIN
            )


def write_headers(ws, row, headers, start_col=1):
    """Escribe una fila de encabezados."""
    for i, h in enumerate(headers):
        cell = ws.cell(row=row, column=start_col + i, value=h)
        apply_style(cell, FONT_HEADER, FILL_HEADER, ALIGN_CENTER, BORDER_THIN)


def write_row(ws, row, values, start_col=1, fills=None, fonts=None, fmt=None):
    """Escribe una fila de datos con estilo."""
    for i, v in enumerate(values):
        cell = ws.cell(row=row, column=start_col + i, value=v)
        f = fills[i] if fills and i < len(fills) else FILL_WHITE
        fn = fonts[i] if fonts and i < len(fonts) else FONT_NORMAL
        al = ALIGN_RIGHT if isinstance(v, (int, float)) else ALIGN_LEFT
        nf = None
        if fmt and i < len(fmt) and fmt[i]:
            nf = fmt[i]
        apply_style(cell, fn, f, al, BORDER_THIN, nf)


def pct(part, total):
    return part / total if total > 0 else 0


def generar_reporte():
    print("Leyendo bases...")
    df_cons = pl.read_parquet(BASE_CONSOLIDADA)
    df_proc = pl.read_parquet(BASE_PROCESADA)
    df_filt = pl.read_parquet(BASE_FILTRADA)

    # ── Cálculos ─────────────────────────────────────────────────────
    # Consolidada
    rucs_total = df_cons["numero_ruc"].n_unique()
    est_con_id = df_cons.filter(pl.col("id_establecimiento").is_not_null())
    est_total_cons = est_con_id["id_establecimiento"].n_unique()
    rucs_con_id = est_con_id["numero_ruc"].n_unique()
    rucs_sin_id = rucs_total - rucs_con_id  # aprox, los que SOLO no tienen id

    # Procesada
    est_proc = df_proc.height
    rucs_proc = df_proc["numero_ruc"].n_unique()

    # Filtrada (establecimientos únicos)
    est_filt = df_filt["id_establecimiento"].n_unique()
    rucs_filt = df_filt["numero_ruc"].n_unique()

    # Tipo contribuyente en procesada
    pn_proc = df_proc.filter(pl.col("tipo_contribuyente") == "PERSONA NATURAL").height
    soc_proc = df_proc.filter(pl.col("tipo_contribuyente") == "SOCIEDAD").height

    # Clase contribuyente en procesada (filtrar PN+RIMPE, existen sociedades RIMPE)
    rimpe_proc = df_proc.filter(
        (pl.col("tipo_contribuyente") == "PERSONA NATURAL")
        & (pl.col("clase_contribuyente") == "RIMPE")
    ).height
    general_proc = df_proc.filter(pl.col("clase_contribuyente") == "GENERAL").height

    # Sociedades por rango (solo ingreso_estimado)
    soc_df = df_proc.filter(pl.col("tipo_contribuyente") == "SOCIEDAD")
    soc_bajo_500k = soc_df.filter(pl.col("ingreso_estimado") < 500_000).height
    soc_peq = soc_df.filter(
        (pl.col("ingreso_estimado") >= 500_000)
        & (pl.col("ingreso_estimado") <= 990_000)
    ).height
    soc_med = soc_df.filter(
        (pl.col("ingreso_estimado") >= 1_000_000)
        & (pl.col("ingreso_estimado") <= 5_000_000)
    ).height
    soc_sobre_5m = soc_df.filter(pl.col("ingreso_estimado") > 5_000_000).height
    soc_gap = soc_df.height - soc_bajo_500k - soc_peq - soc_med - soc_sobre_5m

    # Precisión — por establecimientos y RUCs
    prec_proc = df_proc.group_by("precision").agg(
        pl.len().alias("n_est"),
        pl.col("numero_ruc").n_unique().alias("n_rucs"),
    ).sort("precision")
    prec_proc_dict = {row["precision"]: row for row in prec_proc.iter_rows(named=True)}

    prec_filt = df_filt.group_by("precision").agg(
        pl.col("id_establecimiento").n_unique().alias("n_est"),
        pl.col("numero_ruc").n_unique().alias("n_rucs"),
    ).sort("precision")
    prec_filt_dict = {row["precision"]: row for row in prec_filt.iter_rows(named=True)}

    # Filtrada: segmentos
    pn_filt = df_filt.filter(pl.col("tipo_contribuyente") == "PERSONA NATURAL")[
        "id_establecimiento"
    ].n_unique()

    soc_filt = df_filt.filter(pl.col("tipo_contribuyente") == "SOCIEDAD")
    soc_filt_peq = soc_filt.filter(
        (pl.col("ingreso_estimado") >= 500_000)
        & (pl.col("ingreso_estimado") <= 990_000)
    ).height
    soc_filt_med = soc_filt.filter(
        (pl.col("ingreso_estimado") >= 1_000_000)
        & (pl.col("ingreso_estimado") <= 5_000_000)
    ).height

    # Cobertura contactabilidad/balances en filtrada
    # Necesitamos cruzar filtrada con consolidada para obtener las variables de contacto
    filt_ids = df_filt.select("id_establecimiento").unique()
    est_filt_detalle = est_con_id.join(filt_ids, on="id_establecimiento", how="inner").unique(
        subset=["id_establecimiento"]
    )
    total_est_filt = est_filt_detalle.height
    total_rucs_filt = est_filt_detalle["numero_ruc"].n_unique()

    cob_contacto = {}
    for col in [
        "direccion_completa",
        "email",
        "telefono",
        "telefono_representante",
        "email_representante",
    ]:
        non_null_est = est_filt_detalle.filter(
            pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        ).height
        non_null_rucs = est_filt_detalle.filter(
            pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        )["numero_ruc"].n_unique()
        cob_contacto[col] = (non_null_est, non_null_rucs)

    cob_balance = {}
    for col in ["nombre", "descripcion_rama", "valor_balance_2024"]:
        non_null_est = est_filt_detalle.filter(
            pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        ).height
        non_null_rucs = est_filt_detalle.filter(
            pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        )["numero_ruc"].n_unique()
        cob_balance[col] = (non_null_est, non_null_rucs)

    # Tipo actividad en filtrada
    df_actividades = pl.read_csv(ACTIVIDADES_CSV)
    tipo_act = (
        df_filt.group_by("tipo_actividad")
        .agg(
            pl.col("id_establecimiento").n_unique().alias("establecimientos"),
            pl.col("ingreso_estimado").mean().alias("ingreso_prom_estimado"),
        )
        .sort("establecimientos", descending=True)
    )

    # Top 3 ejemplos (descripcion_corta) por tipo_actividad
    ejemplos_por_tipo = {}
    for tipo in tipo_act["tipo_actividad"].to_list():
        top3 = (
            df_filt.filter(pl.col("tipo_actividad") == tipo)
            .group_by("actividad_economica")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
            .head(3)
            .join(df_actividades, on="actividad_economica", how="left")
        )
        ejemplos = top3["descripcion_corta"].to_list() if "descripcion_corta" in top3.columns else []
        ejemplos_por_tipo[tipo] = " / ".join(e for e in ejemplos if e)

    # ── Crear Excel ──────────────────────────────────────────────────
    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte Ejecutivo"
    ws.sheet_properties.tabColor = "1F4E79"

    NCOLS = 6  # ancho de las tablas principales

    # ── SECCIÓN 1: EMBUDO GENERAL ────────────────────────────────────
    r = 1
    write_title(ws, r, 1, "REPORTE DE PIPELINE - PROYECTO BENDO", NCOLS)
    r += 1
    ws.cell(
        row=r,
        column=1,
        value="Embudo de datos: como se reducen los registros en cada etapa del pipeline",
    )
    apply_style(ws.cell(row=r, column=1), FONT_SUBTITULO, alignment=ALIGN_LEFT)
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=NCOLS)

    r += 2
    write_title(ws, r, 1, "1. EMBUDO DE DATOS", NCOLS)
    r += 1
    write_headers(
        ws,
        r,
        [
            "Etapa",
            "RUCs",
            "Establecimientos",
            "% vs Anterior",
            "Base generada",
            "Detalle",
        ],
    )
    r += 1

    # Fila 1: base_consolidada
    write_row(
        ws,
        r,
        [
            "base_consolidada.parquet",
            rucs_total,
            "",
            "",
            "base_consolidada",
            "Todos los contribuyentes registrados en el SRI",
        ],
        fills=[FILL_LIGHT] * NCOLS,
        fonts=[FONT_BOLD] + [FONT_NORMAL] * 5,
        fmt=[None, "#,##0", None, None, None, None],
    )
    r += 1

    # Fila separador: Filtro 1
    write_row(
        ws,
        r,
        ["  ↓ Filtro 1: Establecimientos activos", "", "", "", "", ""],
        fills=[FILL_WHITE] * NCOLS,
        fonts=[FONT_SUBTITULO] + [FONT_NORMAL] * 5,
    )
    r += 1

    # Fila 2: resultado del filtro 1
    write_row(
        ws,
        r,
        [
            "Con id_establecimiento",
            rucs_con_id,
            est_total_cons,
            pct(rucs_con_id, rucs_total),
            "",
            f"-{rucs_sin_id:,} RUCs sin local activo (0 ventas)",
        ],
        fills=[FILL_YELLOW] * NCOLS,
        fmt=[None, "#,##0", "#,##0", "0.0%", None, None],
    )
    r += 1

    # Fila separador: Agregación
    write_row(
        ws,
        r,
        ["  ↓ Agregacion + ingreso + precision", "", "", "", "", ""],
        fills=[FILL_WHITE] * NCOLS,
        fonts=[FONT_SUBTITULO] + [FONT_NORMAL] * 5,
    )
    r += 1

    # Fila 3: base_procesada
    write_row(
        ws,
        r,
        [
            "base_procesada.parquet",
            rucs_proc,
            est_proc,
            pct(est_proc, est_total_cons),
            "base_procesada",
            "1 fila por establecimiento, con ingreso y precision",
        ],
        fills=[FILL_LIGHT] * NCOLS,
        fonts=[FONT_BOLD] + [FONT_NORMAL] * 5,
        fmt=[None, "#,##0", "#,##0", "0.0%", None, None],
    )
    r += 1

    # Fila separador: Filtro 2
    write_row(
        ws,
        r,
        ["  ↓ Filtro 2: RIMPE (PN) + Sociedades $500K-$5M", "", "", "", "", ""],
        fills=[FILL_WHITE] * NCOLS,
        fonts=[FONT_SUBTITULO] + [FONT_NORMAL] * 5,
    )
    r += 1

    # Fila 4: base_filtrada
    reduccion_filt = est_proc - est_filt
    write_row(
        ws,
        r,
        [
            "base_filtrada.parquet",
            rucs_filt,
            est_filt,
            pct(est_filt, est_proc),
            "base_filtrada",
            f"-{reduccion_filt:,} establ. fuera de segmento",
        ],
        fills=[FILL_GREEN] * NCOLS,
        fonts=[FONT_BOLD] + [FONT_NORMAL] * 5,
        fmt=[None, "#,##0", "#,##0", "0.0%", None, None],
    )
    r += 1

    # Fila: tasa de conversión
    pct_rucs = pct(rucs_filt, rucs_total)
    pct_est = pct(est_filt, est_total_cons)
    write_row(
        ws,
        r,
        ["Tasa de conversion total", pct_rucs, pct_est, "", "", ""],
        fills=[FILL_HEADER] * NCOLS,
        fonts=[FONT_HEADER] * NCOLS,
        fmt=[None, "0.00%", "0.00%", None, None, None],
    )

    # ── SECCIÓN 2: DESGLOSE POR TIPO CONTRIBUYENTE ───────────────────
    r += 3
    write_title(ws, r, 1, "2. DESGLOSE POR TIPO DE CONTRIBUYENTE", NCOLS)
    r += 1
    write_headers(
        ws,
        r,
        ["Segmento", "Procesada", "Filtrada", "% que pasa filtro", "Motivo filtro", ""],
    )
    r += 1

    seg_data = [
        (
            "PERSONA NATURAL - RIMPE",
            rimpe_proc,
            pn_filt,
            "clase_contribuyente == 'RIMPE'",
        ),
        (
            "PERSONA NATURAL - GENERAL",
            general_proc - rimpe_proc + (pn_proc - general_proc),
            0,
            "No califica (no es RIMPE)",
        ),
        (
            "SOCIEDAD - Pequena ($500K-$990K)",
            soc_peq,
            soc_filt_peq,
            "ingreso_estimado en rango",
        ),
        (
            "SOCIEDAD - Mediana ($1M-$5M)",
            soc_med,
            soc_filt_med,
            "ingreso_estimado en rango",
        ),
        ("SOCIEDAD - Bajo $500K", soc_bajo_500k, 0, "Ingresos insuficientes"),
        ("SOCIEDAD - Sobre $5M", soc_sobre_5m, 0, "Ingresos exceden limite"),
        ("SOCIEDAD - Gap ($990K-$1M)", soc_gap, 0, "Fuera de rangos definidos"),
    ]

    for label, proc_n, filt_n, motivo in seg_data:
        fill = FILL_GREEN if filt_n > 0 else FILL_RED
        write_row(
            ws,
            r,
            [
                label,
                proc_n,
                filt_n,
                pct(filt_n, proc_n) if proc_n > 0 else 0,
                motivo,
                "",
            ],
            fills=[fill] * NCOLS,
            fmt=[None, "#,##0", "#,##0", "0.0%", None, None],
        )
        r += 1

    # Nota
    r += 1
    ws.cell(
        row=r,
        column=1,
        value=(
            "Nota: Las sociedades se filtran por ingreso_imputado (trimmed average × 14 periodos)."
        ),
    )
    apply_style(
        ws.cell(row=r, column=1),
        Font(name="Calibri", size=9, italic=True),
        alignment=ALIGN_LEFT,
    )
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=NCOLS)

    # ── SECCIÓN 3: CALIDAD DE DATOS (PRECISIÓN) ─────────────────────
    NCOLS_PREC = 8
    r += 3
    write_title(ws, r, 1, "3. CALIDAD DE DATOS - NIVEL DE PRECISION", NCOLS_PREC)
    r += 1
    write_headers(
        ws,
        r,
        [
            "Nivel",
            "Descripcion",
            "Establ. Procesada",
            "RUCs Procesada",
            "% Procesada",
            "Establ. Filtrada",
            "RUCs Filtrada",
            "% Filtrada",
        ],
        start_col=1,
    )
    r += 1

    prec_info = [
        (0, "Datos con ceros/nulos", FILL_RED),
        (1, "Incompleto (< 14 periodos)", FILL_YELLOW),
        (2, "Completo pero inestable", FILL_YELLOW),
        (3, "Completo y estable", FILL_GREEN),
    ]
    for nivel, desc, fill in prec_info:
        p = prec_proc_dict.get(nivel, {"n_est": 0, "n_rucs": 0})
        f = prec_filt_dict.get(nivel, {"n_est": 0, "n_rucs": 0})
        write_row(
            ws,
            r,
            [
                f"Nivel {nivel}",
                desc,
                p["n_est"],
                p["n_rucs"],
                pct(p["n_est"], est_proc),
                f["n_est"],
                f["n_rucs"],
                pct(f["n_est"], est_filt),
            ],
            fills=[fill] * NCOLS_PREC,
            fmt=[None, None, "#,##0", "#,##0", "0.0%", "#,##0", "#,##0", "0.0%"],
        )
        r += 1

    # ── SECCIÓN 4: COBERTURA DE VARIABLES (BASE FILTRADA) ───────────
    NCOLS_COB = 8
    r += 3
    write_title(
        ws, r, 1, "4. COBERTURA DE VARIABLES (BASE FILTRADA)", NCOLS_COB
    )
    r += 1
    write_headers(
        ws,
        r,
        [
            "Grupo",
            "Variable",
            "Establ. con datos",
            "Total Establ.",
            "% Establ.",
            "RUCs con datos",
            "Total RUCs",
            "% RUCs",
        ],
        start_col=1,
    )
    r += 1

    # Contactabilidad
    for col_name, (n_est, n_rucs) in cob_contacto.items():
        fill = (
            FILL_GREEN
            if pct(n_est, total_est_filt) > 0.5
            else (FILL_YELLOW if pct(n_est, total_est_filt) > 0.2 else FILL_RED)
        )
        write_row(
            ws,
            r,
            [
                "Contactabilidad",
                col_name,
                n_est,
                total_est_filt,
                pct(n_est, total_est_filt),
                n_rucs,
                total_rucs_filt,
                pct(n_rucs, total_rucs_filt),
            ],
            fills=[fill] * NCOLS_COB,
            fmt=[None, None, "#,##0", "#,##0", "0.0%", "#,##0", "#,##0", "0.0%"],
        )
        r += 1

    # Balances
    for col_name, (n_est, n_rucs) in cob_balance.items():
        fill = (
            FILL_GREEN
            if pct(n_est, total_est_filt) > 0.5
            else (FILL_YELLOW if pct(n_est, total_est_filt) > 0.2 else FILL_RED)
        )
        write_row(
            ws,
            r,
            [
                "Balances",
                col_name,
                n_est,
                total_est_filt,
                pct(n_est, total_est_filt),
                n_rucs,
                total_rucs_filt,
                pct(n_rucs, total_rucs_filt),
            ],
            fills=[fill] * NCOLS_COB,
            fmt=[None, None, "#,##0", "#,##0", "0.0%", "#,##0", "#,##0", "0.0%"],
        )
        r += 1

    # ── SECCIÓN 5: DISTRIBUCIÓN POR TIPO DE ACTIVIDAD ────────────────
    NCOLS_ACT = 5
    r += 3
    write_title(
        ws, r, 1, "5. BASE FILTRADA - DISTRIBUCION POR TIPO DE ACTIVIDAD", NCOLS_ACT
    )
    r += 1
    write_headers(
        ws,
        r,
        [
            "Tipo de Actividad",
            "Establecimientos",
            "% del Total",
            "Ingreso Prom. Estimado ($)",
            "Ejemplos (top 3 actividades)",
        ],
    )
    r += 1

    for row_data in tipo_act.iter_rows(named=True):
        tipo = row_data["tipo_actividad"]
        is_top = row_data["establecimientos"] >= 100
        fill = FILL_LIGHT if is_top else FILL_WHITE
        write_row(
            ws,
            r,
            [
                tipo,
                row_data["establecimientos"],
                pct(row_data["establecimientos"], est_filt),
                (
                    round(row_data["ingreso_prom_estimado"], 2)
                    if row_data["ingreso_prom_estimado"]
                    else 0
                ),
                ejemplos_por_tipo.get(tipo, ""),
            ],
            fills=[fill] * NCOLS_ACT,
            fonts=[FONT_BOLD if is_top else FONT_NORMAL] + [FONT_NORMAL] * 4,
            fmt=[None, "#,##0", "0.0%", "$#,##0", None],
        )
        r += 1

    # Total
    write_row(
        ws,
        r,
        ["TOTAL", est_filt, 1.0, "", ""],
        fills=[FILL_HEADER] * NCOLS_ACT,
        fonts=[FONT_HEADER] * NCOLS_ACT,
        fmt=[None, "#,##0", "0.0%", None, None],
    )

    # ── Ajustar anchos de columna ────────────────────────────────────
    col_widths = [42, 18, 20, 22, 22, 18, 18, 80]
    for i, w in enumerate(col_widths):
        ws.column_dimensions[get_column_letter(i + 1)].width = w

    # Freeze panes
    ws.freeze_panes = "A2"

    # ── HOJA 2: GLOSARIO DE COLUMNAS (BASE FILTRADA) ─────────────────
    ws_glos = wb.create_sheet("Glosario")
    ws_glos.sheet_properties.tabColor = "2E75B6"

    glosario = [
        ("id_establecimiento", "Identificador único del establecimiento (RUC + número de establecimiento)"),
        ("numero_ruc", "Registro Único de Contribuyentes del SRI"),
        ("numero_establecimiento", "Número secuencial del establecimiento dentro del RUC"),
        ("razon_social", "Nombre legal registrado en el SRI"),
        ("nombre_fantasia_comercial", "Nombre comercial o de marca del establecimiento"),
        ("clase_contribuyente", "Clasificación tributaria: RIMPE, GENERAL, ESPECIAL, etc."),
        ("tipo_contribuyente", "PERSONA NATURAL o SOCIEDAD"),
        ("actividad_economica", "Código o descripción de la actividad económica principal"),
        ("provincia", "Provincia donde se ubica el establecimiento (parseada de direccion_completa)"),
        ("canton", "Cantón donde se ubica el establecimiento (parseado de direccion_completa)"),
        ("num_periodos", "Cantidad de periodos con datos de facturación (máximo 14)"),
        ("precision", "Nivel de calidad: 0=ceros/nulos, 1=incompleto (<14 periodos), 2=completo inestable, 3=completo estable"),
        ("ingreso_estimado",
         "Ingreso estimado en USD. Precision 1-2: suma de total_facturas por periodo, "
         "reemplazando outliers bajos (<mejor_promedio×0.8) con el mejor_promedio. "
         "Precision 0: mediana del grupo (actividad_economica) × 14 periodos."),
        ("tipo_actividad", "Categoría de actividad económica (mapeada desde actividades_economicas.csv)"),
    ]

    write_title(ws_glos, 1, 1, "GLOSARIO DE COLUMNAS - BASE FILTRADA", 3)
    write_headers(ws_glos, 2, ["Columna", "Tipo", "Descripcion"])
    for i, (col_name, desc) in enumerate(glosario, start=3):
        fill = FILL_LIGHT if i % 2 == 0 else FILL_WHITE
        # Inferir tipo de dato
        tipo = "str"
        if col_name in ("id_establecimiento", "num_periodos", "precision"):
            tipo = "int"
        elif col_name == "ingreso_estimado":
            tipo = "float"
        write_row(ws_glos, i, [col_name, tipo, desc], fills=[fill] * 3)

    ws_glos.column_dimensions["A"].width = 28
    ws_glos.column_dimensions["B"].width = 10
    ws_glos.column_dimensions["C"].width = 90
    ws_glos.freeze_panes = "A3"

    # ── HOJA 3: RUCs POR PROVINCIA ─────────────────────────────────
    ws_prov = wb.create_sheet("RUCs por Provincia")
    ws_prov.sheet_properties.tabColor = "E2EFDA"

    # Para cada RUC: contar establecimientos por provincia y calcular %
    ruc_prov = (
        df_filt.group_by(["numero_ruc", "provincia"])
        .agg(pl.col("id_establecimiento").n_unique().alias("n_est"))
    )
    ruc_total = ruc_prov.group_by("numero_ruc").agg(
        pl.col("n_est").sum().alias("total_est")
    )
    ruc_prov = ruc_prov.join(ruc_total, on="numero_ruc")
    ruc_prov = ruc_prov.with_columns(
        (pl.col("n_est") / pl.col("total_est")).alias("pct_est")
    ).sort(["numero_ruc", "n_est"], descending=[False, True])

    # Pivotar: una fila por RUC, una columna por provincia
    provincias = sorted(
        ruc_prov.filter(pl.col("provincia").is_not_null())["provincia"].unique().to_list()
    )

    write_title(ws_prov, 1, 1, "DISTRIBUCION DE ESTABLECIMIENTOS POR PROVINCIA (% por RUC)", len(provincias) + 2)
    headers_prov = ["RUC", "Total Establ."] + provincias
    write_headers(ws_prov, 2, headers_prov)

    # Construir pivot
    pivot = ruc_prov.pivot(
        on="provincia", index="numero_ruc", values="pct_est",
    ).join(ruc_total, on="numero_ruc").sort("total_est", descending=True)

    r_prov = 3
    for row_data in pivot.iter_rows(named=True):
        ruc = row_data["numero_ruc"]
        total = row_data["total_est"]
        vals = [ruc, total]
        fmts = [None, "#,##0"]
        for prov in provincias:
            v = row_data.get(prov)
            vals.append(v if v is not None else 0)
            fmts.append("0.0%")
        fill = FILL_LIGHT if r_prov % 2 == 0 else FILL_WHITE
        write_row(ws_prov, r_prov, vals, fills=[fill] * len(vals), fmt=fmts)
        r_prov += 1

    ws_prov.column_dimensions["A"].width = 18
    ws_prov.column_dimensions["B"].width = 14
    for i in range(len(provincias)):
        ws_prov.column_dimensions[get_column_letter(i + 3)].width = 16
    ws_prov.freeze_panes = "C3"

    # ── Guardar ──────────────────────────────────────────────────────
    wb.save(REPORTE_XLSX)
    print(f"Reporte guardado en: {REPORTE_XLSX}")


if __name__ == "__main__":
    generar_reporte()
