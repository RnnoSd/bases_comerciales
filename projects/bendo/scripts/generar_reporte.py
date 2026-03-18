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
BASE_CONSOLIDADA = BASES_DIR / "base_consolidada.parquet"
BASE_PROCESADA = BASES_DIR / "base_procesada.parquet"
BASE_FILTRADA = BASES_DIR / "base_filtrada.parquet"
REPORTE_XLSX = BASES_DIR / "reporte_pipeline.xlsx"

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


def apply_style(cell, font=None, fill=None, alignment=None, border=None, number_format=None):
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
            start_row=row, start_column=col,
            end_row=row, end_column=col + colspan - 1,
        )
        for c in range(col + 1, col + colspan):
            apply_style(ws.cell(row=row, column=c), fill=FILL_TITULO, border=BORDER_THIN)


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

    # Clase contribuyente en procesada
    rimpe_proc = df_proc.filter(pl.col("clase_contribuyente") == "RIMPE").height
    general_proc = df_proc.filter(pl.col("clase_contribuyente") == "GENERAL").height

    # Sociedades por rango
    soc_df = df_proc.filter(pl.col("tipo_contribuyente") == "SOCIEDAD")
    soc_bajo_500k = soc_df.filter(pl.col("ingreso_reportado") < 500_000).height
    soc_peq_rep = soc_df.filter(
        (pl.col("ingreso_reportado") >= 500_000) & (pl.col("ingreso_reportado") <= 990_000)
    ).height
    soc_med_rep = soc_df.filter(
        (pl.col("ingreso_reportado") >= 1_000_000) & (pl.col("ingreso_reportado") <= 5_000_000)
    ).height
    soc_sobre_5m = soc_df.filter(pl.col("ingreso_reportado") > 5_000_000).height
    soc_gap = soc_df.height - soc_bajo_500k - soc_peq_rep - soc_med_rep - soc_sobre_5m

    # Precisión
    prec_proc = df_proc.group_by("precision").agg(pl.len().alias("n")).sort("precision")
    prec_dict = {row["precision"]: row["n"] for row in prec_proc.iter_rows(named=True)}

    prec_filt = df_filt.group_by("precision").agg(
        pl.col("id_establecimiento").n_unique().alias("n")
    ).sort("precision")
    prec_filt_dict = {row["precision"]: row["n"] for row in prec_filt.iter_rows(named=True)}

    # Filtrada: segmentos
    pn_filt = df_filt.filter(pl.col("tipo_contribuyente") == "PERSONA NATURAL")["id_establecimiento"].n_unique()
    soc_filt_uniq = df_filt.filter(pl.col("tipo_contribuyente") == "SOCIEDAD")["id_establecimiento"].n_unique()

    soc_rep_filt = df_filt.filter(pl.col("filtro_ingreso_usado") == "ingreso_reportado")
    soc_est_filt = df_filt.filter(pl.col("filtro_ingreso_usado") == "ingreso_estimado")

    soc_rep_peq = soc_rep_filt.filter(
        (pl.col("ingreso_reportado") >= 500_000) & (pl.col("ingreso_reportado") <= 990_000)
    ).height
    soc_rep_med = soc_rep_filt.filter(
        (pl.col("ingreso_reportado") >= 1_000_000) & (pl.col("ingreso_reportado") <= 5_000_000)
    ).height
    soc_est_peq = soc_est_filt.filter(
        (pl.col("ingreso_estimado") >= 500_000) & (pl.col("ingreso_estimado") <= 990_000)
    ).height
    soc_est_med = soc_est_filt.filter(
        (pl.col("ingreso_estimado") >= 1_000_000) & (pl.col("ingreso_estimado") <= 5_000_000)
    ).height

    # Cobertura contactabilidad/balances en consolidada (establ. únicos)
    est_uniq = est_con_id.unique(subset=["id_establecimiento"])
    cob_contacto = {}
    for col in ["direccion_completa", "email", "telefono", "telefono_representante", "email_representante"]:
        non_null = est_uniq.filter(
            pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        ).height
        cob_contacto[col] = non_null

    cob_balance = {}
    for col in ["nombre", "descripcion_rama", "valor_balance_2024"]:
        non_null = est_uniq.filter(
            pl.col(col).is_not_null() & (pl.col(col).cast(pl.Utf8) != "")
        ).height
        cob_balance[col] = non_null

    # Tipo actividad en filtrada
    tipo_act = (
        df_filt.group_by("tipo_actividad")
        .agg(
            pl.col("id_establecimiento").n_unique().alias("establecimientos"),
            pl.col("ingreso_reportado").mean().alias("ingreso_prom_reportado"),
            pl.col("ingreso_estimado").mean().alias("ingreso_prom_estimado"),
        )
        .sort("establecimientos", descending=True)
    )

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
    ws.cell(row=r, column=1, value="Embudo de datos: desde la base consolidada hasta la base filtrada final")
    apply_style(ws.cell(row=r, column=1), FONT_SUBTITULO, alignment=ALIGN_LEFT)
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=NCOLS)

    r += 2
    write_title(ws, r, 1, "1. EMBUDO GENERAL DE DATOS", NCOLS)
    r += 1
    write_headers(ws, r, ["Etapa", "RUCs", "Establecimientos", "% RUCs vs Inicio", "% Establ. vs Anterior", "Motivo de Reduccion"])
    r += 1

    # Fila 1: Consolidada (todos los RUCs)
    write_row(ws, r, [
        "Base Consolidada (todos los RUCs)", rucs_total, "N/A (sin id_establecimiento)",
        1.0, "", "Base origen con todos los registros del SRI"
    ], fills=[FILL_LIGHT]*NCOLS, fonts=[FONT_BOLD]+[FONT_NORMAL]*5,
       fmt=[None, "#,##0", None, "0.0%", None, None])
    r += 1

    # Fila 2: Consolidada con id_establecimiento
    write_row(ws, r, [
        "Con id_establecimiento", rucs_con_id, est_total_cons,
        pct(rucs_con_id, rucs_total), 1.0,
        f"{rucs_sin_id:,} RUCs sin info de establecimiento"
    ], fmt=[None, "#,##0", "#,##0", "0.0%", "0.0%", None])
    r += 1

    # Fila 3: Procesada
    write_row(ws, r, [
        "Base Procesada", rucs_proc, est_proc,
        pct(rucs_proc, rucs_total), pct(est_proc, est_total_cons),
        "1 registro con id_establecimiento nulo descartado"
    ], fills=[FILL_LIGHT]*NCOLS,
       fmt=[None, "#,##0", "#,##0", "0.0%", "0.0%", None])
    r += 1

    # Fila 4: Filtrada
    write_row(ws, r, [
        "Base Filtrada (establ. unicos)", rucs_filt, est_filt,
        pct(rucs_filt, rucs_total), pct(est_filt, est_proc),
        "Solo RIMPE (PN) + Sociedades $500K-$5M"
    ], fills=[FILL_GREEN]*NCOLS, fonts=[FONT_BOLD]+[FONT_NORMAL]*5,
       fmt=[None, "#,##0", "#,##0", "0.0%", "0.0%", None])

    # ── SECCIÓN 2: DESGLOSE POR TIPO CONTRIBUYENTE ───────────────────
    r += 3
    write_title(ws, r, 1, "2. DESGLOSE POR TIPO DE CONTRIBUYENTE", NCOLS)
    r += 1
    write_headers(ws, r, ["Segmento", "Procesada", "Filtrada", "% que pasa filtro", "Motivo filtro", ""])
    r += 1

    seg_data = [
        ("PERSONA NATURAL - RIMPE", rimpe_proc, pn_filt, "clase_contribuyente == 'RIMPE'"),
        ("PERSONA NATURAL - GENERAL", general_proc - rimpe_proc + (pn_proc - general_proc), 0, "No califica (no es RIMPE)"),
        ("SOCIEDAD - Pequena ($500K-$990K)", soc_peq_rep, soc_rep_peq, "ingreso_reportado en rango"),
        ("SOCIEDAD - Mediana ($1M-$5M)", soc_med_rep, soc_rep_med, "ingreso_reportado en rango"),
        ("SOCIEDAD - Bajo $500K", soc_bajo_500k, 0, "Ingresos insuficientes"),
        ("SOCIEDAD - Sobre $5M", soc_sobre_5m, 0, "Ingresos exceden limite"),
        ("SOCIEDAD - Gap ($990K-$1M)", soc_gap, 0, "Fuera de rangos definidos"),
    ]

    for label, proc_n, filt_n, motivo in seg_data:
        fill = FILL_GREEN if filt_n > 0 else FILL_RED
        write_row(ws, r, [
            label, proc_n, filt_n, pct(filt_n, proc_n) if proc_n > 0 else 0, motivo, ""
        ], fills=[fill]*NCOLS,
           fmt=[None, "#,##0", "#,##0", "0.0%", None, None])
        r += 1

    # Nota sobre estimado
    r += 1
    ws.cell(row=r, column=1, value=(
        "Nota: Las sociedades tambien se filtran por ingreso_estimado. "
        f"Por estimado: {soc_est_peq:,} pequenas + {soc_est_med:,} medianas. "
        f"Total establ. unicos en sociedades filtradas: {soc_filt_uniq:,}"
    ))
    apply_style(ws.cell(row=r, column=1), Font(name="Calibri", size=9, italic=True), alignment=ALIGN_LEFT)
    ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=NCOLS)

    # ── SECCIÓN 3: CALIDAD DE DATOS (PRECISIÓN) ─────────────────────
    r += 3
    write_title(ws, r, 1, "3. CALIDAD DE DATOS - NIVEL DE PRECISION", NCOLS)
    r += 1
    write_headers(ws, r, ["Nivel", "Descripcion", "Procesada", "% Procesada", "Filtrada (establ.)", "% Filtrada"])
    r += 1

    prec_info = [
        (0, "Datos incompletos (ceros/nulos)", FILL_RED),
        (1, "Datos completos pero inestables", FILL_YELLOW),
        (2, "Datos completos y estables", FILL_GREEN),
    ]
    for nivel, desc, fill in prec_info:
        n_proc = prec_dict.get(nivel, 0)
        n_filt = prec_filt_dict.get(nivel, 0)
        write_row(ws, r, [
            f"Nivel {nivel}", desc, n_proc, pct(n_proc, est_proc),
            n_filt, pct(n_filt, est_filt)
        ], fills=[fill]*NCOLS,
           fmt=[None, None, "#,##0", "0.0%", "#,##0", "0.0%"])
        r += 1

    # ── SECCIÓN 4: COBERTURA DE VARIABLES ────────────────────────────
    r += 3
    write_title(ws, r, 1, "4. COBERTURA DE VARIABLES (BASE CONSOLIDADA - ESTABL. UNICOS)", NCOLS)
    r += 1
    write_headers(ws, r, ["Grupo", "Variable", "Con datos", "Total", "Cobertura %", ""])
    r += 1

    # Contactabilidad
    for col_name, n in cob_contacto.items():
        fill = FILL_GREEN if pct(n, est_total_cons) > 0.5 else (FILL_YELLOW if pct(n, est_total_cons) > 0.2 else FILL_RED)
        write_row(ws, r, [
            "Contactabilidad", col_name, n, est_total_cons, pct(n, est_total_cons), ""
        ], fills=[fill]*NCOLS,
           fmt=[None, None, "#,##0", "#,##0", "0.0%", None])
        r += 1

    # Balances
    for col_name, n in cob_balance.items():
        fill = FILL_GREEN if pct(n, est_total_cons) > 0.5 else (FILL_YELLOW if pct(n, est_total_cons) > 0.2 else FILL_RED)
        write_row(ws, r, [
            "Balances", col_name, n, est_total_cons, pct(n, est_total_cons), ""
        ], fills=[fill]*NCOLS,
           fmt=[None, None, "#,##0", "#,##0", "0.0%", None])
        r += 1

    # Info ventas
    write_row(ws, r, [
        "Ventas", "numero_facturas / total_facturas / ticket_promedio",
        est_total_cons, est_total_cons, 1.0, "100% (14 periodos)"
    ], fills=[FILL_GREEN]*NCOLS,
       fmt=[None, None, "#,##0", "#,##0", "0.0%", None])
    r += 1

    # Info general
    write_row(ws, r, [
        "Info General", "ruc / razon_social / clase / tipo / actividad",
        est_total_cons, est_total_cons, 1.0, "100%"
    ], fills=[FILL_GREEN]*NCOLS,
       fmt=[None, None, "#,##0", "#,##0", "0.0%", None])

    # ── SECCIÓN 5: DISTRIBUCIÓN POR TIPO DE ACTIVIDAD ────────────────
    r += 3
    write_title(ws, r, 1, "5. BASE FILTRADA - DISTRIBUCION POR TIPO DE ACTIVIDAD", NCOLS)
    r += 1
    write_headers(ws, r, [
        "Tipo de Actividad", "Establecimientos", "% del Total",
        "Ingreso Prom. Reportado ($)", "Ingreso Prom. Estimado ($)", ""
    ])
    r += 1

    for row_data in tipo_act.iter_rows(named=True):
        is_top = row_data["establecimientos"] >= 100
        fill = FILL_LIGHT if is_top else FILL_WHITE
        write_row(ws, r, [
            row_data["tipo_actividad"],
            row_data["establecimientos"],
            pct(row_data["establecimientos"], est_filt),
            round(row_data["ingreso_prom_reportado"], 2) if row_data["ingreso_prom_reportado"] else 0,
            round(row_data["ingreso_prom_estimado"], 2) if row_data["ingreso_prom_estimado"] else 0,
            "",
        ], fills=[fill]*NCOLS, fonts=[FONT_BOLD if is_top else FONT_NORMAL]+[FONT_NORMAL]*5,
           fmt=[None, "#,##0", "0.0%", "$#,##0", "$#,##0", None])
        r += 1

    # Total
    write_row(ws, r, [
        "TOTAL", est_filt, 1.0, "", "", ""
    ], fills=[FILL_HEADER]*NCOLS, fonts=[FONT_HEADER]*NCOLS,
       fmt=[None, "#,##0", "0.0%", None, None, None])

    # ── Ajustar anchos de columna ────────────────────────────────────
    col_widths = [42, 18, 20, 22, 22, 40]
    for i, w in enumerate(col_widths):
        ws.column_dimensions[get_column_letter(i + 1)].width = w

    # Freeze panes
    ws.freeze_panes = "A2"

    # ── Guardar ──────────────────────────────────────────────────────
    wb.save(REPORTE_XLSX)
    print(f"Reporte guardado en: {REPORTE_XLSX}")


if __name__ == "__main__":
    generar_reporte()
