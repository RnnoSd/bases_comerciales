import polars as pl
from pathlib import Path
from rich.console import Console


def cargar_parquets(carpeta: Path) -> dict[str, pl.DataFrame]:
    """Carga todos los parquets de una carpeta en un diccionario."""
    return {f.stem: pl.read_parquet(f) for f in carpeta.glob("*.parquet")}


def preparar_balances(df_balances: pl.DataFrame) -> pl.DataFrame:
    """Limpia y renombra el dataframe de balances."""
    return df_balances.drop(
        "anio", "rama_actividad", "ciiu", "cuenta_numero", "expediente", "cuenta"
    ).rename({"valor": "valor_balance_2024", "ruc": "numero_ruc"})


def consolidar(carpeta: Path) -> pl.DataFrame:
    """Consolida información general, facturación, balances y contactabilidad.

    Parameters
    ----------
    carpeta : Path
        Ruta a la carpeta que contiene los archivos parquet.

    Returns
    -------
    pl.DataFrame
        DataFrame consolidado con todos los cruces.
    """
    dfs = cargar_parquets(carpeta)

    df_info_general = dfs["informacion_general_0"]
    cedula_representantes = dfs["cedula_representantes_0"].select(
        "numero_ruc", "cedula_representante_legal"
    )

    # Separar facturas y balances
    dfs_facturas = {k: v for k, v in dfs.items() if "fact" in k}
    dfs_balances = {k: v for k, v in dfs.items() if "balance" in k}

    # Preparar balances
    dfs_balances["balances_0"] = preparar_balances(dfs_balances["balances_0"])

    # Concatenar facturas y renombrar
    consolidado_facturas = pl.concat(dfs_facturas.values()).rename(
        {"codigo_establecimiento": "numero_establecimiento"}
    )

    # Castear numero_ruc a Int64
    df_info_general = df_info_general.with_columns(pl.col("numero_ruc").cast(pl.Int64))
    consolidado_facturas = consolidado_facturas.with_columns(
        pl.col("numero_ruc").cast(pl.Int64)
    )
    cedula_representantes = cedula_representantes.with_columns(
        pl.col("numero_ruc").cast(pl.Int64)
    )

    # Joins sucesivos
    df = (
        df_info_general.join(
            consolidado_facturas,
            on=["numero_ruc", "numero_establecimiento"],
            how="left",
        )
        .join(dfs_balances["balances_0"], on="numero_ruc", how="left")
        .join(cedula_representantes, on="numero_ruc", how="left")
    )

    # Normalizar cedula_representante_legal a String (10 dígitos con leading zeros)
    df = df.with_columns(
        pl.col("cedula_representante_legal")
        .cast(pl.Int64)
        .cast(pl.Utf8)
        .str.zfill(10)
    )

    # Personas naturales: cédula = RUC (13 dígitos) sin los últimos 3 ("001")
    df = df.with_columns(
        pl.when(
            (pl.col("tipo_contribuyente") == "PERSONA NATURAL")
            & pl.col("cedula_representante_legal").is_null()
        )
        .then(pl.col("numero_ruc").cast(pl.Utf8).str.zfill(13).str.slice(0, 10))
        .otherwise(pl.col("cedula_representante_legal"))
        .alias("cedula_representante_legal")
    )

    return df


if __name__ == "__main__":
    console = Console()
    carpeta = Path(__file__).resolve().parent.parent / "extraer_bendo" / "backups"
    RUTA_base_consolidada = (
        Path(__file__).resolve().parent.parent / "bases" / "base_consolidada.parquet"
    )
    df_final = consolidar(carpeta)
    df_final.write_parquet(RUTA_base_consolidada)
    console.print(
        f"Se ha guardado la consolidación en: {RUTA_base_consolidada.resolve()}"
    )
    console.print(df_final.shape)
