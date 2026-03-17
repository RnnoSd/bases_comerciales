from extraer.config import TraerFactConfig
from extraer.traer_datos import leer_y_guardar_datos_mysql
from rich.console import Console
from pathlib import Path
import sys, polars as pl


def cmd_traer(args):
    console = Console()
    ruta_config = Path(args.config)
    config = TraerFactConfig.cargar(ruta_config)
    dir_config = ruta_config.parent

    ruta_sql = dir_config / "sql" / args.sql
    if not ruta_sql.exists():
        console.print(f"[red]No se encontró: {ruta_sql}[/red]")
        sys.exit(1)

    # Verificar si el SQL tiene placeholders y se necesitan valores
    sql_text = ruta_sql.read_text(encoding="utf8")
    tiene_placeholder = f"{{{config.placeholder_sql}}}" in sql_text

    if tiene_placeholder and not args.valores:
        console.print(
            f"[red]El archivo {args.sql} contiene el placeholder "
            f"'{{{config.placeholder_sql}}}' pero no se proporcionó --valores.[/red]"
        )
        sys.exit(1)

    ruta_backups = dir_config / "backups"
    consulta = config.get_consulta(args.sql)

    # Resolver valores de búsqueda
    valores_busqueda = []
    if args.valores:
        if args.valores[0].endswith(".psv"):
            ruta_psv = Path(args.valores[0])
            try:
                if not ruta_psv.exists():
                    raise FileNotFoundError(
                        f"No se encontró el archivo .psv: {ruta_psv.resolve()}"
                    )
                tabla = pl.read_csv(ruta_psv, separator="|")
                valores_busqueda = list(
                    tabla[config.columna_psv].cast(pl.Utf8)
                )
            except FileNotFoundError as e:
                console.print(f"[red]{e}")
                sys.exit(1)
        else:
            valores_busqueda = args.valores

    ruta_credenciales = Path(args.ruta_credenciales)

    with console.status("Trayendo datos..."):
        leer_y_guardar_datos_mysql(
            valores_busqueda=valores_busqueda,
            ruta_sql=ruta_sql,
            ruta_backups=ruta_backups,
            placeholder_sql=config.placeholder_sql,
            prefijo_salida=consulta.prefijo_salida,
            regex_sufijo=consulta.regex_sufijo,
            ruta_credenciales=ruta_credenciales,
            param_verbose=args.verbose,
        )

    console.print("[green]Se terminó de traer todos los datos.[/green]")
    console.print(f"[blue]Resultados guardados en:[/blue] {ruta_backups.resolve()}")
