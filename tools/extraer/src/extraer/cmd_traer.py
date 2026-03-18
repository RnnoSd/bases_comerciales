from extraer.config import TraerFactConfig
from extraer.traer_datos import leer_y_guardar_datos_mysql
from rich.console import Console
from pathlib import Path
import sys, polars as pl


def _resolver_valores(args, config, console):
    """Resuelve los valores de búsqueda desde --valores o retorna lista vacía."""
    if not args.valores:
        return []

    if args.valores[0].endswith(".psv"):
        ruta_psv = Path(args.valores[0])
        try:
            if not ruta_psv.exists():
                raise FileNotFoundError(
                    f"No se encontró el archivo .psv: {ruta_psv.resolve()}"
                )
            tabla = pl.read_csv(ruta_psv, separator="|")
            return list(tabla[config.columna_psv].cast(pl.Utf8))
        except FileNotFoundError as e:
            console.print(f"[red]{e}")
            sys.exit(1)
    else:
        return args.valores


def _traer_un_sql(args, config, dir_config, nombre_sql, valores_busqueda, console):
    """Ejecuta la extracción para un solo archivo SQL."""
    ruta_sql = dir_config / "sql" / nombre_sql
    if not ruta_sql.exists():
        console.print(f"[red]No se encontró: {ruta_sql}[/red]")
        return False

    sql_text = ruta_sql.read_text(encoding="utf8")
    tiene_placeholder = f"{{{config.placeholder_sql}}}" in sql_text

    if tiene_placeholder and not valores_busqueda:
        console.print(
            f"[red]El archivo {nombre_sql} contiene el placeholder "
            f"'{{{config.placeholder_sql}}}' pero no se proporcionó --valores.[/red]"
        )
        return False

    ruta_backups = dir_config / "backups"
    consulta = config.get_consulta(nombre_sql)

    console.print(f"\n[bold cyan]── {nombre_sql} ──[/bold cyan]")
    leer_y_guardar_datos_mysql(
        valores_busqueda=valores_busqueda,
        ruta_sql=ruta_sql,
        ruta_backups=ruta_backups,
        placeholder_sql=config.placeholder_sql,
        prefijo_salida=consulta.prefijo_salida,
        regex_sufijo=consulta.regex_sufijo,
        ruta_credenciales=Path(args.ruta_credenciales),
        param_verbose=args.verbose,
    )
    return True


def cmd_traer(args):
    console = Console()
    ruta_config = Path(args.config)
    config = TraerFactConfig.cargar(ruta_config)
    dir_config = ruta_config.parent

    valores_busqueda = _resolver_valores(args, config, console)

    if args.sql:
        # Modo individual: un solo archivo SQL
        ok = _traer_un_sql(args, config, dir_config, args.sql, valores_busqueda, console)
        if not ok:
            sys.exit(1)
    else:
        # Modo secuencial: todas las consultas configuradas
        if not config.consultas:
            console.print("[yellow]No hay consultas configuradas en extraer.json.[/yellow]")
            console.print("Usa --sql para especificar un archivo, o agrega consultas al config.")
            sys.exit(1)

        archivos_sql = list(config.consultas.keys())
        console.print(
            f"[bold]Ejecutando {len(archivos_sql)} consultas secuencialmente:[/bold] "
            + ", ".join(archivos_sql)
        )

        for nombre_sql in archivos_sql:
            ok = _traer_un_sql(args, config, dir_config, nombre_sql, valores_busqueda, console)
            if not ok:
                console.print(f"[red]Error procesando {nombre_sql}, continuando...[/red]")

    ruta_backups = dir_config / "backups"
    console.print(f"\n[green]Se terminó de traer todos los datos.[/green]")
    console.print(f"[blue]Resultados guardados en:[/blue] {ruta_backups.resolve()}")
