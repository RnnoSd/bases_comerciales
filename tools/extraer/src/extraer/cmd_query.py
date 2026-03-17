from extraer.config import TraerFactConfig
from rich.console import Console
from rich.syntax import Syntax
from pathlib import Path
import os


def cmd_query(args):
    console = Console()
    ruta_config = Path(args.config)
    config = TraerFactConfig.cargar(ruta_config)
    dir_sql = ruta_config.parent / "sql"

    if not dir_sql.exists():
        console.print(f"[red]No se encontró el directorio: {dir_sql}/[/red]")
        return

    if args.list:
        archivos = sorted(dir_sql.glob("*.sql"))
        if not archivos:
            console.print(f"[yellow]No hay archivos .sql en {dir_sql}/[/yellow]")
            return
        console.print(f"\n[bold]Archivos SQL en {dir_sql}/:[/bold]")
        for sql in archivos:
            marcador = "[green]*[/green]" if sql.name in config.consultas else " "
            console.print(f"  {marcador} {sql.name}")
        if any(sql.name in config.consultas for sql in archivos):
            console.print(f"\n  [green]*[/green] = tiene configuración en consultas")

    elif args.show:
        ruta_sql = dir_sql / args.show
        if not ruta_sql.exists():
            console.print(f"[red]No se encontró: {ruta_sql}[/red]")
            return
        contenido = ruta_sql.read_text(encoding="utf8")
        syntax = Syntax(contenido, "sql", theme="monokai", line_numbers=True)
        console.print(f"\n[bold]Archivo:[/bold] {ruta_sql}\n")
        console.print(syntax)

    elif args.editar:
        ruta_sql = dir_sql / args.editar
        if not ruta_sql.exists():
            console.print(f"[red]No se encontró: {ruta_sql}[/red]")
            return
        editor = args.editor or os.environ.get("EDITOR", "nano")
        console.print(
            f"[bold]Abriendo[/bold] {ruta_sql} [bold]con[/bold] {editor}..."
        )
        os.execlp(editor, editor, str(ruta_sql))
