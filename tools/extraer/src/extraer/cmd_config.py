from extraer.config import TraerFactConfig
from rich.console import Console
from rich.syntax import Syntax
from dataclasses import asdict
from pathlib import Path
import json


def cmd_config(args):
    console = Console()
    ruta_config = Path(args.config)
    config = TraerFactConfig.cargar(ruta_config)
    dir_config = ruta_config.parent

    texto = json.dumps(asdict(config), indent=2, ensure_ascii=False)
    syntax = Syntax(texto, "json", theme="monokai")
    console.print(f"\n[bold]Configuración:[/bold] {ruta_config}\n")
    console.print(syntax)

    dir_sql = dir_config / "sql"
    if dir_sql.exists():
        archivos_sql = sorted(dir_sql.glob("*.sql"))
        if archivos_sql:
            console.print(f"\n[bold]Archivos SQL disponibles:[/bold]")
            for sql in archivos_sql:
                marcador = "[green]*[/green]" if sql.name in config.consultas else " "
                console.print(f"  {marcador} {sql.name}")
            if any(sql.name in config.consultas for sql in archivos_sql):
                console.print(f"\n  [green]*[/green] = tiene configuración en consultas")
        else:
            console.print(f"\n[yellow]No hay archivos .sql en {dir_sql}/[/yellow]")
