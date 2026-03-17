from extraer.config import TraerFactConfig
from rich.console import Console
from pathlib import Path


def cmd_init(args):
    console = Console()

    if args.nombre:
        nombre_proyecto = args.nombre
    else:
        nombre_proyecto = input("Nombre del proyecto (se creará extraer_{nombre}/): ").strip()
        if not nombre_proyecto:
            console.print("[red]Nombre vacío, abortando.[/red]")
            return

    directorio = Path(f"extraer_{nombre_proyecto}")

    if directorio.exists():
        console.print(f"[yellow]Ya existe el directorio {directorio}/. No se sobreescribe.[/yellow]")
        return

    directorio.mkdir()
    (directorio / "sql").mkdir()

    config = TraerFactConfig()
    config.guardar(directorio / "extraer.json")

    ruta_psv = directorio / "valores.psv"
    ruta_psv.write_text(f"{config.columna_psv}\n", encoding="utf8")

    console.print(f"[green]Directorio creado: {directorio}/[/green]")
    console.print(f"  [blue]extraer.json[/blue]  — configuración")
    console.print(f"  [blue]valores.psv[/blue]   — completar con valores a buscar (columna: {config.columna_psv})")
    console.print(f"  [blue]sql/[/blue]          — colocar archivos .sql aquí")
