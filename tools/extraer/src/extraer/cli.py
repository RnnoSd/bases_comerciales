from pathlib import Path
import argparse, sys

from rich.console import Console


def _detectar_proyecto() -> Path | None:
    """Busca directorios extraer_*/extraer.json en el cwd."""
    proyectos = sorted(
        p for p in Path.cwd().glob("extraer_*/extraer.json") if p.is_file()
    )
    console = Console()

    if len(proyectos) == 1:
        return proyectos[0]

    if len(proyectos) > 1:
        console.print("[yellow]Se encontraron varios proyectos:[/yellow]")
        for i, p in enumerate(proyectos, 1):
            console.print(f"  {i}. {p.parent.name}/")
        try:
            opcion = input("Escoge un número: ").strip()
            idx = int(opcion) - 1
            if 0 <= idx < len(proyectos):
                return proyectos[idx]
        except (ValueError, EOFError):
            pass
        console.print("[red]Opción inválida.[/red]")
        return None

    console.print("[red]No se encontró ningún proyecto extraer_*/ en el directorio actual.[/red]")
    console.print("Usa 'extraer init <nombre>' para crear uno.")
    return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Herramienta para extraer datos desde MySQL y guardarlos como Parquet"
    )
    subparsers = parser.add_subparsers(dest="comando")

    # --- init ---
    init_parser = subparsers.add_parser(
        "init", help="Crea un nuevo proyecto de extracción"
    )
    init_parser.add_argument(
        "nombre",
        nargs="?",
        default=None,
        help="Nombre del proyecto (se creará extraer_{nombre}/)",
    )

    # --- config ---
    subparsers.add_parser(
        "config", help="Muestra la configuración y archivos SQL disponibles"
    )

    # --- query ---
    query_parser = subparsers.add_parser(
        "query", help="Gestionar los archivos SQL del proyecto"
    )
    query_action = query_parser.add_mutually_exclusive_group(required=True)
    query_action.add_argument(
        "--list",
        action="store_true",
        help="Lista los archivos .sql disponibles",
    )
    query_action.add_argument(
        "--show",
        type=str,
        metavar="ARCHIVO",
        help="Muestra el contenido de un .sql",
    )
    query_action.add_argument(
        "--editar",
        type=str,
        metavar="ARCHIVO",
        help="Abre un .sql en el editor",
    )
    query_parser.add_argument(
        "--editor",
        type=str,
        default=None,
        help="Editor a usar con --editar (default: $EDITOR o nano)",
    )

    # --- traer ---
    traer_parser = subparsers.add_parser(
        "traer", help="Ejecuta la extracción de datos"
    )
    traer_parser.add_argument(
        "--verbose", action="store_true", help="Activa mensajes detallados"
    )
    traer_parser.add_argument(
        "--sql",
        type=str,
        required=True,
        help="Nombre del archivo .sql dentro de sql/ a ejecutar",
    )
    traer_parser.add_argument(
        "--ruta-credenciales",
        type=str,
        required=True,
        help="Ruta al archivo .env con credenciales de la base de datos",
    )
    traer_parser.add_argument(
        "--valores",
        type=str,
        nargs="+",
        help="Valores a buscar o ruta a archivo .psv (obligatorio si el SQL tiene placeholders)",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    console = Console()

    if args.comando == "init":
        from extraer.cmd_init import cmd_init
        cmd_init(args)
        return

    if not args.comando:
        console.print("[yellow]Usa 'extraer init|config|query|traer'. Usa --help para más info.[/yellow]")
        sys.exit(1)

    # Auto-detectar proyecto
    ruta_config = _detectar_proyecto()
    if not ruta_config:
        sys.exit(1)

    args.config = str(ruta_config)
    console.print(f"[dim]Usando: {ruta_config.parent.name}/[/dim]")

    if args.comando == "config":
        from extraer.cmd_config import cmd_config
        cmd_config(args)
    elif args.comando == "query":
        from extraer.cmd_query import cmd_query
        cmd_query(args)
    elif args.comando == "traer":
        from extraer.cmd_traer import cmd_traer
        cmd_traer(args)


if __name__ == "__main__":
    main()
