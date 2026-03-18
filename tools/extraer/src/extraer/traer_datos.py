from rich.console import Console
import polars as pl
from pathlib import Path
from typing import Dict, List, Any
from decimal import Decimal
from dotenv import load_dotenv
from contextlib import closing
from mysql.connector import ProgrammingError
import mysql.connector, os, sqlparse, re, shutil
from importlib.resources import files, as_file


def resolver_rutas() -> Dict[str, Path]:
    try:
        METARUTA_PACKAGE = files("extraer")
        RUTA_PROYECTO = None
        with as_file(METARUTA_PACKAGE) as RUTA_PACKAGE:
            RUTA_PROYECTO = RUTA_PACKAGE.parent.parent

        if not RUTA_PROYECTO:
            raise FileNotFoundError(
                "No se pudo obtener la ruta del proyecto extraer"
            )
        elif not RUTA_PROYECTO.exists():
            raise FileNotFoundError(
                f"No se encontró el directorio del proyecto: {RUTA_PROYECTO}"
            )

        RUTA_sql_executables = (
            RUTA_PROYECTO / "src/extraer/consulta_formato.sql"
        )
        if not RUTA_sql_executables.exists():
            raise FileNotFoundError(
                f"No se encontró consulta_formato.sql (SE BUSCO EN {RUTA_sql_executables.resolve()})"
            )
        RUTA_backups = RUTA_PROYECTO / "backups"
    except FileNotFoundError as e:
        raise FileNotFoundError(e)

    return {
        "RUTA_sql_executables": RUTA_sql_executables,
        "RUTA_backups": RUTA_backups,
    }


def guardar_resultados(
    query: str,
    nombre: str,
    cur: Any,
    RUTA_backups: Path,
    batch_size: int = 2_000_000,
):
    console = Console()
    cur.execute(query)

    total_filas = 0
    i = 0
    while True:
        i += 1
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        columnas = [desc[0] for desc in cur.description]
        rows = [
            tuple(float(v) if isinstance(v, Decimal) else v for v in fila)
            for fila in rows
        ]
        df_polars = pl.LazyFrame(rows, schema=columnas, orient="row")
        df_polars.sink_parquet(
            RUTA_backups / f"{nombre}_{i}.parquet",
            engine="streaming",
            mkdir=True,
        )
        total_filas += len(rows)

    if total_filas == 0:
        console.print(f"[yellow] No hay datos para {nombre}[/yellow]")
        return

    console.print(f"[green] Se obtuvieron {total_filas} filas para {nombre}[/green]")

    tablas = []
    for ruta in RUTA_backups.glob(f"{nombre}_*.parquet"):
        tabla = pl.scan_parquet(ruta)
        tablas.append(tabla)
    tabla_final = pl.concat(tablas)
    tabla_final.sink_parquet(
        RUTA_backups / f"{nombre}.parquet", engine="streaming"
    )
    for ruta in RUTA_backups.glob(f"{nombre}_*.parquet"):
        ruta.unlink()

    console.print(
        f"[blue in cyan] Archivo guardado: {RUTA_backups / f'{nombre}.parquet'}[/blue in cyan]"
    )


def _es_query_datos(query: str) -> bool:
    return query.strip().upper().startswith("SELECT")


def _crear_conexion(ruta_credenciales: Path) -> mysql.connector.MySQLConnection:
    load_dotenv(ruta_credenciales, override=True)
    user = os.getenv("USER_DATABASE")
    password = os.getenv("PASSWORD_DATABASE")
    host = os.getenv("HOST_DATABASE")
    database = os.getenv("NAME_DATABASE")
    port = os.getenv("PORT_DATABASE")
    if (not user) or (not password) or (not host) or (not database) or (not port):
        raise ValueError(
            "No se ha encontrado una de las siguientes variables de entorno: `user`, `password`, `host`, `database`, `port`."
        )
    return mysql.connector.connect(
        host=host, user=user, password=password, database=database, port=port
    )


def procesar_query(
    conexion: mysql.connector.MySQLConnection,
    query: str,
    RUTA_backups: Path,
    prefijo_salida: str,
    regex_sufijo: str,
    indice: int,
) -> int:
    console = Console()

    if regex_sufijo:
        match = re.search(regex_sufijo, query)
        sufijo = match.group(0) if match else str(indice)
    else:
        sufijo = str(indice)

    nombre = f"{prefijo_salida}_{sufijo}"

    with console.status(f"{nombre}") as status:
        with conexion.cursor() as cur:
            guardar_resultados(
                cur=cur, nombre=nombre, query=query, RUTA_backups=RUTA_backups
            )
        status.update(f"Se ha procesado {nombre}")
    return 0


def leer_y_guardar_datos_mysql(
    ruta_credenciales: Path,
    valores_busqueda: List[str],
    ruta_sql: Path,
    ruta_backups: Path,
    placeholder_sql: str = "rucs_a_buscar",
    prefijo_salida: str = "resultado",
    regex_sufijo: str = "",
    param_verbose: bool = False,
):
    ruta_backups.mkdir(parents=True, exist_ok=True)
    for archivo in ruta_backups.glob(f"{prefijo_salida}*"):
        archivo.unlink()
    console = Console()
    valores_sql = ",".join(
        [v for v in valores_busqueda if v is not None]
    )

    try:
        if not ruta_credenciales.exists():
            raise FileNotFoundError(
                "No se ha encontrado el archivo de variables de entorno."
            )
        if not ruta_sql.exists():
            raise FileNotFoundError(
                f"No se ha encontrado el archivo SQL: {ruta_sql}"
            )
        sql_text = ruta_sql.read_text(encoding="utf8")
        sql_text = sqlparse.format(sql_text, strip_comments=True)
        queries = [
            q.strip().replace(f"{{{placeholder_sql}}}", valores_sql)
            for q in sql_text.split(";")
            if q.strip()
        ]
        if param_verbose:
            dummy_valores_sql = (
                ",".join(valores_busqueda[0:2])
                + f", ...{len(valores_busqueda)}..., "
                + ",".join(valores_busqueda[-3:-1])
            )
            dummy_query = (
                sql_text.split(";")[0]
                .strip()
                .replace(f"{{{placeholder_sql}}}", dummy_valores_sql)
            )
            console.print(f"Una query de ejemplo es:\n{dummy_query}")

        with closing(_crear_conexion(ruta_credenciales)) as conexion:
            indice_datos = 0
            for query in queries:
                if _es_query_datos(query):
                    procesar_query(
                        conexion=conexion,
                        query=query,
                        RUTA_backups=ruta_backups,
                        prefijo_salida=prefijo_salida,
                        regex_sufijo=regex_sufijo,
                        indice=indice_datos,
                    )
                    indice_datos += 1
                else:
                    with conexion.cursor() as cur:
                        cur.execute(query)
                    console.print(f"[dim] Ejecutado: {query[:60]}...[/dim]")

    except ProgrammingError as e:
        console.print(f"[red] El motor de MySQL reporta el siguiente error:\n{e}")
    except FileNotFoundError:
        raise
