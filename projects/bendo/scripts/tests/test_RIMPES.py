import polars as pl
from pathlib import Path

BASES = Path(__file__).resolve().parent.parent.parent / "bases"
proc = pl.read_parquet(BASES / "base_procesada.parquet")
filt = pl.read_parquet(BASES / "base_filtrada.parquet")

rimpe_proc = proc.filter(pl.col("clase_contribuyente") == "RIMPE")
rimpe_filt = filt.filter(pl.col("tipo_contribuyente") == "PERSONA NATURAL")
# IDs que están en procesada pero no en filtrada
perdidos = rimpe_proc.filter(
    ~pl.col("id_establecimiento").is_in(rimpe_filt["id_establecimiento"])
)
print(f"RIMPE en procesada: {rimpe_proc.height}")
print(f"PN en filtrada: {rimpe_filt.height}")
print(f"Perdidos: {perdidos.height}")
print(
    perdidos.select(
        "id_establecimiento", "clase_contribuyente", "tipo_contribuyente"
    ).head(20)
)
