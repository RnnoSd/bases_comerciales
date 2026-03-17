from dataclasses import dataclass, asdict, field
from pathlib import Path
import json


@dataclass
class ConsultaConfig:
    prefijo_salida: str = "resultado"
    regex_sufijo: str = ""


@dataclass
class TraerFactConfig:
    columna_psv: str = "id_establecimiento"
    placeholder_sql: str = "rucs_a_buscar"
    consultas: dict = field(default_factory=dict)

    @classmethod
    def cargar(cls, ruta: Path) -> "TraerFactConfig":
        with open(ruta, "r", encoding="utf8") as f:
            data = json.load(f)
        return cls(**data)

    def guardar(self, ruta: Path) -> None:
        with open(ruta, "w", encoding="utf8") as f:
            json.dump(asdict(self), f, indent=2, ensure_ascii=False)

    def get_consulta(self, nombre_sql: str) -> ConsultaConfig:
        if nombre_sql in self.consultas:
            return ConsultaConfig(**self.consultas[nombre_sql])
        return ConsultaConfig()
