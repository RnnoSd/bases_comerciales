"""
extraer
Paquete aislado para traer datos de facturación desde MySQL.

Estructura:

extraer/
└── src/
    extraer/
    ├── __init__.py
    ├── config.py
    ├── cli.py
    ├── traer_datos.py
    └── consulta_formato.sql
"""

from .traer_datos import leer_y_guardar_datos_mysql
from .config import TraerFactConfig
