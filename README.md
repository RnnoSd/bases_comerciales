# bases_comerciales

Proyecto para consolidar bases_comerciales. Todo respecto a esto, se debe colocar aquí.

## Estructura

```
├── tools/                    # Herramientas compartidas
│   ├── extract/              # Extracción (queries, APIs, scraping)
│   ├── transform/            # Transformación y limpieza
│   ├── load/                 # Carga a destinos
│   ├── validate/             # Validación de datos
│   └── utils/                # Utilidades comunes
│
├── projects/                 # Un directorio por proyecto/base
│   └── _template/            # Plantilla para nuevos proyectos
│       ├── scripts/          # Scripts constructores (versionado)
│       ├── data/             # Datos generados (ignorado por git)
│       ├── output/           # Salidas/reportes (ignorado por git)
│       └── config.yaml       # Configuración del proyecto (versionado)
```

## Uso

Para crear un nuevo proyecto:

```bash
cp -r projects/_template projects/mi_proyecto
```

Los directorios `data/` y `output/` de cada proyecto están excluidos de git.
Solo se versionan los scripts y la configuración.
