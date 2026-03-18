Necesitamos ver la base 'bases/base_consolidada.parquet' y describir todas las columnas que allí aparecen junto con la cobertura de cada una de ellas.
A parte agrupemos a las variables de la base en estas cuatro parte:
+ informacion_general
+ balances
+ informacion_ventas
+ contactabilidad
Empecemos por definir estos modificando este mismo .md. El análisis de cobertura también hazlo sobre este grupo de variables.
Tienes que ser cuidadoso porque hay información repétida pues la base incluye informacion_ventas de varios periodos entonces para esas consultas debes sacar únicos.

## Descripción de la base

**Shape**: 4,565,209 filas x 20 columnas
**Establecimientos únicos**: 38,404
**Periodos disponibles**: 2025/01 a 2026/02 (14 meses)

#### Grupo: informacion_general
| Columna | Tipo | Cobertura (únicos) |
|---|---|---|
| `numero_ruc` | Int64 | 38,404/38,404 (100.0%) |
| `numero_establecimiento` | Int64 | 38,404/38,404 (100.0%) |
| `razon_social` | String | 38,404/38,404 (100.0%) |
| `nombre_fantasia_comercial` | String | 38,404/38,404 (100.0%) |
| `clase_contribuyente` | String | 38,404/38,404 (100.0%) |
| `tipo_contribuyente` | String | 38,404/38,404 (100.0%) |
| `actividad_economica` | String | 38,404/38,404 (100.0%) |
| `id_establecimiento` | Int64 | 38,403/38,404 (100.0%) |

Valores de `clase_contribuyente`: `''`, `GENERAL`, `RIMPE`, `SIMPLIFICADO SOCIEDADES`
Valores de `tipo_contribuyente`: `''`, `PERSONA NATURAL`, `SOCIEDAD`

#### Grupo: balances
| Columna | Tipo | Cobertura (únicos) |
|---|---|---|
| `nombre` | String | 18,863/38,404 (49.1%) |
| `descripcion_rama` | String | 18,863/38,404 (49.1%) |
| `valor_balance_2024` | Float64 | 18,863/38,404 (49.1%) |

#### Grupo: informacion_ventas
Cobertura calculada sobre filas con `periodo` no nulo (304,377 filas = 38,403 establecimientos x hasta 14 periodos).

| Columna | Tipo | Cobertura (filas con periodo) | >0 |
|---|---|---|---|
| `periodo` | String | 304,377/304,377 (100.0%) | - |
| `numero_facturas` | Float64 | 304,377/304,377 (100.0%) | 304,377 (100.0%) |
| `total_facturas` | Float64 | 304,377/304,377 (100.0%) | 304,362 (100.0%) |
| `ticket_promedio` | Float64 | 304,377/304,377 (100.0%) | 304,073 (99.9%) |

#### Grupo: contactabilidad
| Columna | Tipo | Cobertura (únicos) |
|---|---|---|
| `direccion_completa` | String | 15,171/38,404 (39.5%) |
| `email` | String | 15,171/38,404 (39.5%) |
| `telefono` | Float64 | 15,171/38,404 (39.5%) |
| `telefono_representante` | Float64 | 15,171/38,404 (39.5%) |
| `email_representante` | String | 15,171/38,404 (39.5%) |

---

# Crear actividades_economicas:
De la columna `actividad_economica` por favor guarda las diferentes actividades_economicas en la menor cantidad de grupos que no se sobrepongan. Este resultado metela dentro de una tabla en un archivo plano dónde indica a cada `actividad_economica` le asignas una clase de `tipo_actividad` que argupa de manera comercial esto.
# Crear base_procesada
Por cada `id_establecimiento` necesito `ingreso_reportado` e `ingreso_estimado` que para `ingreso_reportado` sería la suma de montos y para el estimado sería "`ticket_promedio`*`numero_facturas`" por periodo y luego la suma de ellos, pero también añade una columna que diga `precisión`, que a cada `id_establecimiento` le dice que nivel de confianza en la información acumula en el ingreso, es decir, nivel 0 poseé algún periodo con 0 o nulo en cualquiera de las tres variables utilizadas. nivel 1 no tiene ningún campo vacío pero no es estable todo lo reportado está por encima de un threshold por variable que es calcular (escoger el mejor promedio es decir, se utiliza solo los periodos que para el promedio mantienen la distancia de esta con cada uno de los involucrados lo menor posible) y con eso haces un rango +- 20% de ese promedio.

Cuando tengamos eso, entonces debemos crear una 'bases/base_filtrada.parquet' a partir de la otra realizando el mapeo con el `tipo_actividad` creado, aplicando estos filtros
## FILTRO:
* Personas Naturales: RIMPE Negocio Popular (esto lo sacas de la columna `clase_contribuyente`)
* Sociedades: Pequeñas empresas (ingresos desde $500K a $990K) y medianas empresas (ingresos desde 1M a 5M)
Luego agrupa por 'actividad_economica'

Aquí, podemos implementar algo como establecer un parametro mínimo para filtrar. Me refiero a que si la información está muy vacia entonces cierto registro se debe eliminar y pasar a una base llamada base_descartados.parquet y los que pasaron se llamará base_seleccionados.parquet. La pregunta es cómo escoger aquello, y si esos parametros se escogen calculando alguna base entonces podemos realizar iteraciones sobre bases hasta encontrar un parametro estable. Pero eso aún no lo tengo claro.

Con esa tabla necesito hacer un resumen descriptivo. En este sentido cobertura de variables de cada `tipo_actividad` y en los casos que quepa un promedio.

Esto se debe consolidar en un script de python dentro del directorio scripts Usa python 3.11. Y los resumenes tienen que usar polars

Cuando tengas eso. Debemos realizar un informe con los resumenes sacados al final de este módulo.
Vuelve a leer este .md luego de las modificaciones para que todo quedé claro.
