Necesitamos ver la base 'bases/base.parquet' y describir todas las columnas que allí aparecen junto con la cobertura de cada una de ellas.
# Crear actividades_economicas:
De la columna `actividad_economica` por favor guarda las diferentes actividades_economicas en la menor cantidad de grupos que no se sobrepongan. Este resultado metela dentro de una tabla en un archivo plano dónde indica a cada `actividad_economica` le asignas una clase de `tipo_actividad` que argupa de manera comercial esto. 

Cuando tengamos eso, entonces debemos crear una 'bases/base_filtrada.parquet' a partir de la otra realizando el mapeo con el `tipo_actividad` creado, aplicando estos filtros
## FILTRO: 
* Personas Naturales: RIMPE Negocio Popular (esto lo sacas de la columna `clase_contribuyente`)
* Sociedades: Pequeñas empresas (ingresos desde $500K a $990K) y medianas empresas (ingresos desde 1M a 5M)
Luego agrupa por 'actividad_economica' 

Aquí, podemos implementar algo como establecer un parametro mínimo para filtrar. Me refiero a que si la información está muy vacia entonces cierto registro se debe eliminar y pasar a una base llamada base_descartados.parquet y los que pasaron se llamará base_seleccionados.parquet. La pregunta es cómo escoger aquello, y si esos parametros se escogen calculando alguna base entonces podemos realizar iteraciones sobre bases hasta encontrar un parametro estable. Pero eso aún no lo tengo claro.

Con esa tabla necesito hacer un resumen descriptivo. En este sentido cobertura de variables de cada `tipo_actividad` y en los casos que quepa un promedio.

Esto se debe consolidar en un script de python. Y los resumenes tienen que usar polars

Cuando tengas eso. Debemos realizar un informe con los resumenes sacados al final de este módulo.

