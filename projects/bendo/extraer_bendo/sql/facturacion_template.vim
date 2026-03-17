" Script para generar y mostrar queries SQL con saltos de línea
for y in range(2025, 2025)
  for m in range(1, 12)

    let lineas = [
          \ 'SELECT',
          \ '  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,',
          \ '  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,',
          \ '  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,',
          \ '  DATE_FORMAT(fecha, "%Y/%m") AS periodo,',
          \ '  total',
          \ 'FROM df_data_general_facturas_' . y . '_' . printf("%02d;", m),
          \ ''
          \ ]

    put = lineas

  endfor
endfor

