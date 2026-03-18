" Script para generar y mostrar queries SQL con saltos de línea
for y in range(2025, 2025)
  for m in range(1, 12)

    let lineas = [
          \ 'SELECT',
          \ '  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,',
          \ '  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,',
          \ '  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,',
          \ '  DATE_FORMAT(fecha, "%Y/%m") AS periodo,',
          \ '  clave_acceso,',
          \ '  total',
          \ 'FROM df_data_general_facturas_' . y . '_' . printf("%02d", m),
          \ 'WHERE ruc_vendedor NOT IN (SELECT DISTINCT numero_ruc FROM temp_rucs_no_acpetados_union)',
          \ 'LIMIT 10;',
          \ ''
          \ ]

    put = lineas

  endfor
endfor


