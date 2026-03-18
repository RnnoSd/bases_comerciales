" Script para generar y mostrar queries SQL con saltos de línea
for y in range(2025, 2026)
  for m in range(1, 12)
    if y == 2026 && m > 2
      continue
    endif

    let lineas = [
          \ 'SELECT',
          \ '  CAST(ruc AS UNSIGNED) AS numero_ruc,',
          \ '  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,',
          \ '  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,',
          \ '  "' . y. '/' . printf("%02d", m) . '" AS periodo,',
          \ '  numero_facturas,',
          \ '  total_facturas,',
          \ '  ticket_promedio',
          \ 'FROM df_resumen_tendencias_establecimiento_' . y . '_' . printf("%02d;", m),
          \ ''
          \ ]

    put = lineas

  endfor
endfor


