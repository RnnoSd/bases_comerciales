SELECT 
  sri.direccion_completa,
  iess.ruc_empleador AS numero_ruc,
  iess_contacto.cedula_afiliado AS cedula_representante_legal
FROM base_iess iess
JOIN base_rucs_sri sri
ON iess.ruc_empleador = sri.numero_ruc
JOIN base_iess iess_contacto
ON iess_contacto.cedula_afiliado = sri.identificacion_representante_legal
WHERE numero_ruc IN ({rucs_seleccionados})
GROUP BY iess.ruc_empleador;
