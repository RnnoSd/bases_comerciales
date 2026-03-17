SELECT 
  sri.direccion_completa,
  iess.ruc_empleador AS numero_ruc,
  '' AS email,
  iess.telefono_sucursal AS telefono,
  iess_contacto.telefono_afiliado AS telefono_representante,
  iess_contacto.email_afiliado AS email_representante
FROM base_iess iess
JOIN base_rucs_sri sri
ON iess.ruc_empleador = sri.numero_ruc
JOIN base_iess iess_contacto
ON iess_contacto.cedula_afiliado = iess.identificacion_representante_legal
GROUP BY ruc_empleador;
