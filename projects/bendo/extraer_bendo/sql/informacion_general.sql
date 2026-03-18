SELECT 
  numero_ruc,
  numero_establecimiento, 
  razon_social, 
  nombre_fantasia_comercial,
  clase_contribuyente, 
  tipo_contribuyente, 
  actividad_economica 
FROM data_fact.base_rucs_sri 
WHERE estado_contribuyente != 'SUSPENDIDO';
