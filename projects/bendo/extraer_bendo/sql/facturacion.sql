
-- CREATE TEMPORARY TABLE temp_rucs_no_aceptados_union AS 
-- WITH rucs_no_aceptados_bases_catastros AS (
--   SELECT 
--     DISTINCT ruc AS numero_ruc 
--   FROM base_catastro_grandes_contribuyentes
-- 
--   UNION ALL
-- 
--   SELECT 
--     DISTINCT ruc AS numero_ruc
--   FROM base_catastro_empresas_fantasma
-- ),
-- rucs_no_aceptados_estados_base_rucs_sri AS (
--   SELECT
--     DISTINCT numero_ruc
--   FROM base_rucs_sri
--   WHERE cod_estado_contribuyente NOT IN ("ACTIVO")
-- )
-- SELECT numero_ruc
-- FROM rucs_no_aceptados_estados_base_rucs_sri
-- 
-- UNION ALL
-- 
-- SELECT numero_ruc
-- FROM rucs_no_aceptados_bases_catastros;


SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_01 LIMIT 10;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_02;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_03;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_04;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_05;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_06;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_07;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_08;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_09;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_10;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_11;

SELECT
  CAST(ruc_vendedor AS UNSIGNED) AS numero_ruc,
  CAST(codigo_establecimiento AS UNSIGNED) AS codigo_establecimiento,
  CAST(id_establecimiento AS UNSIGNED) AS id_establecimiento,
  DATE_FORMAT(fecha, "%Y/%m") AS periodo,
  clave_acceso,
  total
FROM df_data_general_facturas_2025_12;

