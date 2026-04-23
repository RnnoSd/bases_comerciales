WITH base_sweet_coffee AS (
  SELECT * FROM (
    SELECT 992106891001 AS ruc, 'MALL DEL SOL' AS centro_comercial, 'Sweet & Coffee' AS cafeteria, 40 AS codigo_establecimiento
    UNION ALL SELECT 992106891001, 'MALL DEL SOL', 'Sweet & Coffee', 146
    UNION ALL SELECT 1792141486001, 'MALL DEL SOL', 'Juan Valdez', 24
    UNION ALL SELECT 1792141486001, 'MALL DEL SOL', 'Juan Valdez', 59
    UNION ALL SELECT 992106891001, 'RIOCENTRO EL DORADO', 'Sweet & Coffee', 73
    UNION ALL SELECT 992106891001, 'RIOCENTRO EL DORADO', 'Sweet & Coffee', 72
    UNION ALL SELECT 992106891001, 'RIOCENTRO CEIBOS', 'Sweet & Coffee', 8
    UNION ALL SELECT 992106891001, 'RIOCENTRO CEIBOS', 'Sweet & Coffee', 116
    UNION ALL SELECT 1792141486001, 'RIOCENTRO CEIBOS', 'Juan Valdez', 19
    UNION ALL SELECT 1792141486001, 'SAN MARINO', 'Juan Valdez', 4
    UNION ALL SELECT 992106891001, 'SAN MARINO', 'Sweet & Coffee', 4
    UNION ALL SELECT 1792141486001, 'QUIPORT', 'Juan Valdez', 33
  ) AS t
)
SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_01' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_01 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_02' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_02 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_03' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_03 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_04' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_04 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_05' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_05 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_06' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_06 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_07' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_07 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_08' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_08 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_09' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_09 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_10' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_10 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_11' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_11 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2025_12' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2025_12 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2026_01' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2026_01 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2026_02' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2026_02 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL

UNION ALL

SELECT
    bsc.centro_comercial,
    bsc.cafeteria,
    bsc.ruc,
    bsc.codigo_establecimiento,
    bf.total,
    '2026_03' AS periodo
FROM base_sweet_coffee bsc
LEFT JOIN df_data_general_facturas_2026_03 bf
       ON bf.ruc_vendedor          = bsc.ruc
      AND bf.codigo_establecimiento = bsc.codigo_establecimiento
WHERE bsc.ruc IS NOT NULL AND bsc.codigo_establecimiento IS NOT NULL;
