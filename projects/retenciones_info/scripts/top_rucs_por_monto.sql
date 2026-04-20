SELECT
    numero_ruc,
    SUM(total_facturas)  AS total_monto,
    SUM(numero_facturas) AS total_facturas
FROM read_parquet('../extraer_retenciones_info/backups/ventas_*.parquet')
GROUP BY numero_ruc
ORDER BY total_monto DESC;
