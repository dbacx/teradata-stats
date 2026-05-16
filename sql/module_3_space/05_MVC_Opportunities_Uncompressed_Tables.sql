-- =============================================================================
-- Component 3.05: MVC Opportunities for Uncompressed Tables
-- =============================================================================
-- Description : Identifies large tables (>10GB) that have ZERO compressed 
--               columns. Immediate candidate for block-level or MVC compression.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

WITH Tablas_Fisicas AS (
    SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm_Bytes
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 10737418240 -- > 10 GB
),
Compresion_Status AS (
    SELECT DatabaseName, TableName, 
           COUNT(ColumnName) AS Total_Columns,
           SUM(CASE WHEN Compressible = 'C' THEN 1 ELSE 0 END) AS Compressed_Columns
    FROM DBC.ColumnsV
    GROUP BY 1, 2
)
SELECT 
    tf.DatabaseName,
    tf.TableName,
    CAST(tf.CurrentPerm_Bytes / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    cs.Total_Columns,
    'Zero Compression. Implement MVC via ALTER TABLE.' AS Diagnostico
FROM Tablas_Fisicas tf
INNER JOIN Compresion_Status cs
    ON tf.DatabaseName = cs.DatabaseName
    AND tf.TableName = cs.TableName
WHERE cs.Compressed_Columns = 0
  AND tf.DatabaseName NOT IN ('DBC', 'PDCRDATA', 'SYSDBA')
ORDER BY Size_GB DESC;