-- =============================================================================
-- Component 3.04: Suspected Duplicate Objects (Heuristics)
-- =============================================================================
-- Description : Detects potential duplicate tables based on common backup 
--               naming conventions (_BKP, _OLD, _TMP) and significant size.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT 
    DatabaseName,
    TableName,
    CAST(SUM(CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    CreateTimeStamp,
    'Posible Backup No Depurado' AS Razon
FROM DBC.TableSizeV
WHERE (
    UPPER(TableName) LIKE '%[_]BKP' ESCAPE '[' OR
    UPPER(TableName) LIKE '%[_]OLD' ESCAPE '[' OR
    UPPER(TableName) LIKE '%[_]TMP' ESCAPE '[' OR
    UPPER(TableName) LIKE '%[_]202%' ESCAPE '[' -- Tablas con año reciente hardcodeado
)
AND DatabaseName NOT IN ('DBC', 'PDCRDATA', 'SYSDBA')
GROUP BY 1, 2, 4
HAVING SUM(CurrentPerm) > 524288000 -- > 500 MB (Ignoramos basuritas)
ORDER BY Size_GB DESC;