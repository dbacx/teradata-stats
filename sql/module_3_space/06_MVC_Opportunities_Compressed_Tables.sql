-- =============================================================================
-- Component 3.06: MVC Opportunities for Compressed Tables
-- =============================================================================
-- Description : Identifies tables with some compression, but missing obvious 
--               candidates like DATE, BYTEINT, or CHAR(1) columns.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

WITH Tablas_Parcialmente_Comprimidas AS (
    SELECT DatabaseName, TableName
    FROM DBC.ColumnsV
    GROUP BY 1, 2
    HAVING SUM(CASE WHEN Compressible = 'C' THEN 1 ELSE 0 END) > 0
)
SELECT 
    c.DatabaseName,
    c.TableName,
    c.ColumnName,
    c.ColumnType,
    'Low-Hanging Fruit: ' || 
    CASE c.ColumnType 
        WHEN 'DA' THEN 'DATE' 
        WHEN 'I1' THEN 'BYTEINT' 
        WHEN 'CF' THEN 'CHAR(1)' 
    END || ' type uncompressed.' AS Diagnostico
FROM DBC.ColumnsV c
INNER JOIN Tablas_Parcialmente_Comprimidas tpc
    ON c.DatabaseName = tpc.DatabaseName
    AND c.TableName = tpc.TableName
INNER JOIN DBC.TableSizeV ts
    ON c.DatabaseName = ts.DatabaseName
    AND c.TableName = ts.TableName
WHERE c.Compressible <> 'C'
  AND c.ColumnType IN ('DA', 'I1', 'CF') 
  AND c.ColumnLength = 1 -- Específico para CHAR(1)
  AND c.DatabaseName NOT IN (
    'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT','TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM','LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR','TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA', 'PDCRSTG','SYSDBA','CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB','TDAAS_DB','baradmin','BARUSERS','Crashdumps','External_AP','lab1canales','labs_group_lab','LockLogShredder','PDCRADM','PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
	)
GROUP BY 1, 2, 3, 4
HAVING SUM(ts.CurrentPerm) > 5368709120 -- Solo tablas pesadas > 5GB
ORDER BY 1, 2;