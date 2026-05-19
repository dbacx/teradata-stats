-- =============================================================================
-- Component 3.03: Suspected Unused Objects
-- =============================================================================
-- Description : Identifies tables > 1GB that have zero recorded accesses 
--               in DBQL over the last 30 days. Safe read-only output.
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- =============================================================================

WITH Tablas_Fisicas AS (
    SELECT DatabaseName, TableName, SUM(CAST(CurrentPerm AS FLOAT)) AS CurrentPerm_Bytes
    FROM DBC.TableSizeV
    WHERE UPPER(DatabaseName) NOT IN (
    'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT','TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM','LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR','TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA', 'PDCRSTG','SYSDBA','CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB','TDAAS_DB','baradmin','BARUSERS','Crashdumps','External_AP','lab1canales','labs_group_lab','LockLogShredder','PDCRADM','PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
	)
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 1073741824 -- > 1 GB
),
Uso_PDCR AS (
    SELECT ObjectDatabaseName, ObjectTableName, SUM(FreqofUse) AS Access_Count
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate >= CURRENT_DATE - 30
      AND ObjectType = 'Tab'
    GROUP BY 1, 2
)
SELECT 
    tf.DatabaseName, 
    tf.TableName,
    CAST(tf.CurrentPerm_Bytes / 1073741824.0 AS DECIMAL(18,2)) AS Size_GB,
    COALESCE(u.Access_Count, 0) AS Accesos_30D,
    'Flagged for Archiving Review' AS Status
FROM Tablas_Fisicas tf
LEFT JOIN Uso_PDCR u
    ON tf.DatabaseName = u.ObjectDatabaseName
    AND tf.TableName = u.ObjectTableName
WHERE COALESCE(u.Access_Count, 0) = 0
ORDER BY Size_GB DESC;