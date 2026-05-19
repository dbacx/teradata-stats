-- =============================================================================
-- Component 3.08: Top 20 Tables By Size
-- =============================================================================
-- Description : Identifies the 20 largest physical tables in the cluster.
--               Includes Skew calculation to detect bad Primary Index choices.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT TOP 20
    DatabaseName,
    TableName,
    CAST(SUM(CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Total_Size_GB,
    CAST(((MAX(CurrentPerm) - AVG(CurrentPerm)) / NULLIF(AVG(CurrentPerm), 0)) * 100 AS DECIMAL(5,2)) AS Skew_Pct
FROM DBC.TableSizeV
WHERE DatabaseName NOT IN (
    'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT','TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM','LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR','TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA', 'PDCRSTG','SYSDBA','CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB','TDAAS_DB','baradmin','BARUSERS','Crashdumps','External_AP','lab1canales','labs_group_lab','LockLogShredder','PDCRADM','PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
	)
GROUP BY 1, 2
ORDER BY Total_Size_GB DESC;