-- =============================================================================
-- Component 3.01: Database Space Utilization (AMP-Aware)
-- =============================================================================
-- Description : Calculates space utilization considering AMP Skew.
--               Fixed Numeric Overflow (2616) using FLOAT casting.
--               Formatted for direct plotting in Streamlit/Pandas.
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT 
    DatabaseName,
    CAST(SUM(CAST(MaxPerm AS FLOAT)) / 1073741824.0 AS DECIMAL(18,2)) AS MaxPerm_GB,
    CAST(SUM(CAST(CurrentPerm AS FLOAT)) / 1073741824.0 AS DECIMAL(18,2)) AS CurrentPerm_GB,
    CAST((MAX(CAST(CurrentPerm AS FLOAT)) * (HASHAMP()+1)) / 1073741824.0 AS DECIMAL(18,2)) AS Effective_Space_GB,
    CAST((SUM(CAST(CurrentPerm AS FLOAT)) / NULLIF(SUM(CAST(MaxPerm AS FLOAT)), 0)) * 100 AS DECIMAL(5,2)) AS Global_Util_Pct,
    CAST(((MAX(CAST(CurrentPerm AS FLOAT)) * (HASHAMP()+1)) / NULLIF(SUM(CAST(MaxPerm AS FLOAT)), 0)) * 100 AS DECIMAL(5,2)) AS Effective_Util_Pct,
    CAST(((MAX(CAST(CurrentPerm AS FLOAT)) - AVG(CAST(CurrentPerm AS FLOAT))) / NULLIF(AVG(CAST(CurrentPerm AS FLOAT)), 0)) * 100 AS DECIMAL(5,2)) AS Skew_Pct
FROM DBC.DiskSpaceV
WHERE UPPER(DatabaseName) NOT IN (
    'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT','TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM','LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR','TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA', 'PDCRSTG','SYSDBA','CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB','TDAAS_DB','baradmin','BARUSERS','Crashdumps','External_AP','lab1canales','labs_group_lab','LockLogShredder','PDCRADM','PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
	)
GROUP BY 1
HAVING SUM(MaxPerm) > 0
ORDER BY Effective_Util_Pct DESC;
