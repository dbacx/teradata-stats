-- =============================================================================
-- Component 3.07: Top 20 Databases By Used Size
-- =============================================================================
-- Description : Executive summary of the 20 largest databases.
--               Outputs discrete numeric columns (Net vs Fallback) perfect 
--               for stacked/clustered bar charts in Streamlit/Pandas.
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT TOP 20
    ts.DatabaseName,
    CAST(SUM(CAST(ts.CurrentPerm AS FLOAT)) / 1073741824.0 AS DECIMAL(18,2)) AS Total_Size_GB,
    CAST(SUM(CASE WHEN t.ProtectionType = 'F' THEN CAST(ts.CurrentPerm AS FLOAT) / 2.0 ELSE CAST(ts.CurrentPerm AS FLOAT) END) / 1073741824.0 AS DECIMAL(18,2)) AS Net_Data_GB
FROM DBC.TableSizeV ts
INNER JOIN DBC.TablesV t
    ON ts.DatabaseName = t.DatabaseName
    AND ts.TableName = t.TableName
WHERE UPPER(ts.DatabaseName) NOT IN (
    'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT','TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM','LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR','TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA', 'PDCRSTG','SYSDBA','CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB','TDAAS_DB','baradmin','BARUSERS','Crashdumps','External_AP','lab1canales','labs_group_lab','LockLogShredder','PDCRADM','PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
	)
GROUP BY 1
ORDER BY Total_Size_GB DESC;