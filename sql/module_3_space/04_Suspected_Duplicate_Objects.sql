-- =============================================================================
-- Component 3.04: Suspected Duplicate Objects (Heuristics)
-- =============================================================================
-- Description : Detects potential duplicate tables based on backup naming 
--               conventions (_BKP, _OLD, _TMP) and significant size.
--               Fixed error 5628 by joining with DBC.TablesV.
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- =============================================================================

-- =============================================================================
-- Component: Suspected Backup / Duplicate Tables
-- =============================================================================
-- Description : Identifies tables with naming conventions associated with
--               backup or temporary copies (_BK, _BKP, _OLD, _COPY, etc.)
--               consuming more than 1 GB. Useful for storage reclaim analysis.
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- Change Log  : v1.1.0 - Consolidated 3 BK patterns into '%[_]BK%' to capture
--               _BK (end), _BKN (numbered), _BK_date variants.
--               Added LastAlterTimeStamp and TableKind filter.
-- =============================================================================

SELECT
     ts.DatabaseName
    ,ts.TableName
    ,t.TableKind
    ,CAST(SUM(CAST(ts.CurrentPerm AS FLOAT)) / 1073741824.0 AS DECIMAL(18,2)) AS Size_GB
    ,CAST(t.CreateTimeStamp    AS DATE FORMAT 'YYYY-MM-DD') AS Created_Date
    ,CAST(t.LastAlterTimeStamp AS DATE FORMAT 'YYYY-MM-DD') AS Last_Alter_Date
    ,'Review Required' AS Status
FROM DBC.TableSizeV ts
INNER JOIN DBC.TablesV t
    ON  ts.DatabaseName = t.DatabaseName
    AND ts.TableName    = t.TableName
WHERE t.TableKind IN ('T','O')          -- solo tablas de usuario (excl. views, macros, journals)
  AND (
         UPPER(ts.TableName) LIKE '%[_]BK%'      ESCAPE '['  -- _BK, _BK_, _BKP, _BKP18, _BK_Feb14
      OR UPPER(ts.TableName) LIKE '%[_]BACKUP%'   ESCAPE '['
      OR UPPER(ts.TableName) LIKE '%[_]OLD%'      ESCAPE '['
      OR UPPER(ts.TableName) LIKE '%[_]TMP%'      ESCAPE '['
      OR UPPER(ts.TableName) LIKE '%[_]202%'     ESCAPE '['  -- _2024xx, _20251231, etc.
      OR UPPER(ts.TableName) LIKE '%[_]COPY%'    ESCAPE '['
      OR UPPER(ts.TableName) LIKE '%[_]COPIA%'   ESCAPE '['
  )
  AND UPPER(ts.DatabaseName) NOT IN (
      'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT'     ,'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM'     ,'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR'     ,'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA'     ,'CONSOLE','MLDB','PDCRADMIN','SYSUDTLIB','TD_SYSFNLIB'     ,'TDAAS_DB','BARADMIN','BARUSERS','CRASHDUMPS','EXTERNAL_AP'     'LAB1CANALES','LABS_GROUP_LAB','LOCKLOGSHREDDER','PDCRADM'     ,'PDCRINFO','SYSJDBC','SYSUIF','TD_SYSGPL','TD_SYSXML'
  )
GROUP BY 1, 2, 3, 5, 6, 7
ORDER BY Size_GB DESC;