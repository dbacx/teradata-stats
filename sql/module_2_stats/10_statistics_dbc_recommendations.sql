-- =============================================================================
-- Component   : DBC Recommendations
-- =============================================================================
-- Description : Identifies missing statistics in system databases (DBC, PDCRDATA,
--               PDCRINFO). Uses DBC.TablesV and DBC.StatsV to detect system tables
--               without statistics. Excludes internal transactional/journal tables
--               to protect system integrity. MEDIUM severity for system performance.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Con_Datos AS (
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    WHERE DatabaseName IN ('DBC', 'PDCRDATA', 'PDCRINFO')
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 0 
)
SELECT DISTINCT 
    t.DatabaseName                                          AS DatabaseName, 
    t.TableName                                             AS TableName,
    'TABLE LEVEL'                                           AS ObjectName,
    'DBC Recommendations'                                   AS FindingCategory,
    CAST(NULL AS TIMESTAMP(0))                              AS LastCollectTimeStamp,
    'COLLECT STATISTICS ' || TRIM(t.DatabaseName) || '.' || TRIM(t.TableName) || ';' AS Action_SQL
FROM DBC.TablesV t
INNER JOIN Tablas_Con_Datos td
    ON t.DatabaseName = td.DatabaseName
    AND t.TableName = td.TableName
LEFT JOIN DBC.StatsV s 
    ON t.DatabaseName = s.DatabaseName 
    AND t.TableName = s.TableName
WHERE t.DatabaseName IN ('DBC', 'PDCRDATA', 'PDCRINFO') 
  AND t.TableKind = 'T' 
  AND s.TableName IS NULL
  AND t.TableName NOT IN (
      'ChangedRowJournal', 
      'LocalSessionStatusTable', 
      'LocalTransactionStatusTable', 
      'OnlineJournalInfoTbl', 
      'OrdSysChngTable', 
      'ReconfigJournalTbl', 
      'RecoveryLockTable', 
      'RecoveryPJTable', 
      'SavedTransactionStatusTable', 
      'SysRcvStatJournal', 
      'TransientJournal', 
      'UtilityLockJournalTable',
      'UpdateSpace',
      'AccessLog'
  )
ORDER BY 1, 2;
