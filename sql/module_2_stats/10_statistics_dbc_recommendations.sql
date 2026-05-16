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


WITH 
-- 1. Mapeo exacto del KB de Teradata (Tabla vs Cantidad Mínima de Estadísticas Exigidas)
DBC_KB_Requirements (TableName, Expected_Stats) AS (
    -- Forzamos el tamaño en el primer registro para evitar que Teradata trunque los demás
    SELECT CAST('AccessRights' AS VARCHAR(128)), 21 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'ConnectRulesTbl', 5 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Dbase', 9 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Hosts', 2 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Owners', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Roles', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'RoleGrants', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Profiles', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'TVFields', 14 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'TVM', 12 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Indexes', 21 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'StatsTbl', 2 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'ObjectUsage', 10 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'UDFInfo', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'UDTInfo', 2 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'TableConstraints', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'TempTables', 1 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'DatabaseSpace', 5 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'GlobalDBSpace', 3 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'Maps', 4 FROM (SELECT 1 AS dummy) a UNION ALL
    SELECT 'MapGrants', 3 FROM (SELECT 1 AS dummy) a
),
-- 2. Conteo actual y frescura de estadísticas en el Diccionario
DBC_Current_Stats AS (
    SELECT 
        TableName, 
        COUNT(DISTINCT StatsId) AS Current_Stats,
        MAX(LastCollectTimeStamp) AS Ultima_Recoleccion
    FROM DBC.StatsV
    WHERE DatabaseName = 'DBC' AND StatsId <> 0
    GROUP BY 1
),
-- 3. Blindaje físico para PDCR (Ignoramos logs vacíos < 10MB)
Tablas_Fisicas_PDCR AS (
    SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV
    WHERE DatabaseName IN ('PDCRDATA', 'PDCRINFO')
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 10485760 
)

-- =====================================================================
-- BLOQUE A: Auditoría del Diccionario (DBC) contra el KB0026099 Oficial
-- =====================================================================
SELECT 
    'DBC' AS DatabaseName,
    kb.TableName,
    CASE 
        WHEN c.Current_Stats IS NULL OR c.Current_Stats < kb.Expected_Stats 
            THEN 'KB_VIOLATION: Faltan estadísticas (Esperadas: ' || TRIM(CAST(kb.Expected_Stats AS VARCHAR(5))) || ', Actuales: ' || TRIM(CAST(COALESCE(c.Current_Stats, 0) AS VARCHAR(5))) || ')'
        WHEN c.Ultima_Recoleccion < CURRENT_DATE - 7 
            THEN 'STALE_DICTIONARY: Estadísticas obsoletas (Última: ' || CAST(CAST(c.Ultima_Recoleccion AS DATE) AS VARCHAR(15)) || ')'
    END AS Diagnostico,
    '-- Ejecutar script nativo DIPDBCSTATS o recolectar manualmente según KB0026099' AS Action_SQL
FROM DBC_KB_Requirements kb
LEFT JOIN DBC_Current_Stats c ON kb.TableName = c.TableName
WHERE c.Current_Stats IS NULL 
   OR c.Current_Stats < kb.Expected_Stats 
   OR c.Ultima_Recoleccion < CURRENT_DATE - 7

UNION ALL

-- =====================================================================
-- BLOQUE B: Auditoría de Tablas Huérfanas Masivas en PDCR
-- =====================================================================
SELECT 
    t.DatabaseName, 
    t.TableName,
    'MISSING_STATS: Tabla de log masiva sin ninguna estadística' AS Diagnostico,
    'COLLECT SUMMARY STATISTICS ON ' || TRIM(t.DatabaseName) || '.' || TRIM(t.TableName) || ';' AS Action_SQL
FROM DBC.TablesV t
INNER JOIN Tablas_Fisicas_PDCR tf
    ON t.DatabaseName = tf.DatabaseName
    AND t.TableName = tf.TableName
LEFT JOIN DBC.StatsV s 
    ON t.DatabaseName = s.DatabaseName 
    AND t.TableName = s.TableName
WHERE t.DatabaseName IN ('PDCRDATA', 'PDCRINFO') 
  AND t.TableKind IN ('T', 'O') 
  AND s.TableName IS NULL -- 100% huérfanas
  AND t.TableName NOT IN (
      'ChangedRowJournal', 'LocalSessionStatusTable', 'LocalTransactionStatusTable', 
      'OnlineJournalInfoTbl', 'OrdSysChngTable', 'ReconfigJournalTbl', 
      'RecoveryLockTable', 'RecoveryPJTable', 'SavedTransactionStatusTable', 
      'SysRcvStatJournal', 'TransientJournal', 'UtilityLockJournalTable',
      'UpdateSpace', 'AccessLog'
  )
ORDER BY 1, 2;