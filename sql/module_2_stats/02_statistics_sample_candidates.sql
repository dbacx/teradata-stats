-- =============================================================================
-- Component 02: Sample Candidates (Aligned with Teradata Best Practices)
-- =============================================================================
-- Description : Identifies indexed columns with high uniqueness ratio (>=95% NDV)
--               that should use USING SAMPLE instead of FULL SCAN.
--               Enforces official rules:
--               - Target must be an Index (IndexNumber IS NOT NULL).
--               - Table must not be skewed (AMP Variance <= 5%).
--               - Excludes small tables and Global Temp / Join Indexes.
-- 
-- Version     : 1.2.0
-- Date        : 2026-05-15
-- Modificado  : 2026-05-15 - Integración de reglas de Data Skew y validación de Índices
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Analisis_Skew_Fisico AS (
    SELECT
        DatabaseName,
        TableName,
        SUM(CurrentPerm)  AS TotalPerm,
        CAST(
            (MAX(CurrentPerm) - AVG(CurrentPerm))
            / NULLIF(AVG(CurrentPerm), 0) * 100
        AS DECIMAL(18,4)) AS Skew_Pct
    FROM DBC.TableSizeV
    GROUP BY DatabaseName, TableName
    HAVING SUM(CurrentPerm) > 10485760
       AND COUNT(CASE WHEN CurrentPerm > 0 THEN 1 END) > (COUNT(*) * 0.5)
),
Uso_Estadisticas AS (
    SELECT
        ObjectDatabaseName,
        ObjectTableName,	
        SUM(FreqofUse) AS Accesos_PDCR
    FROM PDCRINFO.DBQLObjTbl_Hst
    WHERE LogDate      BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE
      AND ObjectType    = 'Tab'
      AND ObjectDatabaseName NOT IN (
            'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
            'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
            'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
            'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA','CONSOLE'
      )
    GROUP BY ObjectDatabaseName, ObjectTableName
),
Stats_Agrupadas AS (
    SELECT
        DatabaseName,
        TableName,
        StatsId,
        MAX(IndexNumber)      AS IndexNumber,
        MAX(RowCount)         AS RowCount,
        MAX(UniqueValueCount) AS UniqueValueCount,
        MAX(SampleSignature)  AS SampleSignature,
        COUNT(ColumnName)     AS Num_Columns
    FROM DBC.StatsV
    WHERE StatsId      <> 0
      AND IndexNumber  IS NOT NULL
      AND COALESCE(UPPER(ColumnName), '') <> 'PARTITION'
    GROUP BY DatabaseName, TableName, StatsId
),
Cols_Ranked AS (
    SELECT
        s2.DatabaseName,
        s2.TableName,
        s2.StatsId,
        CAST(TRIM(s2.ColumnName) AS VARCHAR(128)) AS ColumnName,
        ROW_NUMBER() OVER (
            PARTITION BY s2.DatabaseName, s2.TableName, s2.StatsId
            ORDER BY COALESCE(idx2.ColumnPosition, 999), s2.ColumnName
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY s2.DatabaseName, s2.TableName, s2.StatsId
        ) AS cnt
    FROM DBC.StatsV s2
    LEFT JOIN DBC.IndicesV idx2
        ON  idx2.DatabaseName = s2.DatabaseName
        AND idx2.TableName    = s2.TableName
        AND idx2.ColumnName   = s2.ColumnName
        AND idx2.IndexNumber  = s2.IndexNumber
    WHERE s2.StatsId     <> 0
      AND s2.IndexNumber IS NOT NULL
),
Cols_Por_Stats AS (
    SELECT
        DatabaseName,
        TableName,
        StatsId,
        MAX(cnt) AS Total_Cols,
        CAST(
            MAX(CASE WHEN rn = 1 THEN ColumnName ELSE '' END)
            || CASE WHEN MAX(cnt) > 1
               THEN ',' || MAX(CASE WHEN rn = 2 THEN ColumnName ELSE '' END)
               ELSE '' END
            || CASE WHEN MAX(cnt) > 2
               THEN ',' || MAX(CASE WHEN rn = 3 THEN ColumnName ELSE '' END)
               ELSE '' END
            || CASE WHEN MAX(cnt) > 3
               THEN ',' || MAX(CASE WHEN rn = 4 THEN ColumnName ELSE '' END)
               ELSE '' END
            || CASE WHEN MAX(cnt) > 4
               THEN ',' || MAX(CASE WHEN rn = 5 THEN ColumnName ELSE '' END)
               ELSE '' END
            || CASE WHEN MAX(cnt) > 5
               THEN ',' || MAX(CASE WHEN rn = 6 THEN ColumnName ELSE '' END)
               ELSE '' END
        AS VARCHAR(800)) AS Column_List
    FROM Cols_Ranked
    GROUP BY DatabaseName, TableName, StatsId
)
SELECT
    sa.DatabaseName,
    sa.TableName,
    sa.RowCount                                                      AS Filas_Tabla,
    sa.UniqueValueCount                                              AS Valores_Unicos,
    CAST(
        (CAST(sa.UniqueValueCount AS FLOAT) / NULLIF(sa.RowCount, 0)) * 100
    AS DECIMAL(5,2))                                                 AS Porcentaje_Unicidad,
    idx.IndexType,
    CAST(
        CAST('COLLECT STATISTICS USING SYSTEM SAMPLE COLUMN (' AS VARCHAR(60))
        || cols.Column_List
        || CAST(') ON ' AS VARCHAR(5))
        || CAST(TRIM(sa.DatabaseName) AS VARCHAR(128))
        || CAST('.' AS VARCHAR(1))
        || CAST(TRIM(sa.TableName) AS VARCHAR(128))
        || CAST(';' AS VARCHAR(1))
    AS VARCHAR(1200))                                                AS Action_SQL
FROM Stats_Agrupadas sa
INNER JOIN DBC.TablesV t
    ON  sa.DatabaseName = t.DatabaseName
    AND sa.TableName    = t.TableName
INNER JOIN Analisis_Skew_Fisico sk
    ON  sa.DatabaseName = sk.DatabaseName
    AND sa.TableName    = sk.TableName
INNER JOIN Uso_Estadisticas u
    ON  sa.DatabaseName = u.ObjectDatabaseName
    AND sa.TableName    = u.ObjectTableName
INNER JOIN (
    SELECT
        DatabaseName,
        TableName,
        IndexNumber,
        IndexType
    FROM DBC.IndicesV
    WHERE IndexType IN ('P','Q','K','S','J')
    GROUP BY DatabaseName, TableName, IndexNumber, IndexType
) idx
    ON  sa.DatabaseName = idx.DatabaseName
    AND sa.TableName    = idx.TableName
    AND sa.IndexNumber  = idx.IndexNumber
INNER JOIN Cols_Por_Stats cols
    ON  sa.DatabaseName = cols.DatabaseName
    AND sa.TableName    = cols.TableName
    AND sa.StatsId      = cols.StatsId
WHERE t.TableKind IN ('T','O')
  AND t.AuthName        IS NULL
  AND sk.Skew_Pct       <= 5.00
  AND sa.RowCount        > 100000
  AND (CAST(sa.UniqueValueCount AS FLOAT) / NULLIF(sa.RowCount, 0)) >= 0.95
  AND (sa.SampleSignature IS NULL
       OR UPPER(sa.SampleSignature) NOT LIKE 'SDP%')
  AND idx.IndexType IN ('Q', 'U') 
  AND sa.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA','CONSOLE'
  )
ORDER BY sa.RowCount DESC,
         Porcentaje_Unicidad DESC;