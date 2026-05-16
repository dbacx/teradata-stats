-- =============================================================================
-- Component   : Multicolumn MaxValueLength
-- =============================================================================
-- Description : Identifies multicolumn statistics (ExpressionCount > 1) with
--               MaxValueLength <= 25, suggesting potential truncation issues.
--               Uses DBC.StatsV and DBC.TablesV to detect multicolumn stats that
--               may need USING MAXVALUELENGTH adjustment. Excludes system databases
--               and SUMMARY stats. LOW severity optimization opportunity.
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Modificado  : 2026-05-13 - Integración y optimización de motor SQL para Módulo 2
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Tablas_Con_Datos AS (
    -- Validamos que la tabla tenga espacio físico
    SELECT DatabaseName, TableName
    FROM DBC.TableSizeV
    GROUP BY 1, 2
    HAVING SUM(CurrentPerm) > 0 
),
Calculo_Longitud_Fisica AS (
    -- Calculamos los bytes reales usando OREPLACE nativo y CHR para evitar conflictos del parser
    SELECT 
        s.DatabaseName, 
        s.TableName, 
        s.ColumnName AS StatsColumnName, 
        COALESCE(s.MaxValueLength, 25) AS MaxValueLength,
        SUM(col.ColumnLength) AS Total_Len
    FROM DBC.StatsV s
    INNER JOIN DBC.ColumnsV col 
        ON s.DatabaseName = col.DatabaseName 
       AND s.TableName = col.TableName 
       -- El cruce infalible: limpia espacios (CHR 32) y comillas dobles (CHR 34)
       AND ',' || OREPLACE(OREPLACE(s.ColumnName, CHR(32), ''), CHR(34), '') || ',' 
           LIKE '%,' || TRIM(col.ColumnName) || ',%'
    WHERE s.ExpressionCount > 1 
      AND COALESCE(s.MaxValueLength, 25) <= 25 -- Filtramos las estancadas en el límite por defecto
      AND s.StatsId <> 0
      AND s.DatabaseName NOT IN (
        'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
        'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
        'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
        'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
    GROUP BY 1, 2, 3, 4
    HAVING SUM(col.ColumnLength) > 25 -- Filtramos las que físicamente superan el límite
),
Impacto_CPU_Tablas AS (
    -- Calculamos el impacto en CPU (30 días) para priorizar por las más costosas
    SELECT 
        o.ObjectDatabaseName,
        o.ObjectTableName,
        SUM(q.AMPCPUTime + q.ParserCPUTime) AS Total_Impact_CPU
    FROM PDCRINFO.DBQLObjTbl_Hst o
    INNER JOIN PDCRINFO.DBQLogTbl_Hst q
        ON o.LogDate = q.LogDate
        AND o.QueryID = q.QueryID
    WHERE o.LogDate BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE 
      AND o.ObjectType = 'Tab'
      AND o.ObjectDatabaseName NOT IN (
          'DBC','DBCMNGR','SYSLIB','TDQCD','TDSTATS','TDMAPS','TDBCMGMT',
          'TD_SERVER_DB','VAL','SYSTEMFE','SYSSPATIAL','VIEWPOINT','TDWM',
          'LOCKLOGLSHREDDER','SQLJ','SYSBAR','SYSADMIN','SYS_CALENDAR',
          'TD_ANALYTICS_DB','PDCRTPCD','PDCRDATA','PDCRSTG','SYSDBA', 'CONSOLE'
      )
    GROUP BY 1, 2
)
SELECT DISTINCT 
    cl.DatabaseName, 
    cl.TableName, 
    cl.StatsColumnName AS ColumnName, 
    cl.MaxValueLength, 
    cl.Total_Len, 
    CAST(COALESCE(c.Total_Impact_CPU, 0) AS DECIMAL(18,2)) AS Total_Impact_CPU,
    -- Comando exacto con el margen de seguridad inyectado al Total_Len real
    'COLLECT STATISTICS USING MAXVALUELENGTH ' || TRIM(CAST(cl.Total_Len + 10 AS VARCHAR(10))) || ' COLUMN (' || TRIM(cl.StatsColumnName) || ') ON ' || TRIM(cl.DatabaseName) || '.' || TRIM(cl.TableName) || ';' AS Action_SQL
FROM Calculo_Longitud_Fisica cl
INNER JOIN DBC.TablesV t 
    ON cl.DatabaseName = t.DatabaseName 
    AND cl.TableName = t.TableName
INNER JOIN Tablas_Con_Datos td 
    ON cl.DatabaseName = td.DatabaseName 
    AND cl.TableName = td.TableName
LEFT JOIN Impacto_CPU_Tablas c 
    ON cl.DatabaseName = c.ObjectDatabaseName 
    AND cl.TableName = c.ObjectTableName
WHERE t.TableKind = 'T'
AND t.AuthName is null
ORDER BY Total_Impact_CPU DESC, cl.DatabaseName, cl.TableName;