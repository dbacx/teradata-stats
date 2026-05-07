-- =============================================================================
-- Component   : Sqltextinfo040
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT CASE WHEN DBKind = 'D' THEN 'Database'
            WHEN DBKind = 'U' THEN 'User'
            ELSE 'Others'

             END (VARCHAR (20)) (NAMED ObjectType) , COUNT (1) (NAMED ObjectCount)

  FROM dbc.databases

 GROUP BY 1

UNION ALL
SELECT CASE WHEN TABLEKIND = 'T' THEN 'Table'
            WHEN TABLEKIND = 'V' THEN 'View'
            WHEN TABLEKIND = 'M' THEN 'Macro'
            WHEN TABLEKIND = 'P' THEN 'Procedure'

             END (NAMED ObjectType) , COUNT (1) (NAMED ObjectCount)

  FROM dbc.TablesV

 WHERE tablekind IN ('T', 'V', 'M', 'P')

 GROUP BY 1

 ORDER BY 1 ;