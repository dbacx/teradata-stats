-- =============================================================================
-- Component   : Sqltextinfo042
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

SELECT CAST((CAST(ADD_MONTHS(DATE - (EXTRACT (MONTH
                                              FROM (DATE))(TITLE 'Month')) + 1 , - 1) AS FORMAT 'mmm')) AS CHAR (3)) || '_' || CAST((EXTRACT (YEAR
                                                                                                                                              FROM (ADD_MONTHS(DATE - (EXTRACT (MONTH
                                                                                                                                                                                FROM (DATE))(TITLE 'Month')) + 1 , - 2)))(TITLE 'Year')) AS CHAR (4)) ;