-- =============================================================================
-- Component   : Sqltextinfo007
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
--
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================
 LOCK ROW

   FOR ACCESS
SELECT c.calendar_date (NAMED LogDate),
       CAST(calendar_date AS DATE FORMAT 'e4') (CHAR (3)) (NAMED DoW) , SUM (CASE
                                                                                                                  WHEN loghour = 0 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                 0) AS INT)
                                                                                                                  ELSE 0
                                                                                                              END) AS _0_,

       SUM (CASE
                                                                                                                      WHEN loghour = 1 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                     0) AS INT)
                                                                                                                      ELSE 0
                                                                                                                  END) AS _1_,

       SUM (CASE
                                                                                                                          WHEN loghour = 2 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                         0) AS INT)
                                                                                                                          ELSE 0
                                                                                                                      END) AS _2_,

       SUM (CASE
                                                                                                                              WHEN loghour = 3 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                             0) AS INT)
                                                                                                                              ELSE 0
                                                                                                                          END) AS _3_,

       SUM (CASE
                                                                                                                                  WHEN loghour = 4 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                 0) AS INT)
                                                                                                                                  ELSE 0
                                                                                                                              END) AS _4_,

       SUM (CASE
                                                                                                                                      WHEN loghour = 5 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                     0) AS INT)
                                                                                                                                      ELSE 0
                                                                                                                                  END) AS _5_,

       SUM (CASE
                                                                                                                                          WHEN loghour = 6 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                         0) AS INT)
                                                                                                                                          ELSE 0
                                                                                                                                      END) AS _6_,

       SUM (CASE
                                                                                                                                              WHEN loghour = 7 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                             0) AS INT)
                                                                                                                                              ELSE 0
                                                                                                                                          END) AS _7_,

       SUM (CASE
                                                                                                                                                  WHEN loghour = 8 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                 0) AS INT)
                                                                                                                                                  ELSE 0
                                                                                                                                              END) AS _8_,

       SUM (CASE
                                                                                                                                                      WHEN loghour = 9 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                     0) AS INT)
                                                                                                                                                      ELSE 0
                                                                                                                                                  END) AS _9_,

       SUM (CASE
                                                                                                                                                          WHEN loghour = 10 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                          0) AS INT)
                                                                                                                                                          ELSE 0
                                                                                                                                                      END) AS _10_,

       SUM (CASE
                                                                                                                                                              WHEN loghour = 11 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                              0) AS INT)
                                                                                                                                                              ELSE 0
                                                                                                                                                          END) AS _11_,

       SUM (CASE
                                                                                                                                                                  WHEN loghour = 12 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                  0) AS INT)
                                                                                                                                                                  ELSE 0
                                                                                                                                                              END) AS _12_,

       SUM (CASE
                                                                                                                                                                      WHEN loghour = 13 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                      0) AS INT)
                                                                                                                                                                      ELSE 0
                                                                                                                                                                  END) AS _13_,

       SUM (CASE
                                                                                                                                                                          WHEN loghour = 14 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                          0) AS INT)
                                                                                                                                                                          ELSE 0
                                                                                                                                                                      END) AS _14_,

       SUM (CASE
                                                                                                                                                                              WHEN loghour = 15 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                              0) AS INT)
                                                                                                                                                                              ELSE 0
                                                                                                                                                                          END) AS _15_,

       SUM (CASE
                                                                                                                                                                                  WHEN loghour = 16 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                  0) AS INT)
                                                                                                                                                                                  ELSE 0
                                                                                                                                                                              END) AS _16_,

       SUM (CASE
                                                                                                                                                                                      WHEN loghour = 17 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                      0) AS INT)
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                                  END) AS _17_,

       SUM (CASE
                                                                                                                                                                                          WHEN loghour = 18 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                          0) AS INT)
                                                                                                                                                                                          ELSE 0
                                                                                                                                                                                      END) AS _18_,

       SUM (CASE
                                                                                                                                                                                              WHEN loghour = 19 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                              0) AS INT)
                                                                                                                                                                                              ELSE 0
                                                                                                                                                                                          END) AS _19_,

       SUM (CASE
                                                                                                                                                                                                  WHEN loghour = 20 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                                  0) AS INT)
                                                                                                                                                                                                  ELSE 0
                                                                                                                                                                                              END) AS _20_,

       SUM (CASE
                                                                                                                                                                                                      WHEN loghour = 21 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                                      0) AS INT)
                                                                                                                                                                                                      ELSE 0
                                                                                                                                                                                                  END) AS _21_,

       SUM (CASE
                                                                                                                                                                                                          WHEN loghour = 22 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                                          0) AS INT)
                                                                                                                                                                                                          ELSE 0
                                                                                                                                                                                                      END) AS _22_,

       SUM (CASE
                                                                                                                                                                                                              WHEN loghour = 23 THEN CAST(Round((CPUUtilPctWMCOD / 100) * 100,

                                                                                                                                                                                                                                              0) AS INT)
                                                                                                                                                                                                              ELSE 0
                                                                                                                                                                                                          END) AS _23_

  FROM

        (SELECT a.LogDate,
               a.Loghour,
               SUM (TotalCPUServSec + TotalCPUExecSec) AS CPUBusy,
               SUM (TotalCPUServSec + TotalCPUExecSec + TotalCPUWaitIOSec + TotalCPUIdleSec) AS CPUTotal,

               (CPUBUSY / NULLIFZERO (CPUTotal)) * 100 (DEC (3,

                                                                                                                                        0)) AS CPUUtilPct,

               (CAST(CPUUtilPct AS DEC (10,

                                                                                                                   3)) / (CAST(MAXIMUM (WM_COD_CPU) AS DEC (10,

                                                                                                                                                            3)) / 10)) * 100 (DEC (18,

                                                                                                                                                                                   3)) (NAMED CPUUtilPctWMCOD)

          FROM pdcrinfo.ResUsageSumHr_hst a

         INNER JOIN pdcrinfo.ResUsageSPMA b
            ON a.LogDate = b.TheDate

           AND a.Loghour = (EXTRACT (HOUR
                               FROM b.thetime)(TITLE 'Hour'))

         WHERE a.LogDate BETWEEN dAtE'2026-04-01' AND dAtE'2026-04-30'

         GROUP BY 1,
                  2
       ) a

 RIGHT OUTER JOIN pdcrinfo.CALENDAR c
    ON a.logdate = c.calendar_date

 WHERE c.calendar_date BETWEEN dAtE'2026-04-01' AND dAtE'2026-04-30'

 GROUP BY 1

 ORDER BY 1 ;