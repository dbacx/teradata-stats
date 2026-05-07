-- =============================================================================
-- Component   : Sqltextinfo006
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
SELECT TRIM ((TRIM ((EXTRACT (MONTH
                              FROM (logDATE))(TITLE 'Month'))) || '/' || TRIM ((EXTRACT (DAY
                                                                                         FROM (logDATE))(TITLE 'Day'))) || '/' || TRIM ((EXTRACT (YEAR
                                                                                                                                                  FROM (logDATE))(TITLE 'Year'))))) AS Logdate,
       WDName,
       SUM (QUERY) AS QryCnt,

       SUM (DelayQry) AS DelayQryCnt,

       MAXIMUM (QryRT) AS MaxQueryRT,

       AVG (QryRT) AS AvgQueryRT,

       MINIMUM (QryRT) AS MinQueryRT,

       MAXIMUM (AMPCPUTIME) AS MaxCPUTime,

       AVG (AMPCPUTIME) AS AvgCPUTime,

       MINIMUM (AMPCPUTIME) AS MinCPUTime,

       SUM (AMPCPUTIME) AS TotalCPUTime,

       MAXIMUM (QryRTDelayPct) AS MaxQryRTDelayPct,

       AVG (QryRTDelayPct) AS AvgQryRTDelayPct,

       MINIMUM (QryRTDelayPct) AS MinQryRTDelayPct,

       MAXIMUM (Delay) AS MaxDelay,

       AVG (Delay) AS AvgDelay,

       MINIMUM (Delay) AS MinDelay,

       AvgDelay / NULLIFZERO (AvgQueryRT) * 100 AS QryRTDelayPct,

       SUM (QryCntRTSec_LT10) AS QryCntRTSec_LT10Cnt,

       SUM (QryCntRTSec_10_30) AS QryCntRTSec_10_30Cnt,

       SUM (QryCntRTSec_30_60) AS QryCntRTSec_30_60Cnt,

       SUM (QryCntRTMin_1_5) AS QryCntRTMin_1_5Cnt,

       SUM (QryCntRTMin_5_10) AS QryCntRTMin_5_10Cnt,

       SUM (QryCntRTMin_10_20) AS QryCntRTMin_10_20Cnt,

       SUM (QryCntRTMin_20_30) AS QryCntRTMin_20_30Cnt,

       SUM (QryCntRTMin_30_60) AS QryCntRTMin_30_60Cnt,

       SUM (QryCntRTMin_60_120) AS QryCntRTMin_60_120Cnt,

       SUM (QryCntRTMin_GT120) AS QryCntRTMin_GT120Cnt,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTSec_LT10Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                 2))) / DelayQryCnt) * 100)

             END AS QryCntRTSec_LT10Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTSec_10_30Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                  2))) / DelayQryCnt) * 100)

             END AS QryCntRTSec_10_30Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTSec_30_60Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                  2))) / DelayQryCnt) * 100)

             END AS QryCntRTSec_30_60Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_1_5Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_1_5Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_5_10Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                 2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_5_10Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_10_20Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                  2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_10_20Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_20_30Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                  2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_20_30Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_30_60Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                  2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_30_60Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_60_120Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                   2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_60_120Pct,

       CASE
                                                                                                                                                                                                                                                                                WHEN DelayQryCnt = 0 THEN 0

            ELSE (((QryCntRTMin_GT120Cnt(DEC (15,

                                                                                                                                                                                                                                                                                                                  2))) / DelayQryCnt) * 100)

             END AS QryCntRTMin_GT120Pct

  FROM

        (SELECT Logdate,
               (EXTRACT (HOUR
                               FROM ql.starttime)(TITLE 'Hour')) AS LogHour,
               WDName,

               1 AS QUERY,
               CASE WHEN DelayTime IS NOT NULL THEN 1
                    ELSE 0

                     END AS DelayQry,

               DelayTime AS Delay,

               ((EXTRACT (SECOND
                       FROM firstresptime)(TITLE 'Second')) + ((EXTRACT (MINUTE
                                                                         FROM firstresptime)(TITLE 'Minute')) * 60) + ((EXTRACT (HOUR
                                                                                                                                 FROM firstresptime)(TITLE 'Hour')) * 3600) + (86400 * (CAST(firstresptime AS DATE) - CAST(firststeptime AS DATE)))) - ((EXTRACT (SECOND
                                                                                                                                                                                                                                                                  FROM firststeptime)(TITLE 'Second')) + ((EXTRACT (MINUTE
                                                                                                                                                                                                                                                                                                                    FROM firststeptime)(TITLE 'Minute')) * 60) + ((EXTRACT (HOUR
                                                                                                                                                                                                                                                                                                                                                                            FROM firststeptime)(TITLE 'Hour')) * 3600)) (DEC (15,

                                                                                                                                                                                                                                                                                                                                                                                                                              2)) AS QryRT,
               AMPCPUTIME AS AMPCPUTIME,

               DelayTime / NULLIFZERO (QryRT) * 100 AS QryRTDelayPct,
               CASE
                                                                       WHEN DelayTime < 10 THEN 1
                    ELSE 0

                     END AS QryCntRTSec_LT10,

               CASE WHEN DelayTime >= 10
                     AND DelayTime < 30 THEN 1
                    ELSE 0

                     END AS QryCntRTSec_10_30,

               CASE WHEN DelayTime >= 30
                     AND DelayTime < 60 THEN 1
                    ELSE 0

                     END AS QryCntRTSec_30_60,

               CASE WHEN DelayTime >= 60
                     AND DelayTime < 300 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_1_5,

               CASE WHEN DelayTime >= 300
                     AND DelayTime < 600 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_5_10,

               CASE WHEN DelayTime >= 600
                     AND DelayTime < 1200 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_10_20,

               CASE WHEN DelayTime >= 1200
                     AND DelayTime < 1800 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_20_30,

               CASE WHEN DelayTime >= 1800
                     AND DelayTime < 3600 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_30_60,

               CASE WHEN DelayTime >= 3600
                     AND DelayTime < 7200 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_60_120,
               CASE WHEN DelayTime >= 7200 THEN 1
                    ELSE 0

                     END AS QryCntRTMin_GT120

          FROM pdcrinfo.DBQLogTblRpt_hst ql

         WHERE ql.Logdate BETWEEN '2026-04-01' AND '2026-04-30'

           AND ql.WDName IS NOT NULL

           AND ql.AMPCPUTime > 0
       ) ql

 INNER JOIN pdcrinfo.CALENDAR c
    ON ql.logdate = c.calendar_date

 INNER JOIN pdcrinfo.ShiftHour s
    ON LogHour = s.shifthour

 WHERE c.calendar_date BETWEEN dAtE'2026-04-01' AND dAtE'2026-04-30'

 GROUP BY 1,
          2 ;