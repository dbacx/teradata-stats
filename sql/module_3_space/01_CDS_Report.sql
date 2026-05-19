-- =============================================================================
-- Component 3.02: CDS Report: Customer Data Space Utilization
-- =============================================================================
-- Description : System-level summary of CDS consumption vs licensed capacity.
--               CurPermCDS = logical uncompressed size (what CDS license
--               measures). Adjust CDS_CAPACITY_TB to match the contracted
--               license before running.
-- Version     : 1.1.0
-- Author      : Ricardo Enciso
-- Notes       : DBC.CDSTablesizeV includes ALL databases (system + user).
--               If system DBs should be excluded, add a WHERE clause filtering
--               DatabaseName in AggregatedData CTE.
--               CDS_CAPACITY_TB is a single point of change at the top.
-- =============================================================================

WITH CDS_Config AS (
    -- *** Ajustar aqui la capacidad contratada en TB ***
    SELECT 20.0 AS CDS_Capacity_TB
),
AggregatedData AS (
    SELECT
         CAST(SUM(CurPerm)              / 1099511627776.0 AS DECIMAL(18,4)) AS CurrentPerm_TB
        ,CAST(SUM(CurPermWithoutFallback)/ 1099511627776.0 AS DECIMAL(18,4)) AS NoFallback_TB
        ,CAST(SUM(CurPermCDS)           / 1099511627776.0 AS DECIMAL(18,4)) AS CDSConsumed_TB
    FROM DBC.CDSTablesizeV
),
Parameters AS (
    SELECT '01. CurrentPerm (TB)'       AS Parameter, a.CurrentPerm_TB                                    AS Value FROM AggregatedData a CROSS JOIN CDS_Config c
    UNION ALL
    SELECT '02. NoFallback (TB)',        a.NoFallback_TB                                                   FROM AggregatedData a CROSS JOIN CDS_Config c
    UNION ALL
    SELECT '03. CDS Capacity (TB)',      c.CDS_Capacity_TB                                                FROM AggregatedData a CROSS JOIN CDS_Config c
    UNION ALL
    SELECT '04. CDS Consumed (TB)',      a.CDSConsumed_TB                                                  FROM AggregatedData a CROSS JOIN CDS_Config c
    UNION ALL
    SELECT '05. CDS Available (TB)',     CASE WHEN c.CDS_Capacity_TB > a.CDSConsumed_TB
                                              THEN c.CDS_Capacity_TB - a.CDSConsumed_TB
                                              ELSE 0 END                                                   FROM AggregatedData a CROSS JOIN CDS_Config c
    UNION ALL
    SELECT '06. CDS Utilization (%)',    CAST(
                                              CASE WHEN c.CDS_Capacity_TB > 0
                                                   THEN a.CDSConsumed_TB * 100.0 / c.CDS_Capacity_TB
                                                   ELSE NULL END
                                         AS DECIMAL(6,2))                                                  FROM AggregatedData a CROSS JOIN CDS_Config c
    UNION ALL
    SELECT '07. Compression Ratio',      CAST(
                                              CASE WHEN a.CDSConsumed_TB > 0
                                                   THEN a.CurrentPerm_TB / a.CDSConsumed_TB
                                                   ELSE NULL END
                                         AS DECIMAL(6,2))                                                  FROM AggregatedData a CROSS JOIN CDS_Config c
)
SELECT * FROM Parameters ORDER BY Parameter;