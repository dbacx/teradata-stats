-- =============================================================================
-- Component 3.02: Space Capacity Forecast Report (Historical Baseline)
-- =============================================================================
-- Description : Extracts the 6-month historical baseline required to feed a 
--               linear regression projection model in Python/Streamlit.
--               Aggregates TotalCurPerm and TotalMaxPerm precisely at the end 
--               of each month, excluding system databases.
--               Safe FLOAT casting prevents Numeric Overflow (Error 2616).
-- Version     : 1.2.0
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

WITH Month_End_Dates AS (
    -- Extraemos exactamente el último día de los últimos 6 meses usando LAST_DAY()
    SELECT calendar_date AS LogDate
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN ADD_MONTHS(CURRENT_DATE, -6) AND CURRENT_DATE
      AND calendar_date = LAST_DAY(calendar_date)
)
SELECT 
    -- Formateo directo para el Eje X de Streamlit (Ej: '2025-12')
    CAST(EXTRACT(YEAR FROM p.LogDate) AS VARCHAR(4)) || '-' || 
    LPAD(CAST(EXTRACT(MONTH FROM p.LogDate) AS VARCHAR(2)), 2, '0') AS Month_Year,
    -- Eje Y: CurPerm (Barras Azules)
    CAST(SUM(CAST(p.CurrentPerm AS FLOAT)) / 1099511627776.0 AS DECIMAL(18,2)) AS TotalCurPerm_TB,
    -- Eje Y: MaxPerm (Línea Naranja Sólida)
    CAST(SUM(CAST(p.MaxPerm AS FLOAT)) / 1099511627776.0 AS DECIMAL(18,2)) AS TotalMaxPerm_TB
FROM PDCRINFO.DatabaseSpace_Hst p
INNER JOIN Month_End_Dates m 
    ON p.LogDate = m.LogDate
GROUP BY p.LogDate
ORDER BY p.LogDate ASC;