-- =============================================================================
-- Component 3.07: Top 20 Databases By Used Size
-- =============================================================================
-- Description : Executive summary of the 20 largest databases, highlighting 
--               storage consumed by Fallback protection.
-- Version     : 1.0.0
-- Author      : Ricardo Enciso
-- =============================================================================

SELECT TOP 20
    ts.DatabaseName,
    CAST(SUM(ts.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Total_Size_GB,
    CAST(SUM(CASE WHEN t.ProtectionType = 'F' THEN ts.CurrentPerm / 2 ELSE ts.CurrentPerm END) / (1024.0**3) AS DECIMAL(18,2)) AS Net_Data_GB,
    CAST(SUM(CASE WHEN t.ProtectionType = 'F' THEN ts.CurrentPerm / 2 ELSE 0 END) / (1024.0**3) AS DECIMAL(18,2)) AS Fallback_Waste_GB
FROM DBC.TableSizeV ts
INNER JOIN DBC.TablesV t
    ON ts.DatabaseName = t.DatabaseName
    AND ts.TableName = t.TableName
WHERE ts.DatabaseName NOT IN ('DBC', 'PDCRDATA')
GROUP BY 1
ORDER BY Total_Size_GB DESC;