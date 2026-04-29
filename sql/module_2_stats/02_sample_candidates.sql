-- Component 2: Sample Candidates
-- Identifies columns that should use USING SAMPLE instead of FULL SCAN
-- Concept: High cardinality columns in large tables currently collected at 100%

SELECT 
    S.DatabaseName, 
    S.TableName, 
    S.ColumnName,
    CAST(SUM(T.CurrentPerm) / (1024.0**3) AS DECIMAL(18,2)) AS Size_GB,
    S.RowCount,
    S.UniqueValueCount,
    CAST(CAST(S.UniqueValueCount AS DECIMAL(18,4)) / NULLIF(S.RowCount, 0) AS DECIMAL(18,4)) AS Uniqueness_Ratio,
    S.LastCollectTimeStamp,
    S.SampleSizePct
FROM DBC.StatsV S
INNER JOIN (
    SELECT DatabaseName, TableName, SUM(CurrentPerm) AS CurrentPerm
    FROM DBC.TableSizeV
    GROUP BY 1, 2
) T 
    ON S.DatabaseName = T.DatabaseName 
    AND S.TableName = T.TableName
WHERE S.RowCount > 100000000
  AND (T.CurrentPerm / (1024.0**3)) > 50
  AND S.ColumnName <> '*'
  AND S.SampleSizePct IN (0, 100)
GROUP BY 1, 2, 3, 5, 6, 7, 8, 9
HAVING Uniqueness_Ratio >= 0.95
ORDER BY Size_GB DESC;
