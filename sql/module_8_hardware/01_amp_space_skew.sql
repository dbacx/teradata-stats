-- Module 8 Hardware: AMP Space Skew
-- Calculates total space per AMP to detect disk-level skew
-- Uses DBC.TableSizeV to analyze space distribution across AMPs

LOCKING ROW FOR ACCESS
SELECT 
    Vproc AS AMP_ID,
    SUM(CurrentPerm) AS TotalSpace_Bytes
FROM DBC.TableSizeV
GROUP BY 1
ORDER BY 2 DESC;
