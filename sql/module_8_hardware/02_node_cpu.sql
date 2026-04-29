-- Module 8 Hardware: Node CPU Usage
-- Queries CPU usage by node for the current day
-- Uses DBC.ResUsageSpma for CPU metrics (may be disabled in some environments)

LOCKING ROW FOR ACCESS
SELECT 
    TheDate,
    NodeID,
    SUM(CPUIdle) AS TotalIdle,
    SUM(CPUUServ) AS TotalServ
FROM DBC.ResUsageSpma
WHERE TheDate = CURRENT_DATE
GROUP BY 1, 2
ORDER BY 1, 2;
