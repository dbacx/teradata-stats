-- Module 1 Health: System Information
-- Basic query for version and release information
-- Uses DBC.DBCInfo to retrieve system metadata

LOCKING ROW FOR ACCESS
SELECT 
    InfoKey,
    InfoData
FROM DBC.DBCInfo
ORDER BY InfoKey;
