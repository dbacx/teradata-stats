-- =============================================================================
-- Component   : Password Expiry
-- =============================================================================
-- Description : Identifies users with expired or expiring passwords
-- 
-- Version     : 1.0.0
-- Date        : 2026-04-29
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    CreatorName,
    PasswordLastModDate,
    CAST(CURRENT_DATE - PasswordLastModDate AS INTEGER) AS DaysSincePasswordChange
FROM DBC.UsersV
WHERE PasswordLastModDate < CURRENT_DATE - {password_expiry_days_threshold}
ORDER BY PasswordLastModDate ASC;
