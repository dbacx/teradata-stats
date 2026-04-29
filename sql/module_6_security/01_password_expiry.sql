-- Component 1: Password Expiry
-- Identifies users with expired or expiring passwords
-- Queries DBC.UsersV for password last modification date

LOCKING ROW FOR ACCESS
SELECT 
    UserName,
    CreatorName,
    PasswordLastModDate,
    CAST(CURRENT_DATE - PasswordLastModDate AS INTEGER) AS DaysSincePasswordChange
FROM DBC.UsersV
WHERE PasswordLastModDate < CURRENT_DATE - {password_expiry_days_threshold}
ORDER BY PasswordLastModDate ASC;
