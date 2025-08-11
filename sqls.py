# Daily Report Generation Queries

Based on your requirements, I'll create 15-20 queries for daily report generation that analyze the data from your AppTrace and SysTrace tables. These queries will focus on employee activity patterns, discrepancies between different tracking systems, and data completeness.

## 1. Employees Running AppTrace (Last 2 Weeks)
```sql
SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    app_trace.emp_activity
WHERE 
    cal_date BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE
GROUP BY 
    cal_date
ORDER BY 
    cal_date DESC;
```

## 2. Employees Running Keyboard Data (Last 2 Weeks)
```sql
SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    sys_trace.emp_keyboarddata
WHERE 
    cal_date BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE
GROUP BY 
    cal_date
ORDER BY 
    cal_date DESC;
```

## 3. Employees Running Mouse Data (Last 2 Weeks)
```sql
SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    sys_trace.emp_mousedata
WHERE 
    cal_date BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE
GROUP BY 
    cal_date
ORDER BY 
    cal_date DESC;
```

## 4. Employees Running AppTrace but No Keyboard Data (Yesterday)
```sql
SELECT 
    a.cal_date,
    COUNT(DISTINCT a.emp_id) AS employee_count
FROM 
    app_trace.emp_activity a
LEFT JOIN 
    sys_trace.emp_keyboarddata k ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date
WHERE 
    a.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND k.emp_id IS NULL
GROUP BY 
    a.cal_date;
```

## 5. List of Employees Running AppTrace but No Keyboard Data (Yesterday)
```sql
SELECT DISTINCT 
    a.emp_id,
    a.cal_date
FROM 
    app_trace.emp_activity a
LEFT JOIN 
    sys_trace.emp_keyboarddata k ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date
WHERE 
    a.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND k.emp_id IS NULL;
```

## 6. Employees Running AppTrace but No Mouse Data (Yesterday)
```sql
SELECT 
    a.cal_date,
    COUNT(DISTINCT a.emp_id) AS employee_count
FROM 
    app_trace.emp_activity a
LEFT JOIN 
    sys_trace.emp_mousedata m ON a.emp_id = m.emp_id AND a.cal_date = m.cal_date
WHERE 
    a.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND m.emp_id IS NULL
GROUP BY 
    a.cal_date;
```

## 7. List of Employees Running AppTrace but No Mouse Data (Yesterday)
```sql
SELECT DISTINCT 
    a.emp_id,
    a.cal_date
FROM 
    app_trace.emp_activity a
LEFT JOIN 
    sys_trace.emp_mousedata m ON a.emp_id = m.emp_id AND a.cal_date = m.cal_date
WHERE 
    a.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND m.emp_id IS NULL;
```

## 8. Employees Running Keyboard Data but No AppTrace (Yesterday)
```sql
SELECT 
    k.cal_date,
    COUNT(DISTINCT k.emp_id) AS employee_count
FROM 
    sys_trace.emp_keyboarddata k
LEFT JOIN 
    app_trace.emp_activity a ON k.emp_id = a.emp_id AND k.cal_date = a.cal_date
WHERE 
    k.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND a.emp_id IS NULL
GROUP BY 
    k.cal_date;
```

## 9. Employees Running Mouse Data but No AppTrace (Yesterday)
```sql
SELECT 
    m.cal_date,
    COUNT(DISTINCT m.emp_id) AS employee_count
FROM 
    sys_trace.emp_mousedata m
LEFT JOIN 
    app_trace.emp_activity a ON m.emp_id = a.emp_id AND m.cal_date = a.cal_date
WHERE 
    m.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND a.emp_id IS NULL
GROUP BY 
    m.cal_date;
```

## 10. Employees Running AppTrace but No Keyboard OR Mouse Data (Yesterday)
```sql
SELECT 
    a.cal_date,
    COUNT(DISTINCT a.emp_id) AS employee_count
FROM 
    app_trace.emp_activity a
LEFT JOIN 
    sys_trace.emp_keyboarddata k ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date
LEFT JOIN 
    sys_trace.emp_mousedata m ON a.emp_id = m.emp_id AND a.cal_date = m.cal_date
WHERE 
    a.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND k.emp_id IS NULL
    AND m.emp_id IS NULL
GROUP BY 
    a.cal_date;
```

## 11. Employees Running Keyboard OR Mouse Data but No AppTrace (Yesterday)
```sql
WITH sys_activity AS (
    SELECT DISTINCT emp_id, cal_date FROM sys_trace.emp_keyboarddata WHERE cal_date = CURRENT_DATE - INTERVAL '1 day'
    UNION
    SELECT DISTINCT emp_id, cal_date FROM sys_trace.emp_mousedata WHERE cal_date = CURRENT_DATE - INTERVAL '1 day'
)
SELECT 
    s.cal_date,
    COUNT(DISTINCT s.emp_id) AS employee_count
FROM 
    sys_activity s
LEFT JOIN 
    app_trace.emp_activity a ON s.emp_id = a.emp_id AND s.cal_date = a.cal_date
WHERE 
    a.emp_id IS NULL
GROUP BY 
    s.cal_date;
```

## 12. Employees with WFH/WFO Status but No AppTrace Activity (Yesterday)
```sql
SELECT 
    w.cal_date,
    COUNT(DISTINCT w.emp_id) AS employee_count
FROM 
    app_trace.emp_wfo_wfh_status w
LEFT JOIN 
    app_trace.emp_activity a ON w.emp_id = a.emp_id AND w.cal_date = a.cal_date
WHERE 
    w.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND a.emp_id IS NULL
GROUP BY 
    w.cal_date;
```

## 13. Employees with AppTrace Activity but No HR Employee Central Record (Yesterday)
```sql
-- Assuming hr_employee_central is the HR table with empl_id column
SELECT 
    a.cal_date,
    COUNT(DISTINCT a.emp_id) AS employee_count
FROM 
    app_trace.emp_activity a
LEFT JOIN 
    hr_employee_central h ON a.emp_id = h.empl_id
WHERE 
    a.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND h.empl_id IS NULL
GROUP BY 
    a.cal_date;
```

## 14. Daily CPU/RAM/Disk Usage Summary (Last 7 Days)
```sql
SELECT 
    cal_date,
    AVG(cpu_used_pct) AS avg_cpu_usage,
    AVG(ram_used_pct) AS avg_ram_usage,
    AVG(disk_used_pct) AS avg_disk_usage,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    sys_trace.emp_cpudata
WHERE 
    cal_date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
GROUP BY 
    cal_date
ORDER BY 
    cal_date DESC;
```

## 15. Daily Idle Time Analysis (Last 7 Days)
```sql
SELECT 
    cal_date,
    AVG(tot_idle_time) AS avg_idle_time,
    MAX(tot_idle_time) AS max_idle_time,
    MIN(tot_idle_time) AS min_idle_time,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    app_trace.emp_idletime
WHERE 
    cal_date BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
GROUP BY 
    cal_date
ORDER BY 
    cal_date DESC;
```

## 16. WFH vs WFO Employee Count (Yesterday)
```sql
SELECT 
    cal_date,
    is_wfh,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    app_trace.emp_wfo_wfh_status
WHERE 
    cal_date = CURRENT_DATE - INTERVAL '1 day'
GROUP BY 
    cal_date, is_wfh;
```

## 17. Most Used Applications (Yesterday)
```sql
SELECT 
    app_name,
    COUNT(*) AS usage_count,
    COUNT(DISTINCT emp_id) AS employee_count
FROM 
    app_trace.emp_activity
WHERE 
    cal_date = CURRENT_DATE - INTERVAL '1 day'
GROUP BY 
    app_name
ORDER BY 
    usage_count DESC
LIMIT 10;
```

## 18. Login Data Completeness (Yesterday)
```sql
SELECT 
    COUNT(DISTINCT l.host_name) AS unique_hosts,
    COUNT(DISTINCT CASE WHEN a.emp_id IS NOT NULL THEN a.emp_id END) AS employees_with_apptrace,
    COUNT(DISTINCT CASE WHEN k.emp_id IS NOT NULL THEN k.emp_id END) AS employees_with_keyboard,
    COUNT(DISTINCT CASE WHEN m.emp_id IS NOT NULL THEN m.emp_id END) AS employees_with_mouse
FROM 
    sys_trace.emp_logindata l
LEFT JOIN 
    app_trace.emp_activity a ON l.cal_date = a.cal_date
LEFT JOIN 
    sys_trace.emp_keyboarddata k ON l.cal_date = k.cal_date
LEFT JOIN 
    sys_trace.emp_mousedata m ON l.cal_date = m.cal_date
WHERE 
    l.cal_date = CURRENT_DATE - INTERVAL '1 day';
```

## 19. Employees with Keyboard/Mouse Activity but High Idle Time (Yesterday)
```sql
SELECT 
    i.emp_id,
    i.cal_date,
    i.tot_idle_time,
    COUNT(DISTINCT k.event_time) AS keyboard_events,
    COUNT(DISTINCT m.event_time) AS mouse_events
FROM 
    app_trace.emp_idletime i
LEFT JOIN 
    sys_trace.emp_keyboarddata k ON i.emp_id = k.emp_id AND i.cal_date = k.cal_date
LEFT JOIN 
    sys_trace.emp_mousedata m ON i.emp_id = m.emp_id AND i.cal_date = m.cal_date
WHERE 
    i.cal_date = CURRENT_DATE - INTERVAL '1 day'
    AND i.tot_idle_time > 180 -- More than 3 hours idle time
    AND (k.emp_id IS NOT NULL OR m.emp_id IS NOT NULL)
GROUP BY 
    i.emp_id, i.cal_date, i.tot_idle_time;
```

## 20. Data Ingestion Lag Analysis (Yesterday)
```sql
SELECT 
    'emp_activity' AS table_name,
    AVG(EXTRACT(EPOCH FROM (ingestion_time - created_on))/60 AS avg_lag_minutes
FROM 
    app_trace.emp_activity
WHERE 
    cal_date = CURRENT_DATE - INTERVAL '1 day'

UNION ALL

SELECT 
    'emp_keyboarddata' AS table_name,
    AVG(EXTRACT(EPOCH FROM (ingestion_time - created_on))/60 AS avg_lag_minutes
FROM 
    sys_trace.emp_keyboarddata
WHERE 
    cal_date = CURRENT_DATE - INTERVAL '1 day'

UNION ALL

SELECT 
    'emp_mousedata' AS table_name,
    AVG(EXTRACT(EPOCH FROM (ingestion_time - created_on))/60 AS avg_lag_minutes
FROM 
    sys_trace.emp_mousedata
WHERE 
    cal_date = CURRENT_DATE - INTERVAL '1 day';
```

These queries cover the requirements you specified, focusing on data completeness, discrepancies between tracking systems, and activity patterns. You can adjust the date filters as needed for different reporting periods.