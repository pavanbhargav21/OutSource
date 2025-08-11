
# Enhanced Employee Activity Analysis with Interval Alignment

I'll revise the queries to properly align the login/logout times with hourly intervals from your gold tables, similar to your sample but expanded for all employees over the past 2 weeks.

## 1. Core Activity Analysis with Interval Matching (Past 14 Days)

```sql
WITH EmployeeIntervals AS (
  SELECT
    a.emp_id,
    a.cal_date,
    a.interval,
    a.app_name,
    a.total_time_spent_active,
    a.total_time_spent_idle,
    a.window_lock_time,
    COALESCE(m.total_mouse_count, 0) AS mouse_clicks,
    COALESCE(k.total_keyboard_events, 0) AS key_strokes,
    l.emp_login_time,
    l.emp_logout_time,
    -- Extract hour from interval (e.g., "01:00-02:00" -> 1)
    CAST(SUBSTRING(a.interval, 1, 2) AS INT AS interval_hour,
    -- Create timestamp for interval start (e.g., "2025-08-10 01:00:00")
    TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5), 'yyyy-MM-dd HH:mm') AS interval_start,
    -- Create timestamp for interval end (e.g., "2025-08-10 02:00:00")
    TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 7, 5), 'yyyy-MM-dd HH:mm') AS interval_end
  FROM 
    gold_dashboard.analytics_emp_app_info a
  LEFT JOIN 
    gold_dashboard.analytics_emp_mouseclicks m
    ON a.emp_id = m.emp_id 
    AND a.cal_date = m.cal_date 
    AND a.app_name = m.app_name 
    AND a.interval = m.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_keystrokes k
    ON a.emp_id = k.emp_id 
    AND a.cal_date = k.cal_date 
    AND a.app_name = k.app_name 
    AND a.interval = k.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_login_logout l
    ON a.emp_id = l.emp_id 
    AND a.cal_date = l.shift_date
  WHERE 
    a.cal_date >= DATE_SUB(CURRENT_DATE(), 14)
),

FilteredIntervals AS (
  SELECT * FROM EmployeeIntervals
  WHERE 
    -- Include interval if it's within login/logout window
    (
      emp_login_time IS NOT NULL 
      AND emp_logout_time IS NOT NULL
      AND interval_start >= emp_login_time 
      AND interval_end <= emp_logout_time
    )
    OR
    (
      emp_login_time IS NOT NULL 
      AND emp_logout_time IS NULL 
      AND interval_start >= emp_login_time
    )
)

SELECT
  cal_date,
  DAYNAME(cal_date) AS day_of_week,
  interval,
  COUNT(DISTINCT emp_id) AS active_employees,
  ROUND(SUM(total_time_spent_active)/3600, 2) AS total_active_hours,
  ROUND(AVG(total_time_spent_active)/60, 2) AS avg_active_minutes_per_emp,
  SUM(mouse_clicks) AS total_mouse_clicks,
  SUM(key_strokes) AS total_key_strokes,
  ROUND(SUM(window_lock_time)/3600, 2) AS total_lock_hours
FROM 
  FilteredIntervals
GROUP BY 
  cal_date, day_of_week, interval
ORDER BY 
  cal_date DESC, 
  interval;
```

## 2. Employee Daily Summary with Working Hours Validation

```sql
WITH IntervalData AS (
  SELECT
    a.emp_id,
    a.cal_date,
    l.emp_login_time,
    l.emp_logout_time,
    SUM(a.total_time_spent_active) AS total_active_time,
    SUM(a.total_time_spent_idle) AS total_idle_time,
    SUM(a.window_lock_time) AS total_lock_time,
    SUM(COALESCE(m.total_mouse_count, 0)) AS total_mouse_clicks,
    SUM(COALESCE(k.total_keyboard_events, 0))) AS total_key_strokes,
    -- Count of active intervals (hours with activity)
    COUNT(DISTINCT a.interval) AS active_intervals
  FROM 
    gold_dashboard.analytics_emp_app_info a
  LEFT JOIN 
    gold_dashboard.analytics_emp_mouseclicks m
    ON a.emp_id = m.emp_id 
    AND a.cal_date = m.cal_date 
    AND a.interval = m.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_keystrokes k
    ON a.emp_id = k.emp_id 
    AND a.cal_date = k.cal_date 
    AND a.interval = k.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_login_logout l
    ON a.emp_id = l.emp_id 
    AND a.cal_date = l.shift_date
  WHERE 
    a.cal_date >= DATE_SUB(CURRENT_DATE(), 14)
    AND (
      (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NOT NULL AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5), 'yyyy-MM-dd HH:mm') >= l.emp_login_time AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 7, 5), 'yyyy-MM-dd HH:mm') <= l.emp_logout_time)
      OR
      (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NULL AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5), 'yyyy-MM-dd HH:mm') >= l.emp_login_time)
    )
  GROUP BY 
    a.emp_id, a.cal_date, l.emp_login_time, l.emp_logout_time
)

SELECT
  cal_date,
  DAYNAME(cal_date) AS day_of_week,
  COUNT(DISTINCT emp_id) AS total_employees,
  ROUND(AVG(total_active_time)/3600, 2) AS avg_active_hours,
  ROUND(AVG(total_mouse_clicks), 2) AS avg_mouse_clicks,
  ROUND(AVG(total_key_strokes), 2) AS avg_key_strokes,
  -- Percentage of employees with low activity (<2 active hours)
  ROUND(100.0 * SUM(CASE WHEN total_active_time < 7200 THEN 1 ELSE 0 END) / COUNT(DISTINCT emp_id), 2) AS pct_low_activity,
  -- Percentage of employees with minimal interaction (<100 combined events)
  ROUND(100.0 * SUM(CASE WHEN total_mouse_clicks + total_key_strokes < 100 THEN 1 ELSE 0 END) / COUNT(DISTINCT emp_id), 2) AS pct_minimal_interaction,
  -- Average active intervals (hours) per employee
  ROUND(AVG(active_intervals), 2) AS avg_active_hours_count
FROM 
  IntervalData
GROUP BY 
  cal_date, day_of_week
ORDER BY 
  cal_date DESC;
```

## 3. Application Usage by Time Interval (Heatmap Data)

```sql
WITH AppIntervalUsage AS (
  SELECT
    a.app_name,
    a.interval,
    TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5)), 'yyyy-MM-dd HH:mm') AS interval_start,
    COUNT(DISTINCT a.emp_id) AS active_users,
    SUM(a.total_time_spent_active) AS total_active_time,
    SUM(COALESCE(m.total_mouse_count, 0))) AS total_mouse_clicks,
    SUM(COALESCE(k.total_keyboard_events, 0))) AS total_key_strokes
  FROM 
    gold_dashboard.analytics_emp_app_info a
  LEFT JOIN 
    gold_dashboard.analytics_emp_mouseclicks m
    ON a.emp_id = m.emp_id AND a.cal_date = m.cal_date AND a.interval = m.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_keystrokes k
    ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date AND a.interval = k.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_login_logout l
    ON a.emp_id = l.emp_id AND a.cal_date = l.shift_date
  WHERE 
    a.cal_date >= DATE_SUB(CURRENT_DATE(), 14)
    AND (
      (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NOT NULL AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5)), 'yyyy-MM-dd HH:mm') >= l.emp_login_time AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 7, 5)), 'yyyy-MM-dd HH:mm') <= l.emp_logout_time)
      OR
      (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NULL AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5)), 'yyyy-MM-dd HH:mm') >= l.emp_login_time)
    )
  GROUP BY 
    a.app_name, a.interval, a.cal_date
)

SELECT
  app_name,
  SUBSTRING(interval, 1, 5) AS hour_slot,
  ROUND(AVG(active_users), 2) AS avg_daily_users,
  ROUND(AVG(total_active_time)/60, 2) AS avg_active_minutes,
  ROUND(AVG(total_mouse_clicks), 2) AS avg_mouse_clicks,
  ROUND(AVG(total_key_strokes), 2) AS avg_key_strokes
FROM 
  AppIntervalUsage
GROUP BY 
  app_name, hour_slot
ORDER BY 
  app_name, 
  hour_slot;
```

## 4. Employee Productivity Scorecard (Individual Level)

```sql
WITH EmployeeDaily AS (
  SELECT
    a.emp_id,
    a.cal_date,
    l.emp_login_time,
    l.emp_logout_time,
    SUM(a.total_time_spent_active) AS total_active_time,
    SUM(a.total_time_spent_idle) AS total_idle_time,
    SUM(a.window_lock_time) AS total_lock_time,
    SUM(COALESCE(m.total_mouse_count, 0))) AS total_mouse_clicks,
    SUM(COALESCE(k.total_keyboard_events, 0))) AS total_key_strokes,
    COUNT(DISTINCT a.interval) AS active_intervals
  FROM 
    gold_dashboard.analytics_emp_app_info a
  LEFT JOIN 
    gold_dashboard.analytics_emp_mouseclicks m
    ON a.emp_id = m.emp_id AND a.cal_date = m.cal_date AND a.interval = m.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_keystrokes k
    ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date AND a.interval = k.interval
  LEFT JOIN 
    gold_dashboard.analytics_emp_login_logout l
    ON a.emp_id = l.emp_id AND a.cal_date = l.shift_date
  WHERE 
    a.cal_date >= DATE_SUB(CURRENT_DATE(), 14)
    AND (
      (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NOT NULL AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5)), 'yyyy-MM-dd HH:mm') >= l.emp_login_time AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 7, 5)), 'yyyy-MM-dd HH:mm') <= l.emp_logout_time)
      OR
      (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NULL AND
       TO_TIMESTAMP(CONCAT(a.cal_date, ' ', SUBSTRING(a.interval, 1, 5)), 'yyyy-MM-dd HH:mm') >= l.emp_login_time)
    )
  GROUP BY 
    a.emp_id, a.cal_date, l.emp_login_time, l.emp_logout_time
)

SELECT
  emp_id,
  COUNT(DISTINCT cal_date) AS days_active,
  ROUND(AVG(total_active_time)/3600, 2) AS avg_active_hours_per_day,
  ROUND(AVG(total_mouse_clicks + total_key_strokes)), 2) AS avg_interactions_per_day,
  ROUND(AVG(total_active_time)/NULLIF(AVG(total_mouse_clicks + total_key_strokes), 0)*60, 2) AS seconds_per_interaction,
  ROUND(100.0 * SUM(CASE WHEN total_active_time >= 18000 THEN 1 ELSE 0 END) / COUNT(DISTINCT cal_date), 2) AS pct_days_high_activity,
  ROUND(100.0 * SUM(CASE WHEN total_mouse_clicks + total_key_strokes >= 1000 THEN 1 ELSE 0 END) / COUNT(DISTINCT cal_date), 2) AS pct_days_high_interaction
FROM 
  EmployeeDaily
GROUP BY 
  emp_id
ORDER BY 
  avg_active_hours_per_day DESC;
```

## Key Improvements:

1. **Precise Interval Alignment**: Properly matches login/logout times with hourly intervals using timestamp conversion
2. **Comprehensive Filtering**: Only includes intervals that fall within an employee's logged-in period
3. **All Employees**: Analyzes all employees, not just filtered ones
4. **Time Conversion**: Converts seconds to hours/minutes for better readability
5. **Multiple Metrics**: Tracks active time, idle time, lock time, mouse clicks, and keystrokes
6. **Flexible Date Range**: Uses past 14 days but can be easily adjusted

These queries maintain the exact interval matching logic from your sample while expanding the analysis to all employees and providing more comprehensive reporting.



Here are the SQL queries you can run in Databricks SQL to generate daily shift reports from your `gold_dashboard.emp_login_logout` table:

### 1. Daily Average Working Hours Report (Last 30 Days)

```sql
SELECT 
  DATE(shift_time) AS shift_date,
  DAYNAME(shift_time) AS day_of_week,
  COUNT(DISTINCT emp_id) AS total_employees,
  ROUND(AVG(TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time)), 2) AS avg_working_hours,
  SUM(CASE WHEN TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) >= 9 THEN 1 ELSE 0 END) AS employees_over_9_hours,
  SUM(CASE WHEN TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) < 9 THEN 1 ELSE 0 END) AS employees_under_9_hours
FROM 
  gold_dashboard.emp_login_logout
WHERE 
  shift_time >= DATE_SUB(CURRENT_DATE(), 30)
  AND emp_login_time IS NOT NULL
  AND emp_logout_time IS NOT NULL
GROUP BY 
  DATE(shift_time), DAYNAME(shift_time)
ORDER BY 
  shift_date DESC;
```

### 2. Weekly Pattern Analysis (Average by Day of Week)

```sql
SELECT 
  DAYNAME(shift_time) AS day_of_week,
  COUNT(DISTINCT emp_id) AS avg_employees_per_day,
  ROUND(AVG(TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time)), 2) AS avg_working_hours,
  ROUND(AVG(CASE WHEN TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) >= 9 THEN 1 ELSE 0 END) * 100, 2) AS percentage_over_9_hours
FROM 
  gold_dashboard.emp_login_logout
WHERE 
  shift_time >= DATE_SUB(CURRENT_DATE(), 90) -- Last 90 days for weekly pattern
  AND emp_login_time IS NOT NULL
  AND emp_logout_time IS NOT NULL
GROUP BY 
  DAYNAME(shift_time)
ORDER BY 
  CASE DAYNAME(shift_time)
    WHEN 'Sunday' THEN 1
    WHEN 'Monday' THEN 2
    WHEN 'Tuesday' THEN 3
    WHEN 'Wednesday' THEN 4
    WHEN 'Thursday' THEN 5
    WHEN 'Friday' THEN 6
    WHEN 'Saturday' THEN 7
  END;
```

### 3. Detailed Daily Report (Yesterday)

```sql
SELECT 
  DATE(shift_time) AS shift_date,
  COUNT(DISTINCT emp_id) AS total_employees,
  ROUND(AVG(TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time)), 2) AS avg_working_hours,
  SUM(CASE WHEN TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) >= 9 THEN 1 ELSE 0 END) AS employees_over_9_hours,
  SUM(CASE WHEN TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) < 9 THEN 1 ELSE 0 END) AS employees_under_9_hours,
  ROUND(MIN(TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time)), 2) AS min_hours_worked,
  ROUND(MAX(TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time)), 2) AS max_hours_worked
FROM 
  gold_dashboard.emp_login_logout
WHERE 
  DATE(shift_time) = DATE_SUB(CURRENT_DATE(), 1)
  AND emp_login_time IS NOT NULL
  AND emp_logout_time IS NOT NULL
GROUP BY 
  DATE(shift_time);
```

### 4. Employee List with Long/Short Shifts (Yesterday)

```sql
SELECT 
  emp_id,
  DATE(shift_time) AS shift_date,
  TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) AS hours_worked,
  CASE 
    WHEN TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time) >= 9 THEN 'Over 9 hours'
    ELSE 'Under 9 hours'
  END AS shift_category
FROM 
  gold_dashboard.emp_login_logout
WHERE 
  DATE(shift_time) = DATE_SUB(CURRENT_DATE(), 1)
  AND emp_login_time IS NOT NULL
  AND emp_logout_time IS NOT NULL
ORDER BY 
  hours_worked DESC;
```

### 5. Trend Analysis (Last 12 Weeks by Day of Week)

```sql
SELECT 
  DATE_TRUNC('WEEK', shift_time) AS week_starting,
  DAYNAME(shift_time) AS day_of_week,
  COUNT(DISTINCT emp_id) AS total_employees,
  ROUND(AVG(TIMESTAMPDIFF(HOUR, emp_login_time, emp_logout_time)), 2) AS avg_working_hours
FROM 
  gold_dashboard.emp_login_logout
WHERE 
  shift_time >= DATE_SUB(CURRENT_DATE(), 84) -- 12 weeks
  AND emp_login_time IS NOT NULL
  AND emp_logout_time IS NOT NULL
GROUP BY 
  DATE_TRUNC('WEEK', shift_time), DAYNAME(shift_time)
ORDER BY 
  week_starting DESC, 
  CASE DAYNAME(shift_time)
    WHEN 'Sunday' THEN 1
    WHEN 'Monday' THEN 2
    WHEN 'Tuesday' THEN 3
    WHEN 'Wednesday' THEN 4
    WHEN 'Thursday' THEN 5
    WHEN 'Friday' THEN 6
    WHEN 'Saturday' THEN 7
  END;
```

### Notes for Databricks Implementation:

1. **Time Functions**: 
   - `TIMESTAMPDIFF` is supported in Databricks SQL
   - Alternative for hours calculation: `ROUND((unix_timestamp(emp_logout_time) - unix_timestamp(emp_login_time))/3600, 2)`

2. **Performance**:
   - These queries will benefit if your table is partitioned by `shift_time`
   - Consider caching frequently accessed data

3. **Visualization**:
   - The weekly pattern query (#2) is perfect for a bar chart
   - The trend analysis (#5) works well as a line chart by week

4. **Customization**:
   - Adjust the time windows (30 days, 90 days) as needed
   - Change the 9-hour threshold to match your business rules

All queries are optimized for Databricks SQL and will work with your login/logout timestamp data to provide comprehensive shift analysis.









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