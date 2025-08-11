-- 1. Check for employees with app_trace.emp_activity but missing in analytics_emp_app_info
WITH RawAppActivity AS (
  SELECT DISTINCT 
    emp_id, 
    CAST(created_on AS DATE) AS cal_date,
    'app_trace.emp_activity' AS raw_table
  FROM 
    app_trace.emp_activity
  WHERE 
    created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in gold_dashboard.analytics_emp_app_info' AS missing_in
FROM 
  RawAppActivity r
WHERE 
  NOT EXISTS (
    SELECT 1 
    FROM gold_dashboard.analytics_emp_app_info g 
    WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
  )

UNION ALL

-- 2. Check for employees with app_trace.emp_idletime but missing in corresponding gold table
WITH RawIdleTime AS (
  SELECT DISTINCT 
    emp_id, 
    CAST(created_on AS DATE) AS cal_date,
    'app_trace.emp_idletime' AS raw_table
  FROM 
    app_trace.emp_idletime
  WHERE 
    created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in corresponding gold idle time table' AS missing_in
FROM 
  RawIdleTime r
WHERE 
  NOT EXISTS (
    SELECT 1 
    FROM gold_dashboard.analytics_emp_app_info g  -- Adjust to correct gold table if different
    WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
  )

UNION ALL

-- 3. Check for employees with sys_trace.emp_keyboarddata but missing in analytics_emp_keystrokes
WITH RawKeyboard AS (
  SELECT DISTINCT 
    emp_id, 
    CAST(created_on AS DATE) AS cal_date,
    'sys_trace.emp_keyboarddata' AS raw_table
  FROM 
    sys_trace.emp_keyboarddata
  WHERE 
    created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in gold_dashboard.analytics_emp_keystrokes' AS missing_in
FROM 
  RawKeyboard r
WHERE 
  NOT EXISTS (
    SELECT 1 
    FROM gold_dashboard.analytics_emp_keystrokes g 
    WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
  )

UNION ALL

-- 4. Check for employees with sys_trace.emp_mousedata but missing in analytics_emp_mouseclicks
WITH RawMouse AS (
  SELECT DISTINCT 
    emp_id, 
    CAST(created_on AS DATE) AS cal_date,
    'sys_trace.emp_mousedata' AS raw_table
  FROM 
    sys_trace.emp_mousedata
  WHERE 
    created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in gold_dashboard.analytics_emp_mouseclicks' AS missing_in
FROM 
  RawMouse r
WHERE 
  NOT EXISTS (
    SELECT 1 
    FROM gold_dashboard.analytics_emp_mouseclicks g 
    WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
  )

UNION ALL

-- 5. Check for employees with sys_trace.emp_logindata but missing in analytics_emp_login_logout
WITH RawLogin AS (
  SELECT DISTINCT 
    emp_id, 
    CAST(created_on AS DATE) AS cal_date,
    'sys_trace.emp_logindata' AS raw_table
  FROM 
    sys_trace.emp_logindata
  WHERE 
    created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in gold_dashboard.analytics_emp_login_logout' AS missing_in
FROM 
  RawLogin r
WHERE 
  NOT EXISTS (
    SELECT 1 
    FROM gold_dashboard.analytics_emp_login_logout g 
    WHERE g.emp_id = r.emp_id AND g.shift_date = r.cal_date
  )

ORDER BY 
  cal_date DESC, 
  emp_id;



# SQL Queries for AppTrace Version Analysis (Simplified)

I'll provide simplified versions of the queries without the candidate_id concept, focusing purely on version distribution and changes.

## Query 1: Employee Count by Version (Simplified)

```sql
%sql
SELECT 
  pulse_version,
  COUNT(DISTINCT emp_id) AS employee_count,
  cal_date,
  DAYNAME(cal_date) AS day_name
FROM app_trace.emp_wfo_wfh_status
WHERE cal_date >= date_sub(current_date(), 14)
GROUP BY pulse_version, cal_date, DAYNAME(cal_date)
ORDER BY pulse_version, cal_date DESC
```

## Query 2: Version Change Analysis (New/Old/Same Version)

```sql
%sql
WITH version_changes AS (
  SELECT 
    curr.emp_id,
    curr.cal_date,
    curr.pulse_version AS current_version,
    prev.pulse_version AS previous_version,
    CASE 
      WHEN prev.pulse_version IS NULL THEN 'new_employee'
      WHEN curr.pulse_version = prev.pulse_version THEN 'same_version'
      WHEN 
        split(curr.pulse_version, '\\.')[0] > split(prev.pulse_version, '\\.')[0] OR
        (split(curr.pulse_version, '\\.')[0] = split(prev.pulse_version, '\\.')[0] AND 
         split(curr.pulse_version, '\\.')[1] > split(prev.pulse_version, '\\.')[1]) OR
        (split(curr.pulse_version, '\\.')[0] = split(prev.pulse_version, '\\.')[0] AND 
         split(curr.pulse_version, '\\.')[1] = split(prev.pulse_version, '\\.')[1] AND
         split(curr.pulse_version, '\\.')[2] > split(prev.pulse_version, '\\.')[2]) OR
        (split(curr.pulse_version, '\\.')[0] = split(prev.pulse_version, '\\.')[0] AND 
         split(curr.pulse_version, '\\.')[1] = split(prev.pulse_version, '\\.')[1] AND
         split(curr.pulse_version, '\\.')[2] = split(prev.pulse_version, '\\.')[2] AND
         split(curr.pulse_version, '\\.')[3] > split(prev.pulse_version, '\\.')[3])
      THEN 'newer_version'
      ELSE 'older_version'
    END AS version_change
  FROM 
    (SELECT emp_id, cal_date, pulse_version 
     FROM app_trace.emp_wfo_wfh_status
     WHERE cal_date >= date_sub(current_date(), 14)) curr
  LEFT JOIN 
    (SELECT emp_id, date_add(cal_date, 1) AS next_date, pulse_version 
     FROM app_trace.emp_wfo_wfh_status
     WHERE cal_date >= date_sub(current_date(), 15)) prev
    ON curr.emp_id = prev.emp_id AND curr.cal_date = prev.next_date
),

daily_counts AS (
  SELECT
    cal_date,
    COUNT(DISTINCT emp_id) AS total_employees,
    COUNT(DISTINCT CASE WHEN version_change = 'newer_version' THEN emp_id END) AS moved_to_new_version,
    COUNT(DISTINCT CASE WHEN version_change = 'older_version' THEN emp_id END) AS moved_to_old_version,
    COUNT(DISTINCT CASE WHEN version_change = 'same_version' THEN emp_id END) AS same_version,
    COUNT(DISTINCT CASE WHEN version_change = 'new_employee' THEN emp_id END) AS new_employees
  FROM version_changes
  GROUP BY cal_date
)

SELECT
  d.cal_date,
  DAYNAME(d.cal_date) AS day_name,
  d.total_employees,
  a.emp_activity_count,
  w.wfh_status_count,
  d.moved_to_new_version,
  d.moved_to_old_version,
  d.same_version,
  d.new_employees,
  ROUND((d.moved_to_new_version * 100.0 / NULLIF(d.total_employees - d.new_employees, 0)), 2) AS pct_upgraded,
  ROUND((d.moved_to_old_version * 100.0 / NULLIF(d.total_employees - d.new_employees, 0)), 2) AS pct_downgraded
FROM daily_counts d
LEFT JOIN (
  SELECT cal_date, COUNT(DISTINCT emp_id) AS emp_activity_count
  FROM app_trace.emp_activity
  WHERE cal_date >= date_sub(current_date(), 14)
  GROUP BY cal_date
) a ON d.cal_date = a.cal_date
LEFT JOIN (
  SELECT cal_date, COUNT(DISTINCT emp_id) AS wfh_status_count
  FROM app_trace.emp_wfo_wfh_status
  WHERE cal_date >= date_sub(current_date(), 14)
  GROUP BY cal_date
) w ON d.cal_date = w.cal_date
ORDER BY d.cal_date DESC
```

## Key Features of These Simplified Queries:

1. **Version Distribution Report**:
   - Shows how many employees are running each version of AppTrace
   - Includes date and day of week for analysis
   - Simple count of employees by version

2. **Version Change Analysis**:
   - Tracks employees moving to newer/older versions day-over-day
   - Compares version numbers using semantic versioning logic
   - Includes metrics for:
     - Total employees
     - Employees running AppTrace (emp_activity)
     - Employees with WFH status data
     - Employees moved to newer version
     - Employees moved to older version
     - Employees with same version
     - New employees (no previous day data)
   - Calculates upgrade/downgrade percentages

3. **Simplified Data**:
   - Removed all candidate_id references
   - Focuses purely on version tracking across all employees
   - Maintains all the key metrics you requested

Both queries analyze the last 14 days of data, and you can adjust this time frame by changing the `date_sub(current_date(), 14)` values.




# SQL Queries for Employee Activity Analysis

I'll provide the three requested SQL queries for your Databricks notebook. These queries will analyze the employee activity data over the last 14 days with comparisons to the previous week's same day.

## Query 1: Employee Counts by Trace Type with Weekly Comparison

```sql
%sql
WITH current_week_data AS (
  SELECT 
    cal_date,
    DAYNAME(cal_date) AS day_name,
    COUNT(DISTINCT a.emp_id) AS app_trace_count,
    COUNT(DISTINCT k.emp_id) AS keyboard_count,
    COUNT(DISTINCT m.emp_id) AS mouse_count,
    COUNT(DISTINCT c.emp_id) AS cpu_count
  FROM 
    (SELECT DISTINCT emp_id, cal_date FROM app_trace.emp_activity 
     WHERE cal_date >= date_sub(current_date(), 14)) a
  LEFT JOIN 
    (SELECT DISTINCT emp_id, cal_date FROM sys_trace.emp_keyboarddata) k 
    ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date
  LEFT JOIN 
    (SELECT DISTINCT emp_id, cal_date FROM sys_trace.emp_mousedata) m 
    ON a.emp_id = m.emp_id AND a.cal_date = m.cal_date
  LEFT JOIN 
    (SELECT DISTINCT emp_id, cal_date FROM sys_trace.emp_cpudata) c 
    ON a.emp_id = c.emp_id AND a.cal_date = c.cal_date
  GROUP BY cal_date, DAYNAME(cal_date)
),

previous_week_data AS (
  SELECT 
    date_add(cal_date, 7) AS cal_date,
    app_trace_count AS prev_app_trace_count,
    keyboard_count AS prev_keyboard_count,
    mouse_count AS prev_mouse_count,
    cpu_count AS prev_cpu_count
  FROM current_week_data
  WHERE cal_date < date_sub(current_date(), 7)
)

SELECT 
  c.cal_date,
  c.day_name,
  c.app_trace_count,
  p.prev_app_trace_count,
  c.keyboard_count,
  p.prev_keyboard_count,
  c.mouse_count,
  p.prev_mouse_count,
  c.cpu_count,
  p.prev_cpu_count
FROM current_week_data c
LEFT JOIN previous_week_data p ON c.cal_date = p.cal_date
WHERE c.cal_date >= date_sub(current_date(), 14)
ORDER BY c.cal_date DESC
```

## Query 2: Employees Running App Trace but Not Mouse/Keyboard Data

```sql
%sql
WITH app_trace_employees AS (
  SELECT DISTINCT emp_id, cal_date 
  FROM app_trace.emp_activity 
  WHERE cal_date >= date_sub(current_date(), 14)
),

keyboard_mouse_employees AS (
  SELECT DISTINCT emp_id, cal_date 
  FROM sys_trace.emp_keyboarddata 
  WHERE cal_date >= date_sub(current_date(), 14)
  UNION
  SELECT DISTINCT emp_id, cal_date 
  FROM sys_trace.emp_mousedata 
  WHERE cal_date >= date_sub(current_date(), 14)
),

current_week_counts AS (
  SELECT 
    a.cal_date,
    DAYNAME(a.cal_date) AS day_name,
    COUNT(DISTINCT a.emp_id) AS app_only_count
  FROM app_trace_employees a
  LEFT JOIN keyboard_mouse_employees k ON a.emp_id = k.emp_id AND a.cal_date = k.cal_date
  WHERE k.emp_id IS NULL
  GROUP BY a.cal_date, DAYNAME(a.cal_date)
),

previous_week_counts AS (
  SELECT 
    date_add(cal_date, 7) AS cal_date,
    app_only_count AS prev_app_only_count
  FROM current_week_counts
  WHERE cal_date < date_sub(current_date(), 7)
)

SELECT 
  c.cal_date,
  c.day_name,
  c.app_only_count,
  p.prev_app_only_count
FROM current_week_counts c
LEFT JOIN previous_week_counts p ON c.cal_date = p.cal_date
WHERE c.cal_date >= date_sub(current_date(), 14)
ORDER BY c.cal_date DESC
```

## Query 3: Employees Running Keyboard/Mouse Data but Not App Trace

```sql
%sql
WITH app_trace_employees AS (
  SELECT DISTINCT emp_id, cal_date 
  FROM app_trace.emp_activity 
  WHERE cal_date >= date_sub(current_date(), 14)
),

keyboard_mouse_employees AS (
  SELECT DISTINCT emp_id, cal_date 
  FROM sys_trace.emp_keyboarddata 
  WHERE cal_date >= date_sub(current_date(), 14)
  UNION
  SELECT DISTINCT emp_id, cal_date 
  FROM sys_trace.emp_mousedata 
  WHERE cal_date >= date_sub(current_date(), 14)
),

current_week_counts AS (
  SELECT 
    k.cal_date,
    DAYNAME(k.cal_date) AS day_name,
    COUNT(DISTINCT k.emp_id) AS keyboard_mouse_only_count
  FROM keyboard_mouse_employees k
  LEFT JOIN app_trace_employees a ON k.emp_id = a.emp_id AND k.cal_date = a.cal_date
  WHERE a.emp_id IS NULL
  GROUP BY k.cal_date, DAYNAME(k.cal_date)
),

previous_week_counts AS (
  SELECT 
    date_add(cal_date, 7) AS cal_date,
    keyboard_mouse_only_count AS prev_keyboard_mouse_only_count
  FROM current_week_counts
  WHERE cal_date < date_sub(current_date(), 7)
)

SELECT 
  c.cal_date,
  c.day_name,
  c.keyboard_mouse_only_count,
  p.prev_keyboard_mouse_only_count
FROM current_week_counts c
LEFT JOIN previous_week_counts p ON c.cal_date = p.cal_date
WHERE c.cal_date >= date_sub(current_date(), 14)
ORDER BY c.cal_date DESC
```

These queries will:
1. Show daily counts of employees using each trace type with previous week comparisons
2. Identify employees using App Trace but not Mouse/Keyboard data
3. Identify employees using Mouse/Keyboard data but not App Trace

All queries cover the last 14 days and include day-of-week comparisons to the previous week's same day.





WITH FilteredEmployees AS (
    SELECT emplid
    FROM inbound.hr_employee_central
    WHERE func_mgr_id = manager_id
      AND (termination_dt > CURRENT_TIMESTAMP OR termination_dt IS NULL)
),

FilteredAppInfo AS (
    SELECT 
        emp_id,
        cal_date,
        app_name,
        total_time_spent_active,
        total_time_spent_idle,
        window_lock_time,
        interval
    FROM gold_dashboard.analytics_emp_app_info
    WHERE cal_date BETWEEN prev_start_date AND ext_end_date
      AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

FilteredMouseInfo AS (
    SELECT 
        emp_id,
        cal_date,
        app_name,
        total_mouse_count,
        interval
    FROM gold_dashboard.analytics_emp_mouseclicks
    WHERE cal_date BETWEEN prev_start_date AND ext_end_date
      AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

FilteredKeyInfo AS (
    SELECT 
        emp_id,
        cal_date,
        app_name,
        total_keyboard_events,
        interval
    FROM gold_dashboard.analytics_emp_keystrokes
    WHERE cal_date BETWEEN prev_start_date AND ext_end_date
      AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

FilteredLoginLogout AS (
    SELECT 
        emp_id,
        shift_date,
        emp_late,
        emp_login_time,
        emp_logout_time,
        -- Calculate shift duration in seconds
        CASE 
            WHEN emp_login_time IS NOT NULL AND emp_logout_time IS NOT NULL
            THEN TIMESTAMPDIFF(SECOND, 
                 TO_TIMESTAMP(emp_login_time, 'yyyy-MM-dd HH:mm:ss'),
                 TO_TIMESTAMP(emp_logout_time, 'yyyy-MM-dd HH:mm:ss'))
            ELSE NULL
        END AS shift_duration_seconds
    FROM gold_dashboard.analytics_emp_login_logout
    WHERE shift_date BETWEEN prev_start_date AND end_date
      AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

EmployeeActivity AS (
    SELECT
        i.emp_id,
        l.shift_date AS cal_date,
        MAX(l.emp_login_time) AS emp_login_time,
        MAX(l.emp_logout_time) AS emp_logout_time,
        MAX(l.shift_duration_seconds) AS shift_duration_seconds,
        COALESCE(SUM(i.total_time_spent_active), 0) AS total_active_time,
        COALESCE(SUM(i.total_time_spent_idle), 0) AS total_idle_time,
        COALESCE(SUM(i.window_lock_time), 0) AS total_window_lock_time,
        COALESCE(SUM(m.total_mouse_count), 0) AS total_mouse_clicks,
        COALESCE(SUM(k.total_keyboard_events), 0) AS total_key_strokes
    FROM FilteredAppInfo i
    LEFT JOIN FilteredLoginLogout l
        ON i.emp_id = l.emp_id
       AND (
            -- If login and logout times are both present
            (
                l.emp_login_time IS NOT NULL
                AND l.emp_logout_time IS NOT NULL
                AND DATEADD(hour, -1, TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss'))
                    < TO_TIMESTAMP(CONCAT(i.cal_date, SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
                AND TO_TIMESTAMP(l.emp_logout_time, 'yyyy-MM-dd HH:mm:ss')
                    > TO_TIMESTAMP(CONCAT(i.cal_date, SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
            )
            -- If logout time is NULL, consider all intervals after login
            OR (
                l.emp_logout_time IS NULL
                AND DATEADD(hour, -1, TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss'))
                    < TO_TIMESTAMP(CONCAT(i.cal_date, SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
            )
       )
    LEFT JOIN FilteredMouseInfo m
        ON i.emp_id = m.emp_id
       AND i.cal_date = m.cal_date
       AND i.interval = m.interval
    LEFT JOIN FilteredKeyInfo k
        ON i.emp_id = k.emp_id
       AND i.cal_date = k.cal_date
       AND i.interval = k.interval
    WHERE i.emp_id IN (SELECT emplid FROM FilteredEmployees)
      AND l.shift_date BETWEEN prev_start_date AND end_date
    GROUP BY i.emp_id, l.shift_date
),

PerDayEmployeeSummary AS (
    SELECT
        emp_id,
        cal_date,
        emp_login_time,
        emp_logout_time,
        shift_duration_seconds,
        -- Convert seconds to HH:MM:SS format
        CASE 
            WHEN shift_duration_seconds IS NOT NULL 
            THEN CONCAT(
                LPAD(FLOOR(shift_duration_seconds/3600), 2, '0'), ':',
                LPAD(FLOOR(MOD(shift_duration_seconds,3600)/60), 2, '0'), ':',
                LPAD(MOD(shift_duration_seconds,60), 2, '0'))
            ELSE NULL
        END AS shift_duration_formatted,
        SUM(total_active_time) AS daily_active_time,
        SUM(total_active_time + total_idle_time + total_window_lock_time) AS daily_total_time,
        -- Calculate productivity ratio (active time vs shift duration)
        CASE
            WHEN shift_duration_seconds > 0
            THEN ROUND(SUM(total_active_time)/NULLIF(shift_duration_seconds,0)*100, 2)
            ELSE NULL
        END AS productivity_percentage,
        SUM(total_mouse_clicks) AS total_mouse_clicks,
        SUM(total_key_strokes) AS total_key_strokes
    FROM EmployeeActivity
    GROUP BY emp_id, cal_date, emp_login_time, emp_logout_time, shift_duration_seconds
)

SELECT 
    emp_id,
    cal_date,
    emp_login_time,
    emp_logout_time,
    shift_duration_formatted,
    daily_active_time,
    -- Convert seconds to hours for readability
    ROUND(daily_active_time/3600, 2) AS daily_active_hours,
    daily_total_time,
    ROUND(daily_total_time/3600, 2) AS daily_total_hours,
    productivity_percentage,
    total_mouse_clicks,
    total_key_strokes,
    -- Calculate input rates
    CASE 
        WHEN daily_active_time > 0 
        THEN ROUND(total_mouse_clicks/NULLIF(daily_active_time/3600,0), 2)
        ELSE 0
    END AS mouse_clicks_per_hour,
    CASE 
        WHEN daily_active_time > 0 
        THEN ROUND(total_key_strokes/NULLIF(daily_active_time/3600,0), 2)
        ELSE 0
    END AS keystrokes_per_hour
FROM PerDayEmployeeSummary
ORDER BY cal_date, emp_id;




WITH 
-- 1. Active employees
FilteredEmployees AS (
    SELECT emplid
    FROM inbound.hr_employee_central
    WHERE func_agr_id(manager_id) 
    AND (TERMINATION_DT > CURRENT_TIMESTAMP() OR TERMINATION_DT IS NULL)
),

-- 2. Date range (last 14 days)
date_range AS (
    SELECT 
        DATEADD(day, -14, CURRENT_DATE()) AS start_date,
        CURRENT_DATE() AS end_date
),

-- 3. Base activity data (no app-level detail)
DailyActivity AS (
    SELECT
        e.emp_id,
        l.shift_date AS cal_date,
        SUM(COALESCE(a.total_time_spent_active, 0)) AS total_active_time,
        SUM(COALESCE(a.total_time_spent_idle, 0)) AS total_idle_time,
        SUM(COALESCE(a.window_lock_time, 0)) AS total_lock_time,
        SUM(COALESCE(m.total_mouse_count, 0)) AS total_mouse_clicks,
        SUM(COALESCE(k.total_keyboard_events, 0)) AS total_key_strokes,
        MAX(l.emp_login_time) AS login_time,
        MAX(l.emp_logout_time) AS logout_time,
        BOOL_OR(l.emp_late) AS was_late
    FROM FilteredEmployees e
    LEFT JOIN gold_dashboard.analytics_emp_login_logout l
        ON e.emplid = l.emp_id
        AND l.shift_date BETWEEN (SELECT start_date FROM date_range) 
                            AND (SELECT end_date FROM date_range)
    LEFT JOIN gold_dashboard.analytics_emp_app_info a
        ON e.emplid = a.emp_id
        AND l.shift_date = a.cal_date
        AND a.cal_date BETWEEN (SELECT start_date FROM date_range) 
                          AND (SELECT end_date FROM date_range)
    LEFT JOIN gold_dashboard.analytics_emp_mouseclicks m
        ON e.emplid = m.emp_id
        AND l.shift_date = m.cal_date
        AND m.cal_date BETWEEN (SELECT start_date FROM date_range) 
                           AND (SELECT end_date FROM date_range)
    LEFT JOIN gold_dashboard.analytics_emp_keystrokes k
        ON e.emplid = k.emp_id
        AND l.shift_date = k.cal_date
        AND k.cal_date BETWEEN (SELECT start_date FROM date_range) 
                           AND (SELECT end_date FROM date_range)
    GROUP BY e.emp_id, l.shift_date
    HAVING SUM(COALESCE(a.total_time_spent_active, 0)) > 0 -- Only days with activity
),

-- 4. Daily aggregates (for benchmarks)
DailyBenchmarks AS (
    SELECT
        cal_date,
        COUNT(DISTINCT emp_id) AS active_employees,
        AVG(total_active_time) AS avg_active_time,
        AVG(total_idle_time) AS avg_idle_time,
        AVG(total_lock_time) AS avg_lock_time,
        AVG(total_mouse_clicks) AS avg_mouse_clicks,
        AVG(total_key_strokes) AS avg_key_strokes,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_active_time) AS median_active_time,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_active_time) AS q1_active_time,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_active_time) AS q3_active_time
    FROM DailyActivity
    GROUP BY cal_date
),

-- 5. Employee 14-day averages
EmployeeAverages AS (
    SELECT
        emp_id,
        AVG(total_active_time) AS avg_active_time_14d,
        AVG(total_idle_time) AS avg_idle_time_14d,
        AVG(total_lock_time) AS avg_lock_time_14d,
        AVG(total_mouse_clicks) AS avg_mouse_clicks_14d,
        AVG(total_key_strokes) AS avg_key_strokes_14d,
        COUNT(*) AS days_worked
    FROM DailyActivity
    GROUP BY emp_id
)

-- FINAL REPORT: Three-tiered output
(
    -- Tier 1: Daily Summary (for leadership dashboard)
    SELECT 
        'DAILY_SUMMARY' AS report_type,
        db.cal_date,
        db.active_employees,
        db.avg_active_time/3600 AS avg_active_hours,
        db.avg_idle_time/3600 AS avg_idle_hours,
        ROUND((db.avg_active_time / NULLIF(db.avg_active_time + db.avg_idle_time + db.avg_lock_time, 0)) * 100, 1) AS utilization_pct,
        db.avg_mouse_clicks,
        db.avg_key_strokes,
        NULL AS emp_id,
        NULL AS detail
    FROM DailyBenchmarks db
    
    UNION ALL
    
    -- Tier 2: Employee outliers (for managers)
    SELECT 
        'EMPLOYEE_OUTLIERS' AS report_type,
        da.cal_date,
        NULL AS active_employees,
        da.total_active_time/3600 AS active_hours,
        (da.total_active_time - ea.avg_active_time_14d)/3600 AS deviation_from_avg,
        ROUND((da.total_active_time / NULLIF(db.avg_active_time, 0)) * 100, 1) AS percent_of_daily_avg,
        da.total_mouse_clicks,
        da.total_key_strokes,
        da.emp_id,
        CASE
            WHEN da.total_active_time < db.q1_active_time THEN 'LOW_PRODUCTIVITY'
            WHEN da.total_active_time > db.q3_active_time THEN 'HIGH_PRODUCTIVITY'
            WHEN da.total_idle_time > db.avg_idle_time * 1.5 THEN 'HIGH_IDLE_TIME'
            ELSE NULL
        END AS detail
    FROM DailyActivity da
    JOIN DailyBenchmarks db ON da.cal_date = db.cal_date
    JOIN EmployeeAverages ea ON da.emp_id = ea.emp_id
    WHERE 
        da.total_active_time < db.q1_active_time OR
        da.total_active_time > db.q3_active_time OR
        da.total_idle_time > db.avg_idle_time * 1.5
    
    UNION ALL
    
    -- Tier 3: Detailed anomalies (for HR investigations)
    SELECT 
        'ANOMALIES' AS report_type,
        da.cal_date,
        NULL AS active_employees,
        da.total_active_time/3600 AS active_hours,
        da.total_lock_time/3600 AS lock_hours,
        CASE
            WHEN da.total_active_time > 14400 AND da.total_mouse_clicks < 50 THEN 'POSSIBLE_AUTOMATION'
            WHEN da.was_late AND da.total_lock_time > 3600 THEN 'LATE_WITH_LONG_BREAK'
            WHEN da.logout_time IS NULL THEN 'MISSING_LOGOUT'
            WHEN da.total_active_time > 0 AND da.total_key_strokes < 10 THEN 'MINIMAL_INPUT_ACTIVITY'
            ELSE 'OTHER_ANOMALY'
        END AS anomaly_type,
        da.total_mouse_clicks,
        da.total_key_strokes,
        da.emp_id,
        CONCAT(
            'Login: ', COALESCE(da.login_time, 'missing'), 
            ' | Logout: ', COALESCE(da.logout_time, 'missing'),
            ' | Active: ', ROUND(da.total_active_time/3600,1), 'h',
            ' | Idle: ', ROUND(da.total_idle_time/3600,1), 'h'
        ) AS detail
    FROM DailyActivity da
    WHERE 
        (da.total_active_time > 14400 AND da.total_mouse_clicks < 50) OR
        (da.was_late AND da.total_lock_time > 3600) OR
        (da.logout_time IS NULL) OR
        (da.total_active_time > 0 AND da.total_key_strokes < 10)
)
ORDER BY report_type, cal_date DESC, emp_id;





WITH 
-- 1. First get all active employees
FilteredEmployees AS (
    SELECT emplid
    FROM inbound.hr_employee_central
    WHERE func_agr_id(manager_id) 
    AND (TERMINATION_DT > CURRENT_TIMESTAMP() OR TERMINATION_DT IS NULL)
),

-- 2. Set date parameters (last 14 days)
date_range AS (
    SELECT 
        DATEADD(day, -14, CURRENT_DATE()) AS start_date,
        CURRENT_DATE() AS end_date
),

-- 3. Application usage data
FilteredAppInfo AS (
    SELECT 
        emp_id,
        cal_date,
        app_name,
        total_time_spent_active,
        total_time_spent_idle,
        window_lock_time, 
        interval 
    FROM gold_dashboard.analytics_emp_app_info
    WHERE cal_date BETWEEN (SELECT start_date FROM date_range) 
                      AND (SELECT end_date FROM date_range)
    AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

-- 4. Mouse activity data
FilteredMouseInfo AS (
    SELECT 
        emp_id,
        cal_date,
        app_name,
        total_mouse_count, 
        interval 
    FROM gold_dashboard.analytics_emp_mouseclicks 
    WHERE cal_date BETWEEN (SELECT start_date FROM date_range) 
                      AND (SELECT end_date FROM date_range)
    AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

-- 5. Keyboard activity data
FilteredKeyInfo AS (
    SELECT 
        emp_id,
        cal_date,
        app_name,
        total_keyboard_events, 
        interval 
    FROM gold_dashboard.analytics_emp_keystrokes 
    WHERE cal_date BETWEEN (SELECT start_date FROM date_range) 
                      AND (SELECT end_date FROM date_range)
    AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

-- 6. Login/logout data
FilteredLoginLogout AS (
    SELECT 
        emp_id, 
        shift_date, 
        emp_late,
        emp_login_time, 
        emp_logout_time 
    FROM gold_dashboard.analytics_emp_login_logout 
    WHERE shift_date BETWEEN (SELECT start_date FROM date_range) 
                        AND (SELECT end_date FROM date_range)
    AND emp_id IN (SELECT emplid FROM FilteredEmployees)
),

-- 7. Combined activity data
EmployeeActivity AS (
    SELECT
        i.emp_id,
        l.shift_date AS cal_date,
        i.app_name,
        COALESCE(SUM(i.total_time_spent_active), 0) AS total_active_time, 
        COALESCE(SUM(i.total_time_spent_idle), 0) AS total_idle_time, 
        COALESCE(SUM(i.window_lock_time), 0) AS total_window_lock_time, 
        COALESCE(SUM(m.total_mouse_count), 0) AS total_mouse_clicks, 
        COALESCE(SUM(k.total_keyboard_events), 0) AS total_key_strokes
    FROM FilteredAppInfo i
    LEFT JOIN FilteredLoginLogout l
        ON i.emp_id = l.emp_id
        AND (
            (l.emp_login_time IS NOT NULL
            AND l.emp_logout_time IS NOT NULL
            AND DATEADD(hour, -1, TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss'))
            < TO_TIMESTAMP(CONCAT(i.cal_date, ' ', SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
            AND TO_TIMESTAMP(l.emp_logout_time, 'yyyy-MM-dd HH:mm:ss')
            > TO_TIMESTAMP(CONCAT(i.cal_date, ' ', SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
            OR (l.emp_logout_time IS NULL
            AND DATEADD(hour, -1, TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss'))
            < TO_TIMESTAMP(CONCAT(i.cal_date, ' ', SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
        )
    LEFT JOIN FilteredMouseInfo m
        ON i.emp_id = m.emp_id
        AND i.cal_date = m.cal_date
        AND i.app_name = m.app_name
        AND i.interval = m.interval
    LEFT JOIN FilteredKeyInfo k
        ON i.emp_id = k.emp_id
        AND i.cal_date = k.cal_date
        AND i.app_name = k.app_name
        AND i.interval = k.interval
    WHERE i.emp_id IN (SELECT emplid FROM FilteredEmployees)
    AND l.shift_date BETWEEN (SELECT start_date FROM date_range) 
                        AND (SELECT end_date FROM date_range)
    GROUP BY i.emp_id, l.shift_date, i.app_name
),

-- 8. Daily summary
PerDayEmployeeSummary AS (
    SELECT
        emp_id,
        cal_date,
        SUM(total_active_time) AS daily_active_time,
        SUM(total_idle_time) AS daily_idle_time,
        SUM(total_window_lock_time) AS daily_lock_time,
        SUM(total_active_time + total_idle_time + total_window_lock_time) AS daily_total_time
    FROM EmployeeActivity
    GROUP BY emp_id, cal_date
),

-- 9. Final report data
DailyProductivityReport AS (
    SELECT
        e.emp_id,
        e.cal_date,
        e.daily_active_time,
        e.daily_total_time,
        ROUND((e.daily_active_time / NULLIF(e.daily_total_time, 0)) * 100, 2) AS utilization_percentage,
        COALESCE(m.total_mouse_clicks, 0) AS total_mouse_clicks,
        COALESCE(k.total_key_strokes, 0) AS total_key_strokes,
        ROUND(COALESCE(m.total_mouse_clicks, 0) / NULLIF(e.daily_active_time/3600, 0), 2) AS mouse_clicks_per_hour,
        ROUND(COALESCE(k.total_key_strokes, 0) / NULLIF(e.daily_active_time/3600, 0), 2) AS keystrokes_per_hour,
        CASE 
            WHEN e.daily_active_time > 0 AND (m.total_mouse_clicks < 10 OR k.total_key_strokes < 50) THEN 'LOW_INPUT_ACTIVITY'
            WHEN (e.daily_idle_time / e.daily_total_time) > 0.4 THEN 'HIGH_IDLE_TIME'
            ELSE 'NORMAL'
        END AS anomaly_flag
    FROM PerDayEmployeeSummary e
    LEFT JOIN (
        SELECT emp_id, cal_date, SUM(total_mouse_clicks) AS total_mouse_clicks
        FROM EmployeeActivity GROUP BY emp_id, cal_date
    ) m ON e.emp_id = m.emp_id AND e.cal_date = m.cal_date
    LEFT JOIN (
        SELECT emp_id, cal_date, SUM(total_key_strokes) AS total_key_strokes
        FROM EmployeeActivity GROUP BY emp_id, cal_date
    ) k ON e.emp_id = k.emp_id AND e.cal_date = k.cal_date
)

-- MAIN REPORT QUERY
SELECT 
    r.*,
    l.emp_login_time,
    l.emp_logout_time,
    CASE 
        WHEN l.emp_logout_time IS NULL THEN 'MISSING_LOGOUT'
        WHEN TIMESTAMPDIFF(HOUR, TO_TIMESTAMP(l.emp_login_time), TO_TIMESTAMP(l.emp_logout_time)) < 4 THEN 'SHORT_SHIFT'
        ELSE 'NORMAL_SHIFT'
    END AS shift_anomaly,
    (SELECT STRING_AGG(CONCAT(app_name, ' (', ROUND(app_active_time/3600,1), 'h)'), ', ' ORDER BY app_active_time DESC)
     FROM (
         SELECT emp_id, cal_date, app_name, SUM(total_active_time) AS app_active_time
         FROM EmployeeActivity
         GROUP BY emp_id, cal_date, app_name
         HAVING SUM(total_active_time) > 900
     ) a 
     WHERE a.emp_id = r.emp_id AND a.cal_date = r.cal_date
    ) AS top_applications
FROM DailyProductivityReport r
LEFT JOIN FilteredLoginLogout l ON r.emp_id = l.emp_id AND r.cal_date = l.shift_date
ORDER BY r.cal_date DESC, r.anomaly_flag, r.utilization_percentage DESC;




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