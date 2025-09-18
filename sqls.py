

import pandas as pd
import json

def execute_query(conn, query):
    """Execute SQL query and return DataFrame"""
    return pd.read_sql(query, conn)

def process_period_stats(data):
    """
    Process period stats data and return the final JSON result with Totals and Averages
    """
    if data.empty:
        return json.dumps({
            'empCount': 0,
            'Totals': {
                'Usage Summary': [],
                'InputSummary': {
                    'input_event_count_hr': 0,
                    'prev_trend': 'No Change',
                    'prev_perc': 0
                }
            },
            'Averages': {
                'Usage Summary': [],
                'InputSummary': {
                    'input_event_count_hr': 0,
                    'prev_trend': 'No Change',
                    'prev_perc': 0
                }
            }
        })
    
    # Convert to dictionaries for easier access
    current_data = data[data['period'] == 'CURRENT']
    previous_data = data[data['period'] == 'PREVIOUS']
    
    current_dict = current_data.iloc[0].to_dict() if not current_data.empty else {}
    prev_dict = previous_data.iloc[0].to_dict() if not previous_data.empty else {}
    
    # Extract values with defaults
    emp_count = current_dict.get('emp_count', 0) or prev_dict.get('emp_count', 0)
    current_hours = current_dict.get('total_hours_worked', 0)
    previous_hours = prev_dict.get('total_hours_worked', 0)
    
    # Calculate metrics trends for TOTALS
    totals_metrics_data = []
    totals_metrics = [
        ('Error Handling', 'total_error'),
        ('Enter Usage', 'total_enter'),
        ('Copy Paste', 'total_copy_paste'),
        ('Swivel Chairing', 'total_swivel')
    ]
    
    for metric_name, col_name in totals_metrics:
        current_val = current_dict.get(col_name, 0)
        prev_val = prev_dict.get(col_name, 0)
        
        # Calculate trend
        if prev_val == 0:
            trend = 'No Change'
            change_pct = 0
        else:
            if current_val > prev_val:
                trend = 'Up'
            elif current_val < prev_val:
                trend = 'Down'
            else:
                trend = 'No Change'
            change_pct = round(abs((current_val - prev_val) / prev_val) * 100, 0)
        
        totals_metrics_data.append({
            'metric': metric_name,
            'current_total': current_val,
            'prev_total': prev_val,
            'total_trend': trend,
            'total_change_pct': change_pct
        })
    
    # Calculate metrics trends for AVERAGES
    averages_metrics_data = []
    averages_metrics = [
        ('Error Handling', 'avg_error'),
        ('Enter Usage', 'avg_enter'),
        ('Copy Paste', 'avg_copy_paste'),
        ('Swivel Chairing', 'avg_swivel')
    ]
    
    for metric_name, col_name in averages_metrics:
        current_val = current_dict.get(col_name, 0)
        prev_val = prev_dict.get(col_name, 0)
        
        # Calculate trend
        if prev_val == 0:
            trend = 'No Change'
            change_pct = 0
        else:
            if current_val > prev_val:
                trend = 'Up'
            elif current_val < prev_val:
                trend = 'Down'
            else:
                trend = 'No Change'
            change_pct = round(abs((current_val - prev_val) / prev_val) * 100, 0)
        
        averages_metrics_data.append({
            'metric': metric_name,
            'current_avg': current_val,
            'prev_avg': prev_val,
            'avg_trend': trend,
            'avg_change_pct': change_pct
        })
    
    # Calculate input summary for TOTALS
    current_input_count = current_dict.get('total_keyboard', 0) + current_dict.get('total_mouse', 0)
    prev_input_count = prev_dict.get('total_keyboard', 0) + prev_dict.get('total_mouse', 0)
    
    if prev_input_count == 0:
        input_trend = 'No Change'
        input_change_pct = 0
    else:
        if current_input_count > prev_input_count:
            input_trend = 'Up'
        elif current_input_count < prev_input_count:
            input_trend = 'Down'
        else:
            input_trend = 'No Change'
        input_change_pct = round(abs((current_input_count - prev_input_count) / prev_input_count) * 100, 0)
    
    # Calculate input summary for AVERAGES
    current_avg_input = current_dict.get('avg_keyboard', 0) + current_dict.get('avg_mouse', 0)
    prev_avg_input = prev_dict.get('avg_keyboard', 0) + prev_dict.get('avg_mouse', 0)
    
    if prev_avg_input == 0:
        avg_input_trend = 'No Change'
        avg_input_change_pct = 0
    else:
        if current_avg_input > prev_avg_input:
            avg_input_trend = 'Up'
        elif current_avg_input < prev_avg_input:
            avg_input_trend = 'Down'
        else:
            avg_input_trend = 'No Change'
        avg_input_change_pct = round(abs((current_avg_input - prev_avg_input) / prev_avg_input) * 100, 0)
    
    # Calculate input events per hour
    input_events_per_hr = round(current_input_count / current_hours, 0) if current_hours > 0 else 0
    avg_input_events_per_hr = round(current_avg_input / current_hours, 0) if current_hours > 0 else 0
    
    # Calculate percentages for each metric (for Totals)
    total_keyboard_count = current_dict.get('total_keyboard', 0)
    for metric in totals_metrics_data:
        if total_keyboard_count == 0:
            metric['perc'] = 0
        else:
            metric['perc'] = round((metric['current_total'] / total_keyboard_count) * 100, 0)
    
    # Calculate percentages for each metric (for Averages)
    avg_keyboard_count = current_dict.get('avg_keyboard', 0)
    for metric in averages_metrics_data:
        if avg_keyboard_count == 0:
            metric['perc'] = 0
        else:
            metric['perc'] = round((metric['current_avg'] / avg_keyboard_count) * 100, 0)
    
    # Build final JSON structure
    result = {
        'empCount': int(emp_count),
        'Totals': {
            'Usage Summary': [
                {
                    'metric': m['metric'],
                    'prev_trend': m['total_trend'],
                    'prev_perc': int(m['total_change_pct']),
                    'perc': int(m['perc'])
                }
                for m in totals_metrics_data
            ],
            'InputSummary': {
                'input_event_count_hr': int(input_events_per_hr),
                'prev_trend': input_trend,
                'prev_perc': int(input_change_pct)
            }
        },
        'Averages': {
            'Usage Summary': [
                {
                    'metric': m['metric'],
                    'prev_trend': m['avg_trend'],
                    'prev_perc': int(m['avg_change_pct']),
                    'perc': int(m['perc'])
                }
                for m in averages_metrics_data
            ],
            'InputSummary': {
                'input_event_count_hr': int(avg_input_events_per_hr),
                'prev_trend': avg_input_trend,
                'prev_perc': int(avg_input_change_pct)
            }
        }
    }
    
    return json.dumps(result)

# Example usage
def main():
    conn = your_database_connection_function()
    
    # Your SQL query now includes average columns
    query = """
    WITH FilteredEmployees AS (...)
    -- Your query that now includes:
    -- avg_error, avg_enter, avg_copy_paste, avg_swivel, avg_keyboard, avg_mouse
    SELECT * FROM period_stats ORDER BY period;
    """
    
    try:
        data = execute_query(conn, query)
        print("Raw data from SQL:")
        print(data)
        
        result_json = process_period_stats(data)
        
        # Pretty print the result
        result_dict = json.loads(result_json)
        print("\nFinal JSON Result:")
        print(json.dumps(result_dict, indent=2))
        
        return result_json
        
    except Exception as e:
        print(f"Error: {e}")
        return json.dumps({'error': str(e)})
    
    finally:
        if conn:
            conn.close()






-- Homepage Input KeyMouse Summary Totals, Averages - Optimized
WITH FilteredEmployees AS (
    SELECT
        emplid
    FROM
        inbound.hr_employee_central
    WHERE
        func_mar_id = 45179442
        AND (
            TERMINATION_DT > CURRENT_TIMESTAMP()
            OR TERMINATION_DT IS NULL
        )
),

FilteredLoginLogout AS (
    SELECT
        emp_id,
        shift_date,
        TO_TIMESTAMP(emp_login_time, 'yyyy-MM-dd HH:mm:ss') AS emp_login_time,
        TO_TIMESTAMP(emp_logout_time, 'yyyy-MM-dd HH:mm:ss') AS emp_logout_time
    FROM
        gold_dashboard.analytics_emp_login_logout
    WHERE
        SHIFT_DATE BETWEEN '2025-09-08' AND '2025-09-14'
        AND emp_id IN (
            SELECT
                emplid
            FROM
                FilteredEmployees
        )
),

current_work_hours AS (
    SELECT
        SUM(UNIX_TIMESTAMP(emp_logout_time) - UNIX_TIMESTAMP(emp_login_time)) / 3600 AS total_hours_worked
    FROM
        FilteredLoginLogout
),

keyboard_mouse_combined AS (
    SELECT
        COALESCE(k.EMP_ID, m.EMP_ID) AS emp_id,
        COALESCE(k.SHIFT_DATE, m.SHIFT_DATE) AS shift_date,
        COALESCE(k.period, m.period) AS period,
        COALESCE(k.error_events, 0) AS error_events,
        COALESCE(k.enter_events, 0) AS enter_events,
        COALESCE(k.copy_paste_events, 0) AS copy_paste_events,
        COALESCE(k.swivel_events, 0) AS swivel_events,
        COALESCE(k.other_events, 0) AS other_events,
        COALESCE(k.total_keyboard, 0) AS total_keyboard,
        COALESCE(m.mouse_events, 0) AS mouse_events
    FROM
        (
            SELECT
                EMP_ID,
                SHIFT_DATE,
                CASE
                    WHEN SHIFT_DATE BETWEEN '2025-09-08' AND '2025-09-14' THEN 'CURRENT'
                    WHEN SHIFT_DATE BETWEEN '2025-09-01' AND '2025-09-07' THEN 'PREVIOUS'
                END AS period,
                SUM(BACKSPACE_EVENTS + ESC_EVENTS + DELETE_EVENTS) AS error_events,
                SUM(ENTER_EVENTS) AS enter_events,
                SUM(ALT_TAB_EVENTS) AS swivel_events,
                SUM(OTHER_EVENTS) AS other_events,
                SUM(CTRL_C_EVENTS + CTRL_V_EVENTS) AS copy_paste_events,
                SUM(TOTAL_KEYBOARD_EVENTS) AS total_keyboard
            FROM
                gold_dashboard.analytics_emp_keystrokes
            WHERE
                SHIFT_DATE BETWEEN '2025-09-01' AND '2025-09-14'
                AND emp_id IN (
                    SELECT
                        emplid
                    FROM
                        FilteredEmployees
                )
            GROUP BY
                EMP_ID,
                SHIFT_DATE,
                CASE
                    WHEN SHIFT_DATE BETWEEN '2025-09-08' AND '2025-09-14' THEN 'CURRENT'
                    WHEN SHIFT_DATE BETWEEN '2025-09-01' AND '2025-09-07' THEN 'PREVIOUS'
                END
        ) k
        FULL OUTER JOIN (
            SELECT
                EMP_ID,
                SHIFT_DATE,
                CASE
                    WHEN SHIFT_DATE BETWEEN '2025-09-08' AND '2025-09-14' THEN 'CURRENT'
                    WHEN SHIFT_DATE BETWEEN '2025-09-01' AND '2025-09-07' THEN 'PREVIOUS'
                END AS period,
                SUM(TOTAL_MOUSE_COUNT) AS mouse_events
            FROM
                gold_dashboard.analytics_emp_mouseclicks
            WHERE
                SHIFT_DATE BETWEEN '2025-09-01' AND '2025-09-14'
                AND emp_id IN (
                    SELECT
                        emplid
                    FROM
                        FilteredEmployees
                )
            GROUP BY
                EMP_ID,
                SHIFT_DATE,
                CASE
                    WHEN SHIFT_DATE BETWEEN '2025-09-08' AND '2025-09-14' THEN 'CURRENT'
                    WHEN SHIFT_DATE BETWEEN '2025-09-01' AND '2025-09-07' THEN 'PREVIOUS'
                END
        ) m ON k.EMP_ID = m.EMP_ID
        AND k.SHIFT_DATE = m.SHIFT_DATE
        AND k.period = m.period
),

period_stats AS (
    SELECT
        period,
        SUM(error_events) AS total_error,
        SUM(enter_events) AS total_enter,
        SUM(copy_paste_events) AS total_copy_paste,
        SUM(swivel_events) AS total_swivel,
        SUM(mouse_events) AS total_mouse,
        SUM(total_keyboard) AS total_keyboard,
        SUM(total_keyboard + mouse_events) AS total_input_count
    FROM
        keyboard_mouse_combined
    GROUP BY
        period
),

metrics_pivoted AS (
    SELECT
        period,
        total_error,
        total_enter,
        total_copy_paste,
        total_swivel,
        total_mouse,
        total_keyboard,
        total_input_count
    FROM
        period_stats
),

current_prev_joined AS (
    SELECT
        c.period AS current_period,
        c.total_error AS current_error,
        c.total_enter AS current_enter,
        c.total_copy_paste AS current_copy_paste,
        c.total_swivel AS current_swivel,
        c.total_mouse AS current_mouse,
        c.total_keyboard AS current_keyboard,
        c.total_input_count AS current_input_count,
        p.period AS prev_period,
        p.total_error AS prev_error,
        p.total_enter AS prev_enter,
        p.total_copy_paste AS prev_copy_paste,
        p.total_swivel AS prev_swivel,
        p.total_mouse AS prev_mouse,
        p.total_keyboard AS prev_keyboard,
        p.total_input_count AS prev_input_count
    FROM
        metrics_pivoted c
        LEFT JOIN metrics_pivoted p ON p.period = 'PREVIOUS'
    WHERE
        c.period = 'CURRENT'
),

metrics_totals_trends AS (
    SELECT
        metric,
        current_total,
        COALESCE(prev_total, 0) AS prev_total,
        CASE
            WHEN prev_total IS NULL THEN 'No Change'
            WHEN current_total > prev_total THEN 'Up'
            WHEN current_total < prev_total THEN 'Down'
            ELSE 'No Change'
        END AS total_trend,
        CASE
            WHEN prev_total IS NULL
            OR prev_total = 0 THEN 0
            ELSE ROUND(ABS((current_total - prev_total) / prev_total) * 100, 0)
        END AS total_change_pct
    FROM
        (
            SELECT
                'Error Handling' AS metric,
                current_error AS current_total,
                prev_error AS prev_total
            FROM
                current_prev_joined
            UNION ALL
            SELECT
                'Enter Usage' AS metric,
                current_enter AS current_total,
                prev_enter AS prev_total
            FROM
                current_prev_joined
            UNION ALL
            SELECT
                'Copy Paste' AS metric,
                current_copy_paste AS current_total,
                prev_copy_paste AS prev_total
            FROM
                current_prev_joined
            UNION ALL
            SELECT
                'Swivel Chairing' AS metric,
                current_swivel AS current_total,
                prev_swivel AS prev_total
            FROM
                current_prev_joined
        ) metrics
),

input_summary_totals AS (
    SELECT
        current_keyboard AS total_keyboard_count,
        current_input_count AS total_input_count,
        COALESCE(prev_input_count, 0) AS prev_total_input_count,
        CASE
            WHEN prev_input_count IS NULL THEN 'No Change'
            WHEN current_input_count > prev_input_count THEN 'Up'
            WHEN current_input_count < prev_input_count THEN 'Down'
            ELSE 'No Change'
        END AS total_trend,
        CASE
            WHEN prev_input_count IS NULL
            OR prev_input_count = 0 THEN 0
            ELSE ROUND(
                ABS(current_input_count - prev_input_count) / prev_input_count * 100,
                0
            )
        END AS total_change_pct
    FROM
        current_prev_joined
)

SELECT
    TO_JSON(
        NAMED_STRUCT(
            'empCount',
            COALESCE(
                (
                    SELECT
                        COUNT(emplid)
                    FROM
                        FilteredEmployees
                ),
                0
            ),
            'Totals',
            NAMED_STRUCT(
                'Usage Summary',
                (
                    SELECT
                        COLLECT_LIST(
                            NAMED_STRUCT(
                                'metric',
                                m.metric,
                                'prev_trend',
                                m.total_trend,
                                'prev_perc',
                                m.total_change_pct,
                                'perc',
                                CASE
                                    WHEN ist.total_keyboard_count = 0 THEN 0
                                    ELSE ROUND((m.current_total / ist.total_keyboard_count) * 100, 0)
                                END
                            )
                        )
                    FROM
                        metrics_totals_trends m
                        CROSS JOIN input_summary_totals ist
                ),
                'InputSummary',
                NAMED_STRUCT(
                    'input_event_count_hr',
                    ROUND(
                        ist.total_input_count / NULLIF(
                            (
                                SELECT
                                    total_hours_worked
                                FROM
                                    current_work_hours
                            ),
                            0
                        ),
                        0
                    ),
                    'prev_trend',
                    ist.total_trend,
                    'prev_perc',
                    ist.total_change_pct
                )
            )
        )
    ) AS result_json;



WITH ActivityWithShiftBounds AS (
  SELECT
    i.emp_id,
    i.shift_date,
    i.start_time,
    i.end_time,
    i.interval_start,
    i.interval_end,
    i.emp_login_time,
    i.emp_logout_time,
    i.total_time_active,
    i.total_time_idle,
    i.window_lock_time,
    GREATEST(i.interval_start, i.start_time) AS overlap_start,
    LEAST(i.interval_end, i.end_time) AS overlap_end,
    GREATEST(i.interval_start, i.emp_login_time) AS activity_start,
    LEAST(i.interval_end, i.emp_logout_time) AS activity_end
  FROM FilteredAppInfo i
  LEFT JOIN EmployeeShiftsWithConfig s
    ON i.emp_id = s.emp_id AND i.shift_date = s.shift_date
),

ActivityTimeAllocation AS (
  SELECT
    emp_id,
    shift_date,
    total_time_active,
    total_time_idle,
    window_lock_time,
    CASE 
      WHEN activity_start >= end_time OR activity_end <= start_time THEN 0
      WHEN activity_start >= start_time AND activity_end <= end_time THEN
        EXTRACT(EPOCH FROM (activity_end - activity_start))
      ELSE
        EXTRACT(EPOCH FROM (LEAST(activity_end, end_time) - GREATEST(activity_start, start_time)))
    END AS seconds_within_shift,
    
    CASE 
      WHEN activity_start >= end_time OR activity_end <= start_time THEN
        EXTRACT(EPOCH FROM (activity_end - activity_start))
      WHEN activity_start >= start_time AND activity_end <= end_time THEN 0
      ELSE
        EXTRACT(EPOCH FROM ((activity_end - activity_start) - 
               (LEAST(activity_end, end_time) - GREATEST(activity_start, start_time))))
    END AS seconds_outside_shift
  FROM ActivityWithShiftBounds
  WHERE activity_start < activity_end
),

CurrentShiftData AS (
  SELECT
    a.emp_id,
    a.shift_date,
    SUM(total_time_active) AS total_active_time,
    SUM(total_time_idle) AS total_idle_time,
    SUM(window_lock_time) AS total_lock_time,
    SUM(total_time_active + total_time_idle + window_lock_time) AS total_work_time,
    
    SUM(total_time_active * (seconds_within_shift / NULLIF((seconds_within_shift + seconds_outside_shift), 0))) AS active_within_shift,
    SUM(total_time_idle * (seconds_within_shift / NULLIF((seconds_within_shift + seconds_outside_shift), 0))) AS idle_within_shift,
    SUM(window_lock_time * (seconds_within_shift / NULLIF((seconds_within_shift + seconds_outside_shift), 0))) AS lock_within_shift,
    
    SUM(total_time_active * (seconds_outside_shift / NULLIF((seconds_within_shift + seconds_outside_shift), 0))) AS active_outside_shift,
    SUM(total_time_idle * (seconds_outside_shift / NULLIF((seconds_within_shift + seconds_outside_shift), 0))) AS idle_outside_shift,
    
    MAX(s.pulse_shift_time) AS pulse_shift_time,
    MAX(s.adjusted_active_time) AS adjusted_active_time,
    MAX(s.adjusted_lock_time) AS adjusted_lock_time,
    MIN(s.start_time) AS shift_start_time,
    MAX(s.end_time) AS shift_end_time
    
  FROM ActivityTimeAllocation a
  JOIN EmployeeShiftsWithConfig s
    ON a.emp_id = s.emp_id AND a.shift_date = s.shift_date
  GROUP BY a.emp_id, a.shift_date
),

-- Define date ranges for current and previous periods
DateRanges AS (
  SELECT 
    emp_id,
    shift_date,
    total_active_time,
    total_idle_time,
    total_lock_time,
    total_work_time,
    pulse_shift_time,
    adjusted_active_time,
    adjusted_lock_time,
    shift_start_time,
    shift_end_time,
    -- Define your date ranges here (example: current week vs previous week)
    DATE_TRUNC('week', shift_date) AS current_week_start,
    DATE_TRUNC('week', shift_date) + INTERVAL '6 days' AS current_week_end,
    DATE_TRUNC('week', shift_date) - INTERVAL '7 days' AS prev_week_start,
    DATE_TRUNC('week', shift_date) - INTERVAL '1 day' AS prev_week_end
  FROM CurrentShiftData
),

PreviousPeriodData AS (
  SELECT
    d.emp_id,
    d.shift_date,
    d.total_active_time,
    d.total_idle_time,
    d.total_lock_time,
    d.total_work_time,
    d.pulse_shift_time,
    d.adjusted_active_time,
    d.adjusted_lock_time,
    d.shift_start_time,
    d.shift_end_time,
    d.current_week_start,
    d.current_week_end,
    d.prev_week_start,
    d.prev_week_end,
    
    -- Get previous period data using proper date range comparison
    (SELECT AVG(p.total_work_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_total_work_time,
    
    (SELECT AVG(p.total_active_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_active_time,
    
    (SELECT AVG(p.total_idle_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_idle_time,
    
    (SELECT AVG(p.total_lock_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_lock_time,
    
    (SELECT AVG(p.pulse_shift_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_pulse_shift_time,
    
    (SELECT AVG(p.adjusted_active_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_adjusted_active_time,
    
    (SELECT AVG(p.adjusted_lock_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_adjusted_lock_time,
    
    (SELECT MIN(p.shift_start_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_shift_start_time,
    
    (SELECT MAX(p.shift_end_time) 
     FROM CurrentShiftData p 
     WHERE p.emp_id = d.emp_id 
       AND p.shift_date BETWEEN d.prev_week_start AND d.prev_week_end) AS prev_shift_end_time
    
  FROM DateRanges d
),

TrendAnalysis AS (
  SELECT
    emp_id,
    shift_date,
    total_work_time,
    total_active_time,
    total_idle_time,
    total_lock_time,
    pulse_shift_time,
    adjusted_active_time,
    adjusted_lock_time,
    shift_start_time,
    shift_end_time,
    
    prev_total_work_time,
    prev_active_time,
    prev_idle_time,
    prev_lock_time,
    prev_shift_start_time,
    prev_shift_end_time,
    prev_pulse_shift_time,
    prev_adjusted_active_time,
    prev_adjusted_lock_time,
    
    -- Trend calculations (same as before but with proper date ranges)
    CASE 
      WHEN prev_total_work_time IS NULL THEN 'No Previous Data'
      WHEN total_work_time > prev_total_work_time THEN 'Up'
      WHEN total_work_time < prev_total_work_time THEN 'Down'
      ELSE 'Exact'
    END AS prev_work_trend,
    
    ROUND(CASE 
      WHEN prev_total_work_time IS NULL OR prev_total_work_time = 0 THEN NULL
      ELSE ((total_work_time - prev_total_work_time) / prev_total_work_time) * 100
    END, 2) AS prev_work_perc,
    
    -- ... (other trend calculations remain the same as previous query)
    
    CASE 
      WHEN pulse_shift_time IS NULL THEN 'No Shift Data'
      WHEN total_work_time > (pulse_shift_time * 3600) THEN 'Up'
      WHEN total_work_time < (pulse_shift_time * 3600) THEN 'Down'
      ELSE 'Exact'
    END AS total_work_time_shift_trend,
    
    (pulse_shift_time * 3600) AS total_work_shift_time,
    
    -- ... (other shift trend calculations)
    
    total_work_time - (pulse_shift_time * 3600) AS work_time_diff_seconds,
    total_active_time - (adjusted_active_time * 3600) AS active_time_diff_seconds,
    total_lock_time - (adjusted_lock_time * 3600) AS lock_time_diff_seconds
    
  FROM PreviousPeriodData
)

SELECT
  emp_id,
  shift_date,
  total_work_time,
  total_active_time,
  total_idle_time,
  total_lock_time,
  prev_total_work_time,
  prev_active_time,
  prev_idle_time,
  prev_lock_time,
  prev_work_trend,
  prev_work_perc,
  total_work_time_shift_trend,
  total_work_shift_time,
  work_time_diff_seconds,
  -- ... (all other required columns)
  shift_start_time,
  shift_end_time,
  prev_shift_start_time,
  prev_shift_end_time
  
FROM TrendAnalysis
ORDER BY emp_id, shift_date;












WITH keyboard AS (
    SELECT
        EMP_ID,
        SHIFT_DATE,
        CASE
            WHEN SHIFT_DATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
            WHEN SHIFT_DATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
        END AS period,
        SUM(BACKSPACE_EVENTS + ESC_EVENTS + DELETE_EVENTS) AS error_events,
        SUM(ENTER_EVENTS) AS enter_events,
        SUM(CTRL_C_EVENTS + CTRL_V_EVENTS) AS copy_paste_events,
        SUM(ALT_TAB_EVENTS) AS swivel_events,
        SUM(OTHER_EVENTS) AS other_events,
        SUM(TOTAL_KEYBOARD_EVENTS) AS total_keyboard
    FROM gold_dashboard.analytics_ump_keystrokes
    WHERE SHIFT_DATE BETWEEN :prev_start_date AND :end_date 
        AND emp_id IN (SELECT amplid FROM FilteredEmployees)
    GROUP BY EMP_ID, SHIFT_DATE,
        CASE
            WHEN SHIFT_DATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
            WHEN SHIFT_DATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
        END
),

mouse AS (
    SELECT
        EMP_ID,
        SHIFT_DATE,
        CASE
            WHEN SHIFT_DATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
            WHEN SHIFT_DATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
        END AS period,
        SUM(TOTAL_MOUSE_COUNT) AS mouse_events
    FROM gold_dashboard.analytics_emp_mouseclicks
    WHERE SHIFT_DATE BETWEEN :prev_start_date AND :end_date 
        AND emp_id IN (SELECT amplid FROM FilteredEmployees)
    GROUP BY EMP_ID, SHIFT_DATE,
        CASE
            WHEN SHIFT_DATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
            WHEN SHIFT_DATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
        END
),

combined AS (
    SELECT
        k.EMP_ID,
        k.SHIFT_DATE,
        k.period,
        k.error_events,
        k.enter_events,
        k.copy_paste_events,
        k.swivel_events,
        k.other_events,
        k.total_keyboard,
        COALESCE(m.mouse_events, 0) AS mouse_events
    FROM keyboard k
    LEFT JOIN mouse m
        ON k.EMP_ID = m.EMP_ID
        AND k.SHIFT_DATE = m.SHIFT_DATE
        AND k.period = m.period
),

period_stats AS (
    SELECT
        period,
        -- Totals
        SUM(error_events) AS total_error,
        SUM(enter_events) AS total_enter,
        SUM(copy_paste_events) AS total_copy_paste,
        SUM(swivel_events) AS total_swivel,
        SUM(mouse_events) AS total_mouse,
        SUM(total_keyboard) AS total_keyboard,
        -- Averages
        AVG(error_events) AS avg_error,
        AVG(enter_events) AS avg_enter,
        AVG(copy_paste_events) AS avg_copy_paste,
        AVG(swivel_events) AS avg_swivel,
        AVG(mouse_events) AS avg_mouse,
        AVG(total_keyboard) AS avg_keyboard
    FROM combined
    GROUP BY period
),

current_stats AS (
    SELECT * FROM period_stats WHERE period = 'CURRENT'
),

previous_stats AS (
    SELECT * FROM period_stats WHERE period = 'PREVIOUS'
),

metrics_data AS (
    SELECT
        'Error Handling' AS metric,
        c.total_error AS current_total,
        p.total_error AS prev_total,
        c.avg_error AS current_avg,
        p.avg_error AS prev_avg
    FROM current_stats c
    CROSS JOIN previous_stats p
    
    UNION ALL
    
    SELECT
        'Enter Usage' AS metric,
        c.total_enter AS current_total,
        p.total_enter AS prev_total,
        c.avg_enter AS current_avg,
        p.avg_enter AS prev_avg
    
    UNION ALL
    
    SELECT
        'Copy Paste' AS metric,
        c.total_copy_paste AS current_total,
        p.total_copy_paste AS prev_total,
        c.avg_copy_paste AS current_avg,
        p.avg_copy_paste AS prev_avg
    
    UNION ALL
    
    SELECT
        'Swivel Chairing' AS metric,
        c.total_swivel AS current_total,
        p.total_swivel AS prev_total,
        c.avg_swivel AS current_avg,
        p.avg_swivel AS prev_avg
    FROM current_stats c
    CROSS JOIN previous_stats p
),

-- Separate trend calculations for totals and averages
metrics_totals_trends AS (
    SELECT
        metric,
        current_total,
        COALESCE(prev_total, 0) AS prev_total,
        CASE
            WHEN prev_total IS NULL THEN 'No Change'
            WHEN current_total > prev_total THEN 'UP'
            WHEN current_total < prev_total THEN 'Down'
            ELSE 'No Change'
        END AS total_trend,
        CASE
            WHEN prev_total IS NULL OR prev_total = 0 THEN 0
            ELSE ROUND(((current_total - prev_total) / prev_total) * 100, 2)
        END AS total_change_pct
    FROM metrics_data
),

metrics_avg_trends AS (
    SELECT
        metric,
        current_avg,
        COALESCE(prev_avg, 0) AS prev_avg,
        CASE
            WHEN prev_avg IS NULL THEN 'No Change'
            WHEN current_avg > prev_avg THEN 'UP'
            WHEN current_avg < prev_avg THEN 'Down'
            ELSE 'No Change'
        END AS avg_trend,
        CASE
            WHEN prev_avg IS NULL OR prev_avg = 0 THEN 0
            ELSE ROUND(((current_avg - prev_avg) / prev_avg) * 100, 2)
        END AS avg_change_pct
    FROM metrics_data
),

input_summary_totals AS (
    SELECT
        c.total_keyboard AS total_keyboard_count,
        c.total_mouse AS total_mouse_count,
        (c.total_keyboard + c.total_mouse) AS total_input_count,
        p.total_keyboard AS prev_total_keyboard_count,
        p.total_mouse AS prev_total_mouse_count,
        COALESCE((p.total_keyboard + p.total_mouse), 0) AS prev_total_input_count,
        CASE
            WHEN p.total_keyboard IS NULL OR p.total_mouse IS NULL THEN 'No Change'
            WHEN (c.total_keyboard + c.total_mouse) > (p.total_keyboard + p.total_mouse) THEN 'UP'
            WHEN (c.total_keyboard + c.total_mouse) < (p.total_keyboard + p.total_mouse) THEN 'Down'
            ELSE 'No Change'
        END AS total_trend,
        CASE
            WHEN (p.total_keyboard + p.total_mouse) IS NULL OR (p.total_keyboard + p.total_mouse) = 0 THEN 0
            ELSE ROUND(((c.total_keyboard + c.total_mouse) - (p.total_keyboard + p.total_mouse)) / 
                   (p.total_keyboard + p.total_mouse) * 100, 2)
        END AS total_change_pct
    FROM current_stats c
    CROSS JOIN previous_stats p
),

input_summary_avg AS (
    SELECT
        c.avg_keyboard AS avg_keyboard_count,
        c.avg_mouse AS avg_mouse_count,
        (c.avg_keyboard + c.avg_mouse) AS avg_input_count,
        p.avg_keyboard AS prev_avg_keyboard_count,
        p.avg_mouse AS prev_avg_mouse_count,
        COALESCE((p.avg_keyboard + p.avg_mouse), 0) AS prev_avg_input_count,
        CASE
            WHEN p.avg_keyboard IS NULL OR p.avg_mouse IS NULL THEN 'No Change'
            WHEN (c.avg_keyboard + c.avg_mouse) > (p.avg_keyboard + p.avg_mouse) THEN 'UP'
            WHEN (c.avg_keyboard + c.avg_mouse) < (p.avg_keyboard + p.avg_mouse) THEN 'Down'
            ELSE 'No Change'
        END AS avg_trend,
        CASE
            WHEN (p.avg_keyboard + p.avg_mouse) IS NULL OR (p.avg_keyboard + p.avg_mouse) = 0 THEN 0
            ELSE ROUND(((c.avg_keyboard + c.avg_mouse) - (p.avg_keyboard + p.avg_mouse)) / 
                   (p.avg_keyboard + p.avg_mouse) * 100, 2)
        END AS avg_change_pct
    FROM current_stats c
    CROSS JOIN previous_stats p
)

SELECT to_json(
    named_struct(
        'totals', named_struct(
            'UsageSummary', (SELECT COLLECT_LIST(named_struct(
                'metric', m.metric,
                'count', m.current_total,
                'prev_count', m.prev_total,
                'current_avg', md.current_avg,  -- Keep avg for reference if needed
                'prev_avg', md.prev_avg,        -- Keep avg for reference if needed
                'prev_trend', m.total_trend,
                'prev_perc', m.total_change_pct
            )) FROM metrics_totals_trends m
            JOIN metrics_data md ON m.metric = md.metric),
            'InputSummary', (SELECT named_struct(
                'key_count', total_keyboard_count,
                'mouse_count', total_mouse_count,
                'input_event_count', total_input_count,
                'prev_input_event_count', prev_total_input_count,
                'prev_trend', total_trend,
                'prev_perc', total_change_pct
            ) FROM input_summary_totals)
        ),
        'averages', named_struct(
            'UsageSummary', (SELECT COLLECT_LIST(named_struct(
                'metric', m.metric,
                'count', md.current_total,      -- Keep total for reference if needed
                'prev_count', md.prev_total,     -- Keep total for reference if needed
                'current_avg', m.current_avg,
                'prev_avg', m.prev_avg,
                'prev_trend', m.avg_trend,
                'prev_perc', m.avg_change_pct
            )) FROM metrics_avg_trends m
            JOIN metrics_data md ON m.metric = md.metric),
            'InputSummary', (SELECT named_struct(
                'key_count', avg_keyboard_count,
                'mouse_count', avg_mouse_count,
                'input_event_count', avg_input_count,
                'prev_input_event_count', prev_avg_input_count,
                'prev_trend', avg_trend,
                'prev_perc', avg_change_pct
            ) FROM input_summary_avg)
        )
    )
) AS result_json;







-------------------

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
  p.prev_cpu_count,
  CASE 
    WHEN ABS(c.app_trace_count - c.cpu_count) > 200 THEN 'Review Needed'
    ELSE 'OK'
  END AS review_status
FROM current_week_data c
LEFT JOIN previous_week_data p ON c.cal_date = p.cal_date
WHERE c.cal_date >= date_sub(current_date(), 14)
ORDER BY c.cal_date DESC







SELECT 
  c.cal_date,
  c.day_name,
  c.app_only_count,
  p.prev_app_only_count,
  CASE 
    WHEN c.app_only_count > 200 THEN 'Review Needed'
    ELSE 'OK'
  END AS review_status
FROM current_week_counts c
LEFT JOIN previous_week_counts p ON c.cal_date = p.cal_date
WHERE c.cal_date >= date_sub(current_date(), 14)
ORDER BY c.cal_date DESC







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
        split(curr.pulse_version, '\\.')[0] > split(prev.pulse_version, '\\.') OR
        (split(curr.pulse_version, '\\.') = split(prev.pulse_version, '\\.') AND 
         split(curr.pulse_version, '\\.')[1] > split(prev.pulse_version, '\\.')[1]) OR
        (split(curr.pulse_version, '\\.') = split(prev.pulse_version, '\\.') AND 
         split(curr.pulse_version, '\\.')[1] = split(prev.pulse_version, '\\.')[1] AND
         split(curr.pulse_version, '\\.')[2] > split(prev.pulse_version, '\\.')[2]) OR
        (split(curr.pulse_version, '\\.') = split(prev.pulse_version, '\\.') AND 
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
),

moved_to_old_json AS (
  SELECT 
    cal_date,
    to_json(collect_list(emp_id)) AS moved_to_old_version_employees
  FROM version_changes
  WHERE version_change = 'older_version'
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
  ROUND((d.moved_to_old_version * 100.0 / NULLIF(d.total_employees - d.new_employees, 0)), 2) AS pct_downgraded,
  CASE 
    WHEN d.moved_to_old_version > 0 THEN 'Review Needed'
    ELSE 'OK'
  END AS review_status,
  COALESCE(m.moved_to_old_version_employees, '[]') AS moved_to_old_version_employees
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
LEFT JOIN moved_to_old_json m ON d.cal_date = m.cal_date
ORDER BY d.cal_date DESC










WITH HR_employees AS (
    -- Your existing CTE definition
    SELECT EMPLID, NAME
    FROM ...
),
emp_with_team AS (
    SELECT 
        h.EMPLID,
        h.NAME,
        COALESCE(m.current_team_id, 1) AS team_id
    FROM HR_employees h
    LEFT JOIN analytics_emp_mapping m
        ON h.EMPLID = m.EMPID
),
teams_with_count AS (
    SELECT 
        team_id,
        COUNT(*) AS emp_count
    FROM emp_with_team
    GROUP BY team_id
    HAVING COUNT(*) >= 4
),
filtered_emps AS (
    SELECT e.*
    FROM emp_with_team e
    INNER JOIN teams_with_count tc
        ON e.team_id = tc.team_id
)
SELECT 
    f.team_id,
    t.team_name,
    t.color_code,
    AVG(a.active_time) AS avg_active_time
FROM filtered_emps f
JOIN gold_dashboard.analytics_team_tagging t
    ON f.team_id = t.team_id
JOIN gold_dashboard.analytics_emp_app_info a
    ON f.EMPLID = a.emp_id
WHERE a.activity_date BETWEEN DATE '2025-08-01' AND DATE '2025-08-10'  -- <-- your period filter
GROUP BY f.team_id, t.team_name, t.color_code
ORDER BY f.team_id;



%sql
WITH 
-- Employees running AppTrace each day
app_trace_emps AS (
  SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS emps_running_app_trace,
    COLLECT_LIST(DISTINCT emp_id) AS app_trace_emp_list
  FROM app_trace.emp_activity
  WHERE cal_date >= date_sub(current_date(), 14)
  GROUP BY cal_date
),

-- Employees running SysTrace (keyboard or mouse) each day
sys_trace_emps AS (
  SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS emps_running_sys_trace,
    COLLECT_LIST(DISTINCT emp_id) AS sys_trace_emp_list
  FROM (
    SELECT emp_id, cal_date FROM sys_trace.emp_keyboarddata
    UNION
    SELECT emp_id, cal_date FROM sys_trace.emp_mousedata
  )
  WHERE cal_date >= date_sub(current_date(), 14)
  GROUP BY cal_date
),

-- Employees in AppTrace but missing from HR
app_trace_missing_hr AS (
  SELECT 
    a.cal_date,
    COUNT(DISTINCT a.emp_id) AS emp_app_missing_in_hr,
    COLLECT_LIST(DISTINCT a.emp_id) AS app_missing_hr_list
  FROM app_trace.emp_activity a
  LEFT JOIN hr_employee_central h ON a.emp_id = h.empl_id
  WHERE a.cal_date >= date_sub(current_date(), 14)
    AND h.empl_id IS NULL
  GROUP BY a.cal_date
),

-- Employees in SysTrace but missing from HR
sys_trace_missing_hr AS (
  SELECT 
    s.cal_date,
    COUNT(DISTINCT s.emp_id) AS emp_sys_missing_in_hr,
    COLLECT_LIST(DISTINCT s.emp_id) AS sys_missing_hr_list
  FROM (
    SELECT emp_id, cal_date FROM sys_trace.emp_keyboarddata
    UNION
    SELECT emp_id, cal_date FROM sys_trace.emp_mousedata
  ) s
  LEFT JOIN hr_employee_central h ON s.emp_id = h.empl_id
  WHERE s.cal_date >= date_sub(current_date(), 14)
    AND h.empl_id IS NULL
  GROUP BY s.cal_date
)

-- Final combined report
SELECT 
  COALESCE(a.cal_date, s.cal_date, am.cal_date, sm.cal_date) AS cal_date,
  DAYNAME(COALESCE(a.cal_date, s.cal_date, am.cal_date, sm.cal_date)) AS day_name,
  COALESCE(a.emps_running_app_trace, 0) AS emps_running_app_trace,
  COALESCE(s.emps_running_sys_trace, 0) AS emps_running_sys_trace,
  COALESCE(am.emp_app_missing_in_hr, 0) AS emp_app_missing_in_hr,
  COALESCE(sm.emp_sys_missing_in_hr, 0) AS emp_sys_missing_in_hr,
  a.app_trace_emp_list,
  s.sys_trace_emp_list,
  am.app_missing_hr_list,
  sm.sys_missing_hr_list
FROM app_trace_emps a
FULL OUTER JOIN sys_trace_emps s ON a.cal_date = s.cal_date
FULL OUTER JOIN app_trace_missing_hr am ON a.cal_date = am.cal_date
FULL OUTER JOIN sys_trace_missing_hr sm ON a.cal_date = sm.cal_date
ORDER BY cal_date DESC






%sql
WITH 
-- Employees running AppTrace each day
app_trace_emps AS (
  SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS emps_running_app_trace,
    COLLECT_LIST(DISTINCT emp_id) AS app_trace_emp_list
  FROM app_trace.emp_activity
  WHERE cal_date >= date_sub(current_date(), 14)
  GROUP BY cal_date
),

-- Employees running SysTrace (keyboard or mouse) each day
sys_trace_emps AS (
  SELECT 
    cal_date,
    COUNT(DISTINCT emp_id) AS emps_running_sys_trace,
    COLLECT_LIST(DISTINCT emp_id) AS sys_trace_emp_list
  FROM (
    SELECT emp_id, cal_date FROM sys_trace.emp_keyboarddata
    UNION
    SELECT emp_id, cal_date FROM sys_trace.emp_mousedata
  )
  WHERE cal_date >= date_sub(current_date(), 14)
  GROUP BY cal_date
),

-- Employees in AppTrace but missing from HR
app_trace_missing_hr AS (
  SELECT 
    a.cal_date,
    COUNT(DISTINCT a.emp_id) AS emp_app_missing_in_hr,
    COLLECT_LIST(DISTINCT a.emp_id) AS app_missing_hr_list
  FROM app_trace.emp_activity a
  LEFT JOIN hr_employee_central h ON a.emp_id = h.empl_id
  WHERE a.cal_date >= date_sub(current_date(), 14)
    AND h.empl_id IS NULL
  GROUP BY a.cal_date
),

-- Employees in SysTrace but missing from HR
sys_trace_missing_hr AS (
  SELECT 
    s.cal_date,
    COUNT(DISTINCT s.emp_id) AS emp_sys_missing_in_hr,
    COLLECT_LIST(DISTINCT s.emp_id) AS sys_missing_hr_list
  FROM (
    SELECT emp_id, cal_date FROM sys_trace.emp_keyboarddata
    UNION
    SELECT emp_id, cal_date FROM sys_trace.emp_mousedata
  ) s
  LEFT JOIN hr_employee_central h ON s.emp_id = h.empl_id
  WHERE s.cal_date >= date_sub(current_date(), 14)
    AND h.empl_id IS NULL
  GROUP BY s.cal_date
)

-- Final combined report
SELECT 
  COALESCE(a.cal_date, s.cal_date, am.cal_date, sm.cal_date) AS cal_date,
  DAYNAME(COALESCE(a.cal_date, s.cal_date, am.cal_date, sm.cal_date)) AS day_name,
  COALESCE(a.emps_running_app_trace, 0) AS emps_running_app_trace,
  COALESCE(s.emps_running_sys_trace, 0) AS emps_running_sys_trace,
  COALESCE(am.emp_app_missing_in_hr, 0) AS emp_app_missing_in_hr,
  COALESCE(sm.emp_sys_missing_in_hr, 0) AS emp_sys_missing_in_hr,
  a.app_trace_emp_list,
  s.sys_trace_emp_list,
  am.app_missing_hr_list,
  sm.sys_missing_hr_list
FROM app_trace_emps a
FULL OUTER JOIN sys_trace_emps s ON a.cal_date = s.cal_date
FULL OUTER JOIN app_trace_missing_hr am ON a.cal_date = am.cal_date
FULL OUTER JOIN sys_trace_missing_hr sm ON a.cal_date = sm.cal_date
ORDER BY cal_date DESC





Here are the two optimized queries you requested:

### Query 1: Daily Count of Missing Employees by Category

```sql
WITH RawAppActivity AS (
  SELECT DISTINCT emp_id, CAST(created_on AS DATE) AS cal_date
  FROM app_trace.emp_activity
  WHERE created_on >= DATE_SUB(CURRENT_DATE(), 14)
),
RawKeyboard AS (
  SELECT DISTINCT emp_id, CAST(created_on AS DATE) AS cal_date
  FROM sys_trace.emp_keyboarddata
  WHERE created_on >= DATE_SUB(CURRENT_DATE(), 14)
),
RawMouse AS (
  SELECT DISTINCT emp_id, CAST(created_on AS DATE) AS cal_date
  FROM sys_trace.emp_mousedata
  WHERE created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

SELECT 
  cal_date,
  COUNT(DISTINCT CASE WHEN NOT EXISTS (
    SELECT 1 FROM gold_dashboard.analytics_emp_app_info g 
    WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
  ) THEN r.emp_id END) AS missing_emp_count_app,
  
  COUNT(DISTINCT CASE WHEN NOT EXISTS (
    SELECT 1 FROM gold_dashboard.analytics_emp_keystrokes k 
    WHERE k.emp_id = r.emp_id AND k.cal_date = r.cal_date
  ) THEN r.emp_id END) AS missing_emp_count_key,
  
  COUNT(DISTINCT CASE WHEN NOT EXISTS (
    SELECT 1 FROM gold_dashboard.analytics_emp_mouseclicks m 
    WHERE m.emp_id = r.emp_id AND m.cal_date = r.cal_date
  ) THEN r.emp_id END) AS missing_emp_count_mouse,
  
  CONCAT_WS(', ',
    CASE WHEN COUNT(DISTINCT CASE WHEN NOT EXISTS (
      SELECT 1 FROM gold_dashboard.analytics_emp_app_info g 
      WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
    ) THEN r.emp_id END) > 0 THEN 'App data missing' END,
    
    CASE WHEN COUNT(DISTINCT CASE WHEN NOT EXISTS (
      SELECT 1 FROM gold_dashboard.analytics_emp_keystrokes k 
      WHERE k.emp_id = r.emp_id AND k.cal_date = r.cal_date
    ) THEN r.emp_id END) > 0 THEN 'Keyboard data missing' END,
    
    CASE WHEN COUNT(DISTINCT CASE WHEN NOT EXISTS (
      SELECT 1 FROM gold_dashboard.analytics_emp_mouseclicks m 
      WHERE m.emp_id = r.emp_id AND m.cal_date = r.cal_date
    ) THEN r.emp_id END) > 0 THEN 'Mouse data missing' END
  ) AS missing_reason
FROM 
  (SELECT emp_id, cal_date FROM RawAppActivity
   UNION
   SELECT emp_id, cal_date FROM RawKeyboard
   UNION
   SELECT emp_id, cal_date FROM RawMouse) r
GROUP BY 
  cal_date
ORDER BY 
  cal_date DESC;
```

### Query 2: Detailed List of All Missing Records (Union of All Checks)

```sql
WITH RawAppActivity AS (
  SELECT DISTINCT emp_id, CAST(created_on AS DATE) AS cal_date
  FROM app_trace.emp_activity
  WHERE created_on >= DATE_SUB(CURRENT_DATE(), 14)
),
RawKeyboard AS (
  SELECT DISTINCT emp_id, CAST(created_on AS DATE) AS cal_date
  FROM sys_trace.emp_keyboarddata
  WHERE created_on >= DATE_SUB(CURRENT_DATE(), 14)
),
RawMouse AS (
  SELECT DISTINCT emp_id, CAST(created_on AS DATE) AS cal_date
  FROM sys_trace.emp_mousedata
  WHERE created_on >= DATE_SUB(CURRENT_DATE(), 14)
)

-- App data missing
SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in analytics_emp_app_info' AS missing_in
FROM 
  RawAppActivity r
WHERE 
  NOT EXISTS (
    SELECT 1 FROM gold_dashboard.analytics_emp_app_info g 
    WHERE g.emp_id = r.emp_id AND g.cal_date = r.cal_date
  )

UNION ALL

-- Keyboard data missing
SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in analytics_emp_keystrokes' AS missing_in
FROM 
  RawKeyboard r
WHERE 
  NOT EXISTS (
    SELECT 1 FROM gold_dashboard.analytics_emp_keystrokes k 
    WHERE k.emp_id = r.emp_id AND k.cal_date = r.cal_date
  )

UNION ALL

-- Mouse data missing
SELECT 
  r.emp_id,
  r.cal_date,
  'Missing in analytics_emp_mouseclicks' AS missing_in
FROM 
  RawMouse r
WHERE 
  NOT EXISTS (
    SELECT 1 FROM gold_dashboard.analytics_emp_mouseclicks m 
    WHERE m.emp_id = r.emp_id AND m.cal_date = r.cal_date
  )

ORDER BY 
  cal_date DESC, 
  emp_id, 
  missing_in;
```

### Key Features:

1. **Query 1 (Daily Counts)**:
   - Shows counts per day of employees missing in each gold table
   - Includes a concatenated "missing_reason" column
   - Only shows dates where at least one employee is missing data
   - Clean, aggregated view for dashboard reporting

2. **Query 2 (Detailed List)**:
   - Comprehensive list of every missing record
   - Clearly identifies which gold table is missing data
   - Union of all three checks (app, keyboard, mouse)
   - Useful for detailed troubleshooting

3. **Common Parameters**:
   - Both queries use the same 14-day lookback period
   - Consistent date formatting and filtering
   - Same table relationships and join logic

4. **Performance Optimized**:
   - Uses EXISTS instead of JOINs for better performance
   - DISTINCT in the raw data CTEs to reduce processing
   - Clean sorting for easy review

These queries provide both the high-level summary and detailed breakdown you need for monitoring your data pipeline completeness.






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