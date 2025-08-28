

WITH date_periods AS (
    SELECT 
        '2025-08-18'::DATE AS curr_start,
        '2025-08-24'::DATE AS curr_end,
        '2025-08-11'::DATE AS prev_start,
        '2025-08-17'::DATE AS prev_end
),

filtered_employees AS (
    SELECT emplid FROM FilteredEmployees
),

FilteredLoginLogout AS (
    SELECT
        emp_id,
        shift_date,
        TO_TIMESTAMP(emp_login_time, 'yyyy-MM-dd HH:mm:ss') as emp_login_time,
        TO_TIMESTAMP(emp_logout_time, 'yyyy-MM-dd HH:mm:ss') as emp_logout_time,
        CASE
            WHEN shift_date BETWEEN (SELECT curr_start FROM date_periods) 
                               AND (SELECT curr_end FROM date_periods) THEN 'CURRENT'
            WHEN shift_date BETWEEN (SELECT prev_start FROM date_periods) 
                               AND (SELECT prev_end FROM date_periods) THEN 'PREVIOUS'
        END AS period
    FROM gold_dashboard.analytics_emp_login_logout
    WHERE shift_date BETWEEN (SELECT prev_start FROM date_periods) 
                         AND (SELECT curr_end FROM date_periods)
    AND emp_id IN (SELECT emplid FROM filtered_employees)
    AND is_weekoff IS FALSE 
    AND emp_id IS NOT NULL
),

work_hours AS (
    SELECT
        period,
        SUM(EXTRACT(EPOCH FROM (emp_logout_time - emp_login_time)) / 3600) AS total_hours_worked
    FROM FilteredLoginLogout
    WHERE emp_login_time IS NOT NULL 
      AND emp_logout_time IS NOT NULL
      AND emp_logout_time > emp_login_time
    GROUP BY period
),

keyboard_data AS (
    SELECT
        f.period,
        SUM(k.BACKSPACE_EVENTS + k.ESC_EVENTS + k.DELETE_EVENTS) AS total_error,
        SUM(k.ENTER_EVENTS) AS total_enter,
        SUM(k.CTRL_C_EVENTS + k.CTRL_V_EVENTS) AS total_copy_paste,
        SUM(k.ALT_TAB_EVENTS) AS total_swivel,
        SUM(k.TOTAL_KEYBOARD_EVENTS) AS total_keyboard,
        AVG(k.BACKSPACE_EVENTS + k.ESC_EVENTS + k.DELETE_EVENTS) AS avg_error,
        AVG(k.ENTER_EVENTS) AS avg_enter,
        AVG(k.CTRL_C_EVENTS + k.CTRL_V_EVENTS) AS avg_copy_paste,
        AVG(k.ALT_TAB_EVENTS) AS avg_swivel,
        AVG(k.TOTAL_KEYBOARD_EVENTS) AS avg_keyboard,
        COUNT(DISTINCT k.EMP_ID) AS emp_count
    FROM gold_dashboard.analytics_ump_keystrokes k
    INNER JOIN FilteredLoginLogout f
        ON k.EMP_ID = f.emp_id
        AND k.SHIFT_DATE = f.shift_date
    WHERE k.SHIFT_DATE BETWEEN (SELECT prev_start FROM date_periods) 
                          AND (SELECT curr_end FROM date_periods)
    GROUP BY f.period
),

mouse_data AS (
    SELECT
        f.period,
        SUM(m.TOTAL_MOUSE_COUNT) AS total_mouse,
        AVG(m.TOTAL_MOUSE_COUNT) AS avg_mouse
    FROM gold_dashboard.analytics_emp_mouseclicks m
    INNER JOIN FilteredLoginLogout f
        ON m.EMP_ID = f.emp_id
        AND m.SHIFT_DATE = f.shift_date
    WHERE m.SHIFT_DATE BETWEEN (SELECT prev_start FROM date_periods) 
                          AND (SELECT curr_end FROM date_periods)
    GROUP BY f.period
),

combined_data AS (
    SELECT
        COALESCE(k.period, m.period) AS period,
        COALESCE(k.total_error, 0) AS total_error,
        COALESCE(k.total_enter, 0) AS total_enter,
        COALESCE(k.total_copy_paste, 0) AS total_copy_paste,
        COALESCE(k.total_swivel, 0) AS total_swivel,
        COALESCE(k.total_keyboard, 0) AS total_keyboard,
        COALESCE(m.total_mouse, 0) AS total_mouse,
        COALESCE(k.avg_error, 0) AS avg_error,
        COALESCE(k.avg_enter, 0) AS avg_enter,
        COALESCE(k.avg_copy_paste, 0) AS avg_copy_paste,
        COALESCE(k.avg_swivel, 0) AS avg_swivel,
        COALESCE(k.avg_keyboard, 0) AS avg_keyboard,
        COALESCE(m.avg_mouse, 0) AS avg_mouse,
        COALESCE(k.total_keyboard, 0) + COALESCE(m.total_mouse, 0) AS total_input,
        COALESCE(k.avg_keyboard, 0) + COALESCE(m.avg_mouse, 0) AS avg_input
    FROM keyboard_data k
    FULL OUTER JOIN mouse_data m ON k.period = m.period
),

period_data AS (
    SELECT 
        cd.*,
        wh.total_hours_worked
    FROM combined_data cd
    LEFT JOIN work_hours wh ON cd.period = wh.period
),

metrics_comparison AS (
    SELECT
        metric,
        MAX(CASE WHEN period = 'CURRENT' THEN total_value END) AS current_total,
        MAX(CASE WHEN period = 'PREVIOUS' THEN total_value END) AS prev_total,
        MAX(CASE WHEN period = 'CURRENT' THEN avg_value END) AS current_avg,
        MAX(CASE WHEN period = 'PREVIOUS' THEN avg_value END) AS prev_avg
    FROM period_data,
    LATERAL (
        VALUES 
            ('Error Handling', total_error, avg_error),
            ('Enter Usage', total_enter, avg_enter),
            ('Copy Paste', total_copy_paste, avg_copy_paste),
            ('Swivel Chairing', total_swivel, avg_swivel)
    ) AS m(metric, total_value, avg_value)
    GROUP BY metric
),

final_metrics AS (
    SELECT
        metric,
        current_total,
        prev_total,
        current_avg,
        prev_avg,
        CASE 
            WHEN prev_total IS NULL THEN 'No Change'
            WHEN current_total > prev_total THEN 'UP'
            WHEN current_total < prev_total THEN 'Down'
            ELSE 'No Change'
        END AS total_trend,
        CASE 
            WHEN prev_total IS NULL OR prev_total = 0 THEN 0
            ELSE ROUND(((current_total - prev_total) / prev_total) * 100, 2)
        END AS total_change_pct,
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
    FROM metrics_comparison
),

input_comparison AS (
    SELECT
        curr.total_keyboard AS total_keyboard_count,
        curr.total_mouse AS total_mouse_count,
        curr.total_input AS total_input_count,
        prev.total_input AS prev_total_input_count,
        curr.avg_keyboard AS avg_keyboard_count,
        curr.avg_mouse AS avg_mouse_count,
        curr.avg_input AS avg_input_count,
        prev.avg_input AS prev_avg_input_count,
        curr.total_hours_worked AS current_total_hours,
        prev.total_hours_worked AS prev_total_hours,
        curr.total_input / NULLIF(curr.total_hours_worked, 0) AS current_hourly_rate,
        prev.total_input / NULLIF(prev.total_hours_worked, 0) AS prev_hourly_rate,
        CASE 
            WHEN prev.total_input IS NULL THEN 'No Change'
            WHEN curr.total_input > prev.total_input THEN 'UP'
            WHEN curr.total_input < prev.total_input THEN 'Down'
            ELSE 'No Change'
        END AS total_trend,
        CASE 
            WHEN prev.total_input IS NULL OR prev.total_input = 0 THEN 0
            ELSE ROUND(((curr.total_input - prev.total_input) / prev.total_input) * 100, 2)
        END AS total_change_pct,
        CASE 
            WHEN prev.avg_input IS NULL THEN 'No Change'
            WHEN curr.avg_input > prev.avg_input THEN 'UP'
            WHEN curr.avg_input < prev.avg_input THEN 'Down'
            ELSE 'No Change'
        END AS avg_trend,
        CASE 
            WHEN prev.avg_input IS NULL OR prev.avg_input = 0 THEN 0
            ELSE ROUND(((curr.avg_input - prev.avg_input) / prev.avg_input) * 100, 2)
        END AS avg_change_pct,
        CASE 
            WHEN prev.total_hours_worked IS NULL OR prev.total_hours_worked = 0 THEN 'No Change'
            WHEN (curr.total_input / NULLIF(curr.total_hours_worked, 0)) > 
                 (prev.total_input / NULLIF(prev.total_hours_worked, 0)) THEN 'UP'
            WHEN (curr.total_input / NULLIF(curr.total_hours_worked, 0)) < 
                 (prev.total_input / NULLIF(prev.total_hours_worked, 0)) THEN 'Down'
            ELSE 'No Change'
        END AS hourly_trend,
        CASE 
            WHEN prev.total_hours_worked IS NULL OR prev.total_hours_worked = 0 THEN 0
            ELSE ROUND(((
                (curr.total_input / NULLIF(curr.total_hours_worked, 0)) - 
                (prev.total_input / NULLIF(prev.total_hours_worked, 0))
            ) / NULLIF((prev.total_input / NULLIF(prev.total_hours_worked, 0)), 0) * 100), 2)
        END AS hourly_change_pct
    FROM period_data curr
    CROSS JOIN period_data prev
    WHERE curr.period = 'CURRENT' AND prev.period = 'PREVIOUS'
)

SELECT to_json(
    named_struct(
        'totals', named_struct(
            'UsageSummary', (SELECT COLLECT_LIST(named_struct(
                'metric', metric,
                'count', current_total,
                'prev_count', prev_total,
                'prev_trend', total_trend,
                'prev_perc', total_change_pct
            )) FROM final_metrics),
            'InputSummary', named_struct(
                'key_count', total_keyboard_count,
                'mouse_count', total_mouse_count,
                'input_event_count', total_input_count,
                'prev_input_event_count', prev_total_input_count,
                'total_hours_worked', current_total_hours,
                'prev_total_hours_worked', prev_total_hours,
                'hourly_rate', current_hourly_rate,
                'prev_hourly_rate', prev_hourly_rate,
                'prev_trend', total_trend,
                'prev_perc', total_change_pct,
                'hourly_trend', hourly_trend,
                'hourly_perc', hourly_change_pct
            )
        ),
        'averages', named_struct(
            'UsageSummary', (SELECT COLLECT_LIST(named_struct(
                'metric', metric,
                'current_avg', current_avg,
                'prev_avg', prev_avg,
                'prev_trend', avg_trend,
                'prev_perc', avg_change_pct
            )) FROM final_metrics),
            'InputSummary', named_struct(
                'key_count', avg_keyboard_count,
                'mouse_count', avg_mouse_count,
                'input_event_count', avg_input_count,
                'prev_input_event_count', prev_avg_input_count,
                'prev_trend', avg_trend,
                'prev_perc', avg_change_pct
            )
        )
    )
) AS result_json;





WITH keyboard AS (
    SELECT 
        EMPID,
        SHIFT_DATE,
        CASE 
            WHEN CALDATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
            WHEN CALDATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
        END AS period,
        SUM(BACKSPACE_EVENTS + ESCAPE_EVENTS + DELETE_EVENTS) AS error_events,
        SUM(ENTER_EVENTS) AS enter_events,
        SUM(CONTROL_C_EVENTS + CONTROL_V_EVENTS) AS copy_paste_events,
        SUM(ALLTAB_EVENTS) AS vehicle_events,
        SUM(OTHER_EVENTS) AS other_events,
        SUM(TOTAL_KEYBOARD_EVENTS) AS total_keyboard
    FROM KeyboardEvents
    WHERE CALDATE BETWEEN :prev_start_date AND :end_date
    GROUP BY EMPID, SHIFT_DATE,
             CASE 
                 WHEN CALDATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
                 WHEN CALDATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
             END
),
mouse AS (
    SELECT 
        EMPID,
        SHIFT_DATE,
        CASE 
            WHEN CALDATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
            WHEN CALDATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
        END AS period,
        SUM(TOTAL_MOUSE_COUNT) AS mouse_events
    FROM MouseEvents
    WHERE CALDATE BETWEEN :prev_start_date AND :end_date
    GROUP BY EMPID, SHIFT_DATE,
             CASE 
                 WHEN CALDATE BETWEEN :start_date AND :end_date THEN 'CURRENT'
                 WHEN CALDATE BETWEEN :prev_start_date AND :prev_end_date THEN 'PREVIOUS'
             END
),
combined AS (
    SELECT 
        k.EMPID,
        k.SHIFT_DATE,
        k.period,
        k.error_events,
        k.enter_events,
        k.copy_paste_events,
        k.vehicle_events,
        k.other_events,
        k.total_keyboard,
        m.mouse_events
    FROM keyboard k
    LEFT JOIN mouse m 
           ON k.EMPID = m.EMPID 
          AND k.SHIFT_DATE = m.SHIFT_DATE 
          AND k.period = m.period
),
averages AS (
    SELECT 
        period,
        AVG(error_events) AS avg_error,
        AVG(enter_events) AS avg_enter,
        AVG(copy_paste_events) AS avg_copy_paste,
        AVG(vehicle_events) AS avg_vehicle,
        AVG(mouse_events) AS avg_mouse,
        AVG(total_keyboard) AS avg_keyboard
    FROM combined
    GROUP BY period
),
final AS (
    SELECT
        'Error Events' AS metric,
        c.avg_error AS current_avg,
        p.avg_error AS prev_avg
    FROM averages c
    LEFT JOIN averages p ON p.period = 'PREVIOUS'
    WHERE c.period = 'CURRENT'
    UNION ALL
    SELECT 'Enter Events', c.avg_enter, p.avg_enter
    FROM averages c
    LEFT JOIN averages p ON p.period = 'PREVIOUS'
    WHERE c.period = 'CURRENT'
    UNION ALL
    SELECT 'Copy/Paste Events', c.avg_copy_paste, p.avg_copy_paste
    FROM averages c
    LEFT JOIN averages p ON p.period = 'PREVIOUS'
    WHERE c.period = 'CURRENT'
    UNION ALL
    SELECT 'Vehicle Events', c.avg_vehicle, p.avg_vehicle
    FROM averages c
    LEFT JOIN averages p ON p.period = 'PREVIOUS'
    WHERE c.period = 'CURRENT'
    UNION ALL
    SELECT 'Mouse Events', c.avg_mouse, p.avg_mouse
    FROM averages c
    LEFT JOIN averages p ON p.period = 'PREVIOUS'
    WHERE c.period = 'CURRENT'
),
with_trends AS (
    SELECT
        metric,
        current_avg,
        prev_avg,
        current_avg AS avg_count,   -- ✅ exposing explicitly
        CASE 
            WHEN prev_avg IS NULL THEN 'NEW'
            WHEN current_avg > prev_avg THEN 'UP'
            WHEN current_avg < prev_avg THEN 'DOWN'
            ELSE 'NO CHANGE'
        END AS trend,
        CASE 
            WHEN prev_avg IS NULL OR prev_avg = 0 THEN NULL
            ELSE ROUND(((current_avg - prev_avg) / prev_avg) * 100, 2)
        END AS change_pct
    FROM final
),
input_summary AS (
    SELECT
        c.avg_keyboard AS avg_keyboard_count,
        c.avg_mouse AS avg_mouse_count,
        (c.avg_keyboard + c.avg_mouse) AS input_count,
        CASE 
            WHEN p.avg_keyboard IS NULL OR p.avg_mouse IS NULL THEN 'NEW'
            WHEN (c.avg_keyboard + c.avg_mouse) > (p.avg_keyboard + p.avg_mouse) THEN 'UP'
            WHEN (c.avg_keyboard + c.avg_mouse) < (p.avg_keyboard + p.avg_mouse) THEN 'DOWN'
            ELSE 'NO CHANGE'
        END AS prev_trend,
        CASE 
            WHEN (p.avg_keyboard + p.avg_mouse) IS NULL OR (p.avg_keyboard + p.avg_mouse) = 0 THEN NULL
            ELSE ROUND( ((c.avg_keyboard + c.avg_mouse) - (p.avg_keyboard + p.avg_mouse)) 
                        / (p.avg_keyboard + p.avg_mouse) * 100 , 2)
        END AS prev_perc
    FROM averages c
    LEFT JOIN averages p ON p.period = 'PREVIOUS'
    WHERE c.period = 'CURRENT'
)
SELECT to_json(named_struct(
    'Usage Summary', collect_list(named_struct(
        'metric', metric,
        'avg_count', avg_count,     -- ✅ added
        'current_avg', current_avg,
        'prev_avg', prev_avg,
        'trend', trend,
        'change_pct', change_pct
    )),
    'Input Summary', named_struct(
        'avg_keyboard_count', avg_keyboard_count,
        'avg_mouse_count', avg_mouse_count,
        'input_count', input_count,
        'prev_trend', prev_trend,
        'prev_perc', prev_perc
    )
)) AS result_json
FROM with_trends, input_summary;





WITH shift_events AS (
    SELECT
        l.EMPID,
        l.SHIFT_DATE,
        COUNT(*) AS total_events,
        SUM(CASE WHEN k.EVENT_TYPE IN ('BACKSPACE', 'ESCAPE', 'DELETE') THEN 1 ELSE 0 END) AS error_events,
        SUM(CASE WHEN k.EVENT_TYPE = 'ENTER' THEN 1 ELSE 0 END) AS enter_events,
        SUM(CASE WHEN k.EVENT_TYPE IN ('CTRL+C', 'CTRL+V') THEN 1 ELSE 0 END) AS copy_paste_events,
        SUM(CASE WHEN k.EVENT_TYPE = 'ALT+TAB' THEN 1 ELSE 0 END) AS swivel_events,
        TIMESTAMPDIFF(SECOND, l.EMP_LOGIN_TIME, l.EMP_LOGOUT_TIME) / 3600.0 AS shift_hours
    FROM
        emp_login_logout l
    JOIN
        emp_keyboard_data k
        ON k.EMPID = l.EMPID
        AND k.EVENT_TIME >= l.EMP_LOGIN_TIME
        AND k.EVENT_TIME <= l.EMP_LOGOUT_TIME
    GROUP BY
        l.EMPID,
        l.SHIFT_DATE
)
SELECT
    EMPID,
    AVG(total_events) AS avg_input_activities,
    AVG(error_events) AS avg_error_events,
    AVG(enter_events) AS avg_enter_events,
    AVG(copy_paste_events) AS avg_copy_paste_events,
    AVG(swivel_events) AS avg_swivel_events,
    AVG(shift_hours) AS avg_shift_hours,
    (AVG(total_events) / NULLIF(AVG(shift_hours),0)) AS avg_input_per_hour,
    (AVG(error_events) / NULLIF(AVG(total_events),0)) * 100 AS error_handling_pct,
    (AVG(enter_events) / NULLIF(AVG(total_events),0)) * 100 AS enter_events_pct,
    (AVG(copy_paste_events) / NULLIF(AVG(total_events),0)) * 100 AS copy_paste_pct,
    (AVG(swivel_events) / NULLIF(AVG(total_events),0)) * 100 AS swivel_pct
FROM
    shift_events
WHERE
    SHIFT_DATE BETWEEN '2025-08-01' AND '2025-08-07'  -- your input window
GROUP BY
    EMPID;



@app.get('/get_employee_productivity')
async def get_employee_productivity(
    Authorize: AuthJWT = Depends(),
    authorization: str = Header(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    date_range: Optional[str] = Query(None)
):
    # [Authentication and date handling remains same as previous implementation]

    query = f"""
    WITH FilteredEmployees AS (
        SELECT 
            emplid AS emp_id,
            name,
            COALESCE(country_c, 'Default') AS country
        FROM inbound.hr_employee_central
        WHERE func_mgr_id = '{manager_id}'
        AND (TERMINATION_DT > CURRENT_TIMESTAMP() OR TERMINATION_DT IS NULL)
    ),
    
    EmployeeShiftsWithConfig AS (
        SELECT
            e.emp_id,
            e.name,
            e.country,
            l.shift_date,
            l.shift_start_time,
            l.shift_end_time,
            l.emp_login_time,
            l.emp_logout_time,
            -- Calculate pulse shift length (hours)
            (UNIX_TIMESTAMP(l.shift_end_time) - UNIX_TIMESTAMP(l.shift_start_time)) / 3600.0 AS pulse_shift_length,
            -- Get country config
            COALESCE(c.shift_length_hrs, {DEFAULT_COUNTRY_CONFIG['shift_length']}) AS country_shift_length,
            COALESCE(c.shift_duration_hrs, {DEFAULT_COUNTRY_CONFIG['shift_duration']}) AS country_shift_duration,
            COALESCE(c.break_time_hrs, {DEFAULT_COUNTRY_CONFIG['break_time']}) AS country_break_time,
            -- Calculate adjusted metrics
            (COALESCE(c.shift_duration_hrs, {DEFAULT_COUNTRY_CONFIG['shift_duration']}) * 
             (UNIX_TIMESTAMP(l.shift_end_time) - UNIX_TIMESTAMP(l.shift_start_time)) / 3600.0 / 
             COALESCE(c.shift_length_hrs, {DEFAULT_COUNTRY_CONFIG['shift_length']})) AS adjusted_shift_duration,
            (COALESCE(c.break_time_hrs, {DEFAULT_COUNTRY_CONFIG['break_time']}) * 
             (UNIX_TIMESTAMP(l.shift_end_time) - UNIX_TIMESTAMP(l.shift_start_time)) / 3600.0 / 
             COALESCE(c.shift_length_hrs, {DEFAULT_COUNTRY_CONFIG['shift_length']})) AS adjusted_break_time
        FROM FilteredEmployees e
        JOIN gold_dashboard.analytics_emp_login_logout l
            ON e.emp_id = l.emp_id
        LEFT JOIN inbound.countrywise_shift c
            ON e.country = c.country
        WHERE l.shift_date BETWEEN '{prev_start_date}' AND '{end_date}'
        AND l.shift_start_time IS NOT NULL
        AND l.shift_end_time IS NOT NULL
        AND (UNIX_TIMESTAMP(l.shift_end_time) - UNIX_TIMESTAMP(l.shift_start_time)) > 0
    ),
    
    EmployeeActivity AS (
        SELECT
            esc.*,
            -- Time within official shift boundaries
            COALESCE(SUM(CASE 
                WHEN a.interval BETWEEN esc.shift_start_time AND esc.shift_end_time
                THEN a.total_time_spent_active ELSE 0 END), 0) AS shift_active_time,
            COALESCE(SUM(CASE 
                WHEN a.interval BETWEEN esc.shift_start_time AND esc.shift_end_time
                THEN a.total_time_spent_idle ELSE 0 END), 0) AS shift_idle_time,
            COALESCE(SUM(CASE 
                WHEN a.interval BETWEEN esc.shift_start_time AND esc.shift_end_time
                THEN a.window_lock_time ELSE 0 END), 0) AS shift_window_lock_time,
            
            -- Time within login-logout but outside shift
            COALESCE(SUM(CASE 
                WHEN esc.emp_login_time IS NOT NULL 
                AND esc.emp_logout_time IS NOT NULL
                AND a.interval BETWEEN esc.emp_login_time AND esc.emp_logout_time
                AND a.interval NOT BETWEEN esc.shift_start_time AND esc.shift_end_time
                THEN a.total_time_spent_active ELSE 0 END), 0) AS overtime_active_time,
            COALESCE(SUM(CASE 
                WHEN esc.emp_login_time IS NOT NULL 
                AND esc.emp_logout_time IS NOT NULL
                AND a.interval BETWEEN esc.emp_login_time AND esc.emp_logout_time
                AND a.interval NOT BETWEEN esc.shift_start_time AND esc.shift_end_time
                THEN a.total_time_spent_idle ELSE 0 END), 0) AS overtime_idle_time,
            COALESCE(SUM(CASE 
                WHEN esc.emp_login_time IS NOT NULL 
                AND esc.emp_logout_time IS NOT NULL
                AND a.interval BETWEEN esc.emp_login_time AND esc.emp_logout_time
                AND a.interval NOT BETWEEN esc.shift_start_time AND esc.shift_end_time
                THEN a.window_lock_time ELSE 0 END), 0) AS overtime_window_lock_time,
            
            -- Totals
            (shift_active_time + shift_idle_time + shift_window_lock_time) AS total_shift_time,
            (overtime_active_time + overtime_idle_time + overtime_window_lock_time) AS total_overtime
        FROM EmployeeShiftsWithConfig esc
        LEFT JOIN gold_dashboard.analytics_emp_app_info a
            ON esc.emp_id = a.emp_id
            AND esc.shift_date = a.cal_date
        GROUP BY 
            esc.emp_id, esc.name, esc.country, esc.shift_date,
            esc.shift_start_time, esc.shift_end_time,
            esc.emp_login_time, esc.emp_logout_time,
            esc.pulse_shift_length,
            esc.country_shift_length, esc.country_shift_duration,
            esc.country_break_time, esc.adjusted_shift_duration,
            esc.adjusted_break_time
    )
    
    SELECT 
        *,
        -- Calculate productivity metrics
        (shift_active_time/3600.0 - adjusted_shift_duration) AS shift_productivity_trend,
        (shift_window_lock_time/3600.0 - adjusted_break_time) AS break_compliance_trend,
        (total_shift_time + total_overtime) AS total_working_time
    FROM EmployeeActivity
    """


router.get("/cpu_ram_disk_usage")
async def cpu_ram_disk_usage(Authorize: AuthJWT = Depends(), authorization: str = Header(None), date_range: str, user_type: str):
    if not authorization or not authorization.startswith("Bearer "):
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"msg": "Authorization Token Missing"})

    try:
        Authorize.jwt_required()
        user_identity = Authorize.get_jwt_subject()
        claims = Authorize.get_raw_jwt()
        emp_id = claims.get("user_id")
        
        if not emp_id:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"message": "Invalid Token Claims"})
        
        today = datetime.today()
        output = []
        
        # Common base query for daily view
        daily_base_query = """
        WITH EmployeeLoginTimes AS (
            SELECT 
                e.emplid,
                ell.shift_date,
                ell.emp_login_time,
                ell.emp_logout_time
            FROM {employee_filter} e
            JOIN Inbound.emp_login_logout ell ON e.emplid = ell.emp_id
            WHERE ell.shift_date = '{date}'
        ),
        FilteredCPUData AS (
            SELECT 
                ecd.emp_id,
                ecd.cal_date,
                WINDOW(ecd.created_on, '3 hours') AS window,
                FORMAT(window.start, 'HH:mm') || '-' || FORMAT(window.end, 'HH:mm') AS interval,
                ecd.cpu_used_pct,
                ecd.ram_used_pct,
                ecd.disk_used_pct
            FROM sys_trace.emp_cpudata ecd
            JOIN EmployeeLoginTimes elt ON ecd.emp_id = elt.emplid
            WHERE 
                (
                    ecd.created_on >= elt.emp_login_time AND 
                    ecd.created_on <= elt.emp_logout_time
                )
                AND ecd.emp_id IN (SELECT emplid FROM {employee_filter})
        )
        SELECT 
            interval,
            AVG(cpu_used_pct) AS avg_cpu_used_pct,
            AVG(ram_used_pct) AS avg_ram_used_pct,
            AVG(disk_used_pct) AS avg_disk_used_pct
        FROM FilteredCPUData
        GROUP BY interval
        ORDER BY interval ASC
        """
        
        # Weekly view query
        weekly_base_query = """
        WITH EmployeeLoginTimes AS (
            SELECT 
                e.emplid,
                ell.shift_date,
                ell.emp_login_time,
                ell.emp_logout_time
            FROM {employee_filter} e
            JOIN Inbound.emp_login_logout ell ON e.emplid = ell.emp_id
            WHERE ell.shift_date BETWEEN '{from_date}' AND '{to_date}'
        ),
        DailyAverages AS (
            SELECT 
                ecd.emp_id,
                ecd.cal_date,
                AVG(ecd.cpu_used_pct) AS avg_cpu_used_pct,
                AVG(ecd.ram_used_pct) AS avg_ram_used_pct,
                AVG(ecd.disk_used_pct) AS avg_disk_used_pct
            FROM sys_trace.emp_cpudata ecd
            JOIN EmployeeLoginTimes elt ON ecd.emp_id = elt.emplid
            WHERE 
                (
                    ecd.created_on >= elt.emp_login_time AND 
                    ecd.created_on <= elt.emp_logout_time
                )
                AND ecd.emp_id IN (SELECT emplid FROM {employee_filter})
                AND ecd.cal_date BETWEEN '{from_date}' AND '{to_date}'
            GROUP BY ecd.emp_id, ecd.cal_date
        )
        SELECT 
            cal_date,
            AVG(avg_cpu_used_pct) AS avg_cpu_used_pct,
            AVG(avg_ram_used_pct) AS avg_ram_used_pct,
            AVG(avg_disk_used_pct) AS avg_disk_used_pct
        FROM DailyAverages
        GROUP BY cal_date
        ORDER BY cal_date ASC
        LIMIT 7
        """
        
        # Determine the employee filter based on user type
        if user_type == "manager":
            employee_filter = f"""
            (SELECT emplid, name
            FROM Inbound.hr_employee_central
            WHERE func_mgr_id = '{emp_id}' AND (TERMINATION_DT > CURRENT_TIMESTAMP OR TERMINATION_DT IS NULL))
            """
        elif user_type == "emp":
            employee_filter = f"""
            (SELECT emplid, name
            FROM inbound.hr_employee_central
            WHERE emplid = '{emp_id}' AND (TERMINATION_DT > CURRENT_TIMESTAMP OR TERMINATION_DT IS NULL))
            """
        else:
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"message": "Invalid user type"})
        
        # Format the final query based on date range
        if date_range == "daily":
            query = daily_base_query.format(
                employee_filter=employee_filter,
                date=today.strftime("%Y-%m-%d")
            )
        elif date_range == "weekly":
            from_date = (today - timedelta(days=6)).strftime("%Y-%m-%d")
            to_date = today.strftime("%Y-%m-%d")
            query = weekly_base_query.format(
                employee_filter=employee_filter,
                from_date=from_date,
                to_date=to_date
            )
        else:
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"message": "Invalid date range"})
        
        with DatabricksSession() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            
            if date_range == "daily":
                if result and result[0]:
                    data_dict = {entry["interval"]: entry for entry in result}
                    for interval in ALL_INTERVALS:
                        if interval in data_dict:
                            output.append({
                                "interval": interval,
                                "avg_cpu_used_pct": data_dict[interval][1],
                                "avg_disk_used_pct": data_dict[interval][3],
                                "avg_ram_used_pct": data_dict[interval][2],
                            })
                        else:
                            output.append({
                                "interval": interval,
                                "avg_cpu_used_pct": 0,
                                "avg_disk_used_pct": 0,
                                "avg_ram_used_pct": 0,
                            })
                else:
                    for interval in ALL_INTERVALS:
                        output.append({
                            "interval": interval,
                            "avg_cpu_used_pct": 0,
                            "avg_disk_used_pct": 0,
                            "avg_ram_used_pct": 0,
                        })
                
                output = sorted(output, key=lambda x: ALL_INTERVALS.index(x["interval"]))
            else:  # weekly
                if result and result[0]:
                    for row in result:
                        output.append({
                            "date": row["cal_date"],
                            "avg_cpu_used_pct": row["avg_cpu_used_pct"],
                            "avg_disk_used_pct": row["avg_disk_used_pct"],
                            "avg_ram_used_pct": row["avg_ram_used_pct"],
                        })
                else:
                    # Generate empty data for last 7 days if no results
                    for i in range(7):
                        date = (today - timedelta(days=6-i)).strftime("%Y-%m-%d")
                        output.append({
                            "date": date,
                            "avg_cpu_used_pct": 0,
                            "avg_disk_used_pct": 0,
                            "avg_ram_used_pct": 0,
                        })
            
            return JSONResponse(content=output)
            
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": str(e)})