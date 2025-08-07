
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