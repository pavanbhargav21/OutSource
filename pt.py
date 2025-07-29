# Initialize configuration for Delta Lake MERGE operations (if needed)
spark.sql("SET spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension")
spark.sql("CONFIGURE SPARK SET spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")

# =====================================================================
# STEP 1: Clean up employees no longer in HR system
# =====================================================================

# Count employees who exist in analytics_emp_mapping but:
# 1. Don't exist in HR system (or are terminated)
# 2. Belong to managers that still exist in our system
deleted_count = spark.sql("""
    SELECT COUNT(*) AS deleted_count
    FROM gold_dashboard.analytics_emp_mapping
    WHERE emp_id IN (
        SELECT aem.emp_id
        FROM gold_dashboard.analytics_emp_mapping aem
        LEFT JOIN (
            -- Active employees in HR system (not terminated)
            SELECT emplid 
            FROM inbound.hr_employee_central
            WHERE (TERMINATION_DT > CURRENT_TIMESTAMP() OR TERMINATION_DT IS NULL)
        ) hec ON aem.emp_id = hec.emplid
        WHERE hec.emplid IS NULL  -- No match in HR system
        AND aem.manager_id IN (
            -- Only consider managers that still exist in our system
            SELECT DISTINCT manager_id 
            FROM gold_dashboard.analytics_emp_mapping
        )
    )
""").collect()[0]['deleted_count']

# Create temporary view with only valid employees (inverse of above query)
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW valid_employees AS
    SELECT * 
    FROM gold_dashboard.analytics_emp_mapping
    WHERE emp_id NOT IN (
        SELECT aem.emp_id
        FROM gold_dashboard.analytics_emp_mapping aem
        LEFT JOIN (
            SELECT emplid 
            FROM inbound.hr_employee_central
            WHERE (TERMINATION_DT > CURRENT_TIMESTAMP() OR TERMINATION_DT IS NULL)
        ) hec ON aem.emp_id = hec.emplid
        WHERE hec.emplid IS NULL
        AND aem.manager_id IN (
            SELECT DISTINCT manager_id 
            FROM gold_dashboard.analytics_emp_mapping
        )
    )
""")

# =====================================================================
# STEP 2: Reorganize understaffed teams
# =====================================================================

# Create temporary view identifying employees in understaffed teams (<4 members)
# and prepare their update values (moving to team_id=10)
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW understaffed_updates AS
    WITH team_sizes AS (
        -- Calculate current team sizes (excluding default team_id=1)
        SELECT 
            current_team_id,
            COUNT(*) AS team_size
        FROM valid_employees
        WHERE current_team_id != 1  -- Exclude default team from reorganization
        GROUP BY current_team_id
    ),
    understaffed_employees AS (
        -- Identify employees in teams with <4 members
        SELECT 
            ve.emp_id,
            ve.current_team_id AS original_team_id
        FROM valid_employees ve
        JOIN team_sizes ts ON ve.current_team_id = ts.current_team_id
        WHERE ts.team_size < 4  -- Understaffed threshold
    )
    -- Prepare update values: move to team_id=10 with timestamps
    SELECT 
        emp_id,
        10 AS new_team_id,  -- Default team for understaffed
        original_team_id,
        CURRENT_TIMESTAMP() AS change_ts  -- Uniform timestamp for all changes
    FROM understaffed_employees
""")

# Count how many employees will be moved
moved_count = spark.sql("""
    SELECT COUNT(*) AS moved_count FROM understaffed_updates
""").collect()[0]['moved_count']

# =====================================================================
# STEP 3: Perform MERGE operation to update records
# =====================================================================

# Use MERGE to efficiently update only affected records
spark.sql("""
    MERGE INTO gold_dashboard.analytics_emp_mapping AS target
    USING (
        -- Source data: employees needing updates
        SELECT 
            emp_id,
            new_team_id,
            original_team_id,
            change_ts
        FROM understaffed_updates
    ) AS source
    ON target.emp_id = source.emp_id  -- Match on employee ID
    
    -- For matched records (employees in understaffed teams)
    WHEN MATCHED THEN
        UPDATE SET
            target.current_team_id = source.new_team_id,  -- Move to team 10
            target.last_team_id = source.original_team_id,  -- Preserve original team
            target.last_change_date = source.change_ts,  -- Update change timestamp
            target.next_change_date = NULL,  -- Clear scheduled changes
            target.assignment_type = 'AUTO_DEFAULT',  -- Mark as auto-reassigned
            target.moved_reason = 'UNDERSTAFFED_TEAM',  -- Document reason
            target.updated_at = source.change_ts  -- Update modification timestamp
""")

# =====================================================================
# OUTPUT RESULTS
# =====================================================================

print(f"Cleanup complete:")
print(f"- Deleted {deleted_count} employees no longer in HR system")
print(f"- Moved {moved_count} employees from understaffed teams to default team")
print(f"Note: Default team ID is 10, understaffed threshold is <4 team members")

# Optional: Verify results
# spark.sql("SELECT current_team_id, COUNT(*) FROM gold_dashboard.analytics_emp_mapping GROUP BY current_team_id").show()