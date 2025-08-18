daily job to update leave details to emp_login_logout.

from pyspark.sql import functions as F
from datetime import datetime, timedelta

# ----- Config -----
current_time = datetime.now()
leave_threshold = current_time - timedelta(days=3)

# 1. Read leave records with status "Approved" updated in last 3 days
leave_df = (
    spark.table("inbound.hr_lms")
    .filter(
        (F.col("updated_time_stamp") >= F.lit(leave_threshold)) &
        (F.col("leave_status") == "Approved")
    )
    .select("employee_id", "leave_start_date", "leave_end_date")
    .dropDuplicates()
)

# 2. Explode leave date intervals into emp_id and every leave day
def explode_leave_days(row):
    emp_id = row["employee_id"]
    start = row["leave_start_date"]
    end = row["leave_end_date"]
    # Ensure types are datetime.date
    dates = []
    curr = start
    while curr <= end:
        dates.append((emp_id, curr.strftime("%Y-%m-%d")))
        curr += timedelta(days=1)
    return dates

exploded = leave_df.rdd.flatMap(explode_leave_days)
leave_dates_df = spark.createDataFrame(exploded, ["emp_id", "shift_date"])

# 3. Prepare view for update
leave_dates_df.createOrReplaceTempView("leave_dates_to_update")

# 4. Update gold_dashboard.analytics_emp_login_logout.is_holiday
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING leave_dates_to_update AS source
ON target.EMP_ID = source.emp_id AND target.SHIFT_DATE = source.shift_date
WHEN MATCHED THEN
    UPDATE SET target.IS_HOLIDAY = TRUE
""")

# 5. Print for audit
updated_count = leave_dates_df.count()
print(f"Updated holiday flag for {updated_count} emp_id/shift_date pairs.")


 &#-£&&£-£&£-£-£-£--£-£-£-£--£-£-£-£-£--£-£



daily job for any shift changes and emp-login-logout recalculation.


from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import Window

# -------------------------
# Configurations
# -------------------------

DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"

current_time = datetime.now()
update_threshold = current_time - timedelta(days=3)

pulse_df = spark.table("inbound.pulse_emp_shift_info") \
    .filter(F.col("update_timestamp") >= F.lit(update_threshold)) \
    .select("emp_id", "shift_date", "start_time", "end_time")


def format_shift_times(df):
    # For week-off (start_time == end_time) -> "shift_date 00:00:00"
    # For normal shift (start < end): "shift_date start_time", "shift_date end_time"
    # For overnight: end_time on next date
    
    return df.withColumn(
        "start_time_full",
        F.concat_ws(" ", F.col("shift_date"), F.col("start_time"))
    ).withColumn(
        "end_time_full",
        F.when(F.col("start_time") == F.col("end_time"), 
               F.concat_ws(" ", F.col("shift_date"), F.lit("00:00:00")))
         .when(F.col("end_time") > F.col("start_time"),
               F.concat_ws(" ", F.col("shift_date"), F.col("end_time")))
         .otherwise(
             F.concat_ws(" ", F.date_add(F.col("shift_date"), 1), F.col("end_time"))
         )
    )

pulse_df_full = format_shift_times(pulse_df)

gold_df = spark.table("gold_dashboard.analytics_emp_login_logout") \
    .select("EMP_ID", "SHIFT_DATE", "START_TIME", "END_TIME")


from pyspark.sql import functions as F

# Step 1: Identify shifts that have changed start or end time
compare_df = pulse_df_full.join(
    gold_df,
    (pulse_df_full.emp_id == gold_df.EMP_ID) & (pulse_df_full.shift_date == gold_df.SHIFT_DATE),
    "inner"
).filter(
    (pulse_df_full.start_time_full != gold_df.START_TIME) |
    (pulse_df_full.end_time_full != gold_df.END_TIME)
).select(
    pulse_df_full.emp_id,
    pulse_df_full.shift_date
).distinct()

updated_shift_count = compare_df.count()
print(f"Number of shifts updated in last 3 days: {updated_shift_count}")


# Step xe: Filter activity tables based only on changed shifts (emp_id, shift_date)
act_df = spark.table("app_trace.emp_activity") \
    .join(
        compare_df.withColumnRenamed("shift_date", "cal_date"),
        (F.col("emp_id") == F.col("emp_id")) & (F.col("cal_date") == F.col("cal_date")),
        "inner"
    )

mouse_df = spark.table("sys_trace.emp_mousedata") \
    .join(
        compare_df.withColumnRenamed("shift_date", "cal_date"),
        (F.col("emp_id") == F.col("emp_id")) & (F.col("cal_date") == F.col("cal_date")),
        "inner"
    )

keyboard_df = spark.table("sys_trace.emp_keyboarddata") \
    .join(
        compare_df.withColumnRenamed("shift_date", "cal_date"),
        (F.col("emp_id") == F.col("emp_id")) & (F.col("cal_date") == F.col("cal_date")),
        "inner"
    )

mousekey_df = mouse_df.union(keyboard_df)

# Step x: Prepare distinct activity pairs from changed shifts only
activities_df = compare_df.select(
    F.col("emp_id"),
    F.col("shift_date").alias("cal_date")
).distinct()



# -------------------------
# Add previous date
# -------------------------
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# -------------------------
# Step 2: Load shift data
# -------------------------
shift_df = spark.table("inbound.pulse_emp_shift_info") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# -------------------------
# Step 3: Join current shift
# -------------------------
cur_shift = emp_dates_df.join(
    shift_df,
    (emp_dates_df.emp_id == shift_df.emp_id) & (emp_dates_df.cal_date == shift_df.shift_date),
    how='left'
).select(
    emp_dates_df.emp_id,
    emp_dates_df.cal_date,
    emp_dates_df.prev_date,
    shift_df.start_time.alias("cur_start_time_raw"),
    shift_df.end_time.alias("cur_end_time_raw"),
    shift_df.is_week_off
)

# Step 4: Join previous shift
prev_shift = shift_df \
    .withColumnRenamed("shift_date", "prev_cal_date") \
    .withColumnRenamed("start_time", "prev_start_time_raw") \
    .withColumnRenamed("end_time", "prev_end_time_raw") \
    .withColumnRenamed("emp_id", "emp_id_prev") \
    .withColumnRenamed("is_week_off", "prev_is_week_off")

cur_with_prev = cur_shift.join(
    prev_shift,
    (cur_shift.emp_id == prev_shift.emp_id_prev) & (cur_shift.prev_date == prev_shift.prev_cal_date),
    how="left"
).drop("emp_id_prev", "prev_cal_date")

# Step 5: Apply defaults based on missing data and day of week
final_df = cur_with_prev \
    .withColumn("dow", F.date_format("cal_date", "E")) \
    .withColumn("prev_dow", F.date_format("prev_date", "E")) \
    .withColumn("cur_start_time",
        F.when(F.col("cur_start_time_raw").isNotNull(), F.col("cur_start_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_end_time_raw").isNotNull(), F.col("cur_end_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    .withColumn("prev_start_time",
        F.when(F.col("prev_start_time_raw").isNotNull(), F.col("prev_start_time_raw"))
         .when(F.col("prev_is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_end_time_raw").isNotNull(), F.col("prev_end_time_raw"))
         .when(F.col("prev_is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("cast(cur_end_time as timestamp) + interval 1 day"), "HH:mm:ss")
        ).otherwise(F.col("cur_end_time"))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("cast(prev_end_time as timestamp) + interval 1 day"), "HH:mm:ss")
        ).otherwise(F.col("prev_end_time"))
    ) \
    .withColumn("cur_start_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_start_time"))) \
    .withColumn("cur_end_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_end_time"))) \
    .withColumn("prev_start_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_start_time"))) \
    .withColumn("prev_end_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_end_time"))) \
    .withColumn("cur_end_time_ts",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("cast(cur_end_time_ts as timestamp) + interval 1 day"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("cur_end_time_ts"))
    ) \
    .withColumn("prev_end_time_ts",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("cast(prev_end_time_ts as timestamp) + interval 1 day"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("prev_end_time_ts"))
    ) \
    .withColumn("is_week_off",
        F.when(
            (F.col("cur_start_time") == ZERO_TIME) & (F.col("cur_end_time") == ZERO_TIME),
            F.lit(True)
        ).otherwise(F.lit(False))
    ) \
    .withColumn("prev_is_week_off",
        F.when(
            (F.col("prev_start_time") == ZERO_TIME) & (F.col("prev_end_time") == ZERO_TIME),
            F.lit(True)
        ).otherwise(F.lit(False))
    ) \
    .select(
        "emp_id",
        "cal_date",
        "prev_date",
        "cur_start_time_ts",
        "cur_end_time_ts",
        "prev_start_time_ts",
        "prev_end_time_ts",
        "is_week_off",
        "prev_is_week_off"
    )

# 6. Join with activities
joined_df = act_df.join(
    final_df,
    ["emp_id", "cal_date"],
    "left"
)
joined_mkdf = mousekey_df.join(
    final_df,
    ["emp_id", "cal_date"],
    "left"
)

# 7. Define time windows with special handling for week-offs

jdf_with_windows = joined_df.withColumn(
    "current_window_start",
    F.when(
    (F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
    greatest(
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours"),  # previous day's extended shift end
        F.col("cur_start_time_ts")  # current day shift start (likely 00:00:00 on week off)
    )
).when(
    F.col("is_week_off") == True,
    F.col("cur_start_time_ts")  # consecutive week offs
).otherwise(
    F.expr("cast(cur_start_time_ts as timestamp) - interval 4 hours")  # normal working day
)

).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("cast(cur_end_time_ts as timestamp) + interval 8 hours"))
	.when(
	(F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
	F.concat_ws(" ", F.col("cal_date"),  lit("23:59:59"))
	) 
    .otherwise(F.col("cur_end_time_ts"))
).withColumn(
    "prev_window_start",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.col("prev_end_time_ts")
    ).otherwise(F.col("prev_start_time_ts"))
).withColumn(
    "prev_window_end",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours")
    ).otherwise(F.col("prev_end_time_ts"))
)





jmkdf_with_windows = joined_mkdf.withColumn(
    "current_window_start",
    F.when(
    (F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
    greatest(
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours"), 
        F.col("cur_start_time_ts")  # current day shift start (likely 00:00:00 on week off)
			)
	).when(
		F.col("is_week_off") == True,
		F.col("cur_start_time_ts")  # consecutive week offs
	).otherwise(
		F.expr("cast(cur_start_time_ts as timestamp) - interval 4 hours") )
).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("cast(cur_end_time_ts as timestamp) + interval 8 hours"))
	.when(
	(F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
	F.concat_ws(" ", F.col("cal_date"),  lit("23:59:59"))
	) 
    .otherwise(F.col("cur_end_time_ts"))
).withColumn(
    "prev_window_start",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.col("prev_end_time_ts")
		).otherwise(F.col("prev_start_time_ts"))
).withColumn(
    "prev_window_end",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours")
    ).otherwise(F.col("prev_end_time_ts"))
)

# 8. Classify activities with priority to current day
classified_df = jdf_with_windows.withColumn(
    "is_current_login",
    ~F.col("is_week_off") &
    (F.col("start_time") >= F.col("current_window_start")) &
    (F.col("start_time") <= F.col("current_window_end")) &
    (F.col("app_name") != "WindowLock")
).withColumn(
    "is_prev_logout",
    F.col("prev_window_start").isNotNull() &
    (F.col("start_time") >= F.col("prev_window_start")) &
    (F.col("start_time") <= F.col("prev_window_end")) &
    (F.col("start_time") < F.col("current_window_start"))
).withColumn(
    "is_week_off_activity",
    F.col("is_week_off") &
    (F.col("prev_window_start").isNull() | 
     (F.col("start_time") > F.col("prev_window_end")))
)

classified_mkdf = jmkdf_with_windows.withColumn(
    "is_current_login",
    ~F.col("is_week_off") &
    (F.col("event_time") >= F.col("current_window_start")) &
    (F.col("event_time") <= F.col("current_window_end"))
).withColumn(
    "is_prev_logout",
    F.col("prev_window_start").isNotNull() &
    (F.col("event_time") >= F.col("prev_window_start")) &
    (F.col("event_time") <= F.col("prev_window_end")) &
    (F.col("event_time") < F.col("current_window_start"))
).withColumn(
    "is_week_off_activity",
    F.col("is_week_off") &
    (F.col("prev_window_start").isNull() | 
     (F.col("event_time") > F.col("prev_window_end")))
)

# ---------------------------------------------------
# Step 9. Calculate all potential times
# ---------------------------------------------------
result_df = classified_df.groupBy("emp_id", "cal_date").agg(
    # Current login: Minimum start_time within current window and app_name != "Window Lock"
    F.min(F.when(
        (F.col("is_current_login")) & (F.col("app_name") != "WindowLock"),
        F.col("start_time")
    )).alias("current_login"),
    # Last activity: Maximum start_time within current window
    F.max(F.when(
        (F.col("start_time") >= F.col("current_window_start")) &
        (F.col("start_time") <= F.col("current_window_end")),
        F.col("start_time")
    )).alias("last_activity"),
    # Previous logout candidate (normal): Max start_time in previous window
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("start_time") >= F.col("prev_window_start")) &
        (F.col("start_time") < F.col("prev_window_end")) &
        (F.col("start_time") < F.col("current_window_start")) &
        (~F.col("is_week_off_activity")),
        F.col("start_time")
    )).alias("prev_logout_update_norm"),
    # Previous logout candidate (week-off): Max start_time in prev window if week_off_activity
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("start_time") > F.col("prev_window_start")) &
        (F.col("start_time") < F.col("prev_window_end")) &
        (F.col("is_week_off_activity")),
        F.col("start_time")
    )).alias("prev_logout_update_weekoff"),
    # Week-off login: Min start_time if week_off_activity
    F.min(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("app_name") != "WindowLock") &
        (F.col("start_time") > F.col("prev_window_end")) &
        (F.col("start_time") > F.col("current_window_start")),
        F.col("start_time")
    )).alias("week_off_login"),
    # Week-off logout: Max start_time if week_off_activity
    F.max(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("start_time") > F.col("prev_window_end")),
        F.col("start_time")
    )).alias("week_off_logout"),
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
).withColumn(
    "prev_logout_update",
    F.coalesce(F.col("prev_logout_update_norm"), F.col("prev_logout_update_weekoff"))
)

# Mouse/keyboard side
result_mkdf = classified_mkdf.groupBy("emp_id", "cal_date").agg(
    F.min(F.when(F.col("is_current_login") &
                 (F.col("event_time") >= F.col("current_window_start")) &
                 (F.col("event_time") <= F.col("current_window_end")),
                 F.col("event_time"))).alias("current_login"),
    F.max(F.when(
        (F.col("event_time") >= F.col("current_window_start")) &
        (F.col("event_time") <= F.col("current_window_end")),
        F.col("event_time")
    )).alias("last_mk_activity"),
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("event_time") >= F.col("prev_window_start")) &
        (F.col("event_time") < F.col("prev_window_end")) &
        (~F.col("is_week_off_activity")),
        F.col("event_time")
    )).alias("prev_logout_update_norm"),
    # Previous logout candidate (week-off): Max event_time in prev window if week_off_activity
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("event_time") > F.col("prev_window_start")) &
        (F.col("event_time") < F.col("prev_window_end")) &
        (F.col("is_week_off_activity")),
        F.col("event_time")
    )).alias("prev_logout_update_weekoff"),
    F.min(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("event_time") > F.col("prev_window_end")) &
        (F.col("event_time") > F.col("current_window_start")),
        F.col("event_time")
    )).alias("week_off_login"),
    F.max(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("event_time") > F.col("prev_window_end")),
        F.col("event_time")
    )).alias("week_off_mk_logout"),
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
)

result_mkdf = result_mkdf.withColumn(
    "prev_logout_mk_update",
    F.coalesce(F.col("prev_logout_update_norm"), F.col("prev_logout_update_weekoff"))
)

# ---------------------------------------------------
# Step 10. Determine final times with priority rules
# ---------------------------------------------------
final_result = result_df.withColumn(
    "emp_login_time",
    F.when(
        F.col("is_week_off"),
        F.coalesce(F.col("week_off_login"), F.col("last_activity"))
    ).otherwise(
        F.coalesce(F.col("current_login"), F.col("last_activity"))
    )
).withColumn(
    "emp_logout_time",
    F.when(
        F.col("is_week_off"),
        F.coalesce(F.col("week_off_logout"), F.col("last_activity"))
    ).otherwise(
        F.coalesce(F.col("last_activity"), F.col("current_login"))
    )
)

final_mkresult = result_mkdf.withColumn(
    "emp_logout_time_mk",
    F.when(
        F.col("is_week_off"),
        F.coalesce(F.col("week_off_mk_logout"), F.col("last_mk_activity"))
    ).otherwise(
        F.coalesce(F.col("last_mk_activity"), F.col("current_login"))
    )
)

# Join MK logout with main result
final_result_with_mk = final_result.join(
    final_mkresult.select("emp_id", "cal_date", "emp_logout_time_mk", "prev_logout_mk_update"),
    ["emp_id", "cal_date"],
    "left"
)

# ---------------------------------------------------
# Step 11. Generate previous day updates (both sources)
# ---------------------------------------------------
prev_day_updates = final_result_with_mk.filter(
    F.col("prev_logout_update").isNotNull() | F.col("prev_logout_mk_update").isNotNull()
).select(
    F.col("emp_id").alias("update_emp_id"),
    F.col("prev_cal_date").alias("update_date"),
    F.coalesce(F.col("prev_logout_mk_update"), F.col("prev_logout_update")).alias("new_logout_time")
)

# Final output: prefer MK logout if available
final_output = final_result_with_mk.select(
    "emp_id",
    "cal_date",
    F.coalesce(F.col("emp_logout_time_mk"), F.col("emp_logout_time")).alias("emp_logout_time"),
    "emp_login_time",
    "shift_start_time",
    "shift_end_time",
    "is_week_off"
).orderBy("emp_id", "cal_date")

# Filter out null records
filtered_login_logout = final_output.filter(
    (F.col("emp_id").isNotNull()) &
    (F.col("cal_date").isNotNull()) &
    (F.col("emp_login_time").isNotNull()) &
    (F.col("emp_logout_time").isNotNull()) &
	(F.col("emp_logout_time") >= F.col("emp_login_time"))
)

# Write to Delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_dashboard.analytics_emp_login_logout (
    EMP_ID int,
    EMP_CONTRACTED_HOURS string,
    START_TIME string,
    END_TIME string,
    START_TIME_THRESHOLD string,
    END_TIME_THRESHOLD string,
    EMP_LOGIN_TIME string,
    EMP_LOGOUT_TIME string,
    SHIFT_DATE date,
    SHIFT_COMPLETED string,
    ATTENDENCE_STATUS string,
    LOGIN_STATUS string,
    LOGOUT_STATUS string,
    WORKING_HOURS float,
    UPDATED_ON TIMESTAMP,
    IS_WEEK_OFF BOOLEAN GENERATED ALWAYS AS (
        CASE WHEN unix_timestamp(END_TIME) - unix_timestamp(START_TIME) <= 0 THEN TRUE ELSE FALSE END
    ),
    IS_HOLIDAY BOOLEAN
) USING DELTA
""")

spark.sql("""ALTER TABLE gold_dashboard.analytics_emp_login_logout ALTER COLUMN UPDATED_ON SET DEFAULT current_timestamp()""")
spark.sql("""ALTER TABLE gold_dashboard.analytics_emp_login_logout ALTER COLUMN IS_HOLIDAY SET DEFAULT FALSE""")

# Merge data into target table
filtered_login_logout.createOrReplaceTempView("temp_filtered_login_logout")
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_filtered_login_logout AS source
ON target.EMP_ID = source.emp_id AND target.SHIFT_DATE = source.cal_date
WHEN MATCHED AND target.EMP_LOGIN_TIME IS NOT NULL THEN
    UPDATE SET 
        target.EMP_LOGIN_TIME = CASE 
            WHEN source.emp_login_time < target.EMP_LOGIN_TIME THEN source.emp_login_time 
            ELSE target.EMP_LOGIN_TIME 
        END,
        target.EMP_LOGOUT_TIME = source.emp_logout_time,
        target.START_TIME = source.shift_start_time,
        target.END_TIME = source.shift_end_time
WHEN NOT MATCHED THEN
    INSERT (EMP_ID, START_TIME, END_TIME, EMP_LOGIN_TIME, EMP_LOGOUT_TIME, SHIFT_DATE)
    VALUES (source.emp_id, source.shift_start_time, source.shift_end_time,
            source.emp_login_time, source.emp_logout_time, source.cal_date)
""")

# Update previous day's logout times if needed
prev_day_updates.createOrReplaceTempView("temp_prev_day_updates")
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_prev_day_updates AS source
ON target.EMP_ID = source.update_emp_id AND target.SHIFT_DATE = source.update_date
WHEN MATCHED THEN 
    UPDATE SET target.EMP_LOGOUT_TIME = source.new_logout_time
""")





##&#&#&&#&#&#&#&#&&£&#&#&#&#&&£&#&&£&£&£&£&&£&£&£&£&&£&£&£&£&&£&£&£


login logout modify. for proper login and weekoff changes.



from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window

# -------------------------
# Configurations
# -------------------------
dbutils.widgets.text("dynamic_hours", "24000")
try:
    dynamic_hours = int(dbutils.widgets.get("dynamic_hours"))
except Exception as e:
    print(f"Error parsing dynamic_hours: {e}, using default 1")
    dynamic_hours = 24000

DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 60 * dynamic_hours
print(f"Processing window: {PROCESSING_WINDOW_MINUTES} minutes")

# -------------------------
# Time Setup
# -------------------------
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# -------------------------
# Step 1: Recent activity and distinct emp_id, cal_date
# -------------------------
act_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold))

# Device activities (for logouts)
mouse_df = spark.table("sys_trace.emp_mousedata") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date", "event_time")

keyboard_df = spark.table("sys_trace.emp_keyboarddata") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date", "event_time")

mousekey_df = mouse_df.union(keyboard_df)

activities_df = act_df.select("emp_id", "cal_date").distinct()

# -------------------------
# Add previous date
# -------------------------
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# -------------------------
# Step 2: Load shift data
# -------------------------
shift_df = spark.table("inbound.pulse_emp_shift_info") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# -------------------------
# Step 3: Join current shift
# -------------------------
cur_shift = emp_dates_df.join(
    shift_df,
    (emp_dates_df.emp_id == shift_df.emp_id) & (emp_dates_df.cal_date == shift_df.shift_date),
    how='left'
).select(
    emp_dates_df.emp_id,
    emp_dates_df.cal_date,
    emp_dates_df.prev_date,
    shift_df.start_time.alias("cur_start_time_raw"),
    shift_df.end_time.alias("cur_end_time_raw"),
    shift_df.is_week_off
)

# Step 4: Join previous shift
prev_shift = shift_df \
    .withColumnRenamed("shift_date", "prev_cal_date") \
    .withColumnRenamed("start_time", "prev_start_time_raw") \
    .withColumnRenamed("end_time", "prev_end_time_raw") \
    .withColumnRenamed("emp_id", "emp_id_prev") \
    .withColumnRenamed("is_week_off", "prev_is_week_off")

cur_with_prev = cur_shift.join(
    prev_shift,
    (cur_shift.emp_id == prev_shift.emp_id_prev) & (cur_shift.prev_date == prev_shift.prev_cal_date),
    how="left"
).drop("emp_id_prev", "prev_cal_date")

# Step 5: Apply defaults based on missing data and day of week
final_df = cur_with_prev \
    .withColumn("dow", F.date_format("cal_date", "E")) \
    .withColumn("prev_dow", F.date_format("prev_date", "E")) \
    .withColumn("cur_start_time",
        F.when(F.col("cur_start_time_raw").isNotNull(), F.col("cur_start_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_end_time_raw").isNotNull(), F.col("cur_end_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    .withColumn("prev_start_time",
        F.when(F.col("prev_start_time_raw").isNotNull(), F.col("prev_start_time_raw"))
         .when(F.col("prev_is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_end_time_raw").isNotNull(), F.col("prev_end_time_raw"))
         .when(F.col("prev_is_week_off") == True, F.lit(ZERO_TIME))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("cast(cur_end_time as timestamp) + interval 1 day"), "HH:mm:ss")
        ).otherwise(F.col("cur_end_time"))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("cast(prev_end_time as timestamp) + interval 1 day"), "HH:mm:ss")
        ).otherwise(F.col("prev_end_time"))
    ) \
    .withColumn("cur_start_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_start_time"))) \
    .withColumn("cur_end_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_end_time"))) \
    .withColumn("prev_start_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_start_time"))) \
    .withColumn("prev_end_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_end_time"))) \
    .withColumn("cur_end_time_ts",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("cast(cur_end_time_ts as timestamp) + interval 1 day"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("cur_end_time_ts"))
    ) \
    .withColumn("prev_end_time_ts",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("cast(prev_end_time_ts as timestamp) + interval 1 day"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("prev_end_time_ts"))
    ) \
    .withColumn("is_week_off",
        F.when(
            (F.col("cur_start_time") == ZERO_TIME) & (F.col("cur_end_time") == ZERO_TIME),
            F.lit(True)
        ).otherwise(F.lit(False))
    ) \
    .withColumn("prev_is_week_off",
        F.when(
            (F.col("prev_start_time") == ZERO_TIME) & (F.col("prev_end_time") == ZERO_TIME),
            F.lit(True)
        ).otherwise(F.lit(False))
    ) \
    .select(
        "emp_id",
        "cal_date",
        "prev_date",
        "cur_start_time_ts",
        "cur_end_time_ts",
        "prev_start_time_ts",
        "prev_end_time_ts",
        "is_week_off",
        "prev_is_week_off"
    )

# 6. Join with activities
joined_df = act_df.join(
    final_df,
    ["emp_id", "cal_date"],
    "left"
)
joined_mkdf = mousekey_df.join(
    final_df,
    ["emp_id", "cal_date"],
    "left"
)

# 7. Define time windows with special handling for week-offs

jdf_with_windows = joined_df.withColumn(
    "current_window_start",
    F.when(
    (F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
    greatest(
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours"),  # previous day's extended shift end
        F.col("cur_start_time_ts")  # current day shift start (likely 00:00:00 on week off)
    )
).when(
    F.col("is_week_off") == True,
    F.col("cur_start_time_ts")  # consecutive week offs
).otherwise(
    F.expr("cast(cur_start_time_ts as timestamp) - interval 4 hours")  # normal working day
)

).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("cast(cur_end_time_ts as timestamp) + interval 8 hours"))
	.when(
	(F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
	F.concat_ws(" ", F.col("cal_date"),  lit("23:59:59"))
	) 
    .otherwise(F.col("cur_end_time_ts"))
).withColumn(
    "prev_window_start",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.col("prev_end_time_ts")
    ).otherwise(F.col("prev_start_time_ts"))
).withColumn(
    "prev_window_end",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours")
    ).otherwise(F.col("prev_end_time_ts"))
)





jmkdf_with_windows = joined_mkdf.withColumn(
    "current_window_start",
    F.when(
    (F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
    greatest(
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours"), 
        F.col("cur_start_time_ts")  # current day shift start (likely 00:00:00 on week off)
			)
	).when(
		F.col("is_week_off") == True,
		F.col("cur_start_time_ts")  # consecutive week offs
	).otherwise(
		F.expr("cast(cur_start_time_ts as timestamp) - interval 4 hours") )
).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("cast(cur_end_time_ts as timestamp) + interval 8 hours"))
	.when(
	(F.col("is_week_off") == True) & (F.col("prev_is_week_off") == False),
	F.concat_ws(" ", F.col("cal_date"),  lit("23:59:59"))
	) 
    .otherwise(F.col("cur_end_time_ts"))
).withColumn(
    "prev_window_start",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.col("prev_end_time_ts")
		).otherwise(F.col("prev_start_time_ts"))
).withColumn(
    "prev_window_end",
    F.when(
        (F.col("prev_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.expr("cast(prev_end_time_ts as timestamp) + interval 8 hours")
    ).otherwise(F.col("prev_end_time_ts"))
)

# 8. Classify activities with priority to current day
classified_df = jdf_with_windows.withColumn(
    "is_current_login",
    ~F.col("is_week_off") &
    (F.col("start_time") >= F.col("current_window_start")) &
    (F.col("start_time") <= F.col("current_window_end")) &
    (F.col("app_name") != "WindowLock")
).withColumn(
    "is_prev_logout",
    F.col("prev_window_start").isNotNull() &
    (F.col("start_time") >= F.col("prev_window_start")) &
    (F.col("start_time") <= F.col("prev_window_end")) &
    (F.col("start_time") < F.col("current_window_start"))
).withColumn(
    "is_week_off_activity",
    F.col("is_week_off") &
    (F.col("prev_window_start").isNull() | 
     (F.col("start_time") > F.col("prev_window_end")))
)

classified_mkdf = jmkdf_with_windows.withColumn(
    "is_current_login",
    ~F.col("is_week_off") &
    (F.col("event_time") >= F.col("current_window_start")) &
    (F.col("event_time") <= F.col("current_window_end"))
).withColumn(
    "is_prev_logout",
    F.col("prev_window_start").isNotNull() &
    (F.col("event_time") >= F.col("prev_window_start")) &
    (F.col("event_time") <= F.col("prev_window_end")) &
    (F.col("event_time") < F.col("current_window_start"))
).withColumn(
    "is_week_off_activity",
    F.col("is_week_off") &
    (F.col("prev_window_start").isNull() | 
     (F.col("event_time") > F.col("prev_window_end")))
)

# ---------------------------------------------------
# Step 9. Calculate all potential times
# ---------------------------------------------------
result_df = classified_df.groupBy("emp_id", "cal_date").agg(
    # Current login: Minimum start_time within current window and app_name != "Window Lock"
    F.min(F.when(
        (F.col("is_current_login")) & (F.col("app_name") != "WindowLock"),
        F.col("start_time")
    )).alias("current_login"),
    # Last activity: Maximum start_time within current window
    F.max(F.when(
        (F.col("start_time") >= F.col("current_window_start")) &
        (F.col("start_time") <= F.col("current_window_end")),
        F.col("start_time")
    )).alias("last_activity"),
    # Previous logout candidate (normal): Max start_time in previous window
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("start_time") >= F.col("prev_window_start")) &
        (F.col("start_time") < F.col("prev_window_end")) &
        (F.col("start_time") < F.col("current_window_start")) &
        (~F.col("is_week_off_activity")),
        F.col("start_time")
    )).alias("prev_logout_update_norm"),
    # Previous logout candidate (week-off): Max start_time in prev window if week_off_activity
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("start_time") > F.col("prev_window_start")) &
        (F.col("start_time") < F.col("prev_window_end")) &
        (F.col("is_week_off_activity")),
        F.col("start_time")
    )).alias("prev_logout_update_weekoff"),
    # Week-off login: Min start_time if week_off_activity
    F.min(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("app_name") != "WindowLock") &
        (F.col("start_time") > F.col("prev_window_end")) &
        (F.col("start_time") > F.col("current_window_start")),
        F.col("start_time")
    )).alias("week_off_login"),
    # Week-off logout: Max start_time if week_off_activity
    F.max(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("start_time") > F.col("prev_window_end")),
        F.col("start_time")
    )).alias("week_off_logout"),
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
).withColumn(
    "prev_logout_update",
    F.coalesce(F.col("prev_logout_update_norm"), F.col("prev_logout_update_weekoff"))
)

# Mouse/keyboard side
result_mkdf = classified_mkdf.groupBy("emp_id", "cal_date").agg(
    F.min(F.when(F.col("is_current_login") &
                 (F.col("event_time") >= F.col("current_window_start")) &
                 (F.col("event_time") <= F.col("current_window_end")),
                 F.col("event_time"))).alias("current_login"),
    F.max(F.when(
        (F.col("event_time") >= F.col("current_window_start")) &
        (F.col("event_time") <= F.col("current_window_end")),
        F.col("event_time")
    )).alias("last_mk_activity"),
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("event_time") >= F.col("prev_window_start")) &
        (F.col("event_time") < F.col("prev_window_end")) &
        (~F.col("is_week_off_activity")),
        F.col("event_time")
    )).alias("prev_logout_update_norm"),
    # Previous logout candidate (week-off): Max event_time in prev window if week_off_activity
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) &
        (F.col("event_time") > F.col("prev_window_start")) &
        (F.col("event_time") < F.col("prev_window_end")) &
        (F.col("is_week_off_activity")),
        F.col("event_time")
    )).alias("prev_logout_update_weekoff"),
    F.min(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("event_time") > F.col("prev_window_end")) &
        (F.col("event_time") > F.col("current_window_start")),
        F.col("event_time")
    )).alias("week_off_login"),
    F.max(F.when(
        (F.col("is_week_off_activity")) &
        (F.col("event_time") > F.col("prev_window_end")),
        F.col("event_time")
    )).alias("week_off_mk_logout"),
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
)

result_mkdf = result_mkdf.withColumn(
    "prev_logout_mk_update",
    F.coalesce(F.col("prev_logout_update_norm"), F.col("prev_logout_update_weekoff"))
)

# ---------------------------------------------------
# Step 10. Determine final times with priority rules
# ---------------------------------------------------
final_result = result_df.withColumn(
    "emp_login_time",
    F.when(
        F.col("is_week_off"),
        F.coalesce(F.col("week_off_login"), F.col("last_activity"))
    ).otherwise(
        F.coalesce(F.col("current_login"), F.col("last_activity"))
    )
).withColumn(
    "emp_logout_time",
    F.when(
        F.col("is_week_off"),
        F.coalesce(F.col("week_off_logout"), F.col("last_activity"))
    ).otherwise(
        F.coalesce(F.col("last_activity"), F.col("current_login"))
    )
)

final_mkresult = result_mkdf.withColumn(
    "emp_logout_time_mk",
    F.when(
        F.col("is_week_off"),
        F.coalesce(F.col("week_off_mk_logout"), F.col("last_mk_activity"))
    ).otherwise(
        F.coalesce(F.col("last_mk_activity"), F.col("current_login"))
    )
)

# Join MK logout with main result
final_result_with_mk = final_result.join(
    final_mkresult.select("emp_id", "cal_date", "emp_logout_time_mk", "prev_logout_mk_update"),
    ["emp_id", "cal_date"],
    "left"
)

# ---------------------------------------------------
# Step 11. Generate previous day updates (both sources)
# ---------------------------------------------------
prev_day_updates = final_result_with_mk.filter(
    F.col("prev_logout_update").isNotNull() | F.col("prev_logout_mk_update").isNotNull()
).select(
    F.col("emp_id").alias("update_emp_id"),
    F.col("prev_cal_date").alias("update_date"),
    F.coalesce(F.col("prev_logout_mk_update"), F.col("prev_logout_update")).alias("new_logout_time")
)

# Final output: prefer MK logout if available
final_output = final_result_with_mk.select(
    "emp_id",
    "cal_date",
    F.coalesce(F.col("emp_logout_time_mk"), F.col("emp_logout_time")).alias("emp_logout_time"),
    "emp_login_time",
    "shift_start_time",
    "shift_end_time",
    "is_week_off"
).orderBy("emp_id", "cal_date")

# Filter out null records
filtered_login_logout = final_output.filter(
    (F.col("emp_id").isNotNull()) &
    (F.col("cal_date").isNotNull()) &
    (F.col("emp_login_time").isNotNull()) &
    (F.col("emp_logout_time").isNotNull()) &
	(F.col("emp_logout_time") >= F.col("emp_login_time"))
)

# Write to Delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_dashboard.analytics_emp_login_logout (
    EMP_ID int,
    EMP_CONTRACTED_HOURS string,
    START_TIME string,
    END_TIME string,
    START_TIME_THRESHOLD string,
    END_TIME_THRESHOLD string,
    EMP_LOGIN_TIME string,
    EMP_LOGOUT_TIME string,
    SHIFT_DATE date,
    SHIFT_COMPLETED string,
    ATTENDENCE_STATUS string,
    LOGIN_STATUS string,
    LOGOUT_STATUS string,
    WORKING_HOURS float,
    UPDATED_ON TIMESTAMP,
    IS_WEEK_OFF BOOLEAN GENERATED ALWAYS AS (
        CASE WHEN unix_timestamp(END_TIME) - unix_timestamp(START_TIME) <= 0 THEN TRUE ELSE FALSE END
    ),
    IS_HOLIDAY BOOLEAN
) USING DELTA
""")

spark.sql("""ALTER TABLE gold_dashboard.analytics_emp_login_logout ALTER COLUMN UPDATED_ON SET DEFAULT current_timestamp()""")
spark.sql("""ALTER TABLE gold_dashboard.analytics_emp_login_logout ALTER COLUMN IS_HOLIDAY SET DEFAULT FALSE""")

# Merge data into target table
filtered_login_logout.createOrReplaceTempView("temp_filtered_login_logout")
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_filtered_login_logout AS source
ON target.EMP_ID = source.emp_id AND target.SHIFT_DATE = source.cal_date
WHEN MATCHED AND target.EMP_LOGIN_TIME IS NOT NULL THEN
    UPDATE SET 
        target.EMP_LOGIN_TIME = CASE 
            WHEN source.emp_login_time < target.EMP_LOGIN_TIME THEN source.emp_login_time 
            ELSE target.EMP_LOGIN_TIME 
        END,
        target.EMP_LOGOUT_TIME = source.emp_logout_time,
        target.START_TIME = source.shift_start_time,
        target.END_TIME = source.shift_end_time
WHEN NOT MATCHED THEN
    INSERT (EMP_ID, START_TIME, END_TIME, EMP_LOGIN_TIME, EMP_LOGOUT_TIME, SHIFT_DATE)
    VALUES (source.emp_id, source.shift_start_time, source.shift_end_time,
            source.emp_login_time, source.emp_logout_time, source.cal_date)
""")

# Update previous day's logout times if needed
prev_day_updates.createOrReplaceTempView("temp_prev_day_updates")
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_prev_day_updates AS source
ON target.EMP_ID = source.update_emp_id AND target.SHIFT_DATE = source.update_date
WHEN MATCHED THEN 
    UPDATE SET target.EMP_LOGOUT_TIME = source.new_logout_time
""")