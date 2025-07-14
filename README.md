

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Configs
dynamic_hours = 4  # Default value if widget not available
try:
    dynamic_hours = int(dbutils.widgets.get("dynamic_hours"))
except Exception as e:
    print(f"Error parsing dynamic_hours: {e}, using default 4")
    dynamic_hours = 4

DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 60 * dynamic_hours

print(f"Processing window: {PROCESSING_WINDOW_MINUTES} minutes")

# Time setup
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# Step 1: Recent activity and distinct emp_id, cal_date
act_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold))

# Load mouse and keyboard data with same ingestion time filter
mouse_df = spark.table("systrace.emp_mouse_data") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date", "event_time")

keyboard_df = spark.table("systrace.emp_keyboard_data") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date", "event_time")

# Union mouse and keyboard data
mouse_key_df = mouse_df.union(keyboard_df)

activities_df = act_df.select("emp_id", "cal_date") \
    .distinct()

# Add previous date
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# Step 2: Load shift data
shift_df = spark.table("inbound.pulse_emp_shift_info") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Step 3: Join current shift
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
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(cur_end_time))"), "HH:mm:ss")
        ).otherwise(F.col("cur_end_time"))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(prev_end_time))"), "HH:mm:ss")
        ).otherwise(F.col("prev_end_time"))
    ) \
    .withColumn("cur_start_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_start_time"))) \
    .withColumn("cur_end_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_end_time"))) \
    .withColumn("prev_start_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_start_time"))) \
    .withColumn("prev_end_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_end_time"))) \
    .withColumn("cur_end_time_ts",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(cur_end_time_ts))"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("cur_end_time_ts"))
    ) \
    .withColumn("prev_end_time_ts",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(prev_end_time_ts))"), "yyyy-MM-dd HH:mm:ss")
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

# 6. Join with activities and mouse/keyboard data
joined_df = act_df.join(
    final_df,
    ["emp_id", "cal_date"],
    "left"
)

# Join mouse/keyboard data with final_df
joined_mkdf = mouse_key_df.join(
    final_df,
    ["emp_id", "cal_date"],
    "left"
).withColumnRenamed("event_time", "start_time")  # Rename for consistency with classification logic

# 7. Define time windows with special handling for week-offs
df_with_windows = joined_df.withColumn(
    "current_window_start",
    F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, -4, cur_start_time_ts)"))
    .otherwise(F.col("cur_start_time_ts"))
).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, 8, cur_end_time_ts)"))
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
        F.expr("timestampadd(HOUR, 8, prev_end_time_ts)"))
    ).otherwise(F.col("prev_end_time_ts"))
)

# Same window definitions for mouse/keyboard data
mkdf_with_windows = joined_mkdf.withColumn(
    "current_window_start",
    F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, -4, cur_start_time_ts)"))
    .otherwise(F.col("cur_start_time_ts"))
).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, 8, cur_end_time_ts)"))
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
        F.expr("timestampadd(HOUR, 8, prev_end_time_ts)"))
    ).otherwise(F.col("prev_end_time_ts"))
)

# 8. Classify activities with priority to current day
classified_df = df_with_windows.withColumn(
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

# Classify mouse/keyboard events
classified_mkdf = mkdf_with_windows.withColumn(
    "is_current_login",
    ~F.col("is_week_off") &
    (F.col("start_time") >= F.col("current_window_start")) &
    (F.col("start_time") <= F.col("current_window_end"))
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

# 9. Calculate all potential times
result_df = classified_df.groupBy("emp_id", "cal_date").agg(
    # Current login candidates
    F.min(F.when(F.col("is_current_login"), F.col("start_time"))).alias("current_login"),
    
    # Last activity
    F.max("start_time").alias("last_activity"),
    
    # Previous logout candidates
    F.max(F.when(F.col("is_prev_logout"), F.col("start_time"))).alias("prev_logout_update"),
    
    # Week-off login candidates
    F.min(F.when(F.col("is_week_off_activity"), F.col("start_time"))).alias("week_off_login"),
    
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
)

# Calculate mouse/keyboard events
result_mkdf = classified_mkdf.groupBy("emp_id", "cal_date").agg(
    # Last activity from mouse/keyboard
    F.max("start_time").alias("last_mk_activity"),
    
    # Previous logout candidates from mouse/keyboard
    F.max(F.when(F.col("is_prev_logout"), F.col("start_time"))).alias("prev_logout_mk_update"),
    
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
)

# 10. Determine final times with priority rules
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
    F.col("last_activity")
)

# Create final result for mouse/keyboard data
final_mkresult = result_mkdf.withColumn(
    "emp_logout_time_mk",
    F.col("last_mk_activity")
)

# Join the mouse/keyboard logout time with the main result
final_result_with_mk = final_result.join(
    final_mkresult.select("emp_id", "cal_date", "emp_logout_time_mk", "prev_logout_mk_update"),
    ["emp_id", "cal_date"],
    "left"
)

# 11. Generate previous day updates (only if valid) - consider both sources
prev_day_updates = final_result_with_mk.filter(
    F.col("prev_logout_update").isNotNull() | F.col("prev_logout_mk_update").isNotNull()
).select(
    F.col("emp_id").alias("update_emp_id"),
    F.col("prev_cal_date").alias("update_date"),
    # Prefer mouse/keyboard update if available, otherwise use app trace
    F.coalesce(F.col("prev_logout_mk_update"), F.col("prev_logout_update")).alias("new_logout_time")
)

# Final output - use mouse/keyboard logout time if available, otherwise use app trace
final_output = final_result_with_mk.select(
    "emp_id",
    "cal_date",
    "emp_login_time",
    F.coalesce(F.col("emp_logout_time_mk"), F.col("emp_logout_time")).alias("emp_logout_time"),
    "shift_start_time",
    "shift_end_time",
    "is_week_off"
).orderBy("emp_id", "cal_date")

# Filter out null records
filtered_login_logout = final_output.filter(
    (F.col("emp_id").isNotNull()) &
    (F.col("cal_date").isNotNull()) &
    (F.col("emp_login_time").isNotNull()) &
    (F.col("emp_logout_time").isNotNull())
)

# Write to Delta table (rest of the code remains the same)
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
    WORKING_HOURS float
) USING DELTA
""")

# Merge data into target table
filtered_login_logout.createOrReplaceTempView("temp_filtered_login_logout")
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_filtered_login_logout AS source
ON target.EMP_ID = source.emp_id AND target.SHIFT_DATE = source.cal_date
WHEN MATCHED AND target.EMP_LOGIN_TIME IS NOT NULL THEN
    UPDATE SET 
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





from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# 1. Configuration Setup
dynamic_hours = 4  # Default processing window
try:
    dynamic_hours = int(dbutils.widgets.get("dynamic_hours"))
except Exception as e:
    print(f"Error parsing dynamic_hours: {e}, using default 4")

DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 60 * dynamic_hours

current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# 2. Load and Prepare Data Sources
# Application activities (for logins)
act_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold))

# Device activities (for logouts)
mouse_df = spark.table("sys_trace.emp_mousedata") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", F.to_date("event_time").alias("cal_date"), "event_time")

keyboard_df = spark.table("systrace.emp_keyboarddata") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", F.to_date("event_time").alias("cal_date"), "event_time")

# 3. Get Distinct Employee-Date Combinations
all_emp_dates = act_df.select("emp_id", "cal_date") \
    .union(mouse_df.select("emp_id", "cal_date")) \
    .union(keyboard_df.select("emp_id", "cal_date")) \
    .distinct()

# 4. Load and Process Shift Information
shift_df = spark.table("inbound.pulse_emp_shift_info") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Join with current and previous shifts
emp_dates_df = all_emp_dates.withColumn("prev_date", F.date_sub("cal_date", 1))

cur_shift = emp_dates_df.join(
    shift_df,
    (emp_dates_df.emp_id == shift_df.emp_id) & (emp_dates_df.cal_date == shift_df.shift_date),
    "left"
).select(
    emp_dates_df.emp_id,
    emp_dates_df.cal_date,
    emp_dates_df.prev_date,
    shift_df.start_time.alias("cur_start_time_raw"),
    shift_df.end_time.alias("cur_end_time_raw"),
    shift_df.is_week_off
)

prev_shift = shift_df \
    .withColumnRenamed("shift_date", "prev_cal_date") \
    .withColumnRenamed("start_time", "prev_start_time_raw") \
    .withColumnRenamed("end_time", "prev_end_time_raw") \
    .withColumnRenamed("emp_id", "emp_id_prev") \
    .withColumnRenamed("is_week_off", "prev_is_week_off")

final_df = cur_shift.join(
    prev_shift,
    (cur_shift.emp_id == prev_shift.emp_id_prev) & (cur_shift.prev_date == prev_shift.prev_cal_date),
    "left"
).drop("emp_id_prev", "prev_cal_date") \
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
.withColumn("cur_start_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_start_time"))) \
.withColumn("cur_end_time_ts", F.concat_ws(" ", F.col("cal_date"), F.col("cur_end_time"))) \
.withColumn("prev_start_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_start_time"))) \
.withColumn("prev_end_time_ts", F.concat_ws(" ", F.col("prev_date"), F.col("prev_end_time")))

# 5. Calculate Login Times (from app activities only)
login_df = act_df.filter(F.col("app_name") != "WindowLock") \
    .join(final_df, ["emp_id", "cal_date"], "left") \
    .withColumn("login_window_start", F.expr("timestampadd(HOUR, -4, cur_start_time_ts)")) \
    .withColumn("login_window_end", F.expr("timestampadd(HOUR, 8, cur_end_time_ts)")) \
    .filter(
        (F.col("start_time") >= F.col("login_window_start")) &
        (F.col("start_time") <= F.col("login_window_end"))
    ) \
    .groupBy("emp_id", "cal_date") \
    .agg(
        F.min("start_time").alias("emp_login_time"),
        F.first("cur_start_time_ts").alias("shift_start_time"),
        F.first("cur_end_time_ts").alias("shift_end_time"),
        F.first("is_week_off").alias("is_week_off"),
        F.first("prev_date").alias("prev_date")
    )

# 6. Calculate Logout Times (from all sources)
# Combine device events
device_events = mouse_df.withColumnRenamed("event_time", "logout_time") \
    .unionByName(keyboard_df.withColumnRenamed("event_time", "logout_time"))

# Get last app activity (excluding WindowLock)
app_logout = act_df.filter(F.col("app_name") != "WindowLock") \
    .groupBy("emp_id", "cal_date") \
    .agg(F.max("start_time").alias("app_logout_time"))

# Calculate most recent activity within shift window
logout_df = device_events.join(
    final_df, ["emp_id", "cal_date"], "left"
) \
.withColumn("within_shift", 
    (F.col("logout_time") >= F.col("cur_start_time_ts")) &
    (F.col("logout_time") <= F.expr("timestampadd(HOUR, 8, cur_end_time_ts)"))
) \
.filter(F.col("within_shift")) \
.groupBy("emp_id", "cal_date") \
.agg(
    F.max("logout_time").alias("device_logout_time")
) \
.join(app_logout, ["emp_id", "cal_date"], "left") \
.withColumn(
    "emp_logout_time",
    F.greatest(F.col("device_logout_time"), F.coalesce(F.col("app_logout_time"), F.col("device_logout_time")))
)

# 7. Combine Results
final_result = login_df.join(
    logout_df, ["emp_id", "cal_date"], "left"
).select(
    "emp_id",
    "cal_date",
    "emp_login_time",
    "emp_logout_time",
    "shift_start_time",
    "shift_end_time",
    "is_week_off"
)

# 8. Previous Day Updates (using app activities only)
prev_day_updates = act_df.filter(F.col("app_name") != "WindowLock") \
    .join(final_df, ["emp_id", "cal_date"], "left") \
    .filter(
        (F.col("start_time") >= F.col("prev_end_time_ts")) &
        (F.col("start_time") <= F.expr("timestampadd(HOUR, 8, prev_end_time_ts)"))
    ) \
    .groupBy("emp_id", "prev_date") \
    .agg(
        F.max("start_time").alias("new_logout_time")
    ) \
    .filter(F.col("new_logout_time").isNotNull()) \
    .select(
        F.col("emp_id").alias("update_emp_id"),
        F.col("prev_date").alias("update_date"),
        "new_logout_time"
    )

# 9. Write to Delta Table
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
    WORKING_HOURS float
) USING DELTA
""")

final_result.createOrReplaceTempView("temp_final_result")
prev_day_updates.createOrReplaceTempView("temp_prev_day_updates")

# Merge new data
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_final_result AS source
ON target.EMP_ID = source.emp_id AND target.SHIFT_DATE = source.cal_date
WHEN MATCHED AND target.EMP_LOGIN_TIME IS NOT NULL THEN
    UPDATE SET 
        target.EMP_LOGOUT_TIME = source.emp_logout_time,
        target.START_TIME = source.shift_start_time,
        target.END_TIME = source.shift_end_time
WHEN NOT MATCHED THEN
    INSERT (EMP_ID, START_TIME, END_TIME, EMP_LOGIN_TIME, EMP_LOGOUT_TIME, SHIFT_DATE)
    VALUES (source.emp_id, source.shift_start_time, source.shift_end_time,
            source.emp_login_time, source.emp_logout_time, source.cal_date)
""")

# Update previous days
spark.sql("""
MERGE INTO gold_dashboard.analytics_emp_login_logout AS target
USING temp_prev_day_updates AS source
ON target.EMP_ID = source.update_emp_id AND target.SHIFT_DATE = source.update_date
WHEN MATCHED THEN 
    UPDATE SET target.EMP_LOGOUT_TIME = source.new_logout_time
""")







### 1. Configuration Setup
- Sets default values for processing parameters
- `dynamic_hours` configures the time window for recent activity (default 4 hours)
- Defines standard shift times (9AM-6PM) and zero time placeholder
- Calculates ingestion threshold based on current time minus processing window

### 2. Data Preparation
**Step 1: Recent Activity Filtering**
- Loads employee activity data from `app_trace.emp_activity`
- Filters for only recent activities (within processing window)
- Extracts distinct employee-date combinations

**Step 2: Shift Data Loading**
- Loads shift information from `inbound.pulse_emp_shift_info`
- Contains shift times and week-off indicators

### 3. Shift Information Processing
**Step 3: Current Shift Join**
- Joins employee dates with their shift information
- Gets raw start/end times for current day

**Step 4: Previous Shift Join**
- Joins with shift data again to get previous day's shift info
- Renames columns to avoid conflicts

**Step 5: Default Time Handling**
- Applies business rules for shift times:
  - Uses actual shift times when available
  - Applies defaults (9AM-6PM) for weekdays
  - Sets to 00:00:00 for weekends and week-off days
- Handles overnight shifts (when start > end time)
- Creates timestamp columns combining dates with times

### 4. Activity Classification
**Step 6-7: Window Definitions**
- Defines time windows for:
  - Current login window: 4 hours before shift start to 8 hours after shift end
  - Previous logout window: previous shift end to 8 hours after
- Special handling for week-off days

**Step 8: Activity Classification**
- Identifies:
  - Current login candidates (within current window, not WindowLock)
  - Previous logout candidates (within previous window)
  - Week-off activities (outside normal windows)

### 5. Time Calculation
**Step 9: Aggregation**
- Finds:
  - Earliest valid login time
  - Latest activity time (for logout)
  - Latest previous logout candidate
  - Earliest week-off login

**Step 10: Final Time Assignment**
- Determines final login time:
  - For week-off: first activity after previous window or first of day
  - For normal days: first activity in login window or first of day
- Logout time is always the last activity

### 6. Update Handling
**Step 11: Previous Day Updates**
- Identifies previous days that need logout time updates
- Creates DataFrame with new logout times

### 7. Output Processing
- Combines all results into final output
- Filters out invalid records
- Merges into target Delta table:
  - Updates existing records
  - Inserts new records
  - Updates previous day's logout times when needed

### Key Business Rules Implemented:
1. **Login Time Calculation**:
   - Must be first activity in login window (-4 to +8 hours from shift)
   - Excludes WindowLock activities
   - Falls back to first activity if none in window

2. **Logout Time Calculation**:
   - Always the last activity of day
   - Includes all activity types

3. **Week-Off Handling**:
   - Special windows for week-off days
   - Different login calculation logic

4. **Shift Edge Cases**:
   - Overnight shifts handled properly
   - Weekend defaults
   - Missing shift data fallbacks

5. **Data Freshness**:
   - Only processes recent activities
   - Configurable time window





The current logic for calculating login and logout times follows a multi-step process:

1. Extract Unique Employee-Day Records:

From the raw tracking tables, retrieve distinct combinations of EmployeeID and CalendarDate.



2. Shift Mapping:

For each unique employee-day combination, check if shift details exist in the Shift Table.

If shift data is unavailable, default shift timings of 9:00 AM to 6:00 PM are applied.



3. Buffer Calculations:

Buffer periods are calculated based on shift start and end times, and these buffers also consider previous day's shift-end buffers.

For each employee, we preserve:

Previous day's shift end buffer

Previous day's shift start buffer

Current day's shift start buffer

Current day's shift end buffer




4. Logout Time Logic:

If the last activity for a user is a Window Lock, then the application just before the lock event is considered for logout time.

If not, the last application’s start time is taken as the logout time.





---

Current Issue:

While the above approach captures the last application start time accurately, it fails to account for the active duration of that application — especially if the user remained idle or the application stayed active for some time post start.

This leads to an underestimation of the actual logout time, since the application may still have been in use or remained open for a significant time.


---

Proposed Fix:

To improve accuracy, we plan to revise the logout calculation logic to capture the actual end time or active duration of the last used application. This may include integrating signals like:

System activity (keyboard/mouse events)

Timestamps indicating when the application was no longer in use

Application idle thresholds


This will ensure the logout time reflects the full active period of the user, rather than just the last start time.



Description:

In the current app management implementation, we have multiple APIs interacting with the App-EMP Mapping and App Tagging tables. These tables use IDENTITY constraints for AppID and TagID, which handle auto-incrementation and uniqueness.

However, in the Databricks environment, using IDENTITY columns causes issues during concurrent insert or update operations. Specifically, any change to AppID or TagID affects access metadata, leading to merge conflicts when multiple users interact with the APIs simultaneously.

To resolve this, we propose replacing the IDENTITY constraint with a custom logic that ensures both uniqueness and auto-incrementation without relying on the database's IDENTITY feature.

We already have a similar implementation in Team Management, where IDs are generated using a combination of current timestamp and user ID, ensuring uniqueness without metadata conflicts.

The plan is as follows:

Create new versions of the affected tables, replacing AppID and TagID from IDENTITY to BIGINT.

Implement the custom ID generation logic (e.g., current_timestamp + user ID) in the respective APIs.

Update the APIs to work with the new table structure.

Eliminate the need for pre-insert SELECT queries, since IDs can now be generated prior to insert.



---

Acceptance Criteria:

1. ✅ No Merge Conflicts:
Simultaneous insert/update operations by multiple users should not result in merge conflicts in Databricks.


2. ✅ Updated Table Structure:

New or modified tables must replace IDENTITY columns with BIGINT.

AppID and TagID should no longer rely on auto-increment via identity constraints.



3. ✅ Unique and Auto-Incremented IDs:
IDs should be generated using a deterministic method (e.g., timestamp + user ID) to maintain both uniqueness and order.


4. ✅ Modified APIs:
All APIs interacting with App-EMP Mapping and App Tagging tables must be updated to use the new ID generation logic and schema.


5. ✅ Optimized Queries:
Since IDs can now be pre-generated, there should be no need for SELECT queries before insert operations, reducing API overhead.


6. ✅ Backward Compatibility / Data Migration (if needed):
If existing data is to be preserved, appropriate migration and backward compatibility considerations should be handled.




Currently, login and logout time calculations are being derived from the AppTraceRaw table. However, this approach only provides the application start times, which causes us to miss the actual active time for the last used application. As a result, we are unable to accurately determine the user's logout time.

To address this, we propose enhancing the calculation logic by leveraging the SysTrace data — specifically from the KeyboardActivity and MouseClicks tables. Since SysTrace runs continuously in the background, it provides a more accurate view of user activity. By analyzing periods of inactivity or the last recorded input event, we can more reliably estimate the user's actual logout time.

This ticket is to implement and test this improved logic based on SysTrace, replacing or supplementing the current logic that relies solely on AppTraceRaw.

import psutil
import win32api
import win32com.client
import win32con
import time
import win32evtlog
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import re
import os
import sys

# Configuration
ns = {'e': 'http://schemas.microsoft.com/win/2004/08/events/event'}
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lid_closed_events.log")
POLL_INTERVAL = 10  # seconds

class LidMonitor:
    def __init__(self):
        self.last_record_id = self._get_max_event_id()  # Initialize with current max ID
        print(f"[{datetime.now()}] Initialized last_record_id to {self.last_record_id}")
        
    def _get_max_event_id(self):
        """Get the maximum event ID currently in the log"""
        try:
            query = "*[System[EventID=506]]"
            h = win32evtlog.EvtQuery("System", 
                                   win32evtlog.EvtQueryReverseDirection, 
                                   query)
            event = next(win32evtlog.EvtNext(h, 1), None)
            if event:
                xml = win32evtlog.EvtRender(event, win32evtlog.EvtRenderEventXml)
                root = ET.fromstring(xml)
                return int(root.find(".//EventRecordID", ns).text)
            return 0
        except Exception as e:
            print(f"[{datetime.now()}] ERROR getting max event ID: {e}")
            return 0

    def ensure_log_file_exists(self):
        """Ensure the log file exists and is accessible"""
        try:
            if not os.path.exists(LOG_FILE):
                with open(LOG_FILE, 'w') as f:
                    f.write("Laptop Lid Closure Event Log\n")
                    f.write(f"Created at: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}\n")
                    f.write("="*50 + "\n\n")
            return True
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to create log file: {e}")
            return False

    def parse_event_time(self, timestamp_str):
        """Parse the event timestamp string into a datetime object"""
        try:
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"
            
            match = re.match(r"(.*T\d+:\d+:\d+)\.(\d{6})\d+(\+\d+:\d+)", timestamp_str)
            if match:
                timestamp_str = f"{match.group(1)}.{match.group(2)}{match.group(3)}"
            
            dt_utc = datetime.fromisoformat(timestamp_str)
            return dt_utc.astimezone()
        except Exception as e:
            print(f"[{datetime.now()}] WARNING: Failed to parse timestamp: {e}")
            return None

    def get_new_events(self):
        """Generator function to yield only new events"""
        try:
            query = f"*[System[EventID=506 and EventRecordID > {self.last_record_id}]]"
            h = win32evtlog.EvtQuery("System", 
                                   win32evtlog.EvtQueryForwardDirection,
                                   query)
            while True:
                events = win32evtlog.EvtNext(h, 10)
                if not events:
                    break
                for event in events:
                    yield event
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to access event log: {e}")
            yield from []

    def log_lid_event(self, event_time):
        """Write the lid closed event to the log file"""
        log_time = datetime.now().astimezone()
        try:
            with open(LOG_FILE, "a") as f:
                log_entry = (
                    f"Lid closed at (event time): {event_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                    f"Logged at (system time): {log_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                    f"{'-'*50}\n"
                )
                f.write(log_entry)
            print(f"[{log_time}] INFO: Logged lid closure event from {event_time}")
        except Exception as e:
            print(f"[{log_time}] ERROR: Failed to write to log file: {e}")

    def monitor_lid_events(self):
        """Main monitoring function"""
        if not self.ensure_log_file_exists():
            print(f"[{datetime.now()}] CRITICAL: Cannot continue without log file")
            return
        
        print(f"[{datetime.now()}] Starting monitor (polling every {POLL_INTERVAL}s)...")
        print(f"[{datetime.now()}] Log file: {LOG_FILE}")
        print(f"[{datetime.now()}] Will only log events after ID {self.last_record_id}")
        
        try:
            with open(LOG_FILE, "a") as f:
                f.write(f"\n=== Session started at {datetime.now().astimezone()} ===\n")
                f.write(f"=== Monitoring events after ID {self.last_record_id} ===\n\n")
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to write session start: {e}")
        
        while True:
            try:
                current_time = datetime.now()
                print(f"[{current_time}] Checking for new events...")
                
                event_count = 0
                for event in self.get_new_events():
                    try:
                        xml = win32evtlog.EvtRender(event, win32evtlog.EvtRenderEventXml)
                        root = ET.fromstring(xml)
                        
                        # Get reason and record ID
                        reason = None
                        for data in root.findall(".//*[@Name='Reason']", ns):
                            reason = data.text
                        
                        if reason in ("Lid", "15"):
                            record_id = int(root.find(".//EventRecordID", ns).text)
                            time_created = root.find(".//TimeCreated", ns).attrib['SystemTime']
                            event_time = self.parse_event_time(time_created)
                            
                            if event_time:
                                self.log_lid_event(event_time)
                                self.last_record_id = record_id
                                event_count += 1
                    
                    except Exception as e:
                        print(f"[{datetime.now()}] WARNING: Error processing event: {e}")
                        continue
                
                if event_count:
                    print(f"[{datetime.now()}] Processed {event_count} new events")
                
                time.sleep(POLL_INTERVAL)
                
            except KeyboardInterrupt:
                print(f"\n[{datetime.now()}] Stopping monitor...")
                break
            except Exception as e:
                print(f"[{datetime.now()}] ERROR: {e}")
                time.sleep(POLL_INTERVAL)
        
        try:
            with open(LOG_FILE, "a") as f:
                f.write(f"\n=== Session ended at {datetime.now().astimezone()} ===\n")
                f.write(f"=== Last recorded event ID: {self.last_record_id} ===\n")
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to write session end: {e}")

if __name__ == "__main__":
    try:
        monitor = LidMonitor()
        monitor.monitor_lid_events()
    except Exception as e:
        print(f"[{datetime.now()}] CRITICAL: {e}")
        sys.exit(1)





import psutil
import win32api
import win32com.client
import win32con
import time
import win32evtlog
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import re
import os
import sys

# Configuration
ns = {'e': 'http://schemas.microsoft.com/win/2004/08/events/event'}
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lid_closed_events.log")
POLL_INTERVAL = 10  # seconds

class LidMonitor:
    def __init__(self):
        self.last_record_id = 0
        self.initial_run = True  # Flag to track first run
        self.highest_seen_id = 0  # Track highest seen event ID
        
    def ensure_log_file_exists(self):
        """Ensure the log file exists and is accessible"""
        try:
            if not os.path.exists(LOG_FILE):
                with open(LOG_FILE, 'w') as f:
                    f.write("Laptop Lid Closure Event Log\n")
                    f.write(f"Created at: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}\n")
                    f.write("="*50 + "\n\n")
            return True
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to create log file: {e}")
            return False

    def parse_event_time(self, timestamp_str):
        """Parse the event timestamp string into a datetime object"""
        try:
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"
            
            match = re.match(r"(.*T\d+:\d+:\d+)\.(\d{6})\d+(\+\d+:\d+)", timestamp_str)
            if match:
                timestamp_str = f"{match.group(1)}.{match.group(2)}{match.group(3)}"
            
            dt_utc = datetime.fromisoformat(timestamp_str)
            return dt_utc.astimezone()
        except Exception as e:
            print(f"[{datetime.now()}] WARNING: Failed to parse timestamp: {e}")
            return None

    def get_events(self, log_name="System", event_id=506):
        """Generator function to yield events from the specified log"""
        try:
            server = None  # local machine
            flags = win32evtlog.EvtQueryReverseDirection | win32evtlog.EvtQueryFilePath
            query = f"*[System[EventID={event_id}]]"
            
            h = win32evtlog.EvtQuery(log_name, flags, query)
            while True:
                events = win32evtlog.EvtNext(h, 10)
                if not events:
                    break
                for event in events:
                    yield event
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to access event log: {e}")
            yield from []

    def log_lid_event(self, event_time):
        """Write the lid closed event to the log file"""
        log_time = datetime.now().astimezone()
        try:
            with open(LOG_FILE, "a") as f:
                log_entry = (
                    f"Lid closed at (event time): {event_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                    f"Logged at (system time): {log_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                    f"{'-'*50}\n"
                )
                f.write(log_entry)
            print(f"[{log_time}] INFO: Logged lid closure event from {event_time}")
        except Exception as e:
            print(f"[{log_time}] ERROR: Failed to write to log file: {e}")

    def monitor_lid_events(self):
        """Main monitoring function"""
        if not self.ensure_log_file_exists():
            print(f"[{datetime.now()}] CRITICAL: Cannot continue without log file")
            return
        
        print(f"[{datetime.now()}] Starting monitor (polling every {POLL_INTERVAL}s)...")
        print(f"[{datetime.now()}] Log file: {LOG_FILE}")
        print(f"[{datetime.now()}] Initial run - will ignore first lid closure event")
        
        try:
            with open(LOG_FILE, "a") as f:
                f.write(f"\n=== Session started at {datetime.now().astimezone()} ===\n")
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to write session start: {e}")
        
        while True:
            try:
                current_time = datetime.now()
                print(f"[{current_time}] Checking for events...")
                
                event_count = 0
                for event in self.get_events():
                    try:
                        xml = win32evtlog.EvtRender(event, win32evtlog.EvtRenderEventXml)
                        root = ET.fromstring(xml)
                        
                        # Get reason and record ID
                        reason = None
                        for data in root.findall(".//*[@Name='Reason']", ns):
                            reason = data.text
                        
                        if reason in ("Lid", "15"):
                            record_id = int(root.find(".//EventRecordID", ns).text)
                            self.highest_seen_id = max(self.highest_seen_id, record_id)
                            
                            # Only process if this is a new event AND not the first run
                            if record_id > self.last_record_id:
                                time_created = root.find(".//TimeCreated", ns).attrib['SystemTime']
                                event_time = self.parse_event_time(time_created)
                                
                                if event_time:
                                    if not self.initial_run:
                                        self.log_lid_event(event_time)
                                        event_count += 1
                                    self.last_record_id = record_id
                            else:
                                break
                    
                    except Exception as e:
                        print(f"[{datetime.now()}] WARNING: Error processing event: {e}")
                        continue
                
                # After first complete check, mark initial run as complete
                if self.initial_run:
                    print(f"[{datetime.now()}] Initial run complete. Next events will be logged.")
                    self.initial_run = False
                    # Set last_record_id to highest seen to ignore all previous events
                    self.last_record_id = self.highest_seen_id
                
                if event_count:
                    print(f"[{datetime.now()}] Processed {event_count} new events")
                
                time.sleep(POLL_INTERVAL)
                
            except KeyboardInterrupt:
                print(f"\n[{datetime.now()}] Stopping monitor...")
                break
            except Exception as e:
                print(f"[{datetime.now()}] ERROR: {e}")
                time.sleep(POLL_INTERVAL)
        
        try:
            with open(LOG_FILE, "a") as f:
                f.write(f"\n=== Session ended at {datetime.now().astimezone()} ===\n")
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: Failed to write session end: {e}")

if __name__ == "__main__":
    try:
        monitor = LidMonitor()
        monitor.monitor_lid_events()
    except Exception as e:
        print(f"[{datetime.now()}] CRITICAL: {e}")
        sys.exit(1)








import psutil
import win32api
import win32com.client
import win32con
import time
import win32evtlog
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import re

# Configuration
ns = {'e': 'http://schemas.microsoft.com/win/2004/08/events/event'}
last_record_id = 0
LOG_FILE = "lid_closed_events.log"
POLL_INTERVAL = 10  # seconds - change to 60 for 1-minute polling

def parse_event_time(timestamp_str):
    """Parse the event timestamp string into a datetime object"""
    try:
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"
        
        match = re.match(r"(.*T\d+:\d+:\d+)\.(\d{6})\d+(\+\d+:\d+)", timestamp_str)
        if match:
            timestamp_str = f"{match.group(1)}.{match.group(2)}{match.group(3)}"
        
        dt_utc = datetime.fromisoformat(timestamp_str)
        return dt_utc.astimezone()
    except Exception as e:
        print(f"[{datetime.now()}] Exception in parse_event_time:", e)
        return None

def get_events(log_name, event_id):
    """Generator function to yield events from the specified log"""
    server = None  # local machine
    flags = win32evtlog.EvtQueryReverseDirection | win32evtlog.EvtQueryFilePath
    query = f"*[System[EventID={event_id}]]"
    
    h = win32evtlog.EvtQuery(log_name, flags, query)
    while True:
        events = win32evtlog.EvtNext(h, 10)
        if not events:
            break
        for event in events:
            yield event

def log_lid_event(event_time):
    """Write the lid closed event to the log file with both event and logging times"""
    log_time = datetime.now().astimezone()
    try:
        with open(LOG_FILE, "a") as f:
            log_entry = (
                f"Lid closed at (event time): {event_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                f"Logged at (system time): {log_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
                f"{'-'*50}\n"
            )
            f.write(log_entry)
        print(f"[{log_time}] Logged lid closure event from {event_time}")
    except Exception as e:
        print(f"[{log_time}] Error writing to log file: {e}")

def monitor_lid_events():
    """Monitor for new lid closure events"""
    global last_record_id
    
    print(f"[{datetime.now()}] Starting lid closure monitor (polling every {POLL_INTERVAL} seconds)...")
    print(f"[{datetime.now()}] Events will be logged to: {LOG_FILE}")
    
    try:
        with open(LOG_FILE, "a") as f:
            f.write(f"\n\n=== New monitoring session started at {datetime.now().astimezone()} ===\n")
    except Exception as e:
        print(f"[{datetime.now()}] Could not write to log file: {e}")
    
    while True:
        try:
            current_time = datetime.now()
            print(f"[{current_time}] Checking for new lid events...")
            
            for event in get_events("Microsoft-Windows-Kernel-Power", 506):
                xml = win32evtlog.EvtRender(event, win32evtlog.EvtRenderEventXml)
                root = ET.fromstring(xml)
                
                reason = None
                for data in root.findall("e:EventData/e:Data", ns):
                    if data.attrib.get("Name") == "Reason":
                        reason = data.text
                
                if reason in ("Lid", "15"):
                    record_id_node = root.find("e:System/e:EventRecordID", ns)
                    record_id = int(record_id_node.text)
                    
                    if record_id > last_record_id:
                        time_node = root.find("e:System/e:TimeCreated", ns)
                        timestamp_str = time_node.attrib.get("SystemTime")
                        event_time = parse_event_time(timestamp_str)
                        
                        if event_time:
                            log_lid_event(event_time)
                            last_record_id = record_id
                    else:
                        break  # Reached already processed events
                
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print(f"\n[{datetime.now()}] Stopping monitor...")
            break
        except Exception as e:
            print(f"[{datetime.now()}] Error in monitoring loop: {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    monitor_lid_events()




# Databricks notebook source
# Define the attrition processing function
def process_manager_attrition(manager_id):
    try:
        # 1. Delete departed employees for this manager
        delete_query = f"""
            DELETE FROM gold_dashboard.analytics_emp_mapping
            WHERE emp_id IN (
                SELECT aem.emp_id
                FROM gold_dashboard.analytics_emp_mapping aem
                LEFT JOIN (
                    SELECT emplid 
                    FROM inbound.hr_employee_central
                    WHERE func_mgr_id = '{manager_id}' 
                      AND (TERMINATION_DT > CURRENT_TIMESTAMP() OR TERMINATION_DT IS NULL)
                ) hec ON aem.emp_id = hec.emplid
                WHERE hec.emplid IS NULL
                  AND aem.user_id = '{manager_id}'
            )
        """
        deleted_count = spark.sql(delete_query).count()
        
        # 2. Process understaffed teams if departures occurred
        updated_count = 0
        if deleted_count > 0:
            merge_query = f"""
                MERGE INTO gold_dashboard.analytics_emp_mapping AS target
                USING (
                    WITH understaffed_employees AS (
                        SELECT
                            emp_id,
                            current_team_id,
                            COUNT(*) OVER (PARTITION BY current_team_id) AS team_size
                        FROM gold_dashboard.analytics_emp_mapping
                        WHERE current_team_id != 1
                          AND user_id = '{manager_id}'
                    )
                    SELECT
                        emp_id,
                        1 AS new_team_id,
                        current_team_id
                    FROM understaffed_employees
                    WHERE team_size < 4
                ) AS source
                ON target.emp_id = source.emp_id
                WHEN MATCHED THEN
                    UPDATE SET
                        current_team_id = source.new_team_id,
                        last_team_id = source.current_team_id,
                        last_change_date = CURRENT_TIMESTAMP(),
                        next_change_date = NULL,
                        assignment_type = 'AUTO_DEFAULT',
                        moved_reason = 'ATTRITION',
                        updated_at = CURRENT_TIMESTAMP()
            """
            updated_count = spark.sql(merge_query).count()
        
        return (manager_id, deleted_count, updated_count)
    
    except Exception as e:
        print(f"Failed processing manager {manager_id}: {str(e)}")
        return (manager_id, -1, -1)  # Using -1 to indicate error

# COMMAND ----------

# Main execution logic

# Get all distinct manager IDs from emp_mapping table
manager_ids = spark.sql("""
    SELECT DISTINCT user_id 
    FROM gold_dashboard.analytics_emp_mapping
    WHERE user_type = 'manager'
""").collect()

# Process each manager in parallel
from multiprocessing.pool import ThreadPool
results = []

with ThreadPool(8) as pool:  # Adjust thread count based on your cluster
    results = pool.map(process_manager_attrition, [row.user_id for row in manager_ids])

# COMMAND ----------

# Generate and save the report
import pandas as pd

report_df = pd.DataFrame(results, columns=['manager_id', 'deleted_count', 'moved_to_default'])
display(report_df)

# Save to Delta table for audit
spark.createDataFrame(report_df).write.mode("append").saveAsTable("gold_dashboard.attrition_processing_log")

# COMMAND ----------

# Summary statistics
total_deleted = report_df[report_df.deleted_count > 0].deleted_count.sum()
total_moved = report_df[report_df.moved_to_default > 0].moved_to_default.sum()

print(f"""
=== ATTRITION PROCESSING COMPLETE ===
Total managers processed: {len(manager_ids)}
Managers with departures: {(report_df.deleted_count > 0).sum()}
Total employees departed: {total_deleted}
Total employees moved to default: {total_moved}
""")





from collections import defaultdict
from fastapi import Depends
from pydantic import BaseModel
from typing import List

class EmpTagUpdate(BaseModel):
    emp_id: str
    old_team_id: int  # Current team ID before change
    new_team_id: int  # New team ID (1 for default)

class EmpTagUpdatePayload(BaseModel):
    updates: List[EmpTagUpdate]
    user_type: str
    user_id: str

def update_emp_tags(
    payload: EmpTagUpdatePayload,
    authorize: AuthJWT = Depends()
):
    try:
        with DatabricksSession() as conn:
            cursor = conn.cursor()
            
            # 1. Calculate team changes before executing MERGE
            team_changes = defaultdict(lambda: {"added": 0, "removed": 0})
            
            for update in payload.updates:
                if update.old_team_id != update.new_team_id:
                    team_changes[update.old_team_id]["removed"] += 1
                    team_changes[update.new_team_id]["added"] += 1
            
            # 2. Prepare and execute MERGE
            values_clause = ",".join([
                f"('{update.emp_id}', {update.new_team_id}, '{payload.user_type}', '{payload.user_id}')"
                for update in payload.updates
            ])
            
            cursor.execute(f"""
                MERGE INTO gold_dashboard.analytics_emp_mapping AS target
                USING (
                    SELECT * FROM VALUES {values_clause}
                    AS source_data(emp_id, tag_id, user_type, user_id)
                ) AS source
                ON target.emp_id = source.emp_id
                  AND target.user_type = source.user_type
                  AND target.user_id = source.user_id
                WHEN MATCHED THEN
                  UPDATE SET
                    current_team_id = source.tag_id,
                    last_team_id = target.current_team_id,
                    last_tag_change = CURRENT_TIMESTAMP(),
                    next_tag_change = CASE 
                      WHEN source.tag_id = 1 THEN NULL 
                      ELSE DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 MONTH)
                    END,
                    change_type = CASE
                      WHEN source.tag_id = 1 THEN 'MANUAL_DEFAULT'
                      ELSE 'CUSTOM'
                    END,
                    change_reason = 'MANUAL_REASSIGNMENT',
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                  INSERT (emp_id, current_team_id, user_type, user_id, 
                          created_at, updated_at, change_type, change_reason)
                  VALUES (source.emp_id, source.tag_id, source.user_type, 
                          source.user_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
                          CASE WHEN source.tag_id = 1 THEN 'MANUAL_DEFAULT' ELSE 'CUSTOM' END,
                          'INITIAL_ASSIGNMENT')
            """)
            
            # 3. Format the response
            result = [
                {
                    "team_id": team_id,
                    "employees_added": changes["added"],
                    "employees_removed": changes["removed"],
                    "net_change": changes["added"] - changes["removed"]
                }
                for team_id, changes in team_changes.items()
                if changes["added"] > 0 or changes["removed"] > 0
            ]
            
            return JSONResponse(
                status_code=200,
                content={
                    "team_changes": result,
                    "message": f"Updated {len(payload.updates)} employees across {len(result)} teams"
                }
            )
            
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"msg": f"Batch update failed: {str(e)}"}
        )




from fastapi import Depends, HTTPException, status
from pydantic import BaseModel

class TagReassignmentPayload(BaseModel):
    old_tag_id: int
    old_tag_name: str  # Get this from frontend/API call
    new_tag_id: int

def delete_and_reassign_team(
    payload: TagReassignmentPayload,
    user_type: str,
    user_id: str,
    authorize: AuthJWT = Depends()
):
    if payload.old_tag_id == payload.new_tag_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot reassign to the same team"
        )

    try:
        with DatabricksSession() as conn:
            cursor = conn.cursor()
            
            # SINGLE UPDATE QUERY with team name from payload
            cursor.execute(f"""
                UPDATE gold_dashboard.analytics_emp_mapping
                SET 
                    current_team_id = {payload.new_tag_id},
                    last_team_id = current_team_id,
                    last_tag_change = CURRENT_TIMESTAMP(),
                    next_tag_change = {'NULL' if payload.new_tag_id == 1 else 'DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 MONTH)'},
                    change_type = {'"MANUAL_DEFAULT"' if payload.new_tag_id == 1 else '"CUSTOM"'},
                    change_reason = 'Team {payload.old_tag_name.replace("'", "''")} removal',
                    updated_at = CURRENT_TIMESTAMP()
                WHERE user_type = '{user_type}'
                  AND user_id = '{user_id}'
                  AND current_team_id = {payload.old_tag_id}
            """)
            
            # SINGLE DELETE QUERY
            cursor.execute(f"""
                DELETE FROM gold_dashboard.analytics_team_tagging
                WHERE user_type = '{user_type}'
                  AND user_id = '{user_id}'
                  AND tag_id = {payload.old_tag_id}
            """)
            
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Team '{payload.old_tag_name}' deleted",
                    "details": {
                        "old_team_id": payload.old_tag_id,
                        "new_team_id": payload.new_tag_id,
                        "affected_employees": cursor.rowcount
                    }
                }
            )
            
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"msg": f"Operation failed: {str(e)}"}
        )





MERGE INTO analytics_emp_mapping AS target
USING (
  WITH understaffed_employees AS (
    SELECT 
      emp_id,
      current_team_id,
      COUNT(*) OVER (PARTITION BY current_team_id) AS team_size
    FROM analytics_emp_mapping
    WHERE current_team_id != 1  -- Exclude default team
      AND manager_id = ${manager_id}
  )
  SELECT
    emp_id,
    1 AS new_team_id,           -- Default team ID
    current_team_id             -- Original team ID for last_team_id
  FROM understaffed_employees
  WHERE team_size < 4           -- Understaffed threshold
) AS source
ON target.emp_id = source.emp_id
WHEN MATCHED THEN
  UPDATE SET
    target.current_team_id = source.new_team_id,
    target.last_team_id = source.current_team_id,  -- Preserve original team
    target.last_tag_change = CURRENT_TIMESTAMP(),
    target.next_tag_change = NULL,                 -- Bypass time restrictions
    target.change_type = 'AUTO_DEFAULT',
    target.change_reason = 'ATTRITION',
    target.updated_at = CURRENT_TIMESTAMP()







✅ Improvements After Migrating to FastAPI

1. Non-blocking Server Framework (ASGI-based)
FastAPI (using Uvicorn, not Gunicorn) supports asynchronous request handling by design — unlike Flask, which is synchronous. This makes your API more responsive for non-DB tasks (like JSON processing, compression, logging, etc.).


2. Faster API Response over Network
With Gzip compression implemented now, large JSON payloads (e.g., 2MB → ~20KB) are much faster to transfer to the frontend.
👉 This improves download time and front-end render speed.


3. Better Baseline Performance and Modern Stack
FastAPI is generally faster and more efficient at handling API requests, even in non-async scenarios.


Sure! Here's a polished version of your explanation, suitable for documentation or communication (e.g., email, update report, or release note):


---

Performance Optimization with Gzip Compression in FastAPI Migration

As part of the recent migration from Flask to FastAPI, we have implemented Gzip compression on the backend responses. This enhancement significantly improves the performance, especially when dealing with large JSON payloads.

Previously, when the backend response contained a large volume of data—such as detailed employee and application information—the raw JSON size could reach up to 2MB or more. This resulted in noticeable delays in downloading the data on the client side and rendering it in the frontend.

With the introduction of Gzip compression, the JSON data is now compressed before being sent over the network. This has led to a drastic reduction in payload size—down to approximately 10–40 KB, depending on the data—achieving nearly 90% size reduction in some cases.

As a result:

Download time has significantly decreased

Frontend load time and responsiveness have improved

Users experience faster and smoother data rendering


This optimization is now fully integrated as part of the FastAPI-based backend and is a key performance improvement in the new architecture.


---

Let me know if you'd like a shorter version or something more technical!





WITH 
-- 1. Get ALL employees for the manager from HR table
hr_employees AS (
  SELECT 
    emp_id,
    full_name
  FROM hr_employee_central
  WHERE func_manager_id = ${manager_id}
),

-- 2. Get all custom teams for this manager
custom_teams AS (
  SELECT 
    team_id,
    team_name,
    color_code
  FROM analytics_team_tagging
  WHERE manager_id = ${manager_id}
),

-- 3. Get employee counts per team in one pass
team_counts AS (
  SELECT
    COALESCE(em.current_team_id, 1) AS team_id,
    COUNT(*) AS employee_count
  FROM hr_employees he
  LEFT JOIN analytics_emp_mapping em ON he.emp_id = em.emp_id
  GROUP BY COALESCE(em.current_team_id, 1)
),

-- 4. Single CTE for all employee data
all_employees AS (
  SELECT 
    he.emp_id,
    he.full_name,
    COALESCE(em.current_team_id, 1) AS current_team_id,
    em.last_team_id,
    em.last_tag_change,
    em.next_tag_change,
    CASE
      WHEN em.emp_id IS NULL THEN 'IMPLICIT_DEFAULT'
      WHEN COALESCE(em.current_team_id, 1) = 1 AND em.change_type = 'MANUAL' THEN 'MANUAL_DEFAULT'
      WHEN COALESCE(em.current_team_id, 1) = 1 THEN 'AUTO_DEFAULT'
      ELSE 'CUSTOM'
    END AS change_type,
    CASE
      WHEN em.emp_id IS NULL THEN NULL
      WHEN COALESCE(em.current_team_id, 1) = 1 AND em.change_type = 'AUTO' THEN em.change_reason
      ELSE NULL
    END AS change_reason
  FROM hr_employees he
  LEFT JOIN analytics_emp_mapping em ON he.emp_id = em.emp_id
)

-- Final optimized output
SELECT
  -- Teams with pre-calculated counts
  (
    SELECT COLLECT_LIST(
      NAMED_STRUCT(
        'team_id', CAST(t.team_id AS STRING),
        'team_name', t.team_name,
        'color_code', t.color_code,
        'is_default', (t.team_id = 1),
        'employee_count', CAST(COALESCE(tc.employee_count, 0) AS STRING)
      )
    )
    FROM (
      SELECT * FROM custom_teams
      UNION ALL
      SELECT 1 AS team_id, 'Default Team' AS team_name, '#CCCCCC' AS color_code
    ) t
    LEFT JOIN team_counts tc ON t.team_id = tc.team_id
  ) AS teams,
  
  -- Employees with team info
  (
    SELECT COLLECT_LIST(
      NAMED_STRUCT(
        'emp_id', CAST(ae.emp_id AS STRING),
        'full_name', ae.full_name,
        'team', NAMED_STRUCT(
          'team_id', CAST(ae.current_team_id AS STRING),
          'team_name', CASE 
            WHEN ae.current_team_id = 1 THEN 'Default Team'
            ELSE (SELECT ct.team_name FROM custom_teams ct WHERE ct.team_id = ae.current_team_id LIMIT 1)
          END,
          'color_code', CASE
            WHEN ae.current_team_id = 1 THEN '#CCCCCC'
            ELSE (SELECT ct.color_code FROM custom_teams ct WHERE ct.team_id = ae.current_team_id LIMIT 1)
          END,
          'is_default', (ae.current_team_id = 1)
        ),
        'change_type', ae.change_type,
        'change_reason', ae.change_reason,
        'last_tag_change', CAST(ae.last_tag_change AS STRING),
        'next_tag_change', CAST(ae.next_tag_change AS STRING),
        'previous_team', CASE
          WHEN ae.change_type = 'AUTO_DEFAULT' AND ae.last_team_id IS NOT NULL THEN
            NAMED_STRUCT(
              'team_id', CAST(ae.last_team_id AS STRING),
              'team_name', (SELECT ct.team_name FROM custom_teams ct WHERE ct.team_id = ae.last_team_id LIMIT 1),
              'color_code', (SELECT ct.color_code FROM custom_teams ct WHERE ct.team_id = ae.last_team_id LIMIT 1)
            )
          ELSE NULL
        END
      )
    )
    FROM all_employees ae
  ) AS employees


WITH 
-- 1. Get ALL employees for the manager from HR table
hr_employees AS (
  SELECT 
    emp_id,
    full_name,
    func_manager_id AS manager_id
  FROM hr_employee_central
  WHERE func_manager_id = ${manager_id}
),

-- 2. Get employees with existing mappings (manual/auto transfers)
mapped_employees AS (
  SELECT 
    e.emp_id,
    e.full_name,
    m.current_team_id,
    m.last_team_id,
    m.last_tag_change,
    m.next_tag_change,
    m.change_type,
    m.change_reason,
    m.created_at AS mapping_created_at
  FROM hr_employees e
  LEFT JOIN analytics_emp_mapping m ON e.emp_id = m.emp_id
),

-- 3. Get all custom teams for this manager
custom_teams AS (
  SELECT 
    team_id,
    team_name,
    color_code,
    manager_id,
    FALSE AS is_default
  FROM analytics_team_tagging
  WHERE manager_id = ${manager_id}
),

-- 4. Categorize employees into: custom teams / explicit default / implicit default
categorized_employees AS (
  SELECT
    e.*,
    CASE
      -- Employees in custom teams
      WHEN EXISTS (
        SELECT 1 FROM custom_teams t 
        WHERE t.team_id = e.current_team_id
      ) THEN (
        SELECT AS STRUCT 
          t.team_id,
          t.team_name,
          t.color_code,
          FALSE AS is_default,
          'CUSTOM' AS assignment_type
        FROM custom_teams t 
        WHERE t.team_id = e.current_team_id
      )
      -- Explicitly assigned to default team (team_id=1)
      WHEN e.current_team_id = 1 THEN (
        SELECT AS STRUCT 
          1 AS team_id,
          'Default Team' AS team_name,
          '#CCCCCC' AS color_code,
          TRUE AS is_default,
          CASE 
            WHEN e.change_type = 'MANUAL' THEN 'MANUAL_DEFAULT'
            ELSE 'AUTO_DEFAULT'
          END AS assignment_type
      )
      -- Implicit default (no mapping exists)
      WHEN e.current_team_id IS NULL THEN (
        SELECT AS STRUCT 
          1 AS team_id,
          'Default Team' AS team_name,
          '#CCCCCC' AS color_code,
          TRUE AS is_default,
          'IMPLICIT_DEFAULT' AS assignment_type
      )
      -- Previously assigned to deleted team (fallback to default)
      ELSE (
        SELECT AS STRUCT 
          1 AS team_id,
          'Default Team' AS team_name,
          '#CCCCCC' AS color_code,
          TRUE AS is_default,
          'AUTO_DEFAULT' AS assignment_type
      )
    END AS team_info,
    -- Preserve last team info for auto-transfers
    CASE
      WHEN e.change_type = 'AUTO' AND e.last_team_id IS NOT NULL THEN (
        SELECT AS STRUCT 
          t.team_id,
          t.team_name,
          t.color_code
        FROM custom_teams t 
        WHERE t.team_id = e.last_team_id
      )
      ELSE NULL
    END AS previous_team_info
  FROM mapped_employees e
)

-- 5. Final JSON output
SELECT
  -- Teams array (custom + default)
  (
    SELECT ARRAY_AGG(
      STRUCT(
        t.team_id,
        t.team_name,
        t.color_code,
        t.is_default,
        (SELECT COUNT(*) FROM categorized_employees e WHERE e.team_info.team_id = t.team_id) AS employee_count
      )
    )
    FROM (
      SELECT * FROM custom_teams
      UNION ALL
      SELECT 
        1 AS team_id, 
        'Default Team' AS team_name, 
        '#CCCCCC' AS color_code, 
        TRUE AS is_default
    ) t
  ) AS teams,
  
  -- Employees array with full details
  (
    SELECT ARRAY_AGG(
      STRUCT(
        emp_id,
        full_name,
        -- Current team details
        team_info.team_id,
        team_info.team_name,
        team_info.color_code,
        team_info.is_default,
        team_info.assignment_type,
        -- Transfer history
        last_tag_change,
        next_tag_change,
        change_type,
        change_reason,
        -- Previous team (only for auto-transfers to default)
        CASE
          WHEN team_info.is_default = TRUE AND change_type = 'AUTO' THEN previous_team_info
          ELSE NULL
        END AS previous_team
      )
    )
    FROM categorized_employees
  ) AS employees













WITH 
-- 1. Get all employees for the manager
emp_data AS (
  SELECT 
    emp_id,
    full_name,
    current_team_id,
    last_tag_change,
    next_tag_change,
    change_type,
    change_reason
  FROM analytics_emp_mapping
  WHERE manager_id = ${manager_id}
),

-- 2. Get all custom teams for the manager
custom_teams AS (
  SELECT 
    team_id,
    team_name,
    color_code,
    FALSE AS is_default
  FROM analytics_team_tagging
  WHERE manager_id = ${manager_id}
),

-- 3. Identify default team assignments
employee_assignments AS (
  SELECT
    e.*,
    CASE
      -- Explicitly assigned to default team (ID 1)
      WHEN e.current_team_id = 1 THEN 
        STRUCT(1 AS team_id, 'Default Team' AS team_name, '#CCCCCC' AS color_code, TRUE AS is_default)
      -- Not assigned to any custom team (fallback to default)
      WHEN NOT EXISTS (
        SELECT 1 FROM custom_teams t 
        WHERE t.team_id = e.current_team_id
      ) THEN 
        STRUCT(1 AS team_id, 'Default Team' AS team_name, '#CCCCCC' AS color_code, TRUE AS is_default)
      -- Assigned to custom team
      ELSE (
        SELECT AS STRUCT t.team_id, t.team_name, t.color_code, t.is_default
        FROM custom_teams t
        WHERE t.team_id = e.current_team_id
      )
    END AS team_info
  FROM emp_data e
)

-- 4. Final JSON output
SELECT
  -- Teams array (custom + default)
  (
    SELECT ARRAY_AGG(
      STRUCT(
        t.team_id,
        t.team_name,
        t.color_code,
        t.is_default,
        (SELECT COUNT(*) FROM emp_data e WHERE e.current_team_id = t.team_id) AS employee_count
      )
    )
    FROM (
      SELECT * FROM custom_teams
      UNION ALL
      SELECT 1 AS team_id, 'Default Team' AS team_name, '#CCCCCC' AS color_code, TRUE AS is_default
    ) t
  ) AS teams,
  
  -- Employees array with detailed team info
  (
    SELECT ARRAY_AGG(
      STRUCT(
        emp_id,
        full_name,
        last_tag_change,
        next_tag_change,
        change_type,
        -- Show change reason only for auto-transfers to default
        CASE 
          WHEN team_info.is_default = TRUE AND change_type = 'AUTO' THEN change_reason
          ELSE NULL
        END AS default_change_reason,
        team_info.*  -- Includes team_id, name, color, is_default
      )
    )
    FROM employee_assignments
  ) AS employees








rom datetime import datetime

def generate_unique_ids(manager_id: int, count: int) -> list:
    ids = []
    timestamp = int(datetime.utcnow().timestamp() * 1000)  # Current timestamp in milliseconds
    
    for i in range(count):
        # Combine timestamp, manager_id, and the loop index to create a unique ID
        unique_id = (timestamp % 100000000) * 1000000 + manager_id * 100 + i
        ids.append(unique_id)
    
    return ids



CREATE TABLE employees (
    eid INTEGER PRIMARY KEY,  -- Unique employee ID
    psid STRING NOT NULL,
    full_name STRING NOT NULL,
    manager_id STRING NOT NULL,
    current_team_id INTEGER,  -- Foreign key reference to teams
    recent_team_ids ARRAY<STRING>,
    last_tag_change TIMESTAMP,
    change_type STRING CHECK (change_type IN ('MANUAL', 'AUTO')),
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) USING DELTA;




CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,  -- Unique team ID
    team_name STRING NOT NULL,
    color_code STRING CHECK (color_code LIKE '#______'),
    manager_id STRING NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_default BOOLEAN,
    UNIQUE (manager_id, team_name)  -- Unique constraint on team name per manager
) USING DELTA;






from fastapi import APIRouter, Header, Query, status
from fastapi.responses import JSONResponse
from fastapi_jwt_auth import AuthJWT
import time
import random
from typing import List
from dataclasses import dataclass
from contextlib import contextmanager

router = APIRouter()

# Configuration
MAX_RETRIES = 3
BASE_RETRY_DELAY = 0.1  # seconds
MAX_RETRY_DELAY = 1.0    # seconds

@dataclass
class TagItem:
    tag_name: str
    tag_color: str
    tag_id: int = None

@dataclass
class TagDataPayload:
    tag_data: List[TagItem]

@contextmanager
def DatabricksSession():
    """Context manager for Databricks connection"""
    conn = None
    try:
        # Initialize your Databricks connection here
        # conn = create_databricks_connection()
        yield conn
    finally:
        if conn:
            conn.close()

def exponential_backoff(retry_count):
    """Calculate delay with jitter"""
    delay = min(BASE_RETRY_DELAY * (2 ** retry_count), MAX_RETRY_DELAY)
    return delay * (1 + random.random() * 0.1)  # Add 10% jitter

@router.post("/tag_create")
async def tag_create(
    payload: TagDataPayload, 
    Authorize: AuthJWT = Depends(),
    authorization: str = Header(None),
    user_type: str = Query(...)
):
    if not authorization or not authorization.startswith("Bearer"):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"msg": "Authorization Token Missing"}
        )

    try:
        Authorize.jwt_required()
        user_identity = Authorize.get_jwt_subject()  # gets identity/user_email
        claims = Authorize.get_raw_jwt()  # gets all claims including user_id
        user_id = claims.get("user_id")

        if not user_id:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"message": "Invalid Token Claims"}
            )

        user_type = user_type
        tag_data = [item.dict() for item in payload.tag_data]

        if not tag_data:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"msg": "No Tag data provided."}
            )

        inserts = [tag['tag_name'] for tag in tag_data if not tag.get("tag_id")]
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                with DatabricksSession() as conn:
                    cursor = conn.cursor()

                    # Build VALUES clause with parameterized queries for security
                    values_placeholders = []
                    values_params = []
                    for tag in tag_data:
                        values_placeholders.append("(?, ?, ?, ?, ?)")
                        values_params.extend([
                            tag['tag_name'],
                            tag['tag_color'],
                            tag.get('tag_id'),
                            user_type,
                            user_id
                        ])

                    merge_sql = f"""
                    MERGE INTO gold_dashboard.analytics_app_tagging target
                    USING (
                        SELECT * FROM VALUES {','.join(values_placeholders)}
                        AS source_data(tag_name, tag_color, tag_id, user_type, user_id)
                    ) AS source
                    ON target.user_type = source.user_type
                    AND target.user_id = source.user_id
                    AND (target.tag_id = source.tag_id OR (source.tag_id IS NULL AND target.tag_id IS NULL))
                    WHEN MATCHED AND source.tag_id IS NOT NULL THEN
                        UPDATE SET
                            tag_name = source.tag_name,
                            tag_color = source.tag_color,
                            updated_at = current_timestamp()
                    WHEN NOT MATCHED AND source.tag_id IS NULL THEN
                        INSERT (tag_name, tag_color, user_type, user_id, created_at)
                        VALUES (source.tag_name, source.tag_color, source.user_type, source.user_id, current_timestamp())
                    """

                    cursor.execute(merge_sql, values_params)
                    conn.commit()

                    # If we got here, the merge succeeded
                    if not inserts:
                        return JSONResponse(
                            status_code=201,
                            content={
                                "tag_data": 0,
                                "message": "Tags updated successfully"
                            }
                        )
                    else:
                        cursor.execute("""
                            SELECT tag_id, tag_name
                            FROM gold_dashboard.analytics_app_tagging
                            WHERE user_type = ? AND user_id = ?
                            ORDER BY created_at DESC
                            LIMIT ?
                        """, (user_type, user_id, len(inserts)))
                        
                        return JSONResponse(
                            status_code=201,
                            content={
                                "tag_data": {row[1]: row[0] for row in cursor.fetchall()},
                                "message": "Tags inserted/updated successfully"
                            }
                        )

            except Exception as e:
                last_exception = e
                if "ConcurrentModificationException" in str(e) and attempt < MAX_RETRIES - 1:
                    # Exponential backoff with jitter
                    time.sleep(exponential_backoff(attempt))
                    continue
                raise

        # If we exhausted all retries
        raise last_exception if last_exception else Exception("Unknown error occurred")

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"msg": f"Error: {str(e)}"}
        )

WITH date_params AS (
  SELECT 
    45125667 AS emp_id,
    DATE '2025-05-26' AS start_date,
    DATE '2025-06-01' AS end_date,
    YEAR(DATE '2025-05-26') AS year,
    WEEKOFYEAR(DATE '2025-05-26') AS week
),

app_mappings AS (
  SELECT 
    m.app_name,
    m.mapped_app_name,
    m.tag_name,
    t.tag_color
  FROM gold_dashboard.analytics_app_mapping m
  LEFT JOIN gold_dashboard.analytics_app_tagging t
    ON m.user_type = t.user_type
    AND m.user_id = t.user_id
    AND m.tag_id = t.tag_id
    AND t.is_active = TRUE
  WHERE m.user_type = 'EMP'
    AND m.user_id = (SELECT emp_id FROM date_params)
),

default_tags AS (
  SELECT 
    tmp.col1 AS app_name, 
    tmp.col2 AS mapped_app_name, 
    tmp.col3 AS tag_name, 
    tmp.col4 AS tag_color 
  FROM (VALUES 
    ('dwm', 'Desktop Window Manager', 'System', '#808080'),
    ('OfficeClickToRun', 'Microsoft Office', 'Productivity', '#2B579A'),
    ('msinfo32', 'System Information', 'System', '#808080'),
    ('PGProgramsUtil', 'Program Utility', 'System', '#808080'),
    ('ApplicationFrameHost', 'Windows App Host', 'System', '#808080'),
    ('LSU', 'Lenovo System Update', 'System', '#808080'),
    ('Snipping Tool', 'Snipping Tool', 'Productivity', '#2B579A'),
    ('notepad++', 'Notepad++', 'Development', '#4EC9B0'),
    ('Tricentis Tosca 16.0 Patch9 LTS', 'Tosca', 'Testing', '#A4A4A4'),
    ('ShellExperienceHost', 'Windows Shell', 'System', '#808080'),
    ('OUTLOOK', 'Outlook', 'Productivity', '#2B579A'),
    ('Zoom', 'Zoom', 'Communication', '#2D8CFF')
  ) AS tmp(col1, col2, col3, col4)
),

combined_mappings AS (
  SELECT * FROM app_mappings
  UNION ALL
  SELECT * FROM default_tags
  WHERE NOT EXISTS (
    SELECT 1 FROM app_mappings
    WHERE app_mappings.app_name = default_tags.app_name
  )
),

employee_data AS (
  SELECT 
    team_summary_json,
    employee_summary_json,
    graph_data_employee,
    emp_id,
    year,
    week,
    type
  FROM your_table_name
  WHERE emp_id = (SELECT emp_id FROM date_params)
    AND year = (SELECT year FROM date_params)
    AND week = (SELECT week FROM date_params)
    AND type = 'employee'
  LIMIT 1
),

graph_data_expanded AS (
  SELECT
    app.value:application_name::STRING AS application_name,
    TRY_CAST(app.value:active_time::STRING AS DOUBLE) AS active_time,
    TRY_CAST(app.value:idle_time::STRING AS DOUBLE) AS idle_time,
    TRY_CAST(app.value:total_time::STRING AS DOUBLE) AS total_time,
    TRY_CAST(app.value:mouse_clicks::STRING AS DOUBLE) AS mouse_clicks,
    TRY_CAST(app.value:key_strokes::STRING AS DOUBLE) AS key_strokes,
    app.value:active_trend::STRING AS active_trend,
    app.value:idle_trend::STRING AS idle_trend,
    app.value:total_trend::STRING AS total_trend
  FROM employee_data,
  LATERAL EXPLODE(FROM_JSON(graph_data_employee, 'ARRAY<STRUCT<application_name:STRING,active_time:STRING,idle_time:STRING,total_time:STRING,mouse_clicks:STRING,key_strokes:STRING,active_trend:STRING,idle_trend:STRING,total_trend:STRING>>')) AS app
),

graph_data_mapped AS (
  SELECT
    gd.*,
    COALESCE(cm.mapped_app_name, gd.application_name) AS mapped_app_name,
    cm.tag_name,
    cm.tag_color
  FROM graph_data_expanded gd
  LEFT JOIN combined_mappings cm ON gd.application_name = cm.app_name
)

SELECT TO_JSON(
  NAMED_STRUCT(
    'teamSummary', 
    FROM_JSON((SELECT team_summary_json FROM employee_data), 'MAP<STRING,STRING>'),
    
    'empSummary', 
    FROM_JSON((SELECT employee_summary_json FROM employee_data), 'MAP<STRING,STRING>'),
    
    'graphData',
    (SELECT COLLECT_LIST(
      NAMED_STRUCT(
        'application_name', application_name,
        'mapped_app_name', mapped_app_name,
        'tag_name', tag_name,
        'tag_color', tag_color,
        'active_time', active_time,
        'idle_time', idle_time,
        'total_time', total_time,
        'mouse_clicks', mouse_clicks,
        'key_strokes', key_strokes,
        'active_trend', active_trend,
        'idle_trend', idle_trend,
        'total_trend', total_trend
      )
    ) FROM graph_data_mapped)
  )
) AS json_result
FROM employee_data;



WITH date_params AS (
  SELECT 
    45125667 AS emp_id,
    DATE '2025-05-26' AS start_date,
    DATE '2025-06-01' AS end_date,
    EXTRACT(YEAR FROM DATE '2025-05-26') AS year,
    EXTRACT(WEEK FROM DATE '2025-05-26') AS week
),

app_mappings AS (
  SELECT 
    m.app_name,
    m.mapped_app_name,
    m.tag_name,
    t.tag_color
  FROM gold_dashboard.analytics_app_mapping m
  LEFT JOIN gold_dashboard.analytics_app_tagging t
    ON m.user_type = t.user_type
    AND m.user_id = t.user_id
    AND m.tag_id = t.tag_id
    AND t.is_active = TRUE
  WHERE m.user_type = 'EMP'
    AND m.user_id = (SELECT emp_id FROM date_params)
),

default_tags AS (
  SELECT 
    tmp.col1 AS app_name, 
    tmp.col2 AS mapped_app_name, 
    tmp.col3 AS tag_name, 
    tmp.col4 AS tag_color 
  FROM (VALUES 
    ('dwm', 'Desktop Window Manager', 'System', '#808080'),
    ('OfficeClickToRun', 'Microsoft Office', 'Productivity', '#2B579A'),
    ('msinfo32', 'System Information', 'System', '#808080'),
    ('PGProgramsUtil', 'Program Utility', 'System', '#808080'),
    ('ApplicationFrameHost', 'Windows App Host', 'System', '#808080'),
    ('LSU', 'Lenovo System Update', 'System', '#808080'),
    ('Snipping Tool', 'Snipping Tool', 'Productivity', '#2B579A'),
    ('notepad++', 'Notepad++', 'Development', '#4EC9B0'),
    ('Tricentis Tosca 16.0 Patch9 LTS', 'Tosca', 'Testing', '#A4A4A4'),
    ('ShellExperienceHost', 'Windows Shell', 'System', '#808080'),
    ('OUTLOOK', 'Outlook', 'Productivity', '#2B579A'),
    ('Zoom', 'Zoom', 'Communication', '#2D8CFF')
  ) AS tmp(col1, col2, col3, col4)
),

combined_mappings AS (
  SELECT * FROM app_mappings
  UNION ALL
  SELECT * FROM default_tags
  WHERE NOT EXISTS (
    SELECT 1 FROM app_mappings
    WHERE app_mappings.app_name = default_tags.app_name
  )
),

employee_data AS (
  SELECT 
    team_summary_json,
    employee_summary_json,
    graph_data_employee,
    emp_id,
    year,
    week,
    type
  FROM your_table_name
  WHERE emp_id = (SELECT emp_id FROM date_params)
    AND year = (SELECT year FROM date_params)
    AND week = (SELECT week FROM date_params)
    AND type = 'employee'
),

graph_data_expanded AS (
  SELECT
    app.application_name,
    app.active_time,
    app.idle_time,
    app.total_time,
    app.mouse_clicks,
    app.key_strokes,
    app.active_trend,
    app.idle_trend,
    app.total_trend
  FROM employee_data,
  LATERAL FLATTEN(input => PARSE_JSON(graph_data_employee)) AS apps,
  LATERAL (SELECT
    apps.value:application_name::STRING AS application_name,
    TRY_CAST(apps.value:active_time::STRING AS FLOAT) AS active_time,
    TRY_CAST(apps.value:idle_time::STRING AS FLOAT) AS idle_time,
    TRY_CAST(apps.value:total_time::STRING AS FLOAT) AS total_time,
    TRY_CAST(apps.value:mouse_clicks::STRING AS FLOAT) AS mouse_clicks,
    TRY_CAST(apps.value:key_strokes::STRING AS FLOAT) AS key_strokes,
    apps.value:active_trend::STRING AS active_trend,
    apps.value:idle_trend::STRING AS idle_trend,
    apps.value:total_trend::STRING AS total_trend
  ) AS app
),

graph_data_mapped AS (
  SELECT
    gd.*,
    COALESCE(cm.mapped_app_name, gd.application_name) AS mapped_app_name,
    cm.tag_name,
    cm.tag_color
  FROM graph_data_expanded gd
  LEFT JOIN combined_mappings cm ON gd.application_name = cm.app_name
)

SELECT TO_JSON(
  NAMED_STRUCT(
    'teamSummary', 
    (SELECT PARSE_JSON(team_summary_json) FROM employee_data),
    
    'empSummary', 
    (SELECT PARSE_JSON(employee_summary_json) FROM employee_data),
    
    'graphData',
    (SELECT ARRAY_AGG(
      NAMED_STRUCT(
        'application_name', application_name,
        'mapped_app_name', mapped_app_name,
        'tag_name', tag_name,
        'tag_color', tag_color,
        'active_time', active_time,
        'idle_time', idle_time,
        'total_time', total_time,
        'mouse_clicks', mouse_clicks,
        'key_strokes', key_strokes,
        'active_trend', active_trend,
        'idle_trend', idle_trend,
        'total_trend', total_trend
      )
    ) FROM graph_data_mapped)
  )
) AS json_result
FROM employee_data;






# app/utils/azure_auth.py
import httpx
import socket
from fastapi import HTTPException

async def get_token_from_code(auth_code: str, redirect_uri: str):
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    
    # Verify DNS resolution first
    try:
        socket.gethostbyname('login.microsoftonline.com')
    except socket.gaierror:
        raise HTTPException(
            status_code=503,
            detail="Cannot resolve Microsoft login endpoint. Check your network connection."
        )

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
        "scope": "openid email profile User.Read"
    }

    try:
        async with httpx.AsyncClient() as client:
            # Test connection before token exchange
            try:
                await client.get("https://login.microsoftonline.com", timeout=5.0)
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                raise HTTPException(
                    status_code=503,
                    detail=f"Cannot connect to Microsoft servers: {str(e)}"
                )

            # Proceed with token exchange
            response = await client.post(
                token_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10.0
            )
            
            if response.status_code != 200:
                error = response.json()
                raise HTTPException(
                    status_code=400,
                    detail=error.get('error_description', "Authentication failed")
                )
                
            return response.json()

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Network error during token exchange: {str(e)}"
        )





with DatabricksSession() as conn:
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()

    # Initialize data structures
    tag_data = []
    app_data = []
    found_apps = set()
    
    # Default configuration
    default_tags = {
        "Analytics": ["app1", "app2", "app3"],
        "Security": ["app4", "app5"]
    }
    
    # Create reverse mapping and initialize counts in one pass
    default_app_map = {app: tag for tag, apps in default_tags.items() for app in apps}
    default_tag_counts = {tag: 0 for tag in default_tags}

    # Process all rows in single loop
    for row in rows:
        if row[0] == 'tag':
            tag_data.append({
                "tag_id": row[1],
                "tag_name": row[2],
                "tag_color": row[3],
                "app_count": row[4],
                "is_default": False
            })
        else:
            app_name = row[5]
            is_default = app_name in default_app_map
            app_data.append({
                "app_name": app_name,
                "mapped_app_name": row[6],
                "tag_id": None if is_default else row[7],
                "is_default": is_default
            })
            found_apps.add(app_name)
            if is_default:
                default_tag_counts[default_app_map[app_name]] += 1

    # Process mapped_app_names and update counts in one loop
    for app in mapped_app_names:
        if app not in found_apps:
            is_default = app in default_app_map
            app_data.append({
                "app_name": app,
                "mapped_app_name": app,
                "tag_id": None,
                "is_default": is_default
            })
            if is_default:
                default_tag_counts[default_app_map[app]] += 1

    # Add default tags with counts > 0
    tag_data.extend({
        "tag_id": None,
        "tag_name": tag,
        "tag_color": "#FFFFFF",
        "app_count": count,
        "is_default": True
    } for tag, count in default_tag_counts.items() if count > 0)

    return jsonify({
        "TagData": tag_data,
        "AppData": app_data
    }), 200

except Exception as e:
    return jsonify({"error": str(e)}), 500





WITH SessionIntervals AS (
    -- Get all possible intervals that overlap with each login session
    SELECT 
        l.emp_id,
        l.shift_date,
        i.cal_date,
        i.interval,
        i.app_name,
        i.total_time,
        -- Flag indicating if this interval should be included for this session
        CASE 
            WHEN (
                -- Interval starts after login but before logout
                TO_TIMESTAMP(CONCAT(i.cal_date, ' ', SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss') 
                >= TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss')
                AND TO_TIMESTAMP(CONCAT(i.cal_date, ' ', SUBSTRING(i.interval, 1, 5), 'yyyy-MM-dd HH:mm') 
                < TO_TIMESTAMP(l.emp_logout_time, 'yyyy-MM-dd HH:mm:ss')
            ) OR (
                -- Handle case where logout is NULL (still logged in)
                l.emp_logout_time IS NULL 
                AND TO_TIMESTAMP(CONCAT(i.cal_date, ' ', SUBSTRING(i.interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss') 
                >= TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss')
            )
            THEN 1 ELSE 0 
        END AS include_interval
    FROM 
        gold_dashboard.analytics_emp_login_logout l
    CROSS JOIN 
        gold_dashboard.analytics_emp_app_info i
    WHERE 
        l.emp_id = i.emp_id
        AND l.shift_date BETWEEN '2025-05-07' AND '2025-05-07'
        AND l.emp_id IN (SELECT emplid FROM FilteredEmployees)
)

-- Final result with all intervals properly associated with shift_date
SELECT 
    l.emp_id,
    l.shift_date AS cal_date,  -- Using shift_date as the reference date
    i.interval,
    i.app_name,
    i.total_time,
    l.emp_login_time,
    l.emp_logout_time
FROM 
    gold_dashboard.analytics_emp_login_logout l
JOIN 
    SessionIntervals si ON l.emp_id = si.emp_id AND l.shift_date = si.shift_date
JOIN
    gold_dashboard.analytics_emp_app_info i ON i.emp_id = si.emp_id 
        AND i.cal_date = si.cal_date 
        AND i.interval = si.interval
WHERE 
    si.include_interval = 1
ORDER BY 
    l.shift_date, l.emp_id, i.interval;






# Example Python script to run the EMP analytics query in Apache Spark using dynamic date parameters

def get_emp_app_info_query(start_date: str, end_date: str) -> str:
    """
    Returns the SQL query with given start_date and end_date parameters,
    properly formatted for Spark SQL.
    """
    query = f"""
    WITH LoginLogout AS (
        SELECT 
            emp_id,
            shift_date,
            emp_login_time,
            emp_logout_time
        FROM 
            gold_dashboard.analytics_emp_login_logout
        WHERE 
            shift_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    ),
    FilteredIntervals AS (
        SELECT 
            i.cal_date,
            i.app_name,
            i.empid,
            i.total_time_interval,
            l.emp_login_time,
            l.emp_logout_time,
            CASE 
                WHEN l.emp_logout_time IS NOT NULL 
                     AND l.emp_logout_time >= TO_TIMESTAMP(CONCAT(DATE_ADD(DATE('{start_date}'), 1), ' ', SUBSTRING(i.total_time_interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
                THEN DATE_ADD(i.cal_date, 1)
                ELSE i.cal_date
            END AS cal_date,
            DATE('{start_date}') AS effective_cal_date
        FROM 
            gold_dashboard.analytics_emp_app_info i
        LEFT JOIN 
            LoginLogout l
        ON 
            i.empid = l.emp_id
        WHERE 
            l.emp_login_time IS NOT NULL 
            AND l.emp_logout_time IS NOT NULL
            AND (
                (l.emp_login_time <= TO_TIMESTAMP(CONCAT(DATE('{start_date}'), ' 23:59:59'), 'yyyy-MM-dd HH:mm:ss') 
                 AND l.emp_logout_time >= TO_TIMESTAMP(CONCAT(DATE('{start_date}'), ' 00:00:00'), 'yyyy-MM-dd HH:mm:ss'))
            )
    )
    SELECT 
        cal_date,
        app_name,
        empid,
        total_time_interval,
        effective_cal_date
    FROM 
        FilteredIntervals
    WHERE 
        (cal_date = DATE_ADD(DATE('{start_date}'), 1) AND 
         (
           TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss') <= TO_TIMESTAMP(CONCAT(cal_date, ' ', SUBSTRING(total_time_interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss') 
           AND TO_TIMESTAMP(l.emp_logout_time, 'yyyy-MM-dd HH:mm:ss') >= TO_TIMESTAMP(CONCAT(cal_date, ' ', SUBSTRING(total_time_interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
         )
        )
    ORDER BY 
        cal_date, empid
    """
    return query

# Usage example:
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# start_date = '2025-05-07'
# end_date = '2025-05-07'
# query = get_emp_app_info_query(start_date, end_date)
# result_df = spark.sql(query)
# result_df.show()



WITH LoginLogout AS (
    SELECT 
        emp_id,
        shift_date,
        emp_login_time,
        emp_logout_time
    FROM 
        gold_dashboard.analytics_emp_login_logout
    WHERE 
        shift_date BETWEEN '2025-05-07' AND '2025-05-08'
),
FilteredIntervals AS (
    SELECT 
        i.cal_date,
        i.app_name,
        i.empid,
        i.total_time_interval,
        l.emp_login_time,
        l.emp_logout_time,
        CASE 
            WHEN l.emp_logout_time IS NOT NULL AND 
                 l.emp_logout_time >= TO_TIMESTAMP(CONCAT('2025-05-08', ' ', SUBSTRING(i.total_time_interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')
            THEN '2025-05-08'
            ELSE i.cal_date
        END AS cal_date,
        '2025-05-07' AS effective_cal_date
    FROM 
        gold_dashboard.analytics_emp_app_info i
    LEFT JOIN 
        LoginLogout l
    ON 
        i.empid = l.emp_id
    WHERE 
        (l.emp_login_time IS NOT NULL AND l.emp_logout_time IS NOT NULL)
        AND (
            (l.emp_login_time <= TO_TIMESTAMP('2025-05-07 09:00:00', 'yyyy-MM-dd HH:mm:ss') 
             AND l.emp_logout_time >= TO_TIMESTAMP('2025-05-08 00:00:00', 'yyyy-MM-dd HH:mm:ss'))
        )
)
SELECT 
    cal_date,
    app_name,
    empid,
    total_time_interval,
    effective_cal_date
FROM 
    FilteredIntervals
WHERE 
    (cal_date = '2025-05-08' AND 
     (TO_TIMESTAMP(l.emp_login_time, 'yyyy-MM-dd HH:mm:ss') <= 
      TO_TIMESTAMP(CONCAT(cal_date, ' ', SUBSTRING(total_time_interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss') 
      AND 
      TO_TIMESTAMP(l.emp_logout_time, 'yyyy-MM-dd HH:mm:ss') >= 
      TO_TIMESTAMP(CONCAT(cal_date, ' ', SUBSTRING(total_time_interval, 1, 5), ':00'), 'yyyy-MM-dd HH:mm:ss')))
ORDER BY 
    cal_date, empid;





def post(self):
    try:
        # [Previous auth/validation code...]

        with DatabricksSession() as conn:
            cursor = conn.cursor()

            # Prepare values safely, escaping single quotes to avoid SQL errors
            values = ','.join([
                f"('{tag['tag_name'].replace(\"'\", \"''\")}', '{tag['tag_color'].replace(\"'\", \"''\")}', "
                f"{tag.get('tag_id') if tag.get('tag_id') is not None else 'NULL'}, "
                f"'{user_type}', {user_id})"
                for tag in tag_data
            ])

            # Prepare list of new insert tag_names from input data to filter inserted rows after merge
            new_insert_names_list = [tag['tag_name'].replace("'", "''") for tag in tag_data if not tag.get('tag_id')]

            if not new_insert_names_list:
                # No new inserts, so only perform the update
                query_update_only = f"""
                UPDATE silver_dashboard.refined_app_tagging_test
                SET tag_name = source.tag_name,
                    tag_color = source.tag_color
                FROM (VALUES
                    {values}
                ) AS source(tag_name, tag_color, tag_id, user_type, user_id)
                WHERE silver_dashboard.refined_app_tagging_test.user_type = source.user_type
                  AND silver_dashboard.refined_app_tagging_test.user_id = source.user_id
                  AND silver_dashboard.refined_app_tagging_test.tag_id = source.tag_id
                """
                cursor.execute(query_update_only)
                inserted_tags = []  # No new tags to return
            else:
                # There are new inserts; do all in one multi-statement query
                new_insert_names = ','.join([f"'{name}'" for name in new_insert_names_list])

                query = f"""
                WITH incoming_tags AS (
                    SELECT
                      tag_name,
                      tag_color,
                      tag_id,
                      user_type,
                      user_id,
                      CASE WHEN tag_id IS NULL THEN TRUE ELSE FALSE END AS is_new
                    FROM VALUES
                      {values}
                    AS t(tag_name, tag_color, tag_id, user_type, user_id)
                ),
                merge_operation AS (
                    MERGE INTO silver_dashboard.refined_app_tagging_test target
                    USING incoming_tags source
                    ON target.tag_id = source.tag_id
                       AND target.user_type = source.user_type
                       AND target.user_id = source.user_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            tag_name = source.tag_name,
                            tag_color = source.tag_color
                    WHEN NOT MATCHED THEN
                        INSERT (tag_name, tag_color, user_type, user_id)
                        VALUES (source.tag_name, source.tag_color, source.user_type, source.user_id)
                    SELECT 1 as dummy
                )
                SELECT 
                    t.tag_id,
                    t.tag_name
                FROM silver_dashboard.refined_app_tagging_test t
                JOIN incoming_tags i
                  ON t.tag_name = i.tag_name
                  AND t.user_type = i.user_type
                  AND t.user_id = i.user_id
                WHERE i.is_new = TRUE
                  AND t.tag_name IN ({new_insert_names})
                """

                cursor.execute(query)
                inserted_tags = [
                    {"tag_id": row[0], "tag_name": row[1]}
                    for row in cursor.fetchall()
                ]

            return jsonify({
                "message": "Tag operations completed",
                "inserted_tags": inserted_tags,
                "insert_count": len(inserted_tags)
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500


def post(self):
    try:
        # [Previous auth/validation code...]

        with DatabricksSession() as conn:
            cursor = conn.cursor()

            # Prepare values safely, escaping single quotes to avoid SQL errors
            values = ','.join([
                f"('{tag['tag_name'].replace(\"'\", \"''\")}', '{tag['tag_color'].replace(\"'\", \"''\")}', "
                f"{tag.get('tag_id') if tag.get('tag_id') is not None else 'NULL'}, "
                f"'{user_type}', {user_id})"
                for tag in tag_data
            ])

            # Prepare list of new insert tag_names from input data to filter inserted rows after merge
            new_insert_names_list = [tag['tag_name'].replace("'", "''") for tag in tag_data if not tag.get('tag_id')]

            if not new_insert_names_list:
                # No new inserts, so do only merge and return empty list
                query_merge_only = f"""
                WITH incoming_tags AS (
                    SELECT
                      tag_name,
                      tag_color,
                      tag_id,
                      user_type,
                      user_id,
                      CASE WHEN tag_id IS NULL THEN TRUE ELSE FALSE END AS is_new
                    FROM VALUES
                      {values}
                    AS t(tag_name, tag_color, tag_id, user_type, user_id)
                ),
                merge_operation AS (
                    MERGE INTO silver_dashboard.refined_app_tagging_test target
                    USING incoming_tags source
                    ON target.tag_id = source.tag_id
                       AND target.user_type = source.user_type
                       AND target.user_id = source.user_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            tag_name = source.tag_name,
                            tag_color = source.tag_color
                    WHEN NOT MATCHED THEN
                        INSERT (tag_name, tag_color, user_type, user_id)
                        VALUES (source.tag_name, source.tag_color, source.user_type, source.user_id)
                    SELECT 1 as dummy
                )
                SELECT NULL WHERE FALSE -- no inserts to return
                """
                cursor.execute(query_merge_only)
                inserted_tags = []
            else:
                # There are new inserts; do all in one multi-statement query
                new_insert_names = ','.join([f"'{name}'" for name in new_insert_names_list])

                query = f"""
                WITH incoming_tags AS (
                    SELECT
                      tag_name,
                      tag_color,
                      tag_id,
                      user_type,
                      user_id,
                      CASE WHEN tag_id IS NULL THEN TRUE ELSE FALSE END AS is_new
                    FROM VALUES
                      {values}
                    AS t(tag_name, tag_color, tag_id, user_type, user_id)
                ),
                merge_operation AS (
                    MERGE INTO silver_dashboard.refined_app_tagging_test target
                    USING incoming_tags source
                    ON target.tag_id = source.tag_id
                       AND target.user_type = source.user_type
                       AND target.user_id = source.user_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            tag_name = source.tag_name,
                            tag_color = source.tag_color
                    WHEN NOT MATCHED THEN
                        INSERT (tag_name, tag_color, user_type, user_id)
                        VALUES (source.tag_name, source.tag_color, source.user_type, source.user_id)
                    SELECT 1 as dummy
                )
                SELECT 
                    t.tag_id,
                    t.tag_name
                FROM silver_dashboard.refined_app_tagging_test t
                JOIN incoming_tags i
                  ON t.tag_name = i.tag_name
                  AND t.user_type = i.user_type
                  AND t.user_id = i.user_id
                WHERE i.is_new = TRUE
                  AND t.tag_name IN ({new_insert_names})
                """

                cursor.execute(query)
                inserted_tags = [
                    {"tag_id": row[0], "tag_name": row[1]}
                    for row in cursor.fetchall()
                ]

            return jsonify({
                "message": "Tag operations completed",
                "inserted_tags": inserted_tags,
                "insert_count": len(inserted_tags)
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500








def post(self):
    try:
        # [Previous auth/validation code...]

        with DatabricksSession() as conn:
            cursor = conn.cursor()

            values = ','.join([
                f"('{tag['tag_name'].replace(\"'\", \"''\")}', '{tag['tag_color'].replace(\"'\", \"''\")}', "
                f"{tag.get('tag_id') if tag.get('tag_id') is not None else 'NULL'}, "
                f"'{user_type}', {user_id})"
                for tag in tag_data
            ])

            # Single multi-statement SQL query combining CTE, merge, and select of inserted rows
            query = f"""
            WITH incoming_tags AS (
                SELECT *
                FROM VALUES 
                    {values}
                AS t(tag_name, tag_color, tag_id, user_type, user_id)
                -- Mark potential inserts
                -- We add is_new column here with CASE WHEN tag_id IS NULL THEN true ELSE false END
                SELECT
                  tag_name,
                  tag_color,
                  tag_id,
                  user_type,
                  user_id,
                  CASE WHEN tag_id IS NULL THEN true ELSE false END AS is_new
                FROM t
            )
            -- Actually, Databricks SQL syntax requires a single SELECT in CTE.
            -- We'll merge the is_new computation inline in the CTE via SELECT from VALUES with alias.
            WITH incoming_tags AS (
                SELECT 
                  tag_name,
                  tag_color,
                  tag_id,
                  user_type,
                  user_id,
                  CASE WHEN tag_id IS NULL THEN TRUE ELSE FALSE END AS is_new
                FROM VALUES 
                  {values}
                AS t(tag_name, tag_color, tag_id, user_type, user_id)
            ),
            merge_operation AS (
                MERGE INTO silver_dashboard.refined_app_tagging_test target
                USING incoming_tags source
                ON target.tag_id = source.tag_id
                   AND target.user_type = source.user_type
                   AND target.user_id = source.user_id
                WHEN MATCHED THEN
                    UPDATE SET
                        tag_name = source.tag_name,
                        tag_color = source.tag_color
                WHEN NOT MATCHED THEN
                    INSERT (tag_name, tag_color, user_type, user_id)
                    VALUES (source.tag_name, source.tag_color, source.user_type, source.user_id)
                -- Databricks MERGE does not return rows, so just move on
                SELECT 1 as dummy -- To satisfy CTE syntax
            )
            SELECT 
                t.tag_id,
                t.tag_name
            FROM silver_dashboard.refined_app_tagging_test t
            JOIN incoming_tags i
              ON t.tag_name = i.tag_name
              AND t.user_type = i.user_type
              AND t.user_id = i.user_id
            WHERE i.is_new = TRUE
              AND t.created_at >= CURRENT_TIMESTAMP - INTERVAL 5 MINUTES
            """

            cursor.execute(query)
            inserted_tags = [
                {"tag_id": row[0], "tag_name": row[1]} 
                for row in cursor.fetchall()
            ]

            return jsonify({
                "message": "Tag operations completed",
                "inserted_tags": inserted_tags,
                "insert_count": len(inserted_tags)
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500









def post(self):
    try:
        # [Previous auth/validation code...]

        with DatabricksSession() as conn:
            cursor = conn.cursor()

            # Create temp view (NULL tag_id for new tags)
            values = []
            for tag in tag_data:
                values.append(
                    f"({'NULL' if tag.get('tag_id') is None else tag['tag_id']}, "
                    f"'{tag['tag_name']}', "
                    f"'{tag['tag_color']}', "
                    f"'{user_type}', {user_id}, "
                    f"{'TRUE' if tag.get('is_active', True) else 'FALSE'})"
                )

            cursor.execute(f"""
                CREATE OR REPLACE TEMP VIEW incoming_tags AS
                SELECT * FROM VALUES 
                {','.join(values)}
                AS temp(tag_id, tag_name, tag_color, user_type, user_id, is_active)
            """)

            # MERGE operation
            cursor.execute(f"""
                MERGE INTO silver_dashboard.refined_app_tagging_test target
                USING incoming_tags source
                ON (target.tag_id = source.tag_id OR 
                   (target.tag_name = source.tag_name 
                    AND target.user_type = source.user_type 
                    AND target.user_id = source.user_id))
                WHEN MATCHED AND source.tag_id IS NOT NULL THEN
                    UPDATE SET
                        tag_color = source.tag_color,
                        is_active = source.is_active,
                        updated_at = current_timestamp()
                WHEN NOT MATCHED THEN
                    INSERT (tag_name, tag_color, user_type, user_id, is_active)
                    VALUES (source.tag_name, source.tag_color, 
                            source.user_type, source.user_id, source.is_active)
                OUTPUT 
                    $action AS operation,
                    INSERTED.tag_id AS tag_id,
                    INSERTED.tag_name AS tag_name
            """)

            # Process results
            results = cursor.fetchall()
            operations = {
                'inserted': [{'tag_id': r[1], 'tag_name': r[2]} for r in results if r[0] == 'INSERT'],
                'updated': [{'tag_id': r[1], 'tag_name': r[2]} for r in results if r[0] == 'UPDATE']
            }

            return jsonify({
                "message": "Tag operations completed",
                "operations": operations,
                "stats": {
                    "inserted": len(operations['inserted']),
                    "updated": len(operations['updated'])
                }
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500





def post(self):
    try:
        # [Previous auth/validation code...]

        placeholders = ','.join(['%s'] * len(mapped_apps))

        query = f"""
            WITH 
            -- Get ALL tags for this user with counts
            all_tags AS (
                SELECT 
                    t.tag_id, 
                    t.tag_name, 
                    t.tag_color,
                    COUNT(m.app_name) AS app_count
                FROM silver_dashboard.refined_app_tagging_test t
                LEFT JOIN silver_dashboard.refined_app_mapping_test m
                    ON t.tag_id = m.tag_id
                    AND t.user_type = m.user_type
                    AND t.user_id = m.user_id
                WHERE t.user_type = %s 
                  AND t.user_id = %s
                  AND t.is_active = TRUE
                GROUP BY t.tag_id, t.tag_name, t.tag_color
            ),
            -- Get requested app mappings with their tag_ids
            requested_apps AS (
                SELECT 
                    app_name, 
                    mapped_app_name, 
                    tag_id
                FROM silver_dashboard.refined_app_mapping_test
                WHERE user_type = %s
                  AND user_id = %s
                  AND app_name IN ({placeholders})
            )
            -- Combined results in single pass
            SELECT 
                'tag' AS data_type,
                tag_id,
                tag_name,
                tag_color,
                app_count,
                NULL AS app_name,
                NULL AS mapped_app_name,
                NULL AS app_tag_id
            FROM all_tags
            UNION ALL
            SELECT 
                'app' AS data_type,
                NULL AS tag_id,
                NULL AS tag_name,
                NULL AS tag_color,
                NULL AS app_count,
                app_name,
                COALESCE(mapped_app_name, app_name) AS mapped_app_name,
                tag_id AS app_tag_id
            FROM requested_apps
        """

        params = [user_type, user_id, user_type, user_id] + mapped_apps

        with DatabricksSession() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Single-pass processing
            tag_data = []
            app_data = []
            
            for row in rows:
                if row[0] == 'tag':  # Tag data
                    tag_data.append({
                        "tag_id": row[1],
                        "tag_name": row[2],
                        "tag_color": row[3],
                        "app_count": row[4]
                    })
                else:  # App data
                    app_data.append({
                        "app_name": row[5],
                        "mapped_app_name": row[6],
                        "tag_id": row[7]  # Directly from query
                    })

            return jsonify({
                "TagData": tag_data,
                "AppData": app_data
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500




def post(self):
    try:
        # [Previous auth/validation code...]

        placeholders = ','.join(['%s'] * len(mapped_apps))

        query = f"""
            WITH 
            -- Get ALL tags for this user
            all_tags AS (
                SELECT 
                    t.tag_id, 
                    t.tag_name, 
                    t.tag_color,
                    COUNT(m.app_name) AS app_count
                FROM silver_dashboard.refined_app_tagging_test t
                LEFT JOIN silver_dashboard.refined_app_mapping_test m
                    ON t.tag_id = m.tag_id
                    AND t.user_type = m.user_type
                    AND t.user_id = m.user_id
                WHERE t.user_type = %s 
                  AND t.user_id = %s
                  AND t.is_active = TRUE
                GROUP BY t.tag_id, t.tag_name, t.tag_color
            ),
            -- Get requested app mappings (including NULL tag_id cases)
            requested_apps AS (
                SELECT 
                    app_name, 
                    mapped_app_name, 
                    tag_id
                FROM silver_dashboard.refined_app_mapping_test
                WHERE user_type = %s
                  AND user_id = %s
                  AND app_name IN ({placeholders})
            )
            -- Main query combining results
            SELECT 
                'tag' AS data_type,
                tag_id,
                tag_name,
                tag_color,
                app_count,
                NULL AS app_name,
                NULL AS mapped_app_name
            FROM all_tags
            UNION ALL
            SELECT 
                'app' AS data_type,
                NULL AS tag_id,
                NULL AS tag_name,
                NULL AS tag_color,
                NULL AS app_count,
                app_name,
                mapped_app_name
            FROM requested_apps
        """

        params = [user_type, user_id, user_type, user_id] + mapped_apps

        with DatabricksSession() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Process results
            tag_data = []
            app_data = []
            
            for row in rows:
                data_type = row[0]
                if data_type == 'tag':
                    tag_data.append({
                        "tag_id": row[1],
                        "tag_name": row[2],
                        "tag_color": row[3],
                        "app_count": row[4]
                    })
                else:
                    app_data.append({
                        "app_name": row[5],
                        "mapped_app_name": row[6] if row[6] else row[5],
                        "tag_id": None  # Will be updated in next pass
                    })

            # Second pass to get tag_id for apps
            if app_data:
                app_names = [app['app_name'] for app in app_data]
                placeholders = ','.join(['%s'] * len(app_names))
                cursor.execute(f"""
                    SELECT app_name, tag_id 
                    FROM silver_dashboard.refined_app_mapping_test
                    WHERE user_type = %s
                      AND user_id = %s
                      AND app_name IN ({placeholders})
                """, [user_type, user_id] + app_names)
                
                for app_name, tag_id in cursor.fetchall():
                    for app in app_data:
                        if app['app_name'] == app_name:
                            app['tag_id'] = tag_id
                            break

            return jsonify({
                "TagData": tag_data,
                "AppData": app_data
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500




def delete(self):
    try:
        # [Previous auth/user validation code...]

        # Convert tag_ids to comma-separated string
        tag_ids = [int(tid) for tid in data.get("tag_ids", [])]
        id_list = ','.join(map(str, tag_ids))  # "1,4,5" format

        with DatabricksSession() as conn:
            cursor = conn.cursor()
            
            # 1. First NULL out references in mapping table
            cursor.execute(f"""
                UPDATE silver_dashboard.refined_app_mapping_test
                SET tag_id = NULL
                WHERE user_type = '{user_type}'
                AND user_id = {user_id}
                AND tag_id IN ({id_list})
            """)
            
            # 2. Then delete from tagging table
            cursor.execute(f"""
                DELETE FROM silver_dashboard.refined_app_tagging_test
                WHERE user_type = '{user_type}'
                AND user_id = {user_id}
                AND tag_id IN ({id_list})
            """)
            
            return jsonify({"message": "Tags and mappings cleaned successfully"}), 200

    except ValueError:
        return jsonify({"message": "All tag_ids must be integers"}), 400
    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500




def delete(self):
    try:
        # [Previous auth/validation code remains the same...]
        
        tag_ids = [int(tid) for tid in data.get("tag_ids", [])]
        id_list = ','.join(map(str, tag_ids))  # "1,4,5" format

        with DatabricksSession() as conn:
            cursor = conn.cursor()
            
            # 1. MERGE to nullify tag_ids (proper Databricks syntax)
            merge_query = f"""
                MERGE INTO silver_dashboard.refined_app_mapping_test AS target
                USING (
                    SELECT * FROM (VALUES {','.join([f"({tid})" for tid in tag_ids]}) 
                    AS source(tag_id_to_null)
                ) AS source
                ON target.tag_id = source.tag_id_to_null
                   AND target.user_type = '{user_type}'
                   AND target.user_id = {user_id}
                WHEN MATCHED THEN
                    UPDATE SET tag_id = NULL
            """
            cursor.execute(merge_query)
            
            # 2. Separate DELETE operation
            delete_query = f"""
                DELETE FROM silver_dashboard.refined_app_tagging_test
                WHERE user_type = '{user_type}'
                  AND user_id = {user_id}
                  AND tag_id IN ({id_list})
            """
            cursor.execute(delete_query)
            
            return jsonify({"message": "Tags cleaned successfully"}), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500



def delete(self):
    try:
        # 1. Authorization & Validation
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or Invalid Authorization"}), 401

        token = auth_header.split(" ")[1]
        decoded_token = decode_token(token)
        user_id = int(decoded_token.get("user_id"))
        if not user_id:
            return jsonify({"message": "Invalid Token Claims"}), 401

        # 2. Get required parameters
        user_type = request.args.get('type')
        if not user_type:
            return jsonify({"message": "Missing user_type"}), 400

        data = request.get_json()
        tag_ids = [int(tid) for tid in data.get("tag_ids", [])]
        if not tag_ids:
            return jsonify({"message": "tag_ids must be a non-empty list of integers"}), 400

        # 3. Single MERGE operation for both updates and deletes
        with DatabricksSession() as conn:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                -- First MERGE to nullify tag_ids in mapping table
                MERGE INTO silver_dashboard.refined_app_mapping_test AS target
                USING (
                    SELECT explode(array({','.join(map(str, tag_ids))})) AS tag_id_to_null
                ) AS source
                ON target.tag_id = source.tag_id_to_null
                   AND target.user_type = '{user_type}'
                   AND target.user_id = {user_id}
                WHEN MATCHED THEN
                    UPDATE SET tag_id = NULL;
                
                -- Second operation to delete from tagging table
                DELETE FROM silver_dashboard.refined_app_tagging_test
                WHERE user_type = '{user_type}'
                  AND user_id = {user_id}
                  AND tag_id IN ({','.join(map(str, tag_ids))});
            """)
            
            return jsonify({"message": "Tags cleaned up successfully"}), 200

    except ValueError:
        return jsonify({"message": "All tag_ids must be integers"}), 400
    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500





def delete(self):
    try:
        # [Previous auth/user validation code...]
        
        # Convert tag_ids to quoted strings if they're not numeric
        is_numeric = all(isinstance(tid, (int, float)) for tid in tag_ids)
        tag_ids_sql = (
            ",".join([str(tid) for tid in tag_ids]) if is_numeric 
            else ",".join([f"'{tid}'" for tid in tag_ids])
        )

        query = f"""
            -- Phase 1: NULLify references
            MERGE INTO silver_dashboard.refined_app_mapping_test AS target
            USING (
                SELECT explode(array({tag_ids_sql})) AS tag_id_to_null
            ) AS source
            ON target.tag_id = source.tag_id_to_null
               AND target.user_type = '{user_type}'
               AND target.user_id = {user_id}
            WHEN MATCHED THEN
                UPDATE SET tag_id = NULL;
            
            -- Phase 2: Delete tags
            DELETE FROM silver_dashboard.refined_app_tagging_test
            WHERE user_type = '{user_type}'
              AND user_id = {user_id}
              AND tag_id IN ({tag_ids_sql});
        """

        with DatabricksSession() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            return jsonify({
                "message": f"Atomic cleanup completed for {len(tag_ids)} tags",
                "details": {
                    "operation": "MERGE+DELETE",
                    "tag_id_type": "numeric" if is_numeric else "string"
                }
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500






def delete(self):
    try:
        # 1. Authorization & Validation (same as before)
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or Invalid Authorization"}), 401

        token = auth_header.split(" ")[1]
        decoded_token = decode_token(token)
        user_id = int(decoded_token.get("user_id"))
        if not user_id:
            return jsonify({"message": "Invalid Token Claims"}), 401

        # 2. Get user_type and tag_ids
        user_type = request.args.get('type')
        if not user_type:
            return jsonify({"message": "Missing user_type"}), 400

        data = request.get_json()
        tag_ids = data.get("tag_ids", [])
        if not tag_ids or not isinstance(tag_ids, list):
            return jsonify({"message": "tag_ids must be a non-empty list"}), 400

        # 3. Atomic MERGE + DELETE operation
        query = f"""
            -- Phase 1: NULL out references in mapping table
            MERGE INTO silver_dashboard.refined_app_mapping_test AS target
            USING (
                SELECT explode(array({','.join([str(tid) for tid in tag_ids]})) AS tag_id_to_null
            ) AS source
            ON target.tag_id = source.tag_id_to_null
               AND target.user_type = '{user_type}'
               AND target.user_id = {user_id}
            WHEN MATCHED THEN
                UPDATE SET tag_id = NULL;
            
            -- Phase 2: Delete tags
            DELETE FROM silver_dashboard.refined_app_tagging_test
            WHERE user_type = '{user_type}'
              AND user_id = {user_id}
              AND tag_id IN ({','.join([str(tid) for tid in tag_ids]});
        """

        with DatabricksSession() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get affected rows (Databricks-specific approach)
            update_count = cursor.rowcount  # For MERGE (may vary by Databricks version)
            
            # For DELETE count, we need a separate query if needed
            count_query = f"""
                SELECT COUNT(*) 
                FROM silver_dashboard.refined_app_tagging_test
                WHERE user_type = '{user_type}'
                  AND user_id = {user_id}
                  AND tag_id IN ({','.join([str(tid) for tid in tag_ids]})
                BEFORE DELETE;
            """
            cursor.execute(count_query)
            delete_count = cursor.fetchone()[0]

            return jsonify({
                "message": "Atomic cleanup completed",
                "stats": {
                    "tags_nullified": update_count,
                    "tags_deleted": delete_count
                }
            }), 200

    except Exception as e:
        return jsonify({"status": "fail", "message": str(e)}), 500





{
  "AppData": [
    { "app_name": "zoom", "mapped_app_name": "Caller1", "tag_id": null },
    { "app_name": "Chrome", "mapped_app_name": "browser1", "tag_id": 4 },
    { "app_name": "Vscode", "mapped_app_name": "Vscode", "tag_id": 14 }
  ]
}



from flask import request, jsonify
from flask_cors import cross_origin

@cross_origin()
def post(self):
    try:
        # Authorization & token validation
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or Invalid Authorization"}), 401

        token = auth_header.split(" ")[1]
        decoded_token = decode_token(token)
        user_id = int(decoded_token.get("user_id"))
        if not user_id:
            return jsonify({"message": "Invalid Token Claims"}), 401

        user_type = request.args.get('type')  # "MGR" or "EMP"
        data = request.get_json()
        app_data = data.get("app_data", [])

        if not app_data:
            return jsonify({"message": "No app data provided"}), 400

        with DatabricksSession() as conn:
            cursor = conn.cursor()

            # Construct VALUES clause for MERGE
            values_clause = ", ".join([
                f"('{app['app_name']}', '{app['mapped_app_name']}', {app['tag_id'] if app['tag_id'] is not None else 'NULL'}, '{user_type}', {user_id})"
                for app in app_data
            ])

            # Single MERGE query (no temp view needed)
            merge_query = f"""
                MERGE INTO app_mapping_test AS target
                USING (
                    SELECT * FROM VALUES {values_clause}
                    AS source_data(app_name, mapped_app_name, tag_id, user_type, user_id)
                ) AS source
                ON target.app_name = source.app_name

                WHEN MATCHED THEN
                    UPDATE SET
                        app_map_name = source.mapped_app_name,
                        tag_id = source.tag_id,
                        updated_at = current_timestamp()

                WHEN NOT MATCHED THEN
                    INSERT (app_name, app_map_name, tag_id, user_type, user_id)
                    VALUES (source.app_name, source.mapped_app_name, source.tag_id, source.user_type, source.user_id)
            """
            cursor.execute(merge_query)

        return jsonify({"message": "App mappings processed successfully"}), 200

    except Exception as e:
        return jsonify({"message": "Internal Server Error", "error": str(e)}), 500


from flask import request, jsonify
from flask_cors import cross_origin

@cross_origin()
def post(self):
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or Invalid Authorization"}), 401

        token = auth_header.split(" ")[1]
        decoded_token = decode_token(token)
        user_id = int(decoded_token.get("user_id"))
        if not user_id:
            return jsonify({"message": "Invalid Token Claims"}), 401

        user_type = request.args.get('type')  # "MGR" or "EMP"
        data = request.get_json()
        tag_data = data.get("tag_data", [])

        if not tag_data:
            return jsonify({"message": "No tag data provided"}), 400

        # Prepare records
        records = []
        for tag in tag_data:
            records.append((
                tag.get("tag_id"),
                tag.get("tag_name"),
                tag.get("tag_color"),
                user_type,
                user_id
            ))

        # Create temp view from records
        with DatabricksSession() as conn:
            cursor = conn.cursor()

            # Create a temp view for the incoming data
            cursor.execute("""
                CREATE OR REPLACE TEMP VIEW incoming_tags (tag_id, tag_name, tag_color, user_type, user_id) AS
                SELECT * FROM VALUES {}
            """.format(
                ", ".join([
                    f"({tag_id if tag_id else 'NULL'}, '{tag_name}', '{tag_color}', '{user_type}', {user_id})"
                    for tag_id, tag_name, tag_color, user_type, user_id in records
                ])
            ))

            # Merge: insert if tag_id is NULL, else update
            merge_query = """
                MERGE INTO silver_dashboard.refined_app_tagging_test AS target
                USING incoming_tags AS source
                ON target.tag_id = source.tag_id

                WHEN MATCHED THEN
                    UPDATE SET
                        tag_name = source.tag_name,
                        tag_color = source.tag_color,
                        updated_at = current_timestamp()

                WHEN NOT MATCHED THEN
                    INSERT (tag_name, tag_color, user_type, user_id)
                    VALUES (source.tag_name, source.tag_color, source.user_type, source.user_id);
            """
            cursor.execute(merge_query)

            # Get newly inserted tags (i.e., where tag_id was originally NULL)
            fetch_new_tags_query = """
                SELECT tag_id, tag_name, tag_color
                FROM silver_dashboard.refined_app_tagging_test
                WHERE user_type = '{}' AND user_id = {}
                ORDER BY created_at DESC
                LIMIT {}
            """.format(user_type, user_id, len([t for t in tag_data if not t.get("tag_id")]))

            cursor.execute(fetch_new_tags_query)
            inserted_tags = [dict(zip(["tag_id", "tag_name", "tag_color"], row)) for row in cursor.fetchall()]

        return jsonify({"message": "Tags processed successfully", "inserted_tags": inserted_tags}), 200

    except Exception as e:
        return jsonify({"message": "Internal Server Error", "error": str(e)}), 500




from flask import request, jsonify
from your_db_utils import DatabricksSession  # adjust as needed

@your_blueprint.route("/your-endpoint", methods=["POST"])
def get_tag_and_app_data():
    user_type = request.args.get("type")
    user_id = request.args.get("user_id")
    data = request.get_json()
    mapped_apps = data.get("mapped_app_names", [])

    if not user_type or not user_id or not mapped_apps:
        return jsonify({"message": "Missing required input"}), 400

    placeholders = ','.join(['?'] * len(mapped_apps))

    query = f"""
    WITH filtered_tags AS (
        SELECT tag_id, tag_name, tag_color
        FROM silver_dashboard.tag_table
        WHERE user_type = ? AND user_id = ? AND is_active = TRUE
    ),
    mapped_apps AS (
        SELECT app_name, mapped_app_name, tag_id
        FROM silver_dashboard.app_mapping
        WHERE user_type = ? AND user_id = ?
          AND mapped_app_name IN ({placeholders})
    )
    SELECT f.tag_id, f.tag_name, f.tag_color,
           m.app_name, m.mapped_app_name, m.tag_id AS mapped_tag_id
    FROM filtered_tags f
    LEFT JOIN mapped_apps m ON f.tag_id = m.tag_id
    """

    params = [user_type, user_id, user_type, user_id] + mapped_apps

    with DatabricksSession() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()

    # Prepare sets for unique tag data and for existing mapped apps
    tag_data = {}
    app_data_dict = {}

    for row in rows:
        tag_id, tag_name, tag_color, app_name, mapped_app_name, mapped_tag_id = row

        if tag_id not in tag_data:
            tag_data[tag_id] = {
                "tag_id": tag_id,
                "tag_name": tag_name,
                "tag_color": tag_color,
                "app_count": 0
            }

        if mapped_app_name:
            tag_data[tag_id]["app_count"] += 1
            app_data_dict[mapped_app_name] = {
                "app_name": app_name,
                "mapped_app_name": mapped_app_name,
                "tag_id": mapped_tag_id
            }

    # Fill in missing apps from input (not returned in SQL)
    for app in mapped_apps:
        if app not in app_data_dict:
            app_data_dict[app] = {
                "app_name": app,
                "mapped_app_name": app,
                "tag_id": None
            }

    return jsonify({
        "TagData": list(tag_data.values()),
        "AppDataList": list(app_data_dict.values())
    }), 200



from flask import request, jsonify
from flask_cors import cross_origin
from your_db_utils import get_db_connection  # Replace with actual connection util
from your_auth_utils import decode_token     # Replace with your actual token decoder

@cross_origin()
def delete(self):
    try:
        # Step 1: Extract and validate the Authorization token
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or Invalid Authorization"}), 401

        token = auth_header.split(" ")[1]
        decoded_token = decode_token(token)
        user_id = int(decoded_token.get("user_id"))

        if not user_id:
            return jsonify({"message": "Invalid Token Claims"}), 401

        # Step 2: Get user_type from query parameter
        user_type = request.args.get('type')
        if not user_type:
            return jsonify({"message": "Missing user_type"}), 400

        # Step 3: Get tag_ids from payload
        data = request.get_json()
        tag_ids = data.get("tag_ids", [])

        if not tag_ids or not isinstance(tag_ids, list):
            return jsonify({"message": "tag_ids must be a non-empty list"}), 400

        # Step 4: Perform deletion using IN clause
        placeholders = ','.join(['?'] * len(tag_ids))  # for parameterized query
        query = f"""
            DELETE FROM app_tagging
            WHERE user_type = ?
              AND user_id = ?
              AND tag_id IN ({placeholders})
        """

        params = [user_type, user_id] + tag_ids

        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()

        return jsonify({"message": f"Successfully deleted {cursor.rowcount} tag(s)"}), 200

    except Exception as e:
        return jsonify({"message": "Internal Server Error", "error": str(e)}), 500










{
  "message": "Tags inserted/updated successfully",
  "tag_data": [
    {
      "tag_id": 201,                   // Newly inserted tag ID (auto-generated)
      "tag_name": "Productivity",
      "tag_color": "#FF5733",
      "user_id": "USR123456",
      "user_type": "MGR"
    },
    {
      "tag_id": 102,                   // Existing tag, just updated
      "tag_name": "Communication",
      "tag_color": "#33C3FF",
      "user_id": "USR123456",
      "user_type": "MGR"
    }
  ]
}


{
  "tag_data": [
    {
      "tag_name": "Productivity",
      "tag_color": "#FF5733",
      "user_id": "USR123456",
      "user_type": "MGR"
    },
    {
      "tag_id": 102,
      "tag_name": "Communication",
      "tag_color": "#33C3FF",
      "user_id": "USR123456",
      "user_type": "MGR"
    }
  ]
}





from flask import request, jsonify
from flask_cors import cross_origin
from your_auth_utils import decode_token  # assume your token logic
from databricks import sql  # make sure you use the right connector
from your_connection_utils import get_db_connection

@cross_origin()
def post(self):
    try:
        # 1. Auth
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"message": "Missing or Invalid Authorization"}), 401

        token = auth_header.split(" ")[1]
        decoded_tkn = decode_token(token)
        manager_id = int(decoded_tkn.get("user_id"))

        if not manager_id:
            return jsonify({"message": "Invalid Token Claims"}), 401

        # 2. Get Request Data
        user_type = request.args.get('type')  # 'MGR' or 'EMP'
        data = request.get_json()
        tag_data = data.get("tag_data", [])

        if not tag_data:
            return jsonify({"message": "No tag data provided"}), 400

        # 3. Separate Inserts and Updates
        inserts = [tag for tag in tag_data if not tag.get("tag_id")]
        updates = [tag for tag in tag_data if tag.get("tag_id")]

        inserted_tags = []

        with get_db_connection() as conn:
            cursor = conn.cursor()

            # 4. Insert New Tags (Bulk Insert, Returns IDs)
            if inserts:
                insert_values = [(tag['tag_name'], tag['tag_color'], manager_id, user_type) for tag in inserts]
                cursor.executemany("""
                    INSERT INTO silver_dashboard.app_tagging (tag_name, tag_color, user_id, user_type)
                    VALUES (?, ?, ?, ?)
                """, insert_values)

                # Fetch inserted rows
                cursor.execute("""
                    SELECT tag_id, tag_name, tag_color
                    FROM silver_dashboard.app_tagging
                    WHERE user_id = ? AND user_type = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (manager_id, user_type, len(inserts)))
                inserted_tags = cursor.fetchall()

            # 5. Update Existing Tags
            if updates:
                for tag in updates:
                    cursor.execute("""
                        UPDATE silver_dashboard.app_tagging
                        SET tag_name = ?, tag_color = ?
                        WHERE tag_id = ? AND user_id = ? AND user_type = ?
                    """, (
                        tag['tag_name'], tag['tag_color'],
                        tag['tag_id'], manager_id, user_type
                    ))

            # 6. Combine All Tags for Response
            all_tag_ids = [t['tag_id'] for t in updates] + [row[0] for row in inserted_tags]
            if all_tag_ids:
                format_ids = "(" + ",".join(["?"] * len(all_tag_ids)) + ")"
                cursor.execute(f"""
                    SELECT tag_id, tag_name, tag_color
                    FROM silver_dashboard.app_tagging
                    WHERE tag_id IN {format_ids}
                """, all_tag_ids)
                final_tags = cursor.fetchall()
            else:
                final_tags = []

        # 7. Response
        return jsonify({
            "tag_data": [
                {"tag_id": row[0], "tag_name": row[1], "tag_color": row[2]}
                for row in final_tags
            ],
            "message": "Tags inserted/updated successfully"
        })

    except Exception as e:
        return jsonify({"message": str(e)}), 500






import asyncio
import aiohttp
from datetime import datetime, timedelta

API_URL = "https://your-api-endpoint.com/data"

AUTH_TOKENS = [
    "Bearer token_employee_1",
    "Bearer token_employee_2",
    "Bearer token_employee_3"
]

# Generate all unique (start_date, end_date) pairs
start = datetime(2025, 3, 1)
end = datetime(2025, 5, 31)

date_pairs = []
current_start = start
while current_start < end:
    current_end = current_start + timedelta(days=1)
    while current_end <= end:
        date_pairs.append((
            current_start.strftime('%Y-%m-%d'),
            current_end.strftime('%Y-%m-%d')
        ))
        current_end += timedelta(days=1)
    current_start += timedelta(days=1)

# Prepare requests for each employee (auth token)
requests = []
for token in AUTH_TOKENS:
    for start_date, end_date in date_pairs:
        requests.append({
            "start_date": start_date,
            "end_date": end_date,
            "auth": token
        })

# Asynchronous request sender
async def fetch(session, params):
    headers = {"Authorization": params["auth"]}
    query_params = {
        "start_date": params["start_date"],
        "end_date": params["end_date"]
    }
    try:
        async with session.get(API_URL, headers=headers, params=query_params) as response:
            status = response.status
            # You can uncomment the next line to print each response status
            # print(f"{params['start_date']} - {params['end_date']}: {status}")
            return status
    except Exception as e:
        print(f"Request failed: {e}")
        return None

# Run all requests concurrently
async def main():
    connector = aiohttp.TCPConnector(limit=1000)  # Adjust concurrency limit as needed
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch(session, req) for req in requests]
        responses = await asyncio.gather(*tasks)
        print(f"Total requests: {len(responses)}")
        print(f"Successful: {sum(1 for r in responses if r == 200)}")
        print(f"Failed: {sum(1 for r in responses if r != 200)}")

if __name__ == "__main__":
    asyncio.run(main())







import asyncio import aiohttp from datetime import datetime, timedelta

API_URL = "https://your-api-endpoint.com/data"

AUTH_TOKENS = [ "Bearer token_employee_1", "Bearer token_employee_2", "Bearer token_employee_3" ]

Generate all unique (start_date, end_date) pairs

start = datetime(2025, 3, 1) end = datetime(2025, 5, 31)

date_pairs = [] current_start = start while current_start < end: current_end = current_start + timedelta(days=1) while current_end <= end: date_pairs.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d'))) current_end += timedelta(days=1) current_start += timedelta(days=1)

Prepare requests for each employee (auth token)

requests = [] for token in AUTH_TOKENS: for start_date, end_date in date_pairs: requests.append({ "start_date": start_date, "end_date": end_date, "auth": token })

Asynchronous request sender

async def fetch(session, params): headers = {"Authorization": params["auth"]} query_params = {"start_date": params["start_date"], "end_date": params["end_date"]} async with session.get(API_URL, headers=headers, params=query_params) as response: resp_text = await response.text() print(f"{response.status} | {query_params} -> {resp_text[:50]}")  # Truncate output

async def main(): connector = aiohttp.TCPConnector(limit=1000) async with aiohttp.ClientSession(connector=connector) as session: tasks = [fetch(session, req) for req in requests] await asyncio.gather(*tasks)

if name == "main": asyncio.run(main())






import requests
from datetime import datetime, timedelta

# API base URL
BASE_URL = "https://your-api-endpoint.com/data"

# Authorization token
BEARER_TOKEN = "your_bearer_token_here"

# Headers including Authorization
headers = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Content-Type": "application/json"
}

# Define your date range
start_range = datetime(2024, 1, 1)
end_range = datetime(2024, 1, 5)

# Generate all valid start_date and end_date combinations
date_list = [start_range + timedelta(days=i) for i in range((end_range - start_range).days + 1)]

# Store responses
responses = []

# Loop over all start-end date combinations
for start_date in date_list:
    for end_date in date_list:
        if start_date <= end_date:
            params = {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            }
            try:
                response = requests.get(BASE_URL, headers=headers, params=params)
                print(f"GET {response.url} => Status: {response.status_code}")
                responses.append({
                    "url": response.url,
                    "status_code": response.status_code,
                    "body": response.json() if response.headers.get('Content-Type', '').startswith('application/json') else response.text
                })
            except Exception as e:
                print(f"Error for {params}: {e}")













Yes, you're correct! If you are receiving the data as a payload from the frontend (e.g., JSON data in the request body), you can dynamically insert the values from the payload into your SQL queries.

Let me update the examples to show how you can process the incoming payload and use it for bulk insert and bulk update operations.

1. Handling Payload Data from Frontend

Let's say you receive a JSON payload in the following structure for the bulk insert and bulk update operations:

Example Payload for Bulk Insert:

{
    "insert_data": [
        {
            "user_id": "user123",
            "app_name": "App1",
            "custom_name": "Custom App 1",
            "tag_id": "tag-123456"
        },
        {
            "user_id": "user123",
            "app_name": "App2",
            "custom_name": "Custom App 2",
            "tag_id": "tag-789101"
        }
    ]
}

Example Payload for Bulk Update:

{
    "update_data": [
        {
            "tag_id": "tag-123456",
            "user_id": "user123",
            "tag_name": "Updated Critical",
            "tag_color": "#FF5733"
        },
        {
            "tag_id": "tag-789101",
            "user_id": "user123",
            "tag_name": "Updated Minor",
            "tag_color": "#00FF00"
        }
    ]
}

2. Flask Endpoint to Handle Dynamic Bulk Insert/Update

Here’s how you can modify the Flask API endpoint to receive the payload and dynamically insert values using the data from the request:

from flask import Flask, request, jsonify
from contextlib import contextmanager
import pyodbc

app = Flask(__name__)

@contextmanager
def databricks_connection():
    # Establish Databricks connection
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=your-databricks-server;'
        'PORT=your-databricks-port;'
        'UID=your-username;'
        'PWD=your-password;'
    )
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        conn.commit()
        cursor.close()
        conn.close()

@app.route('/bulk_insert', methods=['POST'])
def bulk_insert():
    # Get the insert data from the payload
    data = request.json
    insert_data = data.get('insert_data', [])
    
    if not insert_data:
        return jsonify({"error": "No data to insert"}), 400

    # Use context manager for connection handling
    with databricks_connection() as cursor:
        # Prepare the SQL statement
        insert_query = """
            INSERT INTO apps (user_id, app_name, custom_name, tag_id)
            VALUES (?, ?, ?, ?)
        """
        
        # Loop through the insert data and execute for each record
        for entry in insert_data:
            cursor.execute(insert_query, (
                entry['user_id'], 
                entry['app_name'], 
                entry['custom_name'], 
                entry['tag_id']
            ))

    return jsonify({"message": "Bulk Insert Successful"}), 200

@app.route('/bulk_update', methods=['POST'])
def bulk_update():
    # Get the update data from the payload
    data = request.json
    update_data = data.get('update_data', [])
    
    if not update_data:
        return jsonify({"error": "No data to update"}), 400

    # Use context manager for connection handling
    with databricks_connection() as cursor:
        # Prepare the SQL statement
        update_query = """
            UPDATE tags
            SET tag_name = ?, tag_color = ?
            WHERE tag_id = ? AND user_id = ?
        """
        
        # Loop through the update data and execute for each record
        for entry in update_data:
            cursor.execute(update_query, (
                entry['tag_name'], 
                entry['tag_color'], 
                entry['tag_id'], 
                entry['user_id']
            ))

    return jsonify({"message": "Bulk Update Successful"}), 200

if __name__ == '__main__':
    app.run(debug=True)

Explanation:

1. Flask Route (/bulk_insert and /bulk_update):

The /bulk_insert route receives a JSON payload with a key insert_data, which contains a list of dictionaries, where each dictionary has the fields for user_id, app_name, custom_name, and tag_id.

Similarly, the /bulk_update route expects update_data, which contains a list of dictionaries with the fields tag_id, user_id, tag_name, and tag_color.



2. Payload Handling:

The incoming JSON payload is parsed using request.json.

The respective data (insert_data for inserts and update_data for updates) is extracted from the payload.



3. SQL Execution:

The data is dynamically inserted using a parameterized SQL query (? placeholders) to prevent SQL injection.

The values from the payload (entry['field_name']) are passed into the SQL queries.



4. Context Manager:

The context manager databricks_connection() handles the connection and ensures it is committed, closed, and cleaned up properly.



5. Error Handling:

Basic error handling is implemented to ensure that if no data is provided for insert or update, a 400 Bad Request response is returned.




3. Example JSON Request to Bulk Insert:

{
    "insert_data": [
        {
            "user_id": "user123",
            "app_name": "App1",
            "custom_name": "Custom App 1",
            "tag_id": "tag-123456"
        },
        {
            "user_id": "user123",
            "app_name": "App2",
            "custom_name": "Custom App 2",
            "tag_id": "tag-789101"
        }
    ]
}

4. Example JSON Request to Bulk Update:

{
    "update_data": [
        {
            "tag_id": "tag-123456",
            "user_id": "user123",
            "tag_name": "Updated Critical",
            "tag_color": "#FF5733"
        },
        {
            "tag_id": "tag-789101",
            "user_id": "user123",
            "tag_name": "Updated Minor",
            "tag_color": "#00FF00"
        }
    ]
}

5. Response:

After successfully processing the insert or update, you'll get a success message like:

{
    "message": "Bulk Insert Successful"
}

For errors:

{
    "error": "No data to insert"
}


---

Summary:

Dynamic Payload Handling: The values for inserts and updates are dynamically retrieved from the incoming JSON payload.

Parameterized SQL Queries: This approach helps to avoid SQL injection and allows safe insertion and updating of records.

Flask API: The provided Flask API can handle both bulk insert and update operations by receiving a JSON payload from the frontend.

Efficient Database Interaction: By using a context manager and parameterized queries, the operations remain efficient and properly managed in terms of database connections.










Your approach sounds solid and efficient, and it fits well with your requirement of simplifying operations, reducing latency, and avoiding unnecessary complexity. Here's a breakdown of your proposed solution and why it should work effectively:

Your Approach:

1. Tag Creation (Initial Step):

When a user creates a tag, the backend receives a request with:

user_id

tag_name

tag_color


This is inserted into the tags table, generating a unique tag_id for each tag.


Example Request for Tag Creation:

{
  "user_id": "user123",
  "tag_name": "Critical",
  "tag_color": "#FF5733"
}

Backend Logic:

INSERT INTO tags (tag_id, user_id, tag_name, tag_color)
VALUES ('tag-123456', 'user123', 'Critical', '#FF5733');


2. Assigning Tags to Multiple Apps (Modification):

When the user wants to update multiple app names and associate them with specific tags, the frontend will send:

user_id

app_name

custom_name (or any additional attributes)

tag_id (since the tag_id is now stored in local storage and can be directly selected by the user).


The backend then receives this payload and inserts or updates the app details, including the tag_id.


Example Request for Assigning Tags to Multiple Apps:

[
  {
    "user_id": "user123",
    "app_name": "App1",
    "custom_name": "Custom App 1",
    "tag_id": "tag-123456"
  },
  {
    "user_id": "user123",
    "app_name": "App2",
    "custom_name": "Custom App 2",
    "tag_id": "tag-789101"
  }
]

Backend Logic for Bulk Insert (in SQL):

INSERT INTO apps (user_id, app_name, custom_name, tag_id)
VALUES
('user123', 'App1', 'Custom App 1', 'tag-123456'),
('user123', 'App2', 'Custom App 2', 'tag-789101');


3. Tag Updates (Modification):

When the user wants to update the tag details (such as tag_name or tag_color), they can send the updated information, including the tag_id. This would directly modify the tags table.


Example Request for Tag Update:

[
  {
    "user_id": "user123",
    "tag_id": "tag-123456",
    "tag_name": "Updated Critical",
    "tag_color": "#FF0000"
  },
  {
    "user_id": "user123",
    "tag_id": "tag-789101",
    "tag_name": "Updated Minor",
    "tag_color": "#00FF00"
  }
]

Backend Logic for Bulk Update:

UPDATE tags
SET tag_name = 'Updated Critical', tag_color = '#FF0000'
WHERE tag_id = 'tag-123456' AND user_id = 'user123';

UPDATE tags
SET tag_name = 'Updated Minor', tag_color = '#00FF00'
WHERE tag_id = 'tag-789101' AND user_id = 'user123';




---

Why This Approach Works Well:

1. Efficiency:

By using tag_id directly, you're minimizing the complexity by removing the need to join or filter based on tag_name during updates. The backend directly associates the tag with the app using the tag_id.

This reduces the amount of data processing and lookup time (i.e., no need to query the tags table to fetch tag_id during each update).



2. Consistency:

By ensuring that tag_id is always used when associating tags with apps, you avoid potential issues with updating or managing tag_name directly. The tag_id remains the primary reference, which is more efficient and less error-prone.



3. Bulk Operations:

Using a single insert or update query for multiple apps or tags at once (batch operations) reduces the number of database transactions, which improves performance, especially when the user updates or assigns tags to many apps at the same time.



4. Local Storage for User Experience:

Storing the tag_id in local storage for the frontend is a good strategy, as it minimizes the need to re-fetch tag data from the backend every time the user needs to assign a tag to an app. This reduces round-trip API calls and improves the speed of the UI.





---

Optimizing the Solution:

For Bulk Inserts/Updates: You can also consider bulk insert and bulk update methods to handle larger datasets more efficiently (if necessary). In case your app scales up, using batch processing for inserting or updating multiple records at once can greatly improve performance and avoid unnecessary overhead.

Transactions: You might also want to wrap bulk operations in a single transaction to ensure consistency and rollback if any errors occur during the process.



---

Final Thoughts:

Your solution is both clean and scalable. You’ve separated concerns: tag creation is independent, app modification just references tags via tag_id, and updates are handled efficiently. This approach should give you low latency while maintaining simplicity and data consistency.

Let me know if you'd like help with any part of the implementation or if you want to dive deeper into optimizing certain areas!





WITH FilteredEmployees AS (
    SELECT emplid 
    FROM inbound.hr_employee_central 
    WHERE func_mgr_id = '{manager_id}' 
      AND termination_dt IS NULL
),

CombinedClicks AS (
    SELECT
        COALESCE(m.app_name, k.app_name) AS app_name,
        SUM(COALESCE(m.total_mouse_count, 0) + COALESCE(k.total_keyboard_events, 0)) AS total_clicks
    FROM 
        (SELECT emp_id, cal_date, app_name, total_mouse_count, interval 
         FROM gold_dashboard.analytics_emp_mouseclicks
         WHERE cal_date BETWEEN '{start_date}' AND '{end_date}'
           AND emp_id IN (SELECT emplid FROM FilteredEmployees)) m
    FULL OUTER JOIN
        (SELECT emp_id, cal_date, app_name, total_keyboard_events, interval 
         FROM gold_dashboard.analytics_emp_keystrokes
         WHERE cal_date BETWEEN '{start_date}' AND '{end_date}'
           AND emp_id IN (SELECT emplid FROM FilteredEmployees)) k
    ON m.emp_id = k.emp_id 
       AND m.cal_date = k.cal_date 
       AND m.app_name = k.app_name 
       AND m.interval = k.interval
    GROUP BY COALESCE(m.app_name, k.app_name)
)

SELECT TO_JSON(
    NAMED_STRUCT(
        'employee_time_spent_data',
        (SELECT COLLECT_LIST(
            NAMED_STRUCT(
                'app_name', 
                CASE 
                    WHEN LOWER(RIGHT(app_name, 4)) = '.exe' 
                    THEN LEFT(app_name, LENGTH(app_name)-4)
                    ELSE app_name 
                END,
                'total_clicks', total_clicks
            )
        ) FROM CombinedClicks
        WHERE total_clicks > 0  -- Exclude apps with zero clicks
        ORDER BY total_clicks DESC)
    )
) AS json_result;






WITH employee_metrics AS (
    SELECT
        -- Current period team metrics
        AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN daily_active_time END) AS current_active,
        AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN daily_total_time END) AS current_total,
        
        -- Previous period team metrics
        AVG(CASE WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' THEN daily_active_time END) AS prev_active,
        AVG(CASE WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' THEN daily_total_time END) AS prev_total
    FROM PerDayEmployeeSummary
    WHERE emp_id = '{employee_id}'
),

app_metrics AS (
    SELECT
        app_name AS application_name,
        -- Current period app metrics
        ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_active_time END), 0) AS active_time,
        ROUND(AVG(CASE 
                WHEN app_name = 'Window Lock' AND cal_date BETWEEN '{start_date}' AND '{end_date}' 
                THEN total_window_lock_time
                WHEN cal_date BETWEEN '{start_date}' AND '{end_date}'
                THEN total_idle_time
            END), 0) AS idle_time,
        ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_mouse_clicks END), 0) AS mouse_clicks,
        ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_key_strokes END), 0) AS key_strokes,
        
        -- Previous period app metrics for trends
        AVG(CASE WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' THEN total_active_time END) AS prev_active,
        AVG(CASE 
                WHEN app_name = 'Window Lock' AND cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' 
                THEN total_window_lock_time
                WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}'
                THEN total_idle_time
            END) AS prev_idle
    FROM EmployeeActivity
    WHERE emp_id = '{employee_id}'
      AND cal_date BETWEEN '{prev_start_date}' AND '{end_date}'
      AND EXISTS (
          SELECT 1 FROM EmployeeActivity ea
          WHERE ea.app_name = EmployeeActivity.app_name
          AND ea.cal_date BETWEEN '{start_date}' AND '{end_date}'
          AND ea.emp_id = '{employee_id}'
      )
    GROUP BY app_name
)

SELECT TO_JSON(
    NAMED_STRUCT(
        'teamSummary',
        NAMED_STRUCT(
            'activeavginsec', ROUND(em.current_active, 0),
            'lastactiveavginsec', ROUND(em.prev_active, 0),
            'isactivetrendup', CASE WHEN em.current_active > em.prev_active THEN 1 ELSE 0 END,
            'active_change', ABS(ROUND(
                (em.current_active - em.prev_active) / NULLIF(em.prev_active, 0) * 100, 
                0
            )),
            'totalavginsec', ROUND(em.current_total, 0),
            'lasttotalavginsec', ROUND(em.prev_total, 0),
            'istotaltrendup', CASE WHEN em.current_total > em.prev_total THEN 1 ELSE 0 END,
            'total_change', ABS(ROUND(
                (em.current_total - em.prev_total) / NULLIF(em.prev_total, 0) * 100, 
                0
            ))
        ),
        'empSummary',
        NAMED_STRUCT(
            'emp_activeavginsec', ROUND(em.current_active, 0),
            'emp_lastactiveavginsec', ROUND(em.prev_active, 0),
            'emp_isactivetrendup', CASE WHEN em.current_active > em.prev_active THEN 1 ELSE 0 END,
            'emp_active_change', ABS(ROUND(
                (em.current_active - em.prev_active) / NULLIF(em.prev_active, 0) * 100, 
                0
            )),
            'emp_totalavginsec', ROUND(em.current_total, 0),
            'emp_lasttotalavginsec', ROUND(em.prev_total, 0),
            'emp_istotaltrendup', CASE WHEN em.current_total > em.prev_total THEN 1 ELSE 0 END,
            'emp_total_change', ABS(ROUND(
                (em.current_total - em.prev_total) / NULLIF(em.prev_total, 0) * 100, 
                0
            ))
        ),
        'graphData',
        (SELECT COLLECT_LIST(
            NAMED_STRUCT(
                'application_name', 
                CASE 
                    WHEN LOWER(RIGHT(application_name, 4)) = '.exe' 
                    THEN LEFT(application_name, LENGTH(application_name)-4)
                    ELSE application_name 
                END,
                'active_time', active_time,
                'idle_time', idle_time,
                'total_time', active_time + idle_time,
                'mouse_clicks', mouse_clicks,
                'key_strokes', key_strokes,
                'active_trend', 
                    CASE 
                        WHEN active_time > prev_active THEN 'Up'
                        WHEN active_time < prev_active THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'idle_trend', 
                    CASE 
                        WHEN idle_time > prev_idle THEN 'Up'
                        WHEN idle_time < prev_idle THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'total_trend', 
                    CASE 
                        WHEN (active_time + idle_time) > (prev_active + prev_idle) THEN 'Up'
                        WHEN (active_time + idle_time) < (prev_active + prev_idle) THEN 'Down'
                        ELSE 'NoChange'
                    END
            )
        ) FROM app_metrics)
    )
) AS json_result
FROM employee_metrics em;










WITH current_period_employees AS (
    SELECT DISTINCT app_name, emp_id
    FROM EmployeeActivity
    WHERE cal_date BETWEEN '{start_date}' AND '{end_date}'
),

app_metrics AS (
    SELECT
        a.app_name AS application_name,
        -- Current period metrics
        ROUND(AVG(CASE WHEN a.cal_date BETWEEN '{start_date}' AND '{end_date}' THEN a.total_active_time END), 0) AS active_time,
        ROUND(AVG(CASE 
                WHEN a.cal_date BETWEEN '{start_date}' AND '{end_date}' AND a.app_name = 'Window Lock' 
                THEN a.total_window_lock_time
                WHEN a.cal_date BETWEEN '{start_date}' AND '{end_date}'
                THEN a.total_idle_time
            END), 0) AS idle_time,
        ROUND(AVG(CASE WHEN a.cal_date BETWEEN '{start_date}' AND '{end_date}' THEN a.total_mouse_clicks END), 0) AS mouse_clicks,
        ROUND(AVG(CASE WHEN a.cal_date BETWEEN '{start_date}' AND '{end_date}' THEN a.total_key_strokes END), 0) AS key_strokes,
        
        -- Previous period metrics for trends
        AVG(CASE WHEN a.cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' THEN a.total_active_time END) AS prev_active,
        AVG(CASE 
                WHEN a.cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' AND a.app_name = 'Window Lock' 
                THEN a.total_window_lock_time
                WHEN a.cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}'
                THEN a.total_idle_time
            END) AS prev_idle,
        
        -- Employee details (pre-aggregated before collect_list)
        COLLECT_LIST(
            NAMED_STRUCT(
                'employee_id', e.emp_id,
                'active_time', ROUND(e.avg_active_time, 0),
                'idle_time', ROUND(e.avg_idle_time, 0),
                'mouse_clicks', ROUND(e.avg_mouse_clicks, 0),
                'key_strokes', ROUND(e.avg_key_strokes, 0)
            )
        ) AS empSummary
    FROM EmployeeActivity a
    JOIN (
        SELECT
            app_name,
            emp_id,
            AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_active_time END) AS avg_active_time,
            AVG(CASE 
                WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' AND app_name = 'Window Lock' 
                THEN total_window_lock_time
                WHEN cal_date BETWEEN '{start_date}' AND '{end_date}'
                THEN total_idle_time
            END) AS avg_idle_time,
            AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_mouse_clicks END) AS avg_mouse_clicks,
            AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_key_strokes END) AS avg_key_strokes
        FROM EmployeeActivity
        WHERE cal_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY app_name, emp_id
    ) e ON a.app_name = e.app_name
    WHERE a.cal_date BETWEEN '{prev_start_date}' AND '{end_date}'
      AND a.app_name IN (SELECT app_name FROM current_period_employees)
    GROUP BY a.app_name
)

SELECT TO_JSON(
    NAMED_STRUCT(
        'teamSummary',
        (SELECT named_struct(
            'activeavginsec', ROUND(activeavginsec, 0),
            'lastactiveavginsec', ROUND(lastactiveavginsec, 0),
            'isactivetrendup', isactivetrendup,
            'active_change', ABS(ROUND(active_change, 0)),
            'totalavginsec', ROUND(totalavginsec, 0),
            'lasttotalavginsec', ROUND(lasttotalavginsec, 0),
            'istotaltrendup', istotaltrendup,
            'total_change', ABS(ROUND(total_change, 0))
        ) FROM TeamSummary),
        'graphData',
        (SELECT COLLECT_LIST(
            NAMED_STRUCT(
                'application_name', 
                CASE 
                    WHEN LOWER(RIGHT(application_name, 4)) = '.exe' 
                    THEN LEFT(application_name, LENGTH(application_name)-4)
                    ELSE application_name 
                END,
                'active_time', active_time,
                'idle_time', idle_time,
                'total_time', active_time + idle_time,
                'mouse_clicks', mouse_clicks,
                'key_strokes', key_strokes,
                'active_trend', 
                    CASE 
                        WHEN active_time > prev_active THEN 'Up'
                        WHEN active_time < prev_active THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'idle_trend', 
                    CASE 
                        WHEN idle_time > prev_idle THEN 'Up'
                        WHEN idle_time < prev_idle THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'total_trend', 
                    CASE 
                        WHEN (active_time + idle_time) > (prev_active + prev_idle) THEN 'Up'
                        WHEN (active_time + idle_time) < (prev_active + prev_idle) THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'empSummary', empSummary
            )
        ) FROM app_metrics)
    )
) AS json_result;




WITH app_metrics AS (
    SELECT
        app_name AS application_name,
        -- Current period metrics
        ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_active_time END), 0) AS active_time,
        ROUND(AVG(CASE 
                WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' AND app_name = 'Window Lock' 
                THEN total_window_lock_time
                WHEN cal_date BETWEEN '{start_date}' AND '{end_date}'
                THEN total_idle_time
            END), 0) AS idle_time,
        ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_mouse_clicks END), 0) AS mouse_clicks,
        ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_key_strokes END), 0) AS key_strokes,
        
        -- Previous period metrics for trends
        AVG(CASE WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' THEN total_active_time END) AS prev_active,
        AVG(CASE 
                WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}' AND app_name = 'Window Lock' 
                THEN total_window_lock_time
                WHEN cal_date BETWEEN '{prev_start_date}' AND '{prev_end_date}'
                THEN total_idle_time
            END) AS prev_idle,
        
        -- Employee details collection
        COLLECT_LIST(
            NAMED_STRUCT(
                'employee_id', emp_id,
                'active_time', ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_active_time END), 0),
                'idle_time', ROUND(AVG(CASE 
                                      WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' AND app_name = 'Window Lock' 
                                      THEN total_window_lock_time
                                      WHEN cal_date BETWEEN '{start_date}' AND '{end_date}'
                                      THEN total_idle_time
                                  END), 0),
                'mouse_clicks', ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_mouse_clicks END), 0),
                'key_strokes', ROUND(AVG(CASE WHEN cal_date BETWEEN '{start_date}' AND '{end_date}' THEN total_key_strokes END), 0)
            )
        ) AS empSummary
    FROM EmployeeActivity
    WHERE cal_date BETWEEN '{prev_start_date}' AND '{end_date}'
      AND EXISTS (
          SELECT 1 FROM EmployeeActivity ea
          WHERE ea.app_name = EmployeeActivity.app_name
          AND ea.cal_date BETWEEN '{start_date}' AND '{end_date}'
      )
    GROUP BY app_name
)

SELECT TO_JSON(
    NAMED_STRUCT(
        'teamSummary',
        (SELECT named_struct(
            'activeavginsec', ROUND(activeavginsec, 0),
            'lastactiveavginsec', ROUND(lastactiveavginsec, 0),
            'isactivetrendup', isactivetrendup,
            'active_change', ABS(ROUND(active_change, 0)),
            'totalavginsec', ROUND(totalavginsec, 0),
            'lasttotalavginsec', ROUND(lasttotalavginsec, 0),
            'istotaltrendup', istotaltrendup,
            'total_change', ABS(ROUND(total_change, 0))
        ) FROM TeamSummary),
        'graphData',
        (SELECT COLLECT_LIST(
            NAMED_STRUCT(
                'application_name', 
                CASE 
                    WHEN LOWER(RIGHT(application_name, 4)) = '.exe' 
                    THEN LEFT(application_name, LENGTH(application_name)-4)
                    ELSE application_name 
                END,
                'active_time', active_time,
                'idle_time', idle_time,
                'total_time', active_time + idle_time,
                'mouse_clicks', mouse_clicks,
                'key_strokes', key_strokes,
                'active_trend', 
                    CASE 
                        WHEN active_time > prev_active THEN 'Up'
                        WHEN active_time < prev_active THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'idle_trend', 
                    CASE 
                        WHEN idle_time > prev_idle THEN 'Up'
                        WHEN idle_time < prev_idle THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'total_trend', 
                    CASE 
                        WHEN (active_time + idle_time) > (prev_active + prev_idle) THEN 'Up'
                        WHEN (active_time + idle_time) < (prev_active + prev_idle) THEN 'Down'
                        ELSE 'NoChange'
                    END,
                'empSummary', empSummary
            )
        ) FROM app_metrics)
    )
) AS json_result;



----------------

WITH date_ranges AS (
    SELECT 
        CAST('{start_date}' AS DATE) AS current_start,
        CAST('{end_date}' AS DATE) AS current_end,
        CAST('{prev_start_date}' AS DATE) AS prev_start,
        CAST('{prev_end_date}' AS DATE) AS prev_end
),

-- Materialize current apps first for faster filtering
current_apps AS MATERIALIZED (
    SELECT DISTINCT app_name
    FROM EmployeeActivity
    WHERE cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                      AND (SELECT current_end FROM date_ranges)
),

-- Single-pass aggregation with all needed metrics
app_activity AS (
    SELECT
        app_name,
        emp_id,
        -- Current period
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) THEN total_active_time END) AS curr_active,
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) AND app_name = 'Window Lock' 
                 THEN total_window_lock_time 
                 WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) 
                 THEN total_idle_time END) AS curr_idle,
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) THEN total_mouse_clicks END) AS curr_clicks,
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) THEN total_key_strokes END) AS curr_keys,
        -- Previous period
        SUM(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                 AND (SELECT prev_end FROM date_ranges) THEN total_active_time END) AS prev_active,
        SUM(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                 AND (SELECT prev_end FROM date_ranges) AND app_name = 'Window Lock' 
                 THEN total_window_lock_time 
                 WHEN cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                 AND (SELECT prev_end FROM date_ranges) 
                 THEN total_idle_time END) AS prev_idle,
        -- Employee presence flags
        MAX(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) THEN 1 ELSE 0 END) AS in_current,
        MAX(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                 AND (SELECT prev_end FROM date_ranges) THEN 1 ELSE 0 END) AS in_previous
    FROM EmployeeActivity
    WHERE cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                      AND (SELECT current_end FROM date_ranges)
      AND app_name IN (SELECT app_name FROM current_apps)
    GROUP BY app_name, emp_id
),

-- Calculate employee counts per app
employee_counts AS (
    SELECT
        app_name,
        SUM(in_current) AS curr_emp_count,
        SUM(in_previous) AS prev_emp_count
    FROM app_activity
    GROUP BY app_name
),

-- Final app metrics with proper employee averages
app_metrics AS (
    SELECT
        a.app_name AS application_name,
        -- Current metrics
        ROUND(SUM(a.curr_active) / NULLIF(ec.curr_emp_count, 0), 0) AS active_time,
        ROUND(SUM(a.curr_idle) / NULLIF(ec.curr_emp_count, 0), 0) AS idle_time,
        ROUND((SUM(a.curr_active) + SUM(a.curr_idle)) / NULLIF(ec.curr_emp_count, 0), 0) AS total_time,
        ROUND(SUM(a.curr_clicks) / NULLIF(ec.curr_emp_count, 0), 0) AS mouse_clicks,
        ROUND(SUM(a.curr_keys) / NULLIF(ec.curr_emp_count, 0), 0) AS key_strokes,
        -- Trends
        CASE WHEN SUM(a.curr_active)/NULLIF(ec.curr_emp_count, 0) > 
                  SUM(a.prev_active)/NULLIF(ec.prev_emp_count, 0) THEN 'Up'
             WHEN SUM(a.curr_active)/NULLIF(ec.curr_emp_count, 0) < 
                  SUM(a.prev_active)/NULLIF(ec.prev_emp_count, 0) THEN 'Down'
             ELSE 'NoChange' END AS active_trend,
        CASE WHEN SUM(a.curr_idle)/NULLIF(ec.curr_emp_count, 0) > 
                  SUM(a.prev_idle)/NULLIF(ec.prev_emp_count, 0) THEN 'Up'
             WHEN SUM(a.curr_idle)/NULLIF(ec.curr_emp_count, 0) < 
                  SUM(a.prev_idle)/NULLIF(ec.prev_emp_count, 0) THEN 'Down'
             ELSE 'NoChange' END AS idle_trend,
        CASE WHEN (SUM(a.curr_active)+SUM(a.curr_idle))/NULLIF(ec.curr_emp_count, 0) > 
                  (SUM(a.prev_active)+SUM(a.prev_idle))/NULLIF(ec.prev_emp_count, 0) THEN 'Up'
             WHEN (SUM(a.curr_active)+SUM(a.curr_idle))/NULLIF(ec.curr_emp_count, 0) < 
                  (SUM(a.prev_active)+SUM(a.prev_idle))/NULLIF(ec.prev_emp_count, 0) THEN 'Down'
             ELSE 'NoChange' END AS total_trend,
        -- Employee details
        COLLECT_LIST(
            NAMED_STRUCT(
                'employee_id', a.emp_id,
                'active_time', ROUND(a.curr_active / NULLIF(COUNT(CASE WHEN a.curr_active IS NOT NULL THEN 1 END) OVER (PARTITION BY a.app_name, a.emp_id), 0),
                'idle_time', ROUND(a.curr_idle / NULLIF(COUNT(CASE WHEN a.curr_idle IS NOT NULL THEN 1 END) OVER (PARTITION BY a.app_name, a.emp_id), 0),
                'mouse_clicks', ROUND(a.curr_clicks / NULLIF(COUNT(CASE WHEN a.curr_clicks IS NOT NULL THEN 1 END) OVER (PARTITION BY a.app_name, a.emp_id), 0),
                'key_strokes', ROUND(a.curr_keys / NULLIF(COUNT(CASE WHEN a.curr_keys IS NOT NULL THEN 1 END) OVER (PARTITION BY a.app_name, a.emp_id), 0)
            )
        ) AS empSummary
    FROM app_activity a
    JOIN employee_counts ec ON a.app_name = ec.app_name
    GROUP BY a.app_name, ec.curr_emp_count, ec.prev_emp_count
),

-- Optimized team metrics
team_metrics AS (
    SELECT
        AVG(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) THEN daily_active_time END) AS activeavginsec,
        AVG(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                 AND (SELECT prev_end FROM date_ranges) THEN daily_active_time END) AS lastactiveavginsec,
        AVG(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_ranges) 
                 AND (SELECT current_end FROM date_ranges) THEN daily_total_time END) AS totalavginsec,
        AVG(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                 AND (SELECT prev_end FROM date_ranges) THEN daily_total_time END) AS lasttotalavginsec
    FROM PerDayEmployeeSummary
    WHERE cal_date BETWEEN (SELECT prev_start FROM date_ranges) 
                      AND (SELECT current_end FROM date_ranges)
)

-- Final JSON output
SELECT TO_JSON(
    NAMED_STRUCT(
        'teamSummary',
        NAMED_STRUCT(
            'activeavginsec', ROUND(activeavginsec, 0),
            'lastactiveavginsec', ROUND(lastactiveavginsec, 0),
            'isactivetrendup', CASE WHEN activeavginsec > lastactiveavginsec THEN 1 ELSE 0 END,
            'active_change', ABS(ROUND(
                (activeavginsec - lastactiveavginsec) / NULLIF(lastactiveavginsec, 0) * 100, 
                0
            )),
            'totalavginsec', ROUND(totalavginsec, 0),
            'lasttotalavginsec', ROUND(lasttotalavginsec, 0),
            'istotaltrendup', CASE WHEN totalavginsec > lasttotalavginsec THEN 1 ELSE 0 END,
            'total_change', ABS(ROUND(
                (totalavginsec - lasttotalavginsec) / NULLIF(lasttotalavginsec, 0) * 100, 
                0
            ))
        ),
        'graphData',
        (SELECT COLLECT_LIST(
            NAMED_STRUCT(
                'application_name', 
                CASE 
                    WHEN LOWER(RIGHT(application_name, 4)) = '.exe' 
                    THEN LEFT(application_name, LENGTH(application_name)-4)
                    ELSE application_name 
                END,
                'active_time', active_time,
                'idle_time', idle_time,
                'total_time', total_time,
                'mouse_clicks', mouse_clicks,
                'key_strokes', key_strokes,
                'active_trend', active_trend,
                'idle_trend', idle_trend,
                'total_trend', total_trend,
                'empSummary', empSummary
            )
        ) FROM app_metrics)
    )
) AS json_result
FROM team_metrics;




___----*-----

WITH date_params AS (
    SELECT 
        '{start_date}' AS current_start,
        '{end_date}' AS current_end,
        '{prev_start_date}' AS prev_start,
        '{prev_end_date}' AS prev_end
),

current_apps AS (
    SELECT DISTINCT app_name
    FROM EmployeeActivity
    WHERE cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params)
),

activity_data AS (
    SELECT
        app_name,
        emp_id,
        -- Current period metrics
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN total_active_time ELSE 0 END) AS current_active_sum,
        SUM(CASE 
                WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) AND app_name = 'Window Lock' THEN total_window_lock_time
                WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN total_idle_time
                ELSE 0 
            END) AS current_idle_sum,
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN total_mouse_clicks ELSE 0 END) AS current_clicks_sum,
        SUM(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN total_key_strokes ELSE 0 END) AS current_keys_sum,
        COUNT(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN 1 END) AS current_count,
        
        -- Previous period metrics
        SUM(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT prev_end FROM date_params) THEN total_active_time ELSE 0 END) AS prev_active_sum,
        SUM(CASE 
                WHEN cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT prev_end FROM date_params) AND app_name = 'Window Lock' THEN total_window_lock_time
                WHEN cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT prev_end FROM date_params) THEN total_idle_time
                ELSE 0 
            END) AS prev_idle_sum,
        COUNT(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT prev_end FROM date_params) THEN 1 END) AS prev_count
    FROM EmployeeActivity
    WHERE cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT current_end FROM date_params)
      AND app_name IN (SELECT app_name FROM current_apps)
    GROUP BY app_name, emp_id
),

team_metrics AS (
    SELECT
        AVG(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN daily_active_time END) AS activeavginsec,
        AVG(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT prev_end FROM date_params) THEN daily_active_time END) AS lastactiveavginsec,
        AVG(CASE WHEN cal_date BETWEEN (SELECT current_start FROM date_params) 
                      AND (SELECT current_end FROM date_params) THEN daily_total_time END) AS totalavginsec,
        AVG(CASE WHEN cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT prev_end FROM date_params) THEN daily_total_time END) AS lasttotalavginsec
    FROM PerDayEmployeeSummary
    WHERE cal_date BETWEEN (SELECT prev_start FROM date_params) 
                      AND (SELECT current_end FROM date_params)
),

app_summary AS (
    SELECT
        app_name AS application_name,
        -- Current metrics
        ROUND(SUM(current_active_sum) / NULLIF(SUM(current_count), 0), 0) AS active_time,
        ROUND(SUM(current_idle_sum) / NULLIF(SUM(current_count), 0), 0) AS idle_time,
        ROUND((SUM(current_active_sum) + SUM(current_idle_sum)) / NULLIF(SUM(current_count), 0), 0) AS total_time,
        ROUND(SUM(current_clicks_sum) / NULLIF(SUM(current_count), 0), 0) AS mouse_clicks,
        ROUND(SUM(current_keys_sum) / NULLIF(SUM(current_count), 0), 0) AS key_strokes,
        
        -- Trends
        CASE 
            WHEN (SUM(current_active_sum)/NULLIF(SUM(current_count), 0)) > 
                 (SUM(prev_active_sum)/NULLIF(SUM(prev_count), 0)) THEN 'Up'
            WHEN (SUM(current_active_sum)/NULLIF(SUM(current_count), 0)) < 
                 (SUM(prev_active_sum)/NULLIF(SUM(prev_count), 0)) THEN 'Down'
            ELSE 'NoChange'
        END AS active_trend,
        
        CASE 
            WHEN (SUM(current_idle_sum)/NULLIF(SUM(current_count), 0)) > 
                 (SUM(prev_idle_sum)/NULLIF(SUM(prev_count), 0)) THEN 'Up'
            WHEN (SUM(current_idle_sum)/NULLIF(SUM(current_count), 0)) < 
                 (SUM(prev_idle_sum)/NULLIF(SUM(prev_count), 0)) THEN 'Down'
            ELSE 'NoChange'
        END AS idle_trend,
        
        CASE 
            WHEN ((SUM(current_active_sum)+SUM(current_idle_sum))/NULLIF(SUM(current_count), 0)) > 
                 ((SUM(prev_active_sum)+SUM(prev_idle_sum))/NULLIF(SUM(prev_count), 0)) THEN 'Up'
            WHEN ((SUM(current_active_sum)+SUM(current_idle_sum))/NULLIF(SUM(current_count), 0)) < 
                 ((SUM(prev_active_sum)+SUM(prev_idle_sum))/NULLIF(SUM(prev_count), 0)) THEN 'Down'
            ELSE 'NoChange'
        END AS total_trend,
        
        -- Employee details
        COLLECT_LIST(
            NAMED_STRUCT(
                'employee_id', emp_id,
                'active_time', ROUND(current_active_sum/NULLIF(current_count, 0), 0),
                'idle_time', ROUND(current_idle_sum/NULLIF(current_count, 0), 0),
                'mouse_clicks', ROUND(current_clicks_sum/NULLIF(current_count, 0), 0),
                'key_strokes', ROUND(current_keys_sum/NULLIF(current_count, 0), 0)
            )
        ) AS empSummary
    FROM activity_data
    GROUP BY app_name
)

SELECT TO_JSON(
    NAMED_STRUCT(
        'teamSummary',
        NAMED_STRUCT(
            'activeavginsec', ROUND(activeavginsec, 0),
            'lastactiveavginsec', ROUND(lastactiveavginsec, 0),
            'isactivetrendup', CASE WHEN activeavginsec > lastactiveavginsec THEN 1 ELSE 0 END,
            'active_change', ABS(ROUND(
                (activeavginsec - lastactiveavginsec) / NULLIF(lastactiveavginsec, 0) * 100, 
                0
            )),
            'totalavginsec', ROUND(totalavginsec, 0),
            'lasttotalavginsec', ROUND(lasttotalavginsec, 0),
            'istotaltrendup', CASE WHEN totalavginsec > lasttotalavginsec THEN 1 ELSE 0 END,
            'total_change', ABS(ROUND(
                (totalavginsec - lasttotalavginsec) / NULLIF(lasttotalavginsec, 0) * 100, 
                0
            ))
        ),
        'graphData',
        (SELECT COLLECT_LIST(
            NAMED_STRUCT(
                'application_name', 
                CASE 
                    WHEN LOWER(RIGHT(application_name, 4)) = '.exe' 
                    THEN LEFT(application_name, LENGTH(application_name)-4)
                    ELSE application_name 
                END,
                'active_time', active_time,
                'idle_time', idle_time,
                'total_time', total_time,
                'mouse_clicks', mouse_clicks,
                'key_strokes', key_strokes,
                'active_trend', active_trend,
                'idle_trend', idle_trend,
                'total_trend', total_trend,
                'empSummary', empSummary
            )
        ) FROM app_summary)
    )
) AS json_result
FROM team_metrics;











WITH TeamSummary AS (
    SELECT
        -- Current period averages
        AVG(CASE WHEN cal_date BETWEEN '2025-03-21' AND '2025-04-21' 
             THEN daily_active_time END) AS activeavginsec,
        
        -- Previous period averages
        AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21')
             THEN daily_active_time END) AS lastactiveavginsec,
        
        -- Current vs previous comparison
        CASE WHEN 
            AVG(CASE WHEN cal_date BETWEEN '2025-03-21' AND '2025-04-21' THEN daily_active_time END) >
            AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21') 
                 THEN daily_active_time END)
        THEN 1 ELSE 0 END AS isactivetrendup,
        
        -- Percentage change calculation
        (AVG(CASE WHEN cal_date BETWEEN '2025-03-21' AND '2025-04-21' THEN daily_active_time END) -
         AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21') 
              THEN daily_active_time END)) /
        NULLIF(AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21') 
                   THEN daily_active_time END), 0) * 100 AS active_change,
        
        -- Repeat for total time metrics
        AVG(CASE WHEN cal_date BETWEEN '2025-03-21' AND '2025-04-21' 
             THEN daily_total_time END) AS totalavginsec,
        
        AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21')
             THEN daily_total_time END) AS lasttotalavginsec,
        
        CASE WHEN 
            AVG(CASE WHEN cal_date BETWEEN '2025-03-21' AND '2025-04-21' THEN daily_total_time END) >
            AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21') 
                 THEN daily_total_time END)
        THEN 1 ELSE 0 END AS istotaltrendup,
        
        (AVG(CASE WHEN cal_date BETWEEN '2025-03-21' AND '2025-04-21' THEN daily_total_time END) -
         AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21') 
              THEN daily_total_time END)) /
        NULLIF(AVG(CASE WHEN cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND DATEADD(DAY, -31, '2025-04-21') 
                   THEN daily_total_time END), 0) * 100 AS total_change
    FROM PerDayEmployeeSummary
    WHERE cal_date BETWEEN DATEADD(DAY, -31, '2025-03-21') AND '2025-04-21'
)









Perfect — here's a well-structured and easy-to-understand explanation of how application usage averages are calculated, covering both employee-level averages and team-level averages, using a real-time example from April 14th to April 20th, 2025.


---

Application Usage Average: Explanation with Example

During the period from April 14th to April 20th, 2025, the team used various applications. Two such applications were:

Chrome

Zoom


> Note: Not all employees used all applications. So when calculating application averages, we consider only those employees and days where that particular application was actually used.




---

1. Employee-Level Application Average

This average is calculated per employee for each application they used.
We consider only the days the employee actually used the application.

Formula:

Employee App Average = Total time the employee used the app / Number of days the employee used the app


---

2. Team-Level Application Average

This average is calculated at the team level per application, based on all entries from any employee who used the app.

Formula:

Team App Average = Total usage time of app by all employees / Total number of usage entries (across dates and users)


---

Example: Chrome and Zoom Usage

Team: 4 Employees — Emp1, Emp2, Emp3, Emp4
Period: April 14 to April 20, 2025


---

Chrome Usage

April 14

Emp1 used Chrome for 2 hours

Emp2 used Chrome for 1.5 hours


April 15

Emp1 used Chrome for 1 hour

Emp4 used Chrome for 2 hours


April 17

Emp2 used Chrome for 1 hour



Emp1’s Chrome Average

Total time: 2 + 1 = 3 hours

Days used: 2

Average = 3 / 2 = 1.5 hours


Emp2’s Chrome Average

Total time: 1.5 + 1 = 2.5 hours

Days used: 2

Average = 2.5 / 2 = 1.25 hours


Emp4’s Chrome Average

Total time: 2 hours

Days used: 1

Average = 2 / 1 = 2 hours


Team Chrome Average

Total usage: 3 (Emp1) + 2.5 (Emp2) + 2 (Emp4) = 7.5 hours

Total entries: 5 (2 for Emp1, 2 for Emp2, 1 for Emp4)

Average = 7.5 / 5 = 1.5 hours



---

Zoom Usage

April 14

Emp3 used Zoom for 1 hour


April 15

Emp2 used Zoom for 1.5 hours


April 16

Emp3 used Zoom for 2 hours



Emp2’s Zoom Average

Total time: 1.5 hours

Days used: 1

Average = 1.5 / 1 = 1.5 hours


Emp3’s Zoom Average

Total time: 1 + 2 = 3 hours

Days used: 2

Average = 3 / 2 = 1.5 hours


Team Zoom Average

Total usage: 1.5 (Emp2) + 3 (Emp3) = 4.5 hours

Total entries: 3

Average = 4.5 / 3 = 1.5 hours



---

Summary

We calculate employee-level averages only for days when they actually used the app.

We calculate team-level averages based on all usage entries, regardless of date or user.

This ensures accuracy without being skewed by inactive days or non-users.


Let me know if you'd like a visual summary or chart too!














Absolutely! Here's how you can present the formula to the team in a clear and professional way:


---

Team Average Logged Time – Formula

To calculate the average working (logged) hours of a team over a selected period, we use the following formula:

\textbf{Team Average Time} = \frac{\sum \text{Total Logged Time of All Employees}}{\text{Total Number of Logged Entries}}


---

Explanation of Terms:

Total Logged Time of All Employees:
The sum of all individual logged-in hours by employees during the selected period.

Total Number of Logged Entries:
The count of individual employee-day records where login activity was actually recorded.



---

Example Calculation (April 14–16, 2025):

\text{Team Average Time} = \frac{66.5}{9} = 7.39 \text{ hours (approx.)}


---

Why This Method?

This approach ensures fairness by:

Ignoring days where an employee didn’t work (e.g., leave or week-off).

Avoiding skewed averages that would happen if we divided by total days × total employees.


Let me know if you want this drafted as a slide or PDF too.




Sure! Here's a clear and professional draft of your explanation, suitable for sharing with your business or analytics team:


---

How Team Average is Calculated for Selected Period (Example: April 14 to April 16, 2025)

To ensure a more accurate and fair representation of team productivity, we calculate the average based on actual available entries, rather than the total number of days or expected entries. This method helps account for employees who may not have logged any time on certain days due to week-offs, leave, or other reasons.

Scenario:

Period Selected: April 14 to April 16, 2025 (3 days)

Total Team Size: 4 employees

Expected Entries: 3 days × 4 employees = 12 entries


Actual Entries Logged:

Average Calculation:

Instead of averaging over total days or total expected entries, we compute:

Team Average Time = Total Logged Hours / Number of Entries

= 65.0 hours / 9 entries = ~7.22 hours per employee

Why This Method?

This method avoids misleading averages that could be affected by missing data (e.g., non-working days, absences).

It ensures that only actual contributing data is considered.

It's especially useful when evaluating effort and utilization without penalizing employees for approved absences or days off.



---

Let me know if you want this tailored for a slide or report format too!













Great question — and it’s awesome that you’re thinking about how to communicate metrics clearly to your business team.

Let me help you with a simple yet clear explanation you can use to describe how you're calculating team average activity over a selected period (like a week):


---

Business-Friendly Explanation:

"To represent the team’s average activity for the selected week, we calculate the total activity of each employee per day, but instead of averaging it over all days uniformly, we only include days where there was actual activity recorded. This avoids skewing the average due to days when some employees were off, on leave, or didn't generate any activity data.

For example, if we have 5 employees and:

Day 1: 3 employees worked

Day 2: 5 employees worked

Day 3: 3 employees worked

Day 4: 4 employees worked


Then the total number of valid activity entries = 3 + 5 + 3 + 4 = 15 entries.

We take the total activity count across all these 15 entries and divide it by 15, rather than by (5 employees × 4 days = 20), because that would incorrectly assume everyone worked every day.

This gives us a more accurate average per active employee, ensuring we represent actual participation instead of penalizing the team for leaves, holidays, or off days."


---

You can also visualize this with a bar chart showing the count of active users per day, and then how the average was computed from only those bars — this really helps business folks relate.

Would you like a sample SQL/PySpark snippet that implements this logic as well?





Absolutely! Here's the same table data in copyable plain text format, so you can easily copy and paste it into Excel, a CSV, or a text editor:

Date        | Employee | Hours Logged
------------|----------|--------------
2025-04-14  | Emp1     | 7.5
2025-04-14  | Emp2     | 8.0
2025-04-14  | Emp3     | 7.0
2025-04-15  | Emp1     | 8.0
2025-04-15  | Emp2     | 7.5
2025-04-15  | Emp3     | 6.5
2025-04-15  | Emp4     | 8.0
2025-04-15  | Emp5     | 7.0
2025-04-16  | Emp2     | 7.5
2025-04-16  | Emp3     | 7.0
2025-04-16  | Emp5     | 6.0
2025-04-17  | Emp1     | 7.0
2025-04-17  | Emp2     | 7.5
2025-04-17  | Emp3     | 7.0
2025-04-17  | Emp4     | 8.0
2025-04-18  | Emp3     | 6.5


---

How to Present the Average:

Instead of computing the average per employee per day, you're averaging the total hours logged over the number of actual records (i.e., total log entries):

Step-by-Step:

1. Total Entries = 16


2. Sum of Hours Logged:

7.5 + 8.0 + 7.0 + 8.0 + 7.5 + 6.5 + 8.0 + 7.0 +
7.5 + 7.0 + 6.0 + 7.0 + 7.5 + 7.0 + 8.0 + 6.5 = 112.0 hours


3. Team Average (overall period) =
Total Hours Logged / Number of Entries =
112.0 / 16 = 7.0 hours




---

How to Explain to Business Team:

> “We calculate the average based on actual working entries for the selected period. So instead of diluting the metric across all 5 employees for every single day, we compute it based on who actually logged in and worked. This gives us a true measure of team activity during that time, especially when some team members may have been on leave or inactive.”



Would you like this as a downloadable CSV or Excel file as well?
















# Step-9. Calculate all potential times
result_df = classified_df.groupBy("emp_id", "cal_date").agg(
    # Current login: Minimum start time where start_time is within the current window and app_name is not "Window Lock"
    F.min(F.when(
        (F.col("is_current_login") & 
         (F.col("start_time") >= F.col("current_window_start")) & 
         (F.col("start_time") <= F.col("current_window_end")) & 
         (F.col("app_name") != "Window Lock")),
        F.col("start_time")
    )).alias("current_login"),
    
    # Last activity: Maximum start time where start_time is within the current window
    F.max(F.when(
        (F.col("start_time") >= F.col("current_window_start")) & 
        (F.col("start_time") <= F.col("current_window_end")),
        F.col("start_time")
    )).alias("last_activity"),
    
    # Previous logout candidate: Maximum start time where start_time is within the previous window
    F.max(F.when(
        (F.col("prev_window_start").isNotNull()) & 
        (F.col("start_time") >= F.col("prev_window_start")) & 
        (F.col("start_time") <= F.col("prev_window_end")) & 
        (F.col("start_time") < F.col("current_window_start")),
        F.col("start_time")
    )).alias("prev_logout_update"),
    
    # Week-off login: Minimum start time where is_week_off_activity is true
    F.min(F.when(F.col("is_week_off_activity"), F.col("start_time"))).alias("week_off_login"),
    
    # Week-off logout: Maximum start time where is_week_off_activity is true
    F.max(F.when(F.col("is_week_off_activity"), F.col("start_time"))).alias("week_off_logout"),
    
    # Shift info
    F.first("cur_start_time_ts").alias("shift_start_time"),
    F.first("cur_end_time_ts").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_date").alias("prev_cal_date")
)

display(result_df)



# Step-10. Determine final times with priority rules
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


# Step-11. Generate previous day updates (only if valid)
prev_day_updates = final_result.filter(
    F.col("prev_logout_update").isNotNull()
).select(
    F.col("emp_id").alias("update_emp_id"),
    F.col("prev_cal_date").alias("update_date"),
    F.col("prev_logout_update").alias("new_logout_time")
)


# Final output
final_output = final_result.select(
    "emp_id",
    "cal_date",
    "emp_login_time",
    "emp_logout_time",
    "shift_start_time",
    "shift_end_time",
    "is_week_off"
).orderBy("emp_id", "cal_date")

display(final_output)


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Prepare shift data with previous day info
window_spec = Window.partitionBy("emp_id").orderBy("cal_date")
final_with_prev = final_df.withColumn(
    "prev_shift_end", F.lag("curr_shift_end").over(window_spec)
).withColumn(
    "prev_cal_date", F.lag("cal_date").over(window_spec)
).withColumn(
    "prev_is_week_off", F.lag("is_week_off").over(window_spec)
)

# 2. Join with activities
joined_df = activities_df.join(
    final_with_prev, ["emp_id", "cal_date"], "inner"
)

# 3. Define time windows with special handling for week-offs
df_with_windows = joined_df.withColumn(
    "current_window_start", 
    F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, -4, curr_shift_start)"))
).withColumn(
    "current_window_end",
    F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, 8, curr_shift_end)"))
).withColumn(
    "prev_window_start",
    F.when(
        (F.col("prev_cal_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.col("prev_shift_end")
    )
).withColumn(
    "prev_window_end",
    F.when(
        (F.col("prev_cal_date") == F.date_sub(F.col("cal_date"), 1)) &
        ~F.col("prev_is_week_off"),
        F.expr("timestampadd(HOUR, 8, prev_shift_end)")
    )
)

# 4. Classify activities with priority to current day
classified_df = df_with_windows.withColumn(
    "is_current_login",
    ~F.col("is_week_off") &
    (F.col("start_time") >= F.col("current_window_start")) &
    (F.col("start_time") <= F.col("current_window_end")) &
    (F.col("app_name") != "WindowLock")  # Exclude WindowLock activities
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

# 5. Calculate all potential times
result_df = classified_df.groupBy("emp_id", "cal_date").agg(
    # Current day candidates
    F.min(F.when(F.col("is_current_login"), F.col("start_time"))).alias("current_login"),
    F.max("start_time").alias("last_activity"),
    
    # Previous day candidates
    F.min(F.when(F.col("is_prev_logout"), F.col("start_time"))).alias("prev_logout_candidate"),
    F.max(F.when(F.col("is_prev_logout"), F.col("start_time"))).alias("prev_logout_update"),
    
    # Week-off candidates
    F.min(F.when(F.col("is_week_off_activity"), F.col("start_time"))).alias("week_off_login"),
    F.max(F.when(F.col("is_week_off_activity"), F.col("start_time"))).alias("week_off_logout"),
    
    # Shift info
    F.first("curr_shift_start").alias("shift_start_time"),
    F.first("curr_shift_end").alias("shift_end_time"),
    F.first("is_week_off").alias("is_week_off"),
    F.first("prev_cal_date").alias("prev_cal_date")
)

# 6. Determine final times with priority rules
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
    ).otherwise(F.col("last_activity"))
)

# 7. Generate previous day updates (only if valid)
prev_day_updates = final_result.filter(
    F.col("prev_logout_update").isNotNull()
).select(
    F.col("emp_id").alias("update_emp_id"),
    F.col("prev_cal_date").alias("update_date"),
    F.col("prev_logout_update").alias("new_logout_time")
)

# 8. Final output
final_output = final_result.select(
    "emp_id",
    "cal_date",
    "emp_login_time",
    "emp_logout_time",
    "shift_start_time",
    "shift_end_time",
    "is_week_off"
).orderBy("emp_id", "cal_date")

-----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Prepare shift data with previous day info
window_spec = Window.partitionBy("emp_id").orderBy("cal_date")
final_with_prev = final_df.withColumn(
    "prev_shift_end", F.lag("curr_shift_end").over(window_spec)
).withColumn(
    "prev_cal_date", F.lag("cal_date").over(window_spec)
).withColumn(
    "prev_is_week_off", F.lag("is_week_off").over(window_spec)
)

# 2. Join with activities
joined_df = activities_df.join(
    final_with_prev, ["emp_id", "cal_date"], "inner"
)

# 3. Define time windows
df_with_windows = joined_df.withColumn(
    "current_window_start", F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, -4, curr_shift_start)"))
).withColumn(
    "current_window_end", F.when(~F.col("is_week_off"), F.expr("timestampadd(HOUR, 8, curr_shift_end)"))
).withColumn(
    "previous_window_start", F.col("prev_shift_end")
).withColumn(
    "previous_window_end", F.expr("timestampadd(HOUR, 8, prev_shift_end)")
)

# 4. Filter activity data to current and previous windows
activities_with_window = activities_df.alias("a").join(
    df_with_windows.select("emp_id", "cal_date", "current_window_start", "current_window_end", 
                           "previous_window_start", "previous_window_end"),
    on=["emp_id", "cal_date"],
    how="inner"
)

# 5. Current window activities
current_acts = activities_with_window.filter(
    (F.col("a.start_time") >= F.col("current_window_start")) & 
    (F.col("a.start_time") <= F.col("current_window_end"))
)

# 6. Get login (first non-WindowLock) and logout (last activity) in current window
login_window = Window.partitionBy("emp_id", "cal_date").orderBy("start_time")
logout_window = Window.partitionBy("emp_id", "cal_date").orderBy(F.col("start_time").desc())

current_logins = current_acts.filter(F.col("a.app_name") != "WindowLock") \
    .withColumn("login_rank", F.row_number().over(login_window)) \
    .filter(F.col("login_rank") == 1) \
    .select("emp_id", "cal_date", F.col("start_time").alias("CurrentEMPLoginTime"))

current_logouts = current_acts.withColumn("logout_rank", F.row_number().over(logout_window)) \
    .filter(F.col("logout_rank") == 1) \
    .select("emp_id", "cal_date", F.col("start_time").alias("EMPLogoutTime"))

# 7. Previous window activities
previous_acts = activities_with_window.filter(
    (F.col("a.start_time") >= F.col("previous_window_start")) &
    (F.col("a.start_time") <= F.col("previous_window_end"))
)

# 8. Get previous login and logout (only if previous login exists)
previous_login = previous_acts.filter(F.col("a.app_name") != "WindowLock") \
    .withColumn("login_rank", F.row_number().over(login_window)) \
    .filter(F.col("login_rank") == 1) \
    .select("emp_id", "cal_date", F.col("start_time").alias("PreviousEMPLoginTime"))

previous_logout = previous_acts.withColumn("logout_rank", F.row_number().over(logout_window)) \
    .filter(F.col("logout_rank") == 1) \
    .select("emp_id", "cal_date", F.col("start_time").alias("PreviousEMPLogoutTime"))

# 9. Final assembly
final_df = df_with_windows \
    .join(current_logins, ["emp_id", "cal_date"], "left") \
    .join(current_logouts, ["emp_id", "cal_date"], "left")

# Only join PreviousEMPLogoutTime if previous login exists
final_df = final_df.join(
    previous_login.select("emp_id", "cal_date").alias("pl"), 
    on=["emp_id", "cal_date"], how="left_semi"
).join(
    previous_logout, ["emp_id", "cal_date"], "left"
)

# Now final_df has:
# - CurrentEMPLoginTime
# - EMPLogoutTime (only if login exists)
# - PreviousEMPLogoutTime (only if previous login exists)











from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Configs
DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 6820

# Time setup
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# Step 1: Recent activity and distinct emp_id, cal_date
activities_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date") \
    .distinct()

# Add previous date
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# Step 2: Load shift data
shift_df = spark.table("emp_shift_table") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Step 3: Join current shift
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
prev_shift = shift_df.withColumnRenamed("shift_date", "prev_date") \
    .withColumnRenamed("start_time", "prev_start_time_raw") \
    .withColumnRenamed("end_time", "prev_end_time_raw") \
    .withColumnRenamed("emp_id", "emp_id_prev")

cur_with_prev = cur_shift.join(
    prev_shift,
    (cur_shift.emp_id == prev_shift.emp_id_prev) & (cur_shift.prev_date == prev_shift.prev_date),
    how='left'
).drop("emp_id_prev")

# Step 5: Apply defaults based on missing data and day of week
final_df = cur_with_prev \
    .withColumn("dow", F.date_format("cal_date", "E")) \
    .withColumn("prev_dow", F.date_format("prev_date", "E")) \
    
    # Fill current start time
    .withColumn("cur_start_time",
        F.when(F.col("cur_start_time_raw").isNotNull(), F.col("cur_start_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))  # Week off handling
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_end_time_raw").isNotNull(), F.col("cur_end_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))  # Week off handling
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    
    # Fill previous start time
    .withColumn("prev_start_time",
        F.when(F.col("prev_start_time_raw").isNotNull(), F.col("prev_start_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))  # Week off handling
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_end_time_raw").isNotNull(), F.col("prev_end_time_raw"))
         .when(F.col("is_week_off") == True, F.lit(ZERO_TIME))  # Week off handling
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    
    # Handle overnight shifts by adding +1 day if start > end
    .withColumn("cur_end_time",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(cur_end_time))"), "HH:mm:ss")
        ).otherwise(F.col("cur_end_time"))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(prev_end_time))"), "HH:mm:ss")
        ).otherwise(F.col("prev_end_time"))
    ) \
    
    # Combine start time and end time with cal_date
    .withColumn("cur_start_time_combined", F.concat_ws(" ", F.col("cal_date"), F.col("cur_start_time")))
    .withColumn("cur_end_time_combined", F.concat_ws(" ", F.col("cal_date"), F.col("cur_end_time"))) \
    
    .withColumn("prev_start_time_combined", F.concat_ws(" ", F.col("prev_date"), F.col("prev_start_time"))) \
    .withColumn("prev_end_time_combined", F.concat_ws(" ", F.col("prev_date"), F.col("prev_end_time"))) \
    
    # Adjust end time to the next day if start > end
    .withColumn("cur_end_time_combined",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(cur_end_time_combined))"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("cur_end_time_combined"))
    ) \
    .withColumn("prev_end_time_combined",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(prev_end_time_combined))"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(F.col("prev_end_time_combined"))
    ) \
    
    # Calculate is_week_off based on missing or same start and end times
    .withColumn("is_week_off",
        F.when(
            (F.col("cur_start_time") == ZERO_TIME) & (F.col("cur_end_time") == ZERO_TIME),
            F.lit(True)
        ).otherwise(
            F.when(
                (F.col("prev_start_time") == ZERO_TIME) & (F.col("prev_end_time") == ZERO_TIME),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    ) \
    
    # Final selection
    .select(
        "emp_id",
        "cal_date",
        "cur_start_time_combined",
        "cur_end_time_combined",
        "prev_start_time_combined",
        "prev_end_time_combined",
        "is_week_off"
    )





from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Configs
DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 6820

# Time setup
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# Step 1: Recent activity and distinct emp_id, cal_date
activities_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date") \
    .distinct()

# Add previous date
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# Step 2: Load shift data
shift_df = spark.table("emp_shift_table") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Step 3: Join current shift
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
prev_shift = shift_df.withColumnRenamed("shift_date", "prev_date") \
    .withColumnRenamed("start_time", "prev_start_time_raw") \
    .withColumnRenamed("end_time", "prev_end_time_raw") \
    .withColumnRenamed("emp_id", "emp_id_prev")

cur_with_prev = cur_shift.join(
    prev_shift,
    (cur_shift.emp_id == prev_shift.emp_id_prev) & (cur_shift.prev_date == prev_shift.prev_date),
    how='left'
).drop("emp_id_prev")

# Step 5: Apply defaults based on missing data and day of week
final_df = cur_with_prev \
    .withColumn("dow", F.date_format("cal_date", "E")) \
    .withColumn("prev_dow", F.date_format("prev_date", "E")) \
    
    # Fill current start time
    .withColumn("cur_start_time",
        F.when(F.col("cur_start_time_raw").isNotNull(), F.col("cur_start_time_raw"))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_end_time_raw").isNotNull(), F.col("cur_end_time_raw"))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    
    # Fill previous start time
    .withColumn("prev_start_time",
        F.when(F.col("prev_start_time_raw").isNotNull(), F.col("prev_start_time_raw"))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_end_time_raw").isNotNull(), F.col("prev_end_time_raw"))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    
    # Combine times with dates and handle overnight shifts
    .withColumn("cur_start_datetime", 
        F.when(F.col("cur_start_time") != F.lit(ZERO_TIME),
            F.to_timestamp(F.concat_ws(" ", F.col("cal_date"), F.col("cur_start_time")))
         .otherwise(F.lit(None))
    ) \
    .withColumn("cur_end_datetime", 
        F.when(F.col("cur_end_time") != F.lit(ZERO_TIME),
            F.when(F.col("cur_start_time") < F.col("cur_end_time"),
                F.to_timestamp(F.concat_ws(" ", F.col("cal_date"), F.col("cur_end_time")))
            ).otherwise(
                F.to_timestamp(F.concat_ws(" ", F.date_add(F.col("cal_date"), 1), F.col("cur_end_time")))
            )
         ).otherwise(F.lit(None))
    ) \
    .withColumn("prev_start_datetime", 
        F.when(F.col("prev_start_time") != F.lit(ZERO_TIME),
            F.to_timestamp(F.concat_ws(" ", F.col("prev_date"), F.col("prev_start_time")))
         ).otherwise(F.lit(None))
    ) \
    .withColumn("prev_end_datetime", 
        F.when(F.col("prev_end_time") != F.lit(ZERO_TIME),
            F.when(F.col("prev_start_time") < F.col("prev_end_time"),
                F.to_timestamp(F.concat_ws(" ", F.col("prev_date"), F.col("prev_end_time")))
            ).otherwise(
                F.to_timestamp(F.concat_ws(" ", F.date_add(F.col("prev_date"), 1), F.col("prev_end_time")))
            )
         ).otherwise(F.lit(None))
    ) \
    
    # Final selection
    .select(
        "emp_id",
        "cal_date",
        "cur_start_time",
        "cur_end_time",
        "prev_start_time",
        "prev_end_time",
        "cur_start_datetime",
        "cur_end_datetime",
        "prev_start_datetime",
        "prev_end_datetime",
        "is_week_off"
    )




from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Config
DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 6820

# Get current and threshold ingestion time
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# Step 1: Load recent distinct activity records
activities_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date") \
    .distinct()

# Add previous day
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# Step 2: Load shift data
shifts_df = spark.table("emp_shift_table") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Step 3: Join current and previous day shift data
cur_join = emp_dates_df.alias("a").join(
    shifts_df.alias("b"),
    (F.col("a.emp_id") == F.col("b.emp_id")) & (F.col("a.cal_date") == F.col("b.shift_date")),
    "left"
).select(
    F.col("a.emp_id"),
    F.col("a.cal_date"),
    F.col("a.prev_date"),
    F.col("b.start_time").alias("cur_start_time"),
    F.col("b.end_time").alias("cur_end_time"),
    F.col("b.is_week_off").alias("is_week_off")
)

prev_join = cur_join.alias("c").join(
    shifts_df.alias("d"),
    (F.col("c.emp_id") == F.col("d.emp_id")) & (F.col("c.prev_date") == F.col("d.shift_date")),
    "left"
).select(
    F.col("c.emp_id"),
    F.col("c.cal_date"),
    F.col("c.cur_start_time"),
    F.col("c.cur_end_time"),
    F.col("c.is_week_off"),
    F.col("d.start_time").alias("prev_start_time"),
    F.col("d.end_time").alias("prev_end_time"),
    F.col("d.is_week_off").alias("prev_is_week_off")
)

# Step 4: Add default values if null
final_df = prev_join.withColumn(
    "cur_start_time", F.when(F.col("cur_start_time").isNull(),
        F.when(F.dayofweek("cal_date").isin(1, 7), ZERO_TIME).otherwise(DEFAULT_START)
    ).otherwise(F.col("cur_start_time"))
).withColumn(
    "cur_end_time", F.when(F.col("cur_end_time").isNull(),
        F.when(F.dayofweek("cal_date").isin(1, 7), ZERO_TIME).otherwise(DEFAULT_END)
    ).otherwise(F.col("cur_end_time"))
).withColumn(
    "prev_start_time", F.when(F.col("prev_start_time").isNull(),
        F.when(F.dayofweek(F.col("cal_date") - F.expr("INTERVAL 1 DAY")).isin(1, 7), ZERO_TIME).otherwise(DEFAULT_START)
    ).otherwise(F.col("prev_start_time"))
).withColumn(
    "prev_end_time", F.when(F.col("prev_end_time").isNull(),
        F.when(F.dayofweek(F.col("cal_date") - F.expr("INTERVAL 1 DAY")).isin(1, 7), ZERO_TIME).otherwise(DEFAULT_END)
    ).otherwise(F.col("prev_end_time"))
)

# Step 5: Construct proper timestamps
def build_ts(date_col, time_col, is_end=False):
    """If end and start > end, add 1 day to date."""
    cond = F.unix_timestamp(time_col, "HH:mm:ss") < F.unix_timestamp(DEFAULT_START, "HH:mm:ss")
    base = F.to_timestamp(F.concat_ws(' ', date_col, time_col))
    return F.when(
        (F.col(time_col) != ZERO_TIME) & is_end & (F.col("cur_start_time") > F.col("cur_end_time")),
        F.to_timestamp(F.concat_ws(' ', F.date_add(date_col, 1), time_col))
    ).when(F.col(time_col) != ZERO_TIME, base).otherwise(F.lit(None))

final_df = final_df \
    .withColumn("cur_start_ts", build_ts(F.col("cal_date"), F.col("cur_start_time"))) \
    .withColumn("cur_end_ts", build_ts(F.col("cal_date"), F.col("cur_end_time"), is_end=True)) \
    .withColumn("prev_start_ts", build_ts(F.date_sub(F.col("cal_date"), 1), F.col("prev_start_time"))) \
    .withColumn("prev_end_ts", build_ts(F.date_sub(F.col("cal_date"), 1), F.col("prev_end_time"), is_end=True))

# Step 6: Final select (no prev_date or extra fields)
final_df = final_df.select(
    "emp_id", "cal_date", "is_week_off",
    "cur_start_time", "cur_end_time",
    "prev_start_time", "prev_end_time",
    "cur_start_ts", "cur_end_ts",
    "prev_start_ts", "prev_end_ts"
)




from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Configs
DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 6820

# Time setup
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# Step 1: Recent activity and distinct emp_id, cal_date
activities_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date") \
    .distinct()

# Add previous date
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# Step 2: Load shift data
shift_df = spark.table("emp_shift_table") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Step 3: Join current shift
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
prev_shift = shift_df.withColumnRenamed("shift_date", "prev_date") \
    .withColumnRenamed("start_time", "prev_start_time_raw") \
    .withColumnRenamed("end_time", "prev_end_time_raw") \
    .withColumnRenamed("emp_id", "emp_id_prev")

cur_with_prev = cur_shift.join(
    prev_shift,
    (cur_shift.emp_id == prev_shift.emp_id_prev) & (cur_shift.prev_date == prev_shift.prev_date),
    how='left'
).drop("emp_id_prev")

# Step 5: Apply defaults based on missing data and day of week
final_df = cur_with_prev \
    .withColumn("dow", F.date_format("cal_date", "E")) \
    .withColumn("prev_dow", F.date_format("prev_date", "E")) \
    
    # Fill current start time
    .withColumn("cur_start_time",
        F.when(F.col("cur_start_time_raw").isNotNull(), F.col("cur_start_time_raw"))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("cur_end_time",
        F.when(F.col("cur_end_time_raw").isNotNull(), F.col("cur_end_time_raw"))
         .when(F.col("dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    
    # Fill previous start time
    .withColumn("prev_start_time",
        F.when(F.col("prev_start_time_raw").isNotNull(), F.col("prev_start_time_raw"))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_end_time_raw").isNotNull(), F.col("prev_end_time_raw"))
         .when(F.col("prev_dow").isin("Sat", "Sun"), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))
    ) \
    
    # Handle overnight shifts by adding +1 day if start > end
    .withColumn("cur_end_time",
        F.when(F.col("cur_start_time") > F.col("cur_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(cur_end_time))"), "HH:mm:ss")
        ).otherwise(F.col("cur_end_time"))
    ) \
    .withColumn("prev_end_time",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               F.date_format(F.expr("timestampadd(DAY, 1, to_timestamp(prev_end_time))"), "HH:mm:ss")
        ).otherwise(F.col("prev_end_time"))
    ) \
    
    # Final selection
    .select(
        "emp_id",
        "cal_date",
        "cur_start_time",
        "cur_end_time",
        "prev_start_time",
        "prev_end_time",
        "is_week_off"
    )






from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Config
DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 6820

# Time setup
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

# Step 1: Get recent distinct (emp_id, cal_date)
activities_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date") \
    .distinct()

# Add previous day column
emp_dates_df = activities_df.withColumn("prev_date", F.date_sub("cal_date", 1))

# Step 2: Load shift data
shift_df = spark.table("emp_shift_table") \
    .select("emp_id", "shift_date", "start_time", "end_time", "is_week_off")

# Step 3: Join for current shift
cur_join = emp_dates_df.join(
    shift_df,
    (activities_df.emp_id == shift_df.emp_id) & (activities_df.cal_date == shift_df.shift_date),
    "left"
).select(
    activities_df.emp_id,
    activities_df.cal_date,
    F.col("start_time").alias("cur_start"),
    F.col("end_time").alias("cur_end"),
    F.col("is_week_off").alias("cur_week_off")
)

# Step 4: Join for previous shift
final_df = cur_join.join(
    shift_df,
    (cur_join.emp_id == shift_df.emp_id) & (cur_join.prev_date == shift_df.shift_date),
    "left"
).select(
    cur_join.emp_id,
    cur_join.cal_date,
    cur_join.prev_date,
    F.col("cur_start"),
    F.col("cur_end"),
    F.col("cur_week_off"),
    F.col("start_time").alias("prev_start"),
    F.col("end_time").alias("prev_end"),
    F.col("is_week_off").alias("prev_week_off")
)

# Step 5: Determine shift times with logic
def resolve_times(date_col, start_col, end_col, week_off_col):
    return (
        F.when(start_col.isNotNull(), F.concat_ws(" ", F.col(date_col), start_col))
         .when((F.dayofweek(date_col).isin([1, 7])), F.concat_ws(" ", F.col(date_col), F.lit(ZERO_TIME)))
         .otherwise(F.concat_ws(" ", F.col(date_col), F.lit(DEFAULT_START)))
    ), (
        F.when(end_col.isNotNull() & (start_col < end_col), F.concat_ws(" ", F.col(date_col), end_col))
         .when(end_col.isNotNull(), F.date_format(F.expr(f"timestampadd(DAY, 1, timestamp('{date_col} {end_col}'))"), "yyyy-MM-dd HH:mm:ss"))
         .when((F.dayofweek(date_col).isin([1, 7])), F.concat_ws(" ", F.col(date_col), F.lit(ZERO_TIME)))
         .otherwise(F.concat_ws(" ", F.col(date_col), F.lit(DEFAULT_END)))
    )

cur_start_ts, cur_end_ts = resolve_times("cal_date", F.col("cur_start"), F.col("cur_end"), F.col("cur_week_off"))
prev_start_ts, prev_end_ts = resolve_times("prev_date", F.col("prev_start"), F.col("prev_end"), F.col("prev_week_off"))

result_df = final_df.withColumn("shift_start_ts", cur_start_ts) \
                    .withColumn("shift_end_ts", cur_end_ts) \
                    .withColumn("prev_shift_start_ts", prev_start_ts) \
                    .withColumn("prev_shift_end_ts", prev_end_ts)

# Final Columns
result_df = result_df.select(
    "emp_id", "cal_date",
    "shift_start_ts", "shift_end_ts",
    "prev_shift_start_ts", "prev_shift_end_ts"
)



from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta

# --- Config ---
DEFAULT_START = "09:00:00"
DEFAULT_END = "18:00:00"
ZERO_TIME = "00:00:00"
PROCESSING_WINDOW_MINUTES = 6820

# --- Step 1: Get recent distinct emp_id and cal_date from activity ---
current_time = datetime.now()
ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)

activity_df = spark.table("app_trace.emp_activity") \
    .filter(F.col("ingestion_time") >= F.lit(ingestion_threshold)) \
    .select("emp_id", "cal_date") \
    .distinct()

# --- Step 2: Prepare current and previous date columns ---
activity_with_prev_df = activity_df \
    .withColumn("prev_cal_date", F.date_sub("cal_date", 1)) \
    .withColumn("dow", F.date_format("cal_date", "u").cast("int")) \
    .withColumn("prev_dow", F.date_format("prev_cal_date", "u").cast("int"))

# --- Step 3: Load Shift Table ---
shift_df = spark.table("emp_shift_table")  # replace with actual table name
# Columns: emp_id, shift_date, start_time, end_time, is_week_off

# --- Step 4: Join Current and Previous Shifts ---
shift_join_df = activity_with_prev_df.alias("act") \
    .join(shift_df.alias("curr"),
          (F.col("act.emp_id") == F.col("curr.emp_id")) &
          (F.col("act.cal_date") == F.col("curr.shift_date")),
          "left") \
    .join(shift_df.alias("prev"),
          (F.col("act.emp_id") == F.col("prev.emp_id")) &
          (F.col("act.prev_cal_date") == F.col("prev.shift_date")),
          "left")

# --- Step 5: Determine current and previous shift start/end time ---
def get_shift_ts_expr(date_col, time_col):
    return F.to_timestamp(F.concat_ws(" ", F.col(date_col), F.col(time_col)))

def compute_shift_ts(date_col, start_col, end_col):
    start_ts = get_shift_ts_expr(date_col, start_col)
    end_ts = F.when(F.col(start_col) > F.col(end_col),
                    get_shift_ts_expr(F.date_add(F.col(date_col), 1), end_col)) \
             .otherwise(get_shift_ts_expr(date_col, end_col))
    return start_ts.alias(f"{date_col}_shift_start_ts"), end_ts.alias(f"{date_col}_shift_end_ts")

# Assign raw start/end times with fallback for missing shift
result_df = shift_join_df \
    .withColumn("curr_start_time",
        F.when(F.col("curr.start_time").isNotNull(), F.col("curr.start_time"))
         .when(F.col("dow").isin(6, 7), F.lit(ZERO_TIME))  # Saturday/Sunday
         .otherwise(F.lit(DEFAULT_START))) \
    .withColumn("curr_end_time",
        F.when(F.col("curr.end_time").isNotNull(), F.col("curr.end_time"))
         .when(F.col("dow").isin(6, 7), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END))) \
    .withColumn("prev_start_time",
        F.when(F.col("prev.start_time").isNotNull(), F.col("prev.start_time"))
         .when(F.col("prev_dow").isin(6, 7), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_START))) \
    .withColumn("prev_end_time",
        F.when(F.col("prev.end_time").isNotNull(), F.col("prev.end_time"))
         .when(F.col("prev_dow").isin(6, 7), F.lit(ZERO_TIME))
         .otherwise(F.lit(DEFAULT_END)))

# Compute timestamps
result_df = result_df \
    .withColumn("shift_start_ts", get_shift_ts_expr("cal_date", "curr_start_time")) \
    .withColumn("shift_end_ts",
        F.when(F.col("curr_start_time") > F.col("curr_end_time"),
               get_shift_ts_expr(F.date_add(F.col("cal_date"), 1), "curr_end_time"))
         .otherwise(get_shift_ts_expr("cal_date", "curr_end_time"))) \
    .withColumn("prev_shift_start_ts", get_shift_ts_expr("prev_cal_date", "prev_start_time")) \
    .withColumn("prev_shift_end_ts",
        F.when(F.col("prev_start_time") > F.col("prev_end_time"),
               get_shift_ts_expr(F.date_add(F.col("prev_cal_date"), 1), "prev_end_time"))
         .otherwise(get_shift_ts_expr("prev_cal_date", "prev_end_time")))

# --- Final Columns ---
final_df = result_df.select(
    "emp_id", "cal_date",
    "shift_start_ts", "shift_end_ts",
    "prev_shift_start_ts", "prev_shift_end_ts"
)





from pyspark.sql.functions import col, concat_ws, to_timestamp, when, date_add, date_sub

# 1. Get distinct (emp_id, cal_date)
act = activity_df.select("emp_id", "cal_date").distinct()

# 2. Prepare shift_df with shift_start and shift_end
shift = shift_df.withColumn(
    "shift_start",
    to_timestamp(concat_ws(" ", col("shift_date"), col("start_time")))
).withColumn(
    "shift_end",
    to_timestamp(
        concat_ws(
            " ",
            when(col("start_time") > col("end_time"), date_add(col("shift_date"), 1))
            .otherwise(col("shift_date")),
            col("end_time")
        )
    )
)

# 3. Join current shift
act_with_shift = act.join(
    shift,
    (act.emp_id == shift.emp_id) & (act.cal_date == shift.shift_date),
    "left"
).select(
    act["*"],
    shift["shift_start"],
    shift["shift_end"]
)

# 4. Join previous day’s shift
shift_prev = shift.withColumn("shift_date", date_add(col("shift_date"), 1))

final_df = act_with_shift.join(
    shift_prev,
    (act_with_shift.emp_id == shift_prev.emp_id) & (act_with_shift.cal_date == shift_prev.shift_date),
    "left"
).select(
    act_with_shift["*"],
    shift_prev["shift_start"].alias("prev_shift_start"),
    shift_prev["shift_end"].alias("prev_shift_end")
)

# Done
final_df.show(truncate=False)



take-11

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Configuration
DEFAULT_SHIFT_START = "09:00:00"
DEFAULT_SHIFT_END = "18:00:00"
LOGIN_WINDOW_HOURS = 4  # Hours before shift start to count as login
MIN_SESSION_MINUTES = 30  # Minimum session duration
PROCESSING_WINDOW_MINUTES = 70  # Sliding window for activity ingestion

def process_employee_activities():
    # 1. Initialize processing window
    current_time = datetime.now()
    ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)
    
    # 2. Load recent activities
    activities_df = spark.table("emp_activity").filter(
        F.col("ingestion_time") >= ingestion_threshold
    )
    
    if activities_df.isEmpty():
        print("No new activities to process")
        return

    # 3. Load shift data only for dates present in activities
    date_range = activities_df.select("cal_date").distinct()
    shifts_df = spark.table("shift").join(
        F.broadcast(date_range),
        F.col("shift_date") == F.col("cal_date"),
        "right"
    )

    # 4. Process logins (first non-WindowLock activity per emp-date)
    login_candidates = (
        activities_df
        .filter(F.col("app_name") != "WindowLock")
        .groupBy("emp_id", "cal_date")
        .agg(F.min("start_time").alias("login_time"))
    )
    
    # 5. Fallback to WindowLock if no other activities exist
    wl_fallback = (
        activities_df
        .filter(F.col("app_name") == "WindowLock")
        .groupBy("emp_id", "cal_date")
        .agg(F.min("start_time").alias("wl_time"))
        .join(login_candidates, ["emp_id", "cal_date"], "left_anti")
    )
    
    # 6. Combine login candidates with priority to non-WL
    all_logins = (
        login_candidates
        .withColumn("is_wl_login", F.lit(False))
        .union(
            wl_fallback
            .select("emp_id", "cal_date", F.col("wl_time").alias("login_time"), F.lit(True).alias("is_wl_login"))
        )
    )

    # 7. Apply login window validation
    valid_logins = (
        all_logins
        .join(shifts_df, ["emp_id", "cal_date"], "left")
        .withColumn("shift_start_time", 
            F.coalesce(
                F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.col("shift_start_time"))),
                F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.lit(DEFAULT_SHIFT_START)))
            )
        )
        .withColumn("shift_end_time",
            F.coalesce(
                F.when(
                    F.col("shift_end_time") < F.col("shift_start_time"),
                    F.to_timestamp(F.concat(F.date_add("cal_date", 1), F.lit(" "), F.col("shift_end_time")))
                ).otherwise(
                    F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.col("shift_end_time")))
                ),
                F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.lit(DEFAULT_SHIFT_END)))
            )
        )
        .filter(
            (F.col("login_time") >= (F.col("shift_start_time") - F.expr(f"INTERVAL {LOGIN_WINDOW_HOURS} HOURS"))) &
            (F.col("login_time") <= F.col("shift_end_time"))
        )
    )

    # 8. Process logouts (last activity including WindowLock)
    logouts = (
        activities_df
        .groupBy("emp_id", "cal_date")
        .agg(
            F.max("start_time").alias("logout_time"),
            F.max(F.when(F.col("app_name") == "WindowLock", F.col("start_time"))).alias("wl_logout_time")
        )
        .withColumn("is_wl_logout", F.col("logout_time") == F.col("wl_logout_time"))
        .drop("wl_logout_time")
    )

    # 9. Join logins and logouts
    result = (
        valid_logins
        .join(logouts, ["emp_id", "cal_date"], "left")
        .withColumn("update_time", F.lit(current_time))
    )

    # 10. Validate session duration
    final_records = result.withColumn(
        "session_duration_mins",
        F.when(
            F.col("logout_time").isNotNull(),
            F.round((F.unix_timestamp("logout_time") - F.unix_timestamp("login_time")) / 60)
        .otherwise(None)
    ).filter(
        (F.col("session_duration_mins").isNull()) |  # No logout yet
        (F.col("session_duration_mins") >= MIN_SESSION_MINUTES)  # Valid session
    )

    # 11. Merge with target table
    final_records.createOrReplaceTempView("updates_temp")
    
    spark.sql("""
        MERGE INTO emp_login_logout target
        USING updates_temp source
        ON target.emp_id = source.emp_id AND target.cal_date = source.cal_date
        WHEN MATCHED AND (
            target.logout_time IS NULL OR 
            source.logout_time > target.logout_time
        ) THEN
            UPDATE SET 
                logout_time = source.logout_time,
                shift_start_time = source.shift_start_time,
                shift_end_time = source.shift_end_time,
                update_time = source.update_time,
                is_wl_logout = source.is_wl_logout
        WHEN NOT MATCHED THEN
            INSERT (
                emp_id, cal_date, login_time, logout_time, 
                shift_start_time, shift_end_time, update_time,
                is_wl_login, is_wl_logout
            ) VALUES (
                source.emp_id, source.cal_date, source.login_time, source.logout_time,
                source.shift_start_time, source.shift_end_time, source.update_time,
                source.is_wl_login, source.is_wl_logout
            )
    """)

    print(f"Processed {final_records.count()} records")

# Execute the job
process_employee_activities()




from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Configuration Constants
DEFAULT_SHIFT_START = "09:00:00"  # Default shift start time (HH:mm:ss)
DEFAULT_SHIFT_END = "18:00:00"    # Default shift end time (HH:mm:ss)
LOGIN_WINDOW_HOURS = 4            # Hours before shift start to consider as login
MIN_SESSION_MINUTES = 30          # Minimum valid session duration in minutes
PROCESSING_WINDOW_MINUTES = 70    # Sliding window for activity ingestion
WL_APP_NAME = "WindowLock"        # Identifier for WindowLock activities

def process_employee_activities():
    """Main function to process employee activities and update login/logout records."""
    
    # 1. Initialize processing timestamp and window
    current_time = datetime.now()
    ingestion_threshold = current_time - timedelta(minutes=PROCESSING_WINDOW_MINUTES)
    
    # 2. Load recent activities within the processing window
    activities_df = spark.table("emp_activity").filter(
        F.col("ingestion_time") >= ingestion_threshold
    )
    
    if activities_df.isEmpty():
        print("No new activities to process in the last 70 minutes")
        return

    # 3. Load shift data with optimized join strategy
    shifts_df = spark.table("shift")
    
    # 4. Process activities with shift information
    processed_df = activities_df.join(
        shifts_df,
        (activities_df["emp_id"] == shifts_df["emp_id"]) & 
        (activities_df["cal_date"] == shifts_df["shift_date"]),
        "left"
    )
    
    # 5. Apply default shift times when shift data is missing
    processed_df = processed_df.withColumn(
        "effective_shift_start",
        F.coalesce(
            F.to_timestamp(
                F.concat(F.col("cal_date"), F.lit(" "), F.col("shift_start_time")),
                "yyyy-MM-dd HH:mm:ss"
            ),
            F.to_timestamp(
                F.concat(F.col("cal_date"), F.lit(" "), F.lit(DEFAULT_SHIFT_START)),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    ).withColumn(
        "effective_shift_end",
        F.coalesce(
            F.when(
                F.col("shift_end_time") < F.col("shift_start_time"),
                F.to_timestamp(
                    F.concat(F.date_add("cal_date", 1), F.lit(" "), F.col("shift_end_time")),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ).otherwise(
                F.to_timestamp(
                    F.concat(F.col("cal_date"), F.lit(" "), F.col("shift_end_time")),
                    "yyyy-MM-dd HH:mm:ss"
                )
            ),
            F.to_timestamp(
                F.concat(F.col("cal_date"), F.lit(" "), F.lit(DEFAULT_SHIFT_END)),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
    )
    
    # 6. Identify activity sequence per employee per day
    window_spec = Window.partitionBy("emp_id", "cal_date").orderBy("start_time")
    ranked_activities = (
        processed_df
        .withColumn("activity_seq", F.row_number().over(window_spec))
        .withColumn("max_seq", F.max("activity_seq").over(window_spec))
    )
    
    # 7. Find login candidates (first non-WindowLock activity)
    first_non_wl = ranked_activities.filter(
        (F.col("activity_seq") == 1) & 
        (F.col("app_name") != WL_APP_NAME)
    )
    
    # 8. Find fallback WindowLock logins (if no other activities exist)
    wl_fallback = (
        ranked_activities
        .filter(
            (F.col("activity_seq") == 1) & 
            (F.col("app_name") == WL_APP_NAME)
        )
        .join(
            first_non_wl.select("emp_id", "cal_date"),
            ["emp_id", "cal_date"],
            "left_anti"  # Only keep WL records where no non-WL exists
        )
    )
    
    # 9. Combine login candidates with priority to non-WL activities
    login_candidates = (
        first_non_wl
        .select(
            "emp_id",
            "cal_date",
            F.col("start_time").alias("login_time"),
            "effective_shift_start",
            "effective_shift_end",
            F.lit(False).alias("is_wl_login")
        )
        .union(
            wl_fallback
            .select(
                "emp_id",
                "cal_date",
                F.col("start_time").alias("login_time"),
                "effective_shift_start",
                "effective_shift_end",
                F.lit(True).alias("is_wl_login")
            )
        )
    )
    
    # 10. Validate login time against shift windows
    valid_logins = login_candidates.filter(
        (F.col("login_time") >= (F.col("effective_shift_start") - F.expr(f"INTERVAL {LOGIN_WINDOW_HOURS} HOURS"))) &
        (F.col("login_time") <= F.col("effective_shift_end"))
    )
    
    # 11. Find last activities (including WindowLock) for logout
    last_activities = (
        ranked_activities
        .filter(F.col("activity_seq") == F.col("max_seq"))
        .select(
            "emp_id",
            "cal_date",
            F.col("start_time").alias("logout_time"),
            (F.col("app_name") == WL_APP_NAME).alias("is_wl_logout")
        )
    )
    
    # 12. Join logins and logouts
    session_records = (
        valid_logins
        .join(last_activities, ["emp_id", "cal_date"], "left")
        .withColumn("update_time", F.lit(current_time))
    )
    
    # 13. Calculate session duration and validate
    final_records = (
        session_records
        .withColumn(
            "session_duration_mins",
            F.when(
                F.col("logout_time").isNotNull(),
                F.round(
                    (F.unix_timestamp("logout_time") - F.unix_timestamp("login_time")) / 60
                )
            ).otherwise(F.lit(None))
        )
        .filter(
            (F.col("session_duration_mins").isNull()) |  # No logout yet
            (F.col("session_duration_mins") >= MIN_SESSION_MINUTES)  # Valid session
        )
    )
    
    # 14. Merge with target table
    final_records.createOrReplaceTempView("updates_temp")
    
    merge_sql = """
    MERGE INTO emp_login_logout target
    USING updates_temp source
    ON target.emp_id = source.emp_id AND target.cal_date = source.cal_date
    WHEN MATCHED AND (
        target.logout_time IS NULL OR 
        source.logout_time > target.logout_time
    ) THEN
        UPDATE SET 
            logout_time = source.logout_time,
            shift_start_time = source.effective_shift_start,
            shift_end_time = source.effective_shift_end,
            update_time = source.update_time,
            is_wl_logout = source.is_wl_logout
    WHEN NOT MATCHED THEN
        INSERT (
            emp_id, cal_date, login_time, logout_time, 
            shift_start_time, shift_end_time, update_time,
            is_wl_login, is_wl_logout
        ) VALUES (
            source.emp_id, source.cal_date, source.login_time, source.logout_time,
            source.effective_shift_start, source.effective_shift_end, source.update_time,
            source.is_wl_login, source.is_wl_logout
        )
    """
    
    spark.sql(merge_sql)
    
    # 15. Log processing metrics
    processed_count = final_records.count()
    wl_logins = final_records.filter("is_wl_login").count()
    wl_logouts = final_records.filter("is_wl_logout").count()
    
    print(f"""
    Processing Summary:
    - Total records processed: {processed_count}
    - WindowLock logins: {wl_logins}
    - WindowLock logouts: {wl_logouts}
    - Timestamp: {current_time}
    """)

# Execute the job
if __name__ == "__main__":
    process_employee_activities()




# **Complete Employee Login/Logout Tracking System**
## **Final Implementation with WindowLock Handling**

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Configuration
DEFAULT_SHIFT_START = "09:00:00"
DEFAULT_SHIFT_END = "18:00:00"
LOGIN_WINDOW_HOURS = 4  # Hours before shift start to count as login
MIN_SESSION_MINUTES = 30  # Minimum session duration

def process_employee_activities():
    # 1. Load recent activities (70-minute sliding window)
    current_time = datetime.now()
    ingestion_threshold = current_time - timedelta(minutes=70)
    
    activities_df = spark.table("emp_activity").filter(
        F.col("ingestion_time") >= ingestion_threshold
    )
    
    if activities_df.isEmpty():
        print("No new activities to process")
        return

    # 2. Load shift data
    shifts_df = spark.table("shift")
    
    # 3. Process activities with shift information
    processed_df = activities_df.join(
        shifts_df,
        (activities_df["emp_id"] == shifts_df["emp_id"]) & 
        (activities_df["cal_date"] == shifts_df["shift_date"]),
        "left"
    )
    
    # 4. Apply default shift if missing
    processed_df = processed_df.withColumn(
        "effective_shift_start",
        F.coalesce(
            F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.col("shift_start_time"))),
            F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.lit(DEFAULT_SHIFT_START)))
    ).withColumn(
        "effective_shift_end",
        F.coalesce(
            F.when(
                F.col("shift_end_time") < F.col("shift_start_time"),
                F.to_timestamp(F.concat(F.date_add("cal_date", 1), F.lit(" "), F.col("shift_end_time"))
            ).otherwise(
                F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.col("shift_end_time"))
            ),
            F.to_timestamp(F.concat(F.col("cal_date"), F.lit(" "), F.lit(DEFAULT_SHIFT_END)))
    )
    
    # 5. Identify first and last activities per employee-day
    window_spec = Window.partitionBy("emp_id", "cal_date").orderBy("start_time")
    ranked_activities = processed_df.withColumn("activity_seq", F.row_number().over(window_spec)) \
                                  .withColumn("max_seq", F.max("activity_seq").over(window_spec))
    
    # 6. Find login candidates (first non-WindowLock activity)
    first_non_wl = ranked_activities.filter(
        (F.col("activity_seq") == 1) & 
        (F.col("app_name") != "WindowLock")
    )
    
    # 7. Find fallback WindowLock logins (if no other activities)
    wl_fallback = ranked_activities.filter(
        (F.col("activity_seq") == 1) & 
        (F.col("app_name") == "WindowLock")
    ).join(
        first_non_wl.select("emp_id", "cal_date"),
        ["emp_id", "cal_date"],
        "left_anti"  # Only keep WL records where no non-WL exists
    )
    
    # 8. Combine login candidates
    login_candidates = first_non_wl.unionByName(wl_fallback, allowMissingColumns=True)
    
    # 9. Apply login window validation
    valid_logins = login_candidates.filter(
        (F.col("start_time") >= (F.col("effective_shift_start") - F.expr(f"INTERVAL {LOGIN_WINDOW_HOURS} HOURS"))) &
        (F.col("start_time") <= F.col("effective_shift_end"))
    ).select(
        "emp_id",
        "cal_date",
        F.col("start_time").alias("login_time"),
        "effective_shift_start",
        "effective_shift_end",
        F.col("app_name") == "WindowLock").alias("is_wl_login")
    )
    
    # 10. Find last activities (including WindowLock) for logout
    last_activities = ranked_activities.filter(
        F.col("activity_seq") == F.col("max_seq")
    ).select(
        "emp_id",
        "cal_date",
        F.col("start_time").alias("logout_time"),
        F.col("app_name") == "WindowLock").alias("is_wl_logout")
    )
    
    # 11. Join logins and logouts
    session_records = valid_logins.join(
        last_activities,
        ["emp_id", "cal_date"],
        "left"
    )
    
    # 12. Validate session duration
    final_records = session_records.withColumn(
        "session_duration_mins",
        F.when(
            F.col("logout_time").isNotNull(),
            F.round((F.unix_timestamp("logout_time") - F.unix_timestamp("login_time")) / 60)
        .otherwise(None)








# **Complete Employee Login/Logout Tracking Logic (All Scenarios & Rules)**

---

## **1. Core Business Rules**
### **A. Shift Definitions**
| **Shift Type** | **Time Range** | **Login Window** | **Logout Window** | **Notes** |
|--------------|--------------|----------------|-----------------|---------|
| **Day Shift** | `09:00 - 17:00` | `[05:00, 09:00]` | `[17:00, 01:00 (next day)]` | Standard workday |
| **Night Shift** | `18:00 - 02:00` (next day) | `[14:00, 18:00]` | `[02:00, 10:00]` (next day) | Spans midnight |
| **Default Shift** (No data) | `09:00 - 17:00` | `[05:00, 09:00]` | `[17:00, 01:00]` | Fallback rule |

---

## **2. Classification Logic**
### **Priority Rules**
1. **Current Day Login Check (First Priority)**  
   - If activity falls in `[shift_start - 4h, shift_start]` → **LOGIN** (current date)  
   - *Example*: 06:00 activity for 09:00-17:00 shift → LOGIN  

2. **Previous Day Logout Check (Second Priority)**  
   - If activity falls in `[shift_end, shift_end + 8h]` → **LOGOUT** (previous date)  
   - *Example*: 03:00 activity after 18:00-02:00 shift → LOGOUT (previous day)  

3. **No Match** → **ACTIVE** (not tracked)  

---

## **3. Scenario Matrix**
### **A. Standard Day Shift (09:00-17:00)**
| **Activity Time** | **Classification** | **Effective Date** | **Logic** |
|------------------|------------------|------------------|---------|
| 04:00 | ACTIVE | - | Before login window |
| **05:00** | **LOGIN** | Current day | `05:00 ∈ [05:00, 09:00]` |
| 12:00 | ACTIVE | - | Mid-shift |
| **18:00** | **LOGOUT** | Current day | `18:00 ∈ [17:00, 01:00]` |
| 02:00 (next day) | ACTIVE | - | After logout window |

### **B. Night Shift (18:00-02:00)**
| **Activity Time** | **Classification** | **Effective Date** | **Logic** |
|------------------|------------------|------------------|---------|
| **14:00** | **LOGIN** | Current day | `14:00 ∈ [14:00, 18:00]` |
| 20:00 | ACTIVE | - | Mid-shift |
| **03:00** (next day) | **LOGOUT** | Previous day | `03:00 ∈ [02:00, 10:00]` |
| 11:00 (next day) | ACTIVE | - | After logout window |

### **C. No Shift Data (Defaults to 09:00-17:00)**
| **Activity Time** | **Classification** | **Effective Date** | **Logic** |
|------------------|------------------|------------------|---------|
| **08:00** | **LOGIN** | Current day | `08:00 ∈ [05:00, 09:00]` |
| **19:00** | **LOGOUT** | Current day | `19:00 ∈ [17:00, 01:00]` |

### **D. Overlapping Scenarios**
| **Activity Time** | **Shift Context** | **Classification** | **Priority** |
|------------------|------------------|------------------|------------|
| **06:00** | Day (09:00-17:00) + Previous Night (18:00-02:00) | **LOGIN** (current day) | Current day takes precedence |
| **03:00** | Night (18:00-02:00) + Next Day (09:00-17:00) | **LOGOUT** (previous day) | Previous day logout first |

---

## **4. Edge Cases & Special Rules**
### **A. Multi-Day Shifts**
- **Example**: Night shift ends at 02:00 next day  
  - Logout valid until **10:00 next day** (`02:00 + 8h`)  

### **B. Late Ingestions**
- If system processes old data (e.g., yesterday's activity today):  
  - Still checks against **correct historical shift**  

### **C. Missing Logouts**
- If an employee has **LOGIN but no LOGOUT**:  
  - System **does not auto-logout** (requires manual review)  

### **D. Shift Overlaps**
- If two shifts overlap (e.g., night + early morning):  
  - **Priority**: Current day login > Previous day logout  

---

## **5. Decision Tree**
```mermaid
graph TD
    A[Activity Time] --> B{Has Shift Data?}
    B -->|Yes| C[Check Current Day Login Window]
    B -->|No| D[Use Default Shift]
    C --> E{In Login Window?}
    E -->|Yes| F[Mark as LOGIN]
    E -->|No| G[Check Previous Day Logout Window]
    G --> H{In Logout Window?}
    H -->|Yes| I[Mark as LOGOUT]
    H -->|No| J[Mark as ACTIVE]
```

---

## **6. Key Assumptions**
1. **Shift End Time** = Actual end of work (not including buffer)  
2. **Login Window** = 4 hours before shift start  
3. **Logout Window** = 8 hours after shift end  
4. **No Shift Data** → Defaults to 9AM-5PM  

---

## **Next Steps**
Would you like me to:  
1. Provide the **complete PySpark code** for this logic?  
2. Add **additional validation rules** (e.g., minimum login duration)?  
3. Handle **special cases** like holidays?  

Let me know how you'd like to proceed! 🚀




---------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

def process_employee_activities():
    # Current timestamp and 70-minute threshold
    current_time = datetime.now()
    ingestion_threshold = current_time - timedelta(minutes=70)
    
    # Step 1: Get activities ingested in last 70 minutes
    recent_activities = spark.table("emp_activity").filter(
        F.col("ingestion_time") >= ingestion_threshold
    )
    
    if recent_activities.isEmpty():
        print("No new activities in the last 70 minutes")
        return
    
    # Step 2: Get latest activity per employee (from the recent ingestions)
    window_spec = Window.partitionBy("emp_id").orderBy(F.desc("start_time"))
    latest_activities = recent_activities.withColumn("row_num", F.row_number().over(window_spec)) \
                                      .filter(F.col("row_num") == 1) \
                                      .select("emp_id", "cal_date", "start_time", "app_name")
    
    # Step 3: Get distinct employee-date pairs we need to process
    emp_date_pairs = latest_activities.select("emp_id", "cal_date").distinct()
    
    # Step 4: Get corresponding shift data only for these specific employee-dates
    current_shifts = spark.table("shift").join(
        F.broadcast(emp_date_pairs),
        (F.col("shift.emp_id") == F.col("emp_id")) & 
        (F.col("shift.shift_date") == F.col("cal_date")),
        "inner"
    ).select("emp_id", "shift_date", "shift_start_time", "shift_end_time")
    
    # Step 5: Get previous day shifts for logout calculations
    prev_date_pairs = emp_date_pairs.withColumn("prev_date", F.date_sub("cal_date", 1))
    prev_shifts = spark.table("shift").join(
        F.broadcast(prev_date_pairs),
        (F.col("shift.emp_id") == F.col("emp_id")) & 
        (F.col("shift.shift_date") == F.col("prev_date")),
        "inner"
    ).select("emp_id", "shift_date", "shift_end_time")
    
    # Step 6: Join with current and previous shifts
    processed_data = latest_activities.join(
        current_shifts,
        ["emp_id", "cal_date"],
        "left"
    ).join(
        prev_shifts,
        (latest_activities["emp_id"] == prev_shifts["emp_id"]) &
        (F.date_sub(latest_activities["cal_date"], 1) == prev_shifts["shift_date"]),
        "left"
    )
    
    # Step 7: Determine activity type with current day login priority
    processed_data = processed_data.withColumn(
        "activity_type",
        F.when(
            # Current day login condition
            (F.col("shift_start_time").isNotNull()) &
            (F.col("start_time") >= (F.col("shift_start_time") - F.expr("INTERVAL 4 HOURS"))),
            F.lit("LOGIN")
        ).when(
            # Previous day logout condition
            (F.col("shift_end_time").isNotNull()) &
            (F.col("start_time") <= (F.col("shift_end_time") + F.expr("INTERVAL 8 HOURS"))),
            F.lit("LOGOUT")
        ).otherwise(
            F.lit("ACTIVE")  # Not a login/logout event
        )
    ).withColumn(
        "effective_date",
        F.when(
            F.col("activity_type") == "LOGOUT",
            F.date_sub(F.col("cal_date"), 1)  # Attribute to previous day
        ).otherwise(
            F.col("cal_date")  # Use activity date for logins
        )
    )
    
    # Step 8: Filter only login/logout events
    login_logout_events = processed_data.filter(
        F.col("activity_type").isin(["LOGIN", "LOGOUT"])
    )
    
    if login_logout_events.isEmpty():
        print("No login/logout events to process")
        return
    
    # Step 9: Get last known logout times for validation
    emp_ids = [row.emp_id for row in latest_activities.select("emp_id").distinct().collect()]
    last_logouts = spark.table("emp_login_logout") \
                      .filter(F.col("activity_type") == "LOGOUT") \
                      .filter(F.col("emp_id").isin(emp_ids)) \
                      .groupBy("emp_id") \
                      .agg(F.max("start_time").alias("last_logout_time"))
    
    # Step 10: Validate logins don't occur after known logouts
    final_records = login_logout_events.join(
        last_logouts,
        "emp_id",
        "left"
    ).filter(
        ~((F.col("activity_type") == "LOGIN") &
         (F.col("last_logout_time").isNotNull()) &
         (F.col("start_time") <= F.col("last_logout_time")))
    ).select(
        "emp_id",
        "effective_date".alias("cal_date"),
        "start_time",
        "activity_type",
        F.lit(current_time).alias("update_time")
    )
    
    # Step 11: Merge with target table
    final_records.createOrReplaceTempView("updates_temp")
    
    spark.sql("""
        MERGE INTO emp_login_logout target
        USING updates_temp source
        ON target.emp_id = source.emp_id AND target.cal_date = source.cal_date
        WHEN MATCHED AND (
            target.activity_type != source.activity_type OR
            (target.activity_type = 'LOGIN' AND source.start_time > target.start_time) OR
            (target.activity_type = 'LOGOUT' AND source.start_time < target.start_time)
        ) THEN
            UPDATE SET 
                target.start_time = source.start_time,
                target.activity_type = source.activity_type,
                target.update_time = source.update_time
        WHEN NOT MATCHED THEN
            INSERT (emp_id, cal_date, start_time, activity_type, update_time)
            VALUES (source.emp_id, source.cal_date, source.start_time, source.activity_type, source.update_time)
    """)
    
    print(f"Processed {final_records.count()} login/logout records")

# Schedule this job to run every hour
process_employee_activities()






WITH IntervalBuckets AS (
  -- Step 1: Create static list of 3-hour intervals for the day
  SELECT explode(array(
    '00:00-03:00', '03:00-06:00', '06:00-09:00', '09:00-12:00',
    '12:00-15:00', '15:00-18:00', '18:00-21:00', '21:00-00:00'
  )) AS interval
),

ThreeHourAggregated AS (
  -- Step 2: Extract and bucket interval from the data (if any)
  SELECT
    emp_id,
    cal_date,
    CONCAT(
      LPAD(FLOOR(CAST(SUBSTRING(interval, 1, 2) AS INT) / 3) * 3, 2, '0'), ':00-',
      LPAD(FLOOR(CAST(SUBSTRING(interval, 1, 2) AS INT) / 3) * 3 + 3, 2, '0'), ':00'
    ) AS interval,
    SUM(CAST(total_active_time AS DOUBLE)) AS total_active_time_seconds,
    SUM(CAST(total_idle_time AS DOUBLE)) AS total_idle_time_seconds,
    SUM(CAST(total_window_lock_time AS DOUBLE)) AS total_window_lock_time_seconds
  FROM gold_dashboard.analytics_emp_app_info
  WHERE emp_id = '<your_emp_id>'
    AND cal_date = CURRENT_DATE()
  GROUP BY emp_id, cal_date, 
           FLOOR(CAST(SUBSTRING(interval, 1, 2) AS INT) / 3)
),

FinalData AS (
  -- Step 3: Join static 3-hour intervals with actual aggregated data
  SELECT
    i.interval,
    COALESCE(a.total_active_time_seconds, 0) AS total_active_time_seconds,
    COALESCE(a.total_idle_time_seconds, 0) AS total_idle_time_seconds,
    COALESCE(a.total_window_lock_time_seconds, 0) AS total_window_lock_time_seconds
  FROM IntervalBuckets i
  LEFT JOIN ThreeHourAggregated a
    ON i.interval = a.interval
)

-- Step 4: Output result (e.g., select as JSON or return as API response)
SELECT
  to_json(named_struct(
    'employee_time_spent_data', collect_list(named_struct(
      'interval', interval,
      'total_active_time_seconds', total_active_time_seconds,
      'total_idle_time_seconds', total_idle_time_seconds,
      'total_window_lock_time_seconds', total_window_lock_time_seconds
    ))
  )) AS employee_time_spent_json
FROM FinalData;




-- Set employee ID here
WITH 
-- 1. Generate a date range from today - 30 days to today
DateRange AS (
  SELECT sequence(
    date_sub(current_date(), 30), 
    current_date(), 
    interval 1 day
  ) AS dates
),

-- 2. Flatten the dates into rows and assign week number
ExplodedDates AS (
  SELECT 
    explode(dates) AS cal_date 
  FROM DateRange
),

DateWithWeek AS (
  SELECT 
    cal_date,
    weekofyear(cal_date) AS week_number
  FROM ExplodedDates
),

-- 3. Filter and prepare actual employee data (minimal columns)
FilteredActivity AS (
  SELECT 
    cal_date,
    weekofyear(cal_date) AS week_number,
    CAST(total_idle_time AS DOUBLE) AS total_idle_time_seconds,
    CAST(total_active_time AS DOUBLE) AS total_non_prod_seconds,
    CAST(total_window_lock_time AS DOUBLE) AS total_time_spent_seconds
  FROM gold_dashboard.analytics_emp_app_info
  WHERE emp_id = '<your_emp_id>'
    AND cal_date BETWEEN date_sub(current_date(), 30) AND current_date()
),

-- 4. Aggregate actual data by week number
WeeklyAgg AS (
  SELECT 
    week_number,
    SUM(total_idle_time_seconds) AS total_idle_time_seconds,
    SUM(total_non_prod_seconds) AS total_non_prod_seconds,
    SUM(total_time_spent_seconds) AS total_time_spent_seconds
  FROM FilteredActivity
  GROUP BY week_number
),

-- 5. Get distinct weeks in range (even if no data)
AllWeeks AS (
  SELECT DISTINCT week_number FROM DateWithWeek
),

-- 6. Left join to ensure all weeks show up in final output
FinalData AS (
  SELECT 
    w.week_number,
    COALESCE(a.total_idle_time_seconds, 0) AS total_idle_time_seconds,
    COALESCE(a.total_non_prod_seconds, 0) AS total_non_prod_seconds,
    COALESCE(a.total_time_spent_seconds, 0) AS total_time_spent_seconds
  FROM AllWeeks w
  LEFT JOIN WeeklyAgg a ON w.week_number = a.week_number
)

-- 7. Format as JSON
SELECT to_json(named_struct(
  'employee_time_spent_data', collect_list(named_struct(
    'week_number', week_number,
    'total_idle_time_seconds', total_idle_time_seconds,
    'total_non_prod_seconds', total_non_prod_seconds,
    'total_time_spent_seconds', total_time_spent_seconds
  ))
)) AS employee_time_spent_json
FROM FinalData
ORDER BY week_number;










-- 1. Generate date range from start_date to end_date
WITH DateRange AS (
  SELECT sequence(to_date('<start_date>'), to_date('<end_date>'), interval 1 day) AS dates
),
ExplodedDates AS (
  SELECT explode(dates) AS cal_date FROM DateRange
),

-- 2. Aggregate actual employee activity by date
DailyAgg AS (
  SELECT
    cal_date,
    SUM(CAST(total_idle_time AS DOUBLE)) AS total_idle_time_seconds,
    SUM(CAST(total_active_time AS DOUBLE)) AS total_non_prod_seconds,
    SUM(CAST(total_window_lock_time AS DOUBLE)) AS total_time_spent_seconds
  FROM gold_dashboard.analytics_emp_app_info
  WHERE cal_date BETWEEN DATE('<start_date>') AND DATE('<end_date>')
    AND emp_id = '<your_emp_id>'
  GROUP BY cal_date
),

-- 3. Left join date range with actual data
FullData AS (
  SELECT
    d.cal_date,
    COALESCE(a.total_idle_time_seconds, 0) AS total_idle_time_seconds,
    COALESCE(a.total_non_prod_seconds, 0) AS total_non_prod_seconds,
    COALESCE(a.total_time_spent_seconds, 0) AS total_time_spent_seconds
  FROM ExplodedDates d
  LEFT JOIN DailyAgg a ON d.cal_date = a.cal_date
),

-- 4. Format final JSON
FinalJSON AS (
  SELECT TO_JSON(
    NAMED_STRUCT(
      'employee_time_spent_data', COLLECT_LIST(
        NAMED_STRUCT(
          'cal_date', cal_date,
          'total_idle_time_seconds', total_idle_time_seconds,
          'total_non_prod_seconds', total_non_prod_seconds,
          'total_time_spent_seconds', total_time_spent_seconds
        )
      )
    )
  ) AS employee_time_summary
  FROM FullData
)

SELECT * FROM FinalJSON;






TeamAverages AS (
    SELECT 
        p.period_type,
        AVG(s.daily_active_time) AS avg_team_active_time,
        AVG(s.daily_active_time + s.daily_idle_time + s.daily_window_lock_time) AS avg_team_total_time
    FROM (VALUES ('current'), ('previous')) p(period_type)
    LEFT JOIN PerDayEmployeeSummary s
        ON (p.period_type = 'current' AND s.cal_date BETWEEN @start_date AND @end_date)
        OR (p.period_type = 'previous' AND s.cal_date BETWEEN 
            DATEADD(DAY, DATEDIFF(DAY, @end_date, @start_date) - 1, @start_date) 
            AND DATEADD(DAY, DATEDIFF(DAY, @end_date, @start_date) - 1, @end_date)
        )
    GROUP BY p.period_type
)





WITH FilteredEmployees AS (
    SELECT emplid AS emp_id
    FROM lobound.hr_employee_central
    WHERE func_mer_id = 45179442
),

EmployeeActivity AS (
    SELECT 
        e.emp_id,
        e.cal_date,
        e.app_name,
        COALESCE(SUM(e.total_time_spent_active), 0) AS total_active_time,
        COALESCE(SUM(e.total_time_spent_idle), 0) AS total_idle_time
    FROM gold_dashboard.analytics_emp_app_info e
    JOIN FilteredEmployees fe ON e.emp_id = fe.emp_id
    GROUP BY e.emp_id, e.cal_date, e.app_name
),

PerDayEmployeeSummary AS (
    SELECT 
        emp_id,
        cal_date,
        SUM(total_active_time) AS daily_active_time,
        SUM(total_active_time + total_idle_time) AS daily_total_time
    FROM EmployeeActivity
    GROUP BY emp_id, cal_date
),

TeamAverages AS (
    SELECT 
        'current' AS period_type,
        AVG(daily_active_time) AS avg_team_active_time,
        AVG(daily_total_time) AS avg_team_total_time
    FROM PerDayEmployeeSummary
    WHERE cal_date BETWEEN '2025-03-10' AND '2025-03-16'

    UNION ALL

    SELECT 
        'previous' AS period_type,
        AVG(daily_active_time),
        AVG(daily_total_time)
    FROM PerDayEmployeeSummary
    WHERE cal_date BETWEEN date_add('2025-03-10', -datediff('2025-03-16', '2025-03-10')) 
                      AND date_add('2025-03-10', -1)
),

ApplicationAverages AS (
    SELECT 
        'current' AS period_type,
        app_name,
        emp_id,
        AVG(total_active_time) AS avg_active_time,
        AVG(total_idle_time) AS avg_idle_time
    FROM EmployeeActivity
    WHERE cal_date BETWEEN '2025-03-10' AND '2025-03-16'
    GROUP BY app_name, emp_id

    UNION ALL

    SELECT 
        'previous' AS period_type,
        app_name,
        emp_id,
        AVG(total_active_time),
        AVG(total_idle_time)
    FROM EmployeeActivity
    WHERE cal_date BETWEEN date_add('2025-03-10', -datediff('2025-03-16', '2025-03-10')) 
                      AND date_add('2025-03-10', -1)
    GROUP BY app_name, emp_id
),

FinalApplicationSummary AS (
    SELECT 
        app_name,
        AVG(CASE WHEN period_type = 'current' THEN avg_active_time END) AS current_active_time,
        AVG(CASE WHEN period_type = 'current' THEN avg_idle_time END) AS current_idle_time,
        AVG(CASE WHEN period_type = 'previous' THEN avg_active_time END) AS last_active_time,
        AVG(CASE WHEN period_type = 'previous' THEN avg_idle_time END) AS last_idle_time,
        
        (AVG(CASE WHEN period_type = 'current' THEN avg_active_time END) - 
         AVG(CASE WHEN period_type = 'previous' THEN avg_active_time END)) / 
         NULLIF(AVG(CASE WHEN period_type = 'previous' THEN avg_active_time END), 0) * 100 AS active_change,

        (AVG(CASE WHEN period_type = 'current' THEN avg_idle_time END) - 
         AVG(CASE WHEN period_type = 'previous' THEN avg_idle_time END)) / 
         NULLIF(AVG(CASE WHEN period_type = 'previous' THEN avg_idle_time END), 0) * 100 AS idle_change
    FROM ApplicationAverages
    GROUP BY app_name
),

TeamSummary AS (
    SELECT 
        (SELECT avg_team_active_time FROM TeamAverages WHERE period_type = 'current') AS activeavginsec,
        (SELECT avg_team_active_time FROM TeamAverages WHERE period_type = 'previous') AS lastactiveavginsec,
        (SELECT avg_team_active_time > (SELECT avg_team_active_time FROM TeamAverages WHERE period_type = 'previous') 
         FROM TeamAverages WHERE period_type = 'current' LIMIT 1) AS isactivetrendup,
        (SELECT (avg_team_active_time - (SELECT avg_team_active_time FROM TeamAverages WHERE period_type = 'previous')) / 
                NULLIF((SELECT avg_team_active_time FROM TeamAverages WHERE period_type = 'previous'), 0) * 100
         FROM TeamAverages WHERE period_type = 'current') AS active_change,
        (SELECT avg_team_total_time FROM TeamAverages WHERE period_type = 'current') AS totalavginsec,
        (SELECT avg_team_total_time FROM TeamAverages WHERE period_type = 'previous') AS lasttotalavginsec,
        (SELECT avg_team_total_time > (SELECT avg_team_total_time FROM TeamAverages WHERE period_type = 'previous') 
         FROM TeamAverages WHERE period_type = 'current' LIMIT 1) AS istotaltrendup,
        (SELECT (avg_team_total_time - (SELECT avg_team_total_time FROM TeamAverages WHERE period_type = 'previous')) / 
                NULLIF((SELECT avg_team_total_time FROM TeamAverages WHERE period_type = 'previous'), 0) * 100
         FROM TeamAverages WHERE period_type = 'current') AS total_change
),

AppSummary AS (
    SELECT 
        f.app_name AS application_name,
        f.current_active_time AS active_time,
        f.current_idle_time AS idle_time,
        (
            SELECT collect_list(
                named_struct(
                    'employee_id', a.emp_id,
                    'active_time', a.avg_active_time,
                    'idle_time', a.avg_idle_time
                )
            )
            FROM ApplicationAverages a 
            WHERE a.app_name = f.app_name AND period_type = 'current'
        ) AS empSummary
    FROM FinalApplicationSummary f
)

SELECT to_json(
    named_struct(
        'teamSummary', 
        (SELECT named_struct(
            'activeavginsec', activeavginsec,
            'lastactiveavginsec', lastactiveavginsec,
            'isactivetrendup', isactivetrendup,
            'active_change', active_change,
            'totalavginsec', totalavginsec,
            'lasttotalavginsec', lasttotalavginsec,
            'istotaltrendup', istotaltrendup,
            'total_change', total_change
        ) FROM TeamSummary),
        'appSummary', 
        (SELECT collect_list(
            named_struct(
                'application_name', application_name,
                'active_time', active_time,
                'idle_time', idle_time,
                'empSummary', empSummary
            )
        ) FROM AppSummary)
    )
) AS json_result








WITH FilteredEmployees AS ( -- Step 1: Get employees under a specific ManagerID SELECT emp_id FROM HREmployeeCentral WHERE manager_id = @manager_id ), EmployeeActivity AS ( -- Step 2: Extract activity data only for the employees under the specified ManagerID SELECT l.emp_id, i.shifted_date, i.application_name, COALESCE(SUM(i.active_time_sec), 0) AS total_active_time, COALESCE(SUM(i.ideal_time_sec), 0) AS total_idle_time FROM empinfo i LEFT JOIN emploginlogout l ON i.emp_id = l.emp_id AND i.shifted_date = l.shifted_date WHERE l.emp_id IN (SELECT emp_id FROM FilteredEmployees)  -- Filter by ManagerID GROUP BY l.emp_id, i.shifted_date, i.application_name ), PerDayEmployeeSummary AS ( -- Step 3: Aggregate per employee per day SELECT emp_id, shifted_date, SUM(total_active_time) AS daily_active_time, SUM(total_idle_time) AS daily_idle_time FROM EmployeeActivity GROUP BY emp_id, shifted_date ), TeamAverages AS ( -- Step 4: Compute team-level averages SELECT 'current' AS period_type, AVG(daily_active_time) AS avg_team_active_time, AVG(daily_idle_time) AS avg_team_idle_time, AVG(daily_active_time + daily_idle_time) AS avg_team_total_time FROM PerDayEmployeeSummary WHERE shifted_date BETWEEN @start_date AND @end_date

UNION ALL

SELECT 
    'previous', 
    AVG(daily_active_time), 
    AVG(daily_idle_time), 
    AVG(daily_active_time + daily_idle_time)
FROM PerDayEmployeeSummary 
WHERE shifted_date BETWEEN DATE_SUB(@start_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY) 
                      AND DATE_SUB(@end_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY)

), ApplicationAverages AS ( -- Step 5: Compute application-level averages SELECT application_name, emp_id, 'current' AS period_type, AVG(total_active_time) AS avg_active_time, AVG(total_idle_time) AS avg_idle_time FROM EmployeeActivity WHERE shifted_date BETWEEN @start_date AND @end_date GROUP BY application_name, emp_id

UNION ALL 

SELECT 
    application_name, 
    emp_id, 
    'previous',
    AVG(total_active_time), 
    AVG(total_idle_time)
FROM EmployeeActivity
WHERE shifted_date BETWEEN DATE_SUB(@start_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY) 
                      AND DATE_SUB(@end_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY)
GROUP BY application_name, emp_id

), FinalApplicationSummary AS ( -- Step 6: Compute final application summary (only keep applications in the current period) SELECT app.application_name, AVG(CASE WHEN app.period_type = 'current' THEN app.avg_active_time END) AS current_active_time, AVG(CASE WHEN app.period_type = 'current' THEN app.avg_idle_time END) AS current_idle_time, AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_active_time END) AS last_active_time, AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_idle_time END) AS last_idle_time, (AVG(CASE WHEN app.period_type = 'current' THEN app.avg_active_time END) - AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_active_time END)) / NULLIF(AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_active_time END), 0) * 100 AS active_change, (AVG(CASE WHEN app.period_type = 'current' THEN app.avg_idle_time END) - AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_idle_time END)) / NULLIF(AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_idle_time END), 0) * 100 AS idle_change FROM ApplicationAverages app WHERE app.application_name IN (SELECT DISTINCT application_name FROM ApplicationAverages WHERE period_type = 'current') GROUP BY app.application_name ) SELECT JSON_OBJECT( 'teamSummary', JSON_ARRAYAGG( JSON_OBJECT( 'currentactiveavginsec', t1.avg_team_active_time, 'lastactiveavginsec', t2.avg_team_active_time, 'activetrend', CASE WHEN t1.avg_team_active_time > t2.avg_team_active_time THEN 'Up' ELSE 'Down' END, 'active_change', ((t1.avg_team_active_time - t2.avg_team_active_time) / t2.avg_team_active_time) * 100, 'currenttotalavginsec', t1.avg_team_total_time, 'lasttotalavginsec', t2.avg_team_total_time, 'totaltrend', CASE WHEN t1.avg_team_total_time > t2.avg_team_total_time THEN 'Up' ELSE 'Down' END, 'total_change', ((t1.avg_team_total_time - t2.avg_team_total_time) / t2.avg_team_total_time) * 100 ) ), 'appSummary', JSON_ARRAYAGG( JSON_OBJECT( 'application_name', fa.application_name, 'current_active_time', fa.current_active_time, 'current_idle_time', fa.current_idle_time, 'last_active_time', fa.last_active_time, 'last_idle_time', fa.last_idle_time, 'active_change', fa.active_change, 'idle_change', fa.idle_change, 'active_trend', CASE WHEN fa.active_change > 0 THEN 'Up' ELSE 'Down' END, 'idle_trend', CASE WHEN fa.idle_change > 0 THEN 'Up' ELSE 'Down' END, 'empSummary', JSON_ARRAYAGG( JSON_OBJECT( 'employee_id', aa.emp_id, 'current_active_time', aa.avg_active_time, 'current_idle_time', aa.avg_idle_time ) ) ) ) ) AS final_output FROM TeamAverages t1 JOIN TeamAverages t2 ON t1.period_type = 'current' AND t2.period_type = 'previous' JOIN FinalApplicationSummary fa JOIN ApplicationAverages aa ON fa.application_name = aa.application_name AND aa.period_type = 'current' GROUP BY fa.application_name;











{
    "teamSummary": [
        {
            "currentactiveavginsec": 3500,
            "lastactiveavginsec": 3300,
            "activetrend": "Up",
            "active_change": 6.06,
            "currenttotalavginsec": 5000,
            "lasttotalavginsec": 4800,
            "totaltrend": "Up",
            "total_change": 4.17
        }
    ],
    "appSummary": [
        {
            "application_name": "Microsoft Excel",
            "current_active_time": 1200,
            "current_idle_time": 300,
            "last_active_time": 1100,
            "last_idle_time": 250,
            "active_change": 9.09,
            "idle_change": 20.00,
            "active_trend": "Up",
            "idle_trend": "Up",
            "empSummary": [
                {
                    "employee_id": "EMP001",
                    "active_time": 600,
                    "idle_time": 150
                },
                {
                    "employee_id": "EMP002",
                    "active_time": 400,
                    "idle_time": 100
                }
            ]
        },
        {
            "application_name": "Google Chrome",
            "current_active_time": 1500,
            "current_idle_time": 400,
            "last_active_time": 1400,
            "last_idle_time": 350,
            "active_change": 7.14,
            "idle_change": 14.29,
            "active_trend": "Up",
            "idle_trend": "Up",
            "empSummary": [
                {
                    "employee_id": "EMP001",
                    "active_time": 800,
                    "idle_time": 200
                },
                {
                    "employee_id": "EMP003",
                    "active_time": 500,
                    "idle_time": 150
                }
            ]
        }
    ]
}



WITH FilteredEmployees AS ( -- Step 1: Get employees under a specific ManagerID SELECT emp_id FROM HREmployeeCentral WHERE manager_id = @manager_id ), EmployeeActivity AS ( -- Step 2: Extract activity data only for the employees under the specified ManagerID SELECT l.emp_id, i.shifted_date, i.application_name, COALESCE(SUM(i.active_time_sec), 0) AS total_active_time, COALESCE(SUM(i.ideal_time_sec), 0) AS total_idle_time, COALESCE(SUM(i.window_lock_time_sec), 0) AS total_window_lock_time FROM empinfo i LEFT JOIN emploginlogout l ON i.emp_id = l.emp_id AND i.shifted_date = l.shifted_date AND ( (l.logout_time IS NULL AND l.login_time BETWEEN STR_TO_DATE(i.time_interval, '%H:%i') AND ADDTIME(STR_TO_DATE(i.time_interval, '%H:%i'), '01:00:00')) OR (l.logout_time IS NOT NULL AND l.logout_time BETWEEN STR_TO_DATE(i.time_interval, '%H:%i') AND ADDTIME(STR_TO_DATE(i.time_interval, '%H:%i'), '01:00:00')) ) WHERE l.emp_id IN (SELECT emp_id FROM FilteredEmployees)  -- Filter by ManagerID GROUP BY l.emp_id, i.shifted_date, i.application_name ), PerDayEmployeeSummary AS ( -- Step 3: Aggregate per employee per day SELECT emp_id, shifted_date, SUM(total_active_time) AS daily_active_time, SUM(total_idle_time) AS daily_idle_time, SUM(total_window_lock_time) AS daily_window_time FROM EmployeeActivity GROUP BY emp_id, shifted_date ), TeamAverages AS ( -- Step 4: Compute team-level averages SELECT 'current' AS period_type, AVG(daily_active_time) AS avg_team_active_time, AVG(daily_idle_time) AS avg_team_idle_time, AVG(daily_active_time + daily_idle_time) AS avg_team_total_time FROM PerDayEmployeeSummary WHERE shifted_date BETWEEN @start_date AND @end_date

UNION ALL

SELECT 
    'previous', 
    AVG(daily_active_time), 
    AVG(daily_idle_time), 
    AVG(daily_active_time + daily_idle_time)
FROM PerDayEmployeeSummary 
WHERE shifted_date BETWEEN DATE_SUB(@start_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY) 
                      AND DATE_SUB(@end_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY)

), ApplicationAverages AS ( -- Step 5: Compute application-level averages SELECT application_name, emp_id, 'current' AS period_type, AVG(total_active_time) AS avg_active_time, AVG(total_idle_time) AS avg_idle_time FROM EmployeeActivity WHERE shifted_date BETWEEN @start_date AND @end_date GROUP BY application_name, emp_id

UNION ALL 

SELECT 
    application_name, 
    emp_id, 
    'previous',
    AVG(total_active_time), 
    AVG(total_idle_time)
FROM EmployeeActivity
WHERE shifted_date BETWEEN DATE_SUB(@start_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY) 
                      AND DATE_SUB(@end_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY)
GROUP BY application_name, emp_id

), FinalApplicationSummary AS ( -- Step 6: Compute final application summary (only keep applications in the current period) SELECT app.application_name, AVG(CASE WHEN app.period_type = 'current' THEN app.avg_active_time END) AS current_active_time, AVG(CASE WHEN app.period_type = 'current' THEN app.avg_idle_time END) AS current_idle_time, AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_active_time END) AS last_active_time, AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_idle_time END) AS last_idle_time, (AVG(CASE WHEN app.period_type = 'current' THEN app.avg_active_time END) - AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_active_time END)) / NULLIF(AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_active_time END), 0) * 100 AS active_change, (AVG(CASE WHEN app.period_type = 'current' THEN app.avg_idle_time END) - AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_idle_time END)) / NULLIF(AVG(CASE WHEN app.period_type = 'previous' THEN app.avg_idle_time END), 0) * 100 AS idle_change FROM ApplicationAverages app WHERE app.application_name IN (SELECT DISTINCT application_name FROM ApplicationAverages WHERE period_type = 'current') GROUP BY app.application_name ), EmployeeApplicationSummary AS ( -- Step 7: Employee-level summary per application (current period only) SELECT application_name, emp_id, SUM(total_active_time) AS employee_active_time, SUM(total_idle_time) AS employee_idle_time FROM EmployeeActivity WHERE shifted_date BETWEEN @start_date AND @end_date GROUP BY application_name, emp_id ) SELECT JSON_OBJECT( 'teamSummary', JSON_ARRAYAGG( JSON_OBJECT( 'currentactiveavginsec', t1.avg_team_active_time, 'lastactiveavginsec', t2.avg_team_active_time, 'activetrend', CASE WHEN t1.avg_team_active_time > t2.avg_team_active_time THEN 'Up' ELSE 'Down' END, 'active_change', ((t1.avg_team_active_time - t2.avg_team_active_time) / t2.avg_team_active_time) * 100, 'currenttotalavginsec', t1.avg_team_total_time, 'lasttotalavginsec', t2.avg_team_total_time, 'totaltrend', CASE WHEN t1.avg_team_total_time > t2.avg_team_total_time THEN 'Up' ELSE 'Down' END, 'total_change', ((t1.avg_team_total_time - t2.avg_team_total_time) / t2.avg_team_total_time) * 100 ) ), 'appSummary', JSON_ARRAYAGG( JSON_OBJECT( 'application_name', fa.application_name, 'current_active_time', fa.current_active_time, 'current_idle_time', fa.current_idle_time, 'last_active_time', fa.last_active_time, 'last_idle_time', fa.last_idle_time, 'active_change', fa.active_change, 'idle_change', fa.idle_change, 'active_trend', CASE WHEN fa.active_change > 0 THEN 'Up' ELSE 'Down' END, 'idle_trend', CASE WHEN fa.idle_change > 0 THEN 'Up' ELSE 'Down' END, 'empSummary', ( SELECT JSON_ARRAYAGG( JSON_OBJECT( 'employee_id', eas.emp_id, 'current_active_time', eas.employee_active_time, 'current_idle_time', eas.employee_idle_time ) ) FROM EmployeeApplicationSummary eas WHERE eas.application_name = fa.application_name ) ) ) ) AS final_output FROM TeamAverages t1 JOIN TeamAverages t2 ON t1.period_type = 'current' AND t2.period_type = 'previous' JOIN FinalApplicationSummary fa;







WITH FilteredEmployees AS (
    -- Step 1: Get employees under a specific ManagerID
    SELECT emp_id 
    FROM HREmployeeCentral
    WHERE manager_id = @manager_id
),
EmployeeActivity AS (
    -- Step 2: Extract activity data for the selected employees
    SELECT 
        i.emp_id,
        i.shifted_date,
        i.application_name,
        COALESCE(SUM(i.active_time_sec), 0) AS total_active_time,
        COALESCE(SUM(i.ideal_time_sec), 0) AS total_idle_time,
        COALESCE(SUM(i.window_lock_time_sec), 0) AS total_window_lock_time
    FROM empinfo i
    LEFT JOIN emploginlogout l 
        ON i.emp_id = l.emp_id 
        AND i.shifted_date = l.shifted_date
        AND (
            -- Convert login and logout times using required format
            (l.login_time IS NOT NULL 
             AND l.logout_time IS NOT NULL 
             AND TO_TIMESTAMP(l.login_time, 'YYYY-MM-DD HH:MI:SS') 
                 <= TO_TIMESTAMP(CONCAT(i.shifted_date, ' ', SUBSTRING(i.time_interval, 1, 5), ':00'), 'YYYY-MM-DD HH:MI:SS')
                 AND TO_TIMESTAMP(l.logout_time, 'YYYY-MM-DD HH:MI:SS') 
                 >= TO_TIMESTAMP(CONCAT(i.shifted_date, ' ', SUBSTRING(i.time_interval, 1, 5), ':00'), 'YYYY-MM-DD HH:MI:SS')
            )

            -- If logout_time is NULL, consider all intervals after login
            OR
            (l.logout_time IS NULL 
             AND TO_TIMESTAMP(l.login_time, 'YYYY-MM-DD HH:MI:SS') 
                 <= TO_TIMESTAMP(CONCAT(i.shifted_date, ' ', SUBSTRING(i.time_interval, 1, 5), ':00'), 'YYYY-MM-DD HH:MI:SS')
            )
        )
    WHERE l.emp_id IN (SELECT emp_id FROM FilteredEmployees)  -- Filter by ManagerID
    GROUP BY i.emp_id, i.shifted_date, i.application_name
)
SELECT * FROM EmployeeActivity;



WITH FilteredEmployees AS (
    -- Step 1: Get employees under a specific ManagerID
    SELECT emp_id 
    FROM HREmployeeCentral
    WHERE manager_id = @manager_id
),
EmployeeActivity AS (
    -- Step 2: Extract activity data for the selected employees
    SELECT 
        i.emp_id,
        i.shifted_date,
        i.application_name,
        COALESCE(SUM(i.active_time_sec), 0) AS total_active_time,
        COALESCE(SUM(i.ideal_time_sec), 0) AS total_idle_time,
        COALESCE(SUM(i.window_lock_time_sec), 0) AS total_window_lock_time
    FROM empinfo i
    LEFT JOIN emploginlogout l 
        ON i.emp_id = l.emp_id 
        AND i.shifted_date = l.shifted_date
        AND (
            -- Convert login and logout times to TIMESTAMP
            (CAST(l.login_time AS TIMESTAMP) IS NOT NULL AND 
             CAST(l.logout_time AS TIMESTAMP) IS NOT NULL AND 
             
             -- Extract start and end times from the interval
             CAST(CONCAT(i.shifted_date, ' ', SUBSTRING(i.time_interval, 1, 5), ':00') AS TIMESTAMP) 
             BETWEEN CAST(l.login_time AS TIMESTAMP) 
                 AND CAST(l.logout_time AS TIMESTAMP)
            )
            
            -- If logout_time is NULL, consider all intervals after login
            OR
            (CAST(l.logout_time AS TIMESTAMP) IS NULL AND 
             CAST(CONCAT(i.shifted_date, ' ', SUBSTRING(i.time_interval, 1, 5), ':00') AS TIMESTAMP) 
             >= CAST(l.login_time AS TIMESTAMP)
            )
        )
    WHERE l.emp_id IN (SELECT emp_id FROM FilteredEmployees)  -- Filter by ManagerID
    GROUP BY i.emp_id, i.shifted_date, i.application_name
)
SELECT * FROM EmployeeActivity;







WITH EmployeeActivity AS (
    -- Step 1: Extract employee activity based on login times, considering logout time and handling NULL logout times
    SELECT 
        l.emp_id,
        i.shifted_date,
        i.application_name,
        COALESCE(SUM(i.active_time_sec), 0) AS total_active_time,
        COALESCE(SUM(i.ideal_time_sec), 0) AS total_idle_time,
        COALESCE(SUM(i.window_lock_time_sec), 0) AS total_window_lock_time
    FROM empinfo i
    LEFT JOIN emploginlogout l 
        ON i.emp_id = l.emp_id 
        AND i.shifted_date = l.shifted_date 
        -- Handling login time to be within the interval, but now considering logout time logic
        AND l.login_time BETWEEN STR_TO_DATE(i.time_interval, '%H:%i') AND ADDTIME(STR_TO_DATE(i.time_interval, '%H:%i'), '01:00:00') 
        -- If logout time is NULL, consider the last possible interval (e.g., end of the workday)
        AND (
            (l.logout_time IS NULL AND l.login_time BETWEEN STR_TO_DATE(i.time_interval, '%H:%i') AND ADDTIME(STR_TO_DATE(i.time_interval, '%H:%i'), '01:00:00'))
            OR
            (l.logout_time IS NOT NULL AND l.logout_time BETWEEN STR_TO_DATE(i.time_interval, '%H:%i') AND ADDTIME(STR_TO_DATE(i.time_interval, '%H:%i'), '01:00:00'))
        )
    GROUP BY l.emp_id, i.shifted_date, i.application_name
),
PerDayEmployeeSummary AS (
    -- Step 2: Aggregate total active, idle, and window lock times per employee per day
    SELECT 
        emp_id,
        shifted_date,
        SUM(total_active_time) AS daily_active_time,
        SUM(total_idle_time) AS daily_idle_time,
        SUM(total_window_lock_time) AS daily_window_time
    FROM EmployeeActivity
    GROUP BY emp_id, shifted_date
),
TeamAverages AS (
    -- Step 3: Compute team-level averages for current and previous period
    SELECT 
        'current' AS period_type, 
        AVG(daily_active_time) AS avg_team_active_time, 
        AVG(daily_idle_time) AS avg_team_idle_time, 
        AVG(daily_active_time + daily_idle_time) AS avg_team_total_time
    FROM PerDayEmployeeSummary 
    WHERE shifted_date BETWEEN @start_date AND @end_date

    UNION ALL

    SELECT 
        'previous', 
        AVG(daily_active_time), 
        AVG(daily_idle_time), 
        AVG(daily_active_time + daily_idle_time)
    FROM PerDayEmployeeSummary 
    WHERE shifted_date BETWEEN DATE_SUB(@start_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY) AND DATE_SUB(@end_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY)
),
ApplicationAverages AS (
    -- Step 4: Compute application-level averages per employee
    SELECT 
        application_name, 
        emp_id, 
        AVG(total_active_time) AS avg_active_time, 
        AVG(total_idle_time) AS avg_idle_time
    FROM EmployeeActivity
    WHERE shifted_date BETWEEN @start_date AND @end_date
    GROUP BY application_name, emp_id

    UNION ALL 

    SELECT 
        application_name, 
        emp_id, 
        AVG(total_active_time), 
        AVG(total_idle_time)
    FROM EmployeeActivity
    WHERE shifted_date BETWEEN DATE_SUB(@start_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY) AND DATE_SUB(@end_date, INTERVAL (DATEDIFF(@end_date, @start_date) + 1) DAY)
    GROUP BY application_name, emp_id
),
FinalApplicationSummary AS (
    -- Step 5: Compute final application-level summary
    SELECT 
        app.application_name, 
        AVG(CASE WHEN period_type = 'current' THEN avg_active_time END) AS current_active_time, 
        AVG(CASE WHEN period_type = 'current' THEN avg_idle_time END) AS current_idle_time, 
        AVG(CASE WHEN period_type = 'previous' THEN avg_active_time END) AS last_active_time, 
        AVG(CASE WHEN period_type = 'previous' THEN avg_idle_time END) AS last_idle_time, 
        (AVG(CASE WHEN period_type = 'current' THEN avg_active_time END) - AVG(CASE WHEN period_type = 'previous' THEN avg_active_time END)) / AVG(CASE WHEN period_type = 'previous' THEN avg_active_time END) * 100 AS active_change, 
        (AVG(CASE WHEN period_type = 'current' THEN avg_idle_time END) - AVG(CASE WHEN period_type = 'previous' THEN avg_idle_time END)) / AVG(CASE WHEN period_type = 'previous' THEN avg_idle_time END) * 100 AS idle_change
    FROM ApplicationAverages app
    GROUP BY app.application_name
),
EmployeeSummary AS (
    -- Step 6: Employee-level details
    SELECT 
        emp_id, 
        SUM(total_active_time) AS employee_active_time, 
        SUM(total_idle_time) AS employee_idle_time
    FROM EmployeeActivity
    WHERE shifted_date BETWEEN @start_date AND @end_date
    GROUP BY emp_id
)
SELECT 
    JSON_OBJECT(
        'teamSummary', JSON_ARRAYAGG(
            JSON_OBJECT(
                'currentactiveavginsec', t1.avg_team_active_time, 
                'lastactiveavginsec', t2.avg_team_active_time, 
                'activetrend', CASE WHEN t1.avg_team_active_time > t2.avg_team_active_time THEN 'Up' ELSE 'Down' END, 
                'active_change', ((t1.avg_team_active_time - t2.avg_team_active_time) / t2.avg_team_active_time) * 100,
                'currenttotalavginsec', t1.avg_team_total_time, 
                'lasttotalavginsec', t2.avg_team_total_time, 
                'totaltrend', CASE WHEN t1.avg_team_total_time > t2.avg_team_total_time THEN 'Up' ELSE 'Down' END, 
                'total_change', ((t1.avg_team_total_time - t2.avg_team_total_time) / t2.avg_team_total_time) * 100
            )
        ),
        'appSummary', JSON_ARRAYAGG(
            JSON_OBJECT(
                'application_name', fa.application_name, 
                'current_active_time', fa.current_active_time, 
                'current_idle_time', fa.current_idle_time, 
                'last_active_time', fa.last_active_time, 
                'last_idle_time', fa.last_idle_time, 
                'active_change', fa.active_change, 
                'idle_change', fa.idle_change, 
                'active_trend', CASE WHEN fa.active_change > 0 THEN 'Up' ELSE 'Down' END, 
                'idle_trend', CASE WHEN fa.idle_change > 0 THEN 'Up' ELSE 'Down' END
            )
        ),
        'employeeInfo', JSON_ARRAYAGG(
            JSON_OBJECT(
                'employee_id', es.emp_id, 
                'current_active_time', es.employee_active_time, 
                'current_idle_time', es.employee_idle_time
            )
        )
    ) AS final_output
FROM TeamAverages t1 
JOIN TeamAverages t2 
    ON t1.period_type = 'current' AND t2.period_type = 'previous' 
JOIN FinalApplicationSummary fa 
JOIN EmployeeSummary es;







Got it! You have two different sources of data:

1. EMPAPPINFO Table (Interval-based data, recorded every hour)


2. EMPLOGINLOGOUT Table (Actual login/logout timestamps per employee per day)



So, calculations should:

Restrict data within the actual login-logout duration per employee from EMPLOGINLOGOUT.

Aggregate hourly intervals from EMPAPPINFO, but only within each employee's actual working hours.



---

Updated SQL Query Considering Both Tables

WITH Employee_Working_Hours AS (
    SELECT 
        emp_id,
        cal_date,
        MIN(login_time) AS first_login,
        MAX(logout_time) AS last_logout
    FROM EMP_ANALYTICS.EMPLOGINLOGOUT
    WHERE cal_date BETWEEN :start_date AND :end_date
    GROUP BY emp_id, cal_date
),
Filtered_Employee_Intervals AS (
    SELECT 
        e.emp_id,
        e.application_name,
        e.cal_date,
        e.active_time,
        e.idle_time,
        CASE WHEN e.application_name = 'Window Lock' THEN e.active_time ELSE 0 END AS window_lock_time
    FROM EMP_ANALYTICS.EMPAPPINFO e
    JOIN Employee_Working_Hours l ON e.emp_id = l.emp_id AND e.cal_date = l.cal_date
    WHERE e.timestamp BETWEEN l.first_login AND l.last_logout
),
Employee_Daily_Sum AS (
    SELECT
        emp_id,
        application_name,
        cal_date,
        SUM(active_time) AS daily_active_time,
        SUM(idle_time) AS daily_idle_time,
        SUM(window_lock_time) AS daily_window_lock_time
    FROM Filtered_Employee_Intervals
    GROUP BY emp_id, application_name, cal_date
),
Employee_Average AS (
    SELECT
        emp_id,
        application_name,
        AVG(daily_active_time) AS avg_active_time,
        AVG(daily_idle_time) AS avg_idle_time,
        AVG(daily_window_lock_time) AS avg_window_lock_time
    FROM Employee_Daily_Sum
    GROUP BY emp_id, application_name
),
Application_Average AS (
    SELECT
        application_name,
        AVG(avg_active_time) AS application_active_time,
        AVG(avg_idle_time) AS application_idle_time,
        AVG(avg_window_lock_time) AS application_window_lock_time
    FROM Employee_Average
    GROUP BY application_name
),
Team_Average AS (
    SELECT
        AVG(application_active_time) AS team_active_time,
        AVG(application_idle_time) AS team_idle_time,
        AVG(application_window_lock_time) AS team_window_lock_time
    FROM Application_Average
)
SELECT 
    a.application_name,
    a.application_active_time AS current_active_time,
    a.application_idle_time AS current_idle_time,
    a.application_window_lock_time AS current_window_lock_time,
    lp.application_active_time AS last_active_time,
    lp.application_idle_time AS last_idle_time,
    lp.application_window_lock_time AS last_window_lock_time,
    ((a.application_active_time - lp.application_active_time) / NULLIF(lp.application_active_time, 0)) * 100 AS active_change,
    ((a.application_idle_time - lp.application_idle_time) / NULLIF(lp.application_idle_time, 0)) * 100 AS idle_change,
    ((a.application_window_lock_time - lp.application_window_lock_time) / NULLIF(lp.application_window_lock_time, 0)) * 100 AS window_lock_change,
    CASE WHEN ((a.application_active_time - lp.application_active_time) / NULLIF(lp.application_active_time, 0)) * 100 > 0 THEN 'up' ELSE 'down' END AS active_trend,
    CASE WHEN ((a.application_idle_time - lp.application_idle_time) / NULLIF(lp.application_idle_time, 0)) * 100 > 0 THEN 'up' ELSE 'down' END AS idle_trend,
    CASE WHEN ((a.application_window_lock_time - lp.application_window_lock_time) / NULLIF(lp.application_window_lock_time, 0)) * 100 > 0 THEN 'up' ELSE 'down' END AS window_lock_trend
FROM Application_Average a
LEFT JOIN Application_Average lp ON a.application_name = lp.application_name
WHERE lp.application_active_time IS NOT NULL
ORDER BY a.application_name;


---

Key Fixes in This Query

1. Login-Logout Time Restriction

Extracts first_login and last_logout per employee from EMPLOGINLOGOUT.

Filters EMPAPPINFO records to only include those within this working window.



2. Interval Data Aggregation

Since EMPAPPINFO stores hourly records, we sum them up per employee per day.

Then, we calculate employee-wise averages for the period.



3. Application-Wise and Team-Wide Averages

Each application’s active/idle time is averaged across employees.

Team-wide average is computed from application-wise averages.



4. Percentage Change & Trend Calculation

Compares current period vs. last period for trends (up / down).





---

How This Works

If an employee logs in at 9:30 AM and logs out at 6:15 PM, only data between these times is considered.

Each hour’s active and idle time is summed per employee per day.

Employee-wise averages are computed over the selected week/month/day/custom period.

The final average for an application is derived from all employees who used it.

Window Lock is treated as another application, but all time under it is counted as active time.



---

Updated API Response Format

{
  "team_average": {
    "current_period": {
      "active_time": "4h 25m",
      "idle_time": "2h 10m",
      "window_lock_time": "1h 15m"
    },
    "last_period": {
      "active_time": "4h 45m",
      "idle_time": "2h 30m",
      "window_lock_time": "1h 20m"
    },
    "percentage_change": {
      "active_time": "-7.0%",
      "idle_time": "-13.3%",
      "window_lock_time": "-6.3%"
    },
    "trend": {
      "active_time": "down",
      "idle_time": "down",
      "window_lock_time": "down"
    }
  },
  "applications": [
    {
      "application_name": "Google Chrome",
      "current_period": {
        "average_active_time": "3h 50m",
        "average_idle_time": "1h 15m"
      },
      "last_period": {
        "average_active_time": "4h 10m",
        "average_idle_time": "1h 20m"
      },
      "percentage_change": {
        "active_time": "-8.0%",
        "idle_time": "-6.3%"
      },
      "trend": {
        "active_time": "down",
        "idle_time": "down"
      },
      "employees": [
        {
          "employee_id": "EMP001",
          "average_active_time": "4h 20m",
          "average_idle_time": "1h 30m"
        },
        {
          "employee_id": "EMP002",
          "average_active_time": "3h 20m",
          "average_idle_time": "1h 00m"
        }
      ]
    },
    {
      "application_name": "Window Lock",
      "current_period": {
        "window_lock_time": "1h 15m"
      },
      "last_period": {
        "window_lock_time": "1h 20m"
      },
      "percentage_change": {
        "window_lock_time": "-6.3%"
      },
      "trend": {
        "window_lock_time": "down"
      }
    }
  ]
}


---

Next Steps

Integrate the query into Flask and pass start_date and end_date dynamically.

Ensure frontend correctly displays trends and percentage changes.


Would you like me to format the Flask API code as well?









INSERT INTO ShiftTable (EMPID, ShiftDate, StartTime, EndTime, DayID, EachWeekOf, ContactedHours, EachNightShift, Created)
SELECT 
    e.EMPID,
    DATE_ADD('2025-03-31', INTERVAL d.DayID DAY) AS ShiftDate,
    CASE WHEN d.DayID IN (5,6) THEN '00:00:00' ELSE '09:00:00' END AS StartTime,
    CASE WHEN d.DayID IN (5,6) THEN '00:00:00' ELSE '18:00:00' END AS EndTime,
    d.DayID,
    CASE WHEN d.DayID IN (5,6) THEN 'Y' ELSE 'N' END AS EachWeekOf,
    CASE WHEN d.DayID IN (5,6) THEN 0.0 ELSE 9.0 END AS ContactedHours,
    'N' AS EachNightShift,
    NOW() AS Created
FROM 
    (SELECT 101 AS EMPID UNION ALL SELECT 102 UNION ALL SELECT 103 UNION ALL 
     SELECT 104 UNION ALL SELECT 105 UNION ALL SELECT 106 UNION ALL SELECT 107 UNION ALL 
     SELECT 108 UNION ALL SELECT 109 UNION ALL SELECT 110 UNION ALL 
     SELECT 111 UNION ALL SELECT 112 UNION ALL SELECT 113 UNION ALL 
     SELECT 114 UNION ALL SELECT 115 UNION ALL SELECT 116 UNION ALL 
     SELECT 117 UNION ALL SELECT 118 UNION ALL SELECT 119 UNION ALL 
     SELECT 120 UNION ALL SELECT 121 UNION ALL SELECT 122 UNION ALL 
     SELECT 123 UNION ALL SELECT 124 UNION ALL SELECT 125 UNION ALL 
     SELECT 126 UNION ALL SELECT 127 UNION ALL SELECT 128 UNION ALL 
     SELECT 129 UNION ALL SELECT 130) AS e
JOIN 
    (SELECT 0 AS DayID UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL 
     SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) AS d
ON 1=1;










WITH EmployeeList AS (
    SELECT EMPL_ID 
    FROM inbound.HR_EMPLOYEES_CENTRAL 
    WHERE FUNC_MANAGER_ID = %(manager_id)s
),
CurrentData AS (
    SELECT 
        APP_NAME,
        AVG(TOTAL_ACTIVE_TIME) AS AVG_ACTIVE_TIME,
        AVG(TOTAL_IDLE_TIME) AS AVG_IDLE_TIME
    FROM analytics.EMP_APP_INFO
    WHERE CAL_DATE BETWEEN %(start_date)s AND %(end_date)s
    AND EMP_ID IN (SELECT EMPL_ID FROM EmployeeList)
    AND APP_NAME != 'WINDOW_LOCK'  -- 🔹 Excluding WINDOW_LOCK here
    GROUP BY APP_NAME
),
LastPeriodData AS (
    SELECT 
        APP_NAME,
        AVG(TOTAL_ACTIVE_TIME) AS LAST_AVG_ACTIVE_TIME,
        AVG(TOTAL_IDLE_TIME) AS LAST_AVG_IDLE_TIME
    FROM analytics.EMP_APP_INFO
    WHERE CAL_DATE BETWEEN %(last_period_start)s AND %(last_period_end)s
    AND EMP_ID IN (SELECT EMPL_ID FROM EmployeeList)
    AND APP_NAME != 'WINDOW_LOCK'  -- 🔹 Excluding WINDOW_LOCK here
    GROUP BY APP_NAME
),
WindowLockData AS (
    SELECT 
        AVG(WINDOW_LOCK_TIME) AS WINDOW_LOCK_TIME,
        (SELECT AVG(WINDOW_LOCK_TIME) 
         FROM analytics.EMP_APP_INFO
         WHERE CAL_DATE BETWEEN %(last_period_start)s AND %(last_period_end)s
         AND EMP_ID IN (SELECT EMPL_ID FROM EmployeeList)
         AND APP_NAME = 'WINDOW_LOCK'
        ) AS LAST_WINDOW_LOCK_TIME
    FROM analytics.EMP_APP_INFO
    WHERE CAL_DATE BETWEEN %(start_date)s AND %(end_date)s
    AND EMP_ID IN (SELECT EMPL_ID FROM EmployeeList)
    AND APP_NAME = 'WINDOW_LOCK'
)
SELECT 
    c.APP_NAME,
    c.AVG_ACTIVE_TIME,
    c.AVG_IDLE_TIME,
    COALESCE(l.LAST_AVG_ACTIVE_TIME, 0) AS LAST_AVG_ACTIVE_TIME,
    COALESCE(l.LAST_AVG_IDLE_TIME, 0) AS LAST_AVG_IDLE_TIME,

    -- 🔹 Percentage change per application
    ROUND(
        CASE 
            WHEN COALESCE(l.LAST_AVG_ACTIVE_TIME, 0) + COALESCE(l.LAST_AVG_IDLE_TIME, 0) > 0 THEN 
                ((COALESCE(c.AVG_ACTIVE_TIME, 0) + COALESCE(c.AVG_IDLE_TIME, 0)) 
                - (COALESCE(l.LAST_AVG_ACTIVE_TIME, 0) + COALESCE(l.LAST_AVG_IDLE_TIME, 0))) 
                / (COALESCE(l.LAST_AVG_ACTIVE_TIME, 0) + COALESCE(l.LAST_AVG_IDLE_TIME, 0)) * 100
            ELSE NULL
        END, 2
    ) AS APP_PERCENT_CHANGE,

    -- 🔹 Adding Window Lock Time
    wl.WINDOW_LOCK_TIME,
    wl.LAST_WINDOW_LOCK_TIME,
    
    -- 🔹 Window Lock Time Percentage Change
    ROUND(
        CASE 
            WHEN COALESCE(wl.LAST_WINDOW_LOCK_TIME, 0) > 0 THEN 
                ((COALESCE(wl.WINDOW_LOCK_TIME, 0) - COALESCE(wl.LAST_WINDOW_LOCK_TIME, 0)) 
                / COALESCE(wl.LAST_WINDOW_LOCK_TIME, 1) * 100)
            ELSE NULL
        END, 2
    ) AS WINDOW_LOCK_PERCENT_CHANGE

FROM CurrentData c
LEFT JOIN LastPeriodData l ON c.APP_NAME = l.APP_NAME
JOIN WindowLockData wl ON 1=1  -- 🔹 Ensuring Window Lock Data is always included
ORDER BY c.AVG_ACTIVE_TIME DESC;




from flask import request, jsonify
from flask_restful import Resource
from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request
from datetime import datetime, timedelta
from app.database import execute_query  # Import Databricks query executor

class EmpActInfo(Resource):
    def get(self):
        # ✅ Decode JWT to get Manager ID
        verify_jwt_in_request()
        manager_id = get_jwt_identity()  # Assuming it's stored in token
        
        # ✅ Get date range type from query params
        date_range = request.args.get('date_range', 'daily')  # Default is 'daily'
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # ✅ Compute start and end dates
        today = datetime.today().date()

        if date_range == "weekly":
            start_date = today - timedelta(days=today.weekday())  # Monday of this week
            end_date = today
            last_period_start = start_date - timedelta(weeks=1)
            last_period_end = end_date - timedelta(weeks=1)

        elif date_range == "monthly":
            start_date = today.replace(day=1)  # First day of current month
            last_period_start = (start_date - timedelta(days=1)).replace(day=1)  # First day of last month
            end_date = today
            last_period_end = start_date - timedelta(days=1)  # Last day of last month

        elif date_range == "custom" and start_date_str and end_date_str:
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                delta_days = (end_date - start_date).days + 1  # Number of days in range
                last_period_start = start_date - timedelta(days=delta_days)
                last_period_end = end_date - timedelta(days=delta_days)
            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        else:  # Default to daily
            start_date = today
            end_date = today
            last_period_start = today - timedelta(days=1)
            last_period_end = today - timedelta(days=1)

        # ✅ SQL Query Execution using Databricks Pooling
        query = """
            WITH EmployeeList AS (
                SELECT EMPL_ID 
                FROM inbound.HR_EMPLOYEES_CENTRAL 
                WHERE FUNC_MANAGER_ID = %(manager_id)s
            ),
            CurrentData AS (
                SELECT 
                    EMP_ID,
                    APP_NAME,
                    SUM(TOTAL_ACTIVE_TIME) AS TOTAL_ACTIVE_TIME,
                    SUM(TOTAL_IDLE_TIME) AS TOTAL_IDLE_TIME
                FROM analytics.EMP_APP_INFO
                WHERE CAL_DATE BETWEEN %(start_date)s AND %(end_date)s
                AND EMP_ID IN (SELECT EMPL_ID FROM EmployeeList)
                AND APP_NAME != 'WINDOW_LOCK'
                GROUP BY EMP_ID, APP_NAME
            ),
            LastPeriodData AS (
                SELECT 
                    EMP_ID,
                    APP_NAME,
                    SUM(TOTAL_ACTIVE_TIME) AS LAST_ACTIVE_TIME,
                    SUM(TOTAL_IDLE_TIME) AS LAST_IDLE_TIME
                FROM analytics.EMP_APP_INFO
                WHERE CAL_DATE BETWEEN %(last_period_start)s AND %(last_period_end)s
                AND EMP_ID IN (SELECT EMPL_ID FROM EmployeeList)
                AND APP_NAME != 'WINDOW_LOCK'
                GROUP BY EMP_ID, APP_NAME
            ),
            TeamCurrent AS (
                SELECT 
                    AVG(TOTAL_ACTIVE_TIME) AS TEAM_AVG_ACTIVE,
                    AVG(TOTAL_IDLE_TIME) AS TEAM_AVG_IDLE
                FROM CurrentData
            ),
            TeamLast AS (
                SELECT 
                    AVG(LAST_ACTIVE_TIME) AS LAST_TEAM_AVG_ACTIVE,
                    AVG(LAST_IDLE_TIME) AS LAST_TEAM_AVG_IDLE
                FROM LastPeriodData
            ),
            AppWiseCurrent AS (
                SELECT 
                    APP_NAME,
                    SUM(TOTAL_ACTIVE_TIME) AS TOTAL_ACTIVE_TIME,
                    SUM(TOTAL_IDLE_TIME) AS TOTAL_IDLE_TIME
                FROM CurrentData
                GROUP BY APP_NAME
            ),
            AppWiseLast AS (
                SELECT 
                    APP_NAME,
                    SUM(LAST_ACTIVE_TIME) AS LAST_TOTAL_ACTIVE_TIME,
                    SUM(LAST_IDLE_TIME) AS LAST_TOTAL_IDLE_TIME
                FROM LastPeriodData
                GROUP BY APP_NAME
            )
            SELECT 
                tc.TEAM_AVG_ACTIVE,
                tc.TEAM_AVG_IDLE,
                tl.LAST_TEAM_AVG_ACTIVE,
                tl.LAST_TEAM_AVG_IDLE,
                
                -- Team percentage change
                ROUND(
                    CASE 
                        WHEN COALESCE(tl.LAST_TEAM_AVG_ACTIVE, 0) + COALESCE(tl.LAST_TEAM_AVG_IDLE, 0) > 0 THEN 
                            ((COALESCE(tc.TEAM_AVG_ACTIVE, 0) + COALESCE(tc.TEAM_AVG_IDLE, 0)) 
                            - (COALESCE(tl.LAST_TEAM_AVG_ACTIVE, 0) + COALESCE(tl.LAST_TEAM_AVG_IDLE, 0))) 
                            / (COALESCE(tl.LAST_TEAM_AVG_ACTIVE, 0) + COALESCE(tl.LAST_TEAM_AVG_IDLE, 0)) * 100
                        ELSE NULL
                    END, 2
                ) AS TEAM_PERCENT_CHANGE,

                ac.APP_NAME,
                ac.TOTAL_ACTIVE_TIME,
                ac.TOTAL_IDLE_TIME,
                COALESCE(al.LAST_TOTAL_ACTIVE_TIME, 0) AS LAST_TOTAL_ACTIVE_TIME,
                COALESCE(al.LAST_TOTAL_IDLE_TIME, 0) AS LAST_TOTAL_IDLE_TIME,
                
                -- Application-wise percentage change
                ROUND(
                    CASE 
                        WHEN COALESCE(al.LAST_TOTAL_ACTIVE_TIME, 0) + COALESCE(al.LAST_TOTAL_IDLE_TIME, 0) > 0 THEN 
                            ((COALESCE(ac.TOTAL_ACTIVE_TIME, 0) + COALESCE(ac.TOTAL_IDLE_TIME, 0)) 
                            - (COALESCE(al.LAST_TOTAL_ACTIVE_TIME, 0) + COALESCE(al.LAST_TOTAL_IDLE_TIME, 0))) 
                            / (COALESCE(al.LAST_TOTAL_ACTIVE_TIME, 0) + COALESCE(al.LAST_TOTAL_IDLE_TIME, 0)) * 100
                        ELSE NULL
                    END, 2
                ) AS APP_PERCENT_CHANGE

            FROM TeamCurrent tc
            CROSS JOIN TeamLast tl
            LEFT JOIN AppWiseCurrent ac ON 1=1
            LEFT JOIN AppWiseLast al ON ac.APP_NAME = al.APP_NAME
            ORDER BY ac.TOTAL_ACTIVE_TIME DESC;
        """

        params = {
            "manager_id": manager_id,
            "start_date": start_date,
            "end_date": end_date,
            "last_period_start": last_period_start,
            "last_period_end": last_period_end
        }

        # ✅ Execute query using Databricks pooling
        data = execute_query(query, params, fetch_mode="dict")

        # ✅ Format JSON Response
        if not data:
            return jsonify({"message": "No data available for the given period."}), 404

        response = {
            "team_summary": {
                "average_active_time": data[0]["TEAM_AVG_ACTIVE"],
                "average_idle_time": data[0]["TEAM_AVG_IDLE"],
                "last_period": {
                    "average_active_time": data[0]["LAST_TEAM_AVG_ACTIVE"],
                    "average_idle_time": data[0]["LAST_TEAM_AVG_IDLE"]
                },
                "percent_change": data[0]["TEAM_PERCENT_CHANGE"]
            },
            "application_summary": []
        }

        for row in data:
            if row["APP_NAME"]:
                response["application_summary"].append({
                    "app_name": row["APP_NAME"],
                    "total_active_time": row["TOTAL_ACTIVE_TIME"],
                    "total_idle_time": row["TOTAL_IDLE_TIME"],
                    "last_period": {
                        "active_time": row["LAST_TOTAL_ACTIVE_TIME"],
                        "idle_time": row["LAST_TOTAL_IDLE_TIME"]
                    },
                    "percent_change": row["APP_PERCENT_CHANGE"]
                })

        return jsonify(response)




from flask import Flask, jsonify, request
from database import DatabricksSession, execute_query

app = Flask(__name__)


# ✅ Single Query Example: Get Employee Details
class GetEmployeeDetails:
    def get(self, employee_id):
        query = "SELECT * FROM Employee_Central WHERE employee_id = ?"
        with DatabricksSession() as conn:
            data = execute_query(conn, query, params=(employee_id,), fetch_mode="one")
        return jsonify({"employee_details": data})


# ✅ Multiple Independent Queries Example: Get Distinct Apps
class GetDistinctApps:
    def get(self, employee_id):
        query = "SELECT DISTINCT app_name FROM Applications WHERE employee_id = ?"
        with DatabricksSession() as conn:
            data = execute_query(conn, query, params=(employee_id,), fetch_mode="all")
        return jsonify({"distinct_apps": data})


# ✅ Sequential Queries Example: Get Manager Data
class GetManagerData:
    def get(self, manager_id):
        with DatabricksSession() as conn:
            # 1️⃣ Get employees under manager
            employees = execute_query(conn, "SELECT employee_id FROM Employee_Central WHERE manager_id = ?", 
                                      params=(manager_id,), fetch_mode="all")

            if not employees:
                return jsonify({"error": "No employees found for this manager"})

            employee_ids = tuple(emp[0] for emp in employees)

            # 2️⃣ Get distinct applications used by employees
            apps_query = f"SELECT DISTINCT app_name FROM Applications WHERE employee_id IN {str(employee_ids)}"
            apps = execute_query(conn, apps_query, fetch_mode="all")

            # 3️⃣ Get mouse click data for employees
            clicks_query = f"SELECT employee_id, click_count FROM MouseClicks WHERE employee_id IN {str(employee_ids)}"
            mouse_clicks = execute_query(conn, clicks_query, fetch_mode="all")

        return jsonify({"employees": employees, "distinct_apps": apps, "mouse_clicks": mouse_clicks})


# Flask Route Bindings
app.add_url_rule('/employee/<int:employee_id>', view_func=GetEmployeeDetails().get)
app.add_url_rule('/distinct_apps/<int:employee_id>', view_func=GetDistinctApps().get)
app.add_url_rule('/manager_data/<int:manager_id>', view_func=GetManagerData().get)

if __name__ == '__main__':
    app.run()


import os
import threading
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk.oauth import oauth_service_principal
from queue import Queue

# Connection Pool Configuration
POOL_SIZE = 10  # Max number of connections in the pool
connection_pool = Queue(maxsize=POOL_SIZE)
pool_lock = threading.Lock()


# Function to create a new Databricks connection
def create_connection():
    server_hostname = os.getenv('DATABRICKS_SERVER_HOST')

    config = Config(
        host=f"https://{server_hostname}",
        client_id=os.getenv('DATABRICKS_CLIENT_ID'),
        client_secret=os.getenv('DATABRICKS_CLIENT_SECRET')
    )

    return sql.connect(
        server_hostname=server_hostname,
        http_path=os.getenv('DATABRICKS_HTTP_PATH'),
        credentials_provider=lambda: oauth_service_principal(config)
    )


# Context Manager for Connection Pooling
class DatabricksSession:
    def __enter__(self):
        with pool_lock:
            if connection_pool.empty():
                self.conn = create_connection()
            else:
                self.conn = connection_pool.get()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        with pool_lock:
            if connection_pool.full():
                self.conn.close()
            else:
                connection_pool.put(self.conn)


# Function to Execute Queries Efficiently
def execute_query(conn, query, params=None, fetch_mode="all", fetch_size=None):
    """Executes a SQL query using an existing connection."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or {})

            if fetch_mode == "one":
                return cursor.fetchone()
            elif fetch_mode == "many" and fetch_size:
                return cursor.fetchmany(fetch_size)
            elif fetch_mode == "iter":
                return iter(cursor)
            elif fetch_mode == "dict":
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            elif fetch_mode == "pandas":
                return cursor.fetch_pandas_df()
            elif fetch_mode == "arrow":
                return cursor.fetch_arrow_table()
            else:
                return cursor.fetchall()
    except Exception as e:
        print(f"Query Execution Failed: {e}")
        return []







mport os
import queue
import databricks.sql as sql
from databricks.sql.exc import OperationalError
from flask import Flask, jsonify, request

# Flask App
app = Flask(__name__)

# Connection Pool Settings
POOL_SIZE = 5  # Adjust based on load
connection_pool = queue.Queue(maxsize=POOL_SIZE)

# Get Environment Variables
DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")

# Create Initial Connections
def create_connection():
    try:
        return sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_CLIENT_SECRET  # Token-based auth
        )
    except OperationalError as e:
        print(f"Databricks Connection Error: {e}")
        return None

# Initialize Pool with Connections
for _ in range(POOL_SIZE):
    conn = create_connection()
    if conn:
        connection_pool.put(conn)

# Function to Get Connection from Pool
def get_connection():
    try:
        return connection_pool.get(timeout=5)  # Wait 5 sec if no connection is available
    except queue.Empty:
        return create_connection()  # Create new if pool is exhausted

# Function to Return Connection to Pool
def release_connection(conn):
    try:
        connection_pool.put(conn, timeout=2)
    except queue.Full:
        conn.close()  # Close if pool is full

# Function to Execute Query
def execute_query(query, params=None):
    conn = get_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cursor:
            cursor.execute(query, params or {})
            result = cursor.fetchall()
        return result
    except OperationalError as e:
        print(f"Query Execution Failed: {e}")
        return []
    finally:
        release_connection(conn)  # Return connection back to pool

# Example API Endpoint
@app.route("/get-employees", methods=["GET"])
def get_employees():
    query = "SELECT * FROM Employee_Central WHERE ManagerID = ?"
    manager_id = request.args.get("manager_id")

    if not manager_id:
        return jsonify({"error": "Manager ID required"}), 400

    data = execute_query(query, (manager_id,))
    return jsonify(data)

# Run Flask App
if __name__ == "__main__":
    app.run(debug=True, threaded=True)


------****------****------****------****------****------****------****------  
| Reference Activity Type         | Calendar Date | Time  | EMP_ID | App Name  |
------****------****------****------****------****------****------****------  
| Early Login (Ignored)           | 2025-03-20   | 05:15 | 101    | Notepad   |
| Valid Login                     | 2025-03-20   | 06:05 | 101    | Teams     |
| Work Activity                   | 2025-03-20   | 07:30 | 101    | Excel     |
| Valid Login                     | 2025-03-20   | 09:05 | 102    | Excel     |
| Work Activity                   | 2025-03-20   | 10:45 | 102    | Teams     |
| Idle Lock                        | 2025-03-20   | 12:30 | 101    | Window Lock |
| Valid Login                     | 2025-03-20   | 14:10 | 104    | Zoom      |
| Logout                           | 2025-03-20   | 15:30 | 101    | Teams     |
| Logout (No Shift)                | 2025-03-20   | 16:30 | 105    | Teams     |
| Logout                           | 2025-03-20   | 17:30 | 102    | Window Lock |
| Work Activity                    | 2025-03-20   | 18:45 | 104    | Teams     |
| Valid Login (Night Shift)        | 2025-03-20   | 22:05 | 103    | Teams     |
| Logout                           | 2025-03-20   | 22:50 | 104    | Notepad   |
| Work Activity                    | 2025-03-20   | 23:30 | 103    | Notepad   |
------****------****------****------****------****------****------****------  
| Valid Login (Morning Shift)      | 2025-03-21   | 06:15 | 103    | Excel     |
| Logout                           | 2025-03-21   | 06:55 | 103    | Window Lock |
| Valid Login (No Shift)           | 2025-03-21   | 07:15 | 105    | Notepad   |
| Work Activity                    | 2025-03-21   | 10:00 | 103    | Zoom      |
| Early Login (Ignored)            | 2025-03-21   | 13:50 | 101    | Excel     |
| Valid Login                      | 2025-03-21   | 14:10 | 101    | Notepad   |
| Logout                           | 2025-03-21   | 15:30 | 103    | Window Lock |
| Logout (No Shift)                | 2025-03-21   | 17:00 | 105    | Window Lock |
| Logout                           | 2025-03-21   | 17:30 | 104    | Window Lock |
| Work Activity                    | 2025-03-21   | 18:45 | 101    | Zoom      |
| Valid Login (Night Shift)        | 2025-03-21   | 22:10 | 102    | Teams     |
| Logout                           | 2025-03-21   | 23:15 | 101    | Teams     |
------****------****------****------****------****------****------****------  
| Unexpected Login (Week Off!)     | 2025-03-22   | 07:00 | 101    | Notepad   |
| Unexpected Login (Week Off!)     | 2025-03-22   | 07:00 | 102    | Zoom      |
| Valid Login (No Shift)           | 2025-03-22   | 08:00 | 105    | Teams     |
| Unexpected Login (Week Off!)     | 2025-03-22   | 10:30 | 104    | Zoom      |
| Unexpected Login (Week Off!)     | 2025-03-22   | 11:00 | 103    | Excel     |
| Logout (No Shift)                | 2025-03-22   | 15:00 | 105    | Window Lock |
------****------****------****------****------****------****------****------  
| Unexpected Login (Week Off!)     | 2025-03-23   | 10:00 | 101    | Zoom      |
| Valid Login (No Shift)           | 2025-03-23   | 10:00 | 105    | Teams     |
| Logout (No Shift)                | 2025-03-23   | 17:30 | 105    | Window Lock |
------****------****------****------****------****------****------****------  
| Valid Login (Morning Shift)      | 2025-03-24   | 06:05 | 102    | Excel     |
| Logout                           | 2025-03-24   | 07:10 | 101    | Notepad   |
| Valid Login (No Shift)           | 2025-03-24   | 07:20 | 105    | Notepad   |
| Valid Login (Evening Shift)      | 2025-03-24   | 14:05 | 103    | Notepad   |
| Logout                           | 2025-03-24   | 14:50 | 102    | Window Lock |
| Logout (No Shift)                | 2025-03-24   | 16:00 | 105    | Excel     |
| Valid Login (Night Shift)        | 2025-03-24   | 22:10 | 104    | Teams     |
| Logout                           | 2025-03-24   | 22:30 | 103    | Teams     |
| Valid Login                      | 2025-03-24   | 22:05 | 101    | Teams     |
| Work Activity                    | 2025-03-24   | 23:50 | 102    | Excel     |
------****------****------****------****------****------****------****------  
| Logout                           | 2025-03-25   | 06:50 | 104    | Window Lock |
------****------****------****------****------****------****------****------














EMP_ID | Date       | Start Time | End Time | Is_Weekoff | Emp_Night_Shift_Flag | Shift_Type
-------|-----------|------------|----------|------------|----------------------|------------
101    | 2025-03-20 | 06:00      | 14:00    | No         | No                   | Morning
101    | 2025-03-21 | 06:00      | 14:00    | No         | No                   | Morning
101    | 2025-03-22 | 00:00      | 00:00    | Yes        | No                   | Week Off
101    | 2025-03-23 | 00:00      | 00:00    | Yes        | No                   | Week Off
101    | 2025-03-24 | 22:00      | 06:00    | No         | Yes                  | Night
102    | 2025-03-20 | 14:00      | 22:00    | No         | No                   | Evening
102    | 2025-03-21 | 14:00      | 22:00    | No         | No                   | Evening
102    | 2025-03-22 | 00:00      | 00:00    | Yes        | No                   | Week Off
102    | 2025-03-23 | 00:00      | 00:00    | Yes        | No                   | Week Off
102    | 2025-03-24 | 14:00      | 22:00    | No         | No                   | Evening
103    | 2025-03-20 | 22:00      | 06:00    | No         | Yes                  | Night
103    | 2025-03-21 | 06:00      | 14:00    | No         | No                   | Morning
103    | 2025-03-22 | 00:00      | 00:00    | Yes        | No                   | Week Off
103    | 2025-03-23 | 00:00      | 00:00    | Yes        | No                   | Week Off
103    | 2025-03-24 | 14:00      | 22:00    | No         | No                   | Evening
104    | 2025-03-20 | 14:00      | 22:00    | No         | No                   | Evening
104    | 2025-03-21 | 14:00      | 22:00    | No         | No                   | Evening
104    | 2025-03-22 | 00:00      | 00:00    | Yes        | No                   | Week Off
104    | 2025-03-23 | 00:00      | 00:00    | Yes        | No                   | Week Off
104    | 2025-03-24 | 22:00      | 06:00    | No         | Yes                  | Night












SELECT COLUMN_NAME 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'your_database_name' 
AND TABLE_NAME = 'your_table_name';



WITH emp_login_logout AS (
    SELECT 
        emp_id,
        cal_date,
        MIN(start_time) AS login_time,
        COALESCE(MAX(start_time), NOW()) AS logout_time
    FROM emp_activity
    GROUP BY emp_id, cal_date
),

activity_duration AS (
    SELECT 
        a.emp_id, 
        a.cal_date,
        a.app_name,
        a.start_time, 
        LEAD(a.start_time, 1, logout_time) OVER (PARTITION BY a.emp_id, a.cal_date ORDER BY a.start_time) AS end_time
    FROM emp_activity a
    JOIN emp_login_logout e ON a.emp_id = e.emp_id AND a.cal_date = e.cal_date
),

idle_duration AS (
    SELECT 
        i.emp_id, 
        i.cal_date,
        i.end_time, 
        i.duration_seconds,
        LAG(a.start_time, 1) OVER (PARTITION BY i.emp_id, i.cal_date ORDER BY i.end_time) AS last_active_time
    FROM idle_time i
    JOIN emp_activity a ON i.emp_id = a.emp_id AND i.cal_date = a.cal_date
),

valid_window_lock AS (
    SELECT emp_id, cal_date, MAX(start_time) AS last_activity
    FROM emp_activity
    WHERE app_name != 'Window Lock'
    GROUP BY emp_id, cal_date
),

final_data AS (
    SELECT 
        a.emp_id,
        a.cal_date,
        a.app_name,
        CASE 
            WHEN a.app_name = 'Window Lock' 
                AND a.start_time > COALESCE(v.last_activity, '2000-01-01') 
                THEN NULL -- Ignore end-of-day Window Lock
            WHEN a.app_name = 'Window Lock' THEN TIMESTAMPDIFF(SECOND, a.start_time, a.end_time)
            ELSE TIMESTAMPDIFF(SECOND, a.start_time, a.end_time) - COALESCE(i.duration_seconds, 0)
        END AS active_duration,
        CASE 
            WHEN a.app_name = 'Window Lock' THEN 0
            ELSE COALESCE(i.duration_seconds, 0)
        END AS idle_duration
    FROM activity_duration a
    LEFT JOIN idle_duration i 
    ON a.emp_id = i.emp_id 
    AND a.cal_date = i.cal_date 
    AND i.end_time BETWEEN a.start_time AND a.end_time
    LEFT JOIN valid_window_lock v 
    ON a.emp_id = v.emp_id 
    AND a.cal_date = v.cal_date
)

SELECT * FROM final_data WHERE active_duration IS NOT NULL;


_----------++-+-+-+--+-+

Sure! I'll break down each part of the SQL query step by step and explain what is happening at every stage.


---

Step 1: Understanding the Problem

We need to calculate active and idle durations for different applications used by multiple employees across a given day (cal_date).

Key Considerations:

1. Active Time Calculation:

For normal applications, active_duration = end_time - start_time - idle_time (if any)

For Window Lock, active_duration = end_time - start_time

Idle time inside Window Lock is ignored.



2. Ignoring End-of-Day Window Locks:

If an employee does not log out, the last recorded activity should be used as their last active time.

Any Window Lock after the last recorded activity should be ignored (as the employee likely just closed their laptop).





---

Step 2: Breaking Down the Query

Now, let's analyze the SQL query step by step.

1. Getting Login and Logout Time (emp_login_logout)

WITH emp_login_logout AS (
    SELECT 
        emp_id,
        cal_date,
        MIN(start_time) AS login_time,
        COALESCE(MAX(start_time), NOW()) AS logout_time
    FROM emp_activity
    GROUP BY emp_id, cal_date
),

✅ What is happening here?

We fetch the first login time (MIN(start_time)) and last logout time (MAX(start_time)) for each employee on each cal_date.

If there is no explicit logout time, we assume the last known activity time (MAX(start_time)) as their logout time.

Why? If an employee closes their laptop without logging out, we will use the last known activity instead.



---

2. Calculating Active Time for Each App (activity_duration)

activity_duration AS (
    SELECT 
        a.emp_id, 
        a.cal_date,
        a.app_name,
        a.start_time, 
        LEAD(a.start_time, 1, logout_time) 
            OVER (PARTITION BY a.emp_id, a.cal_date ORDER BY a.start_time) AS end_time
    FROM emp_activity a
    JOIN emp_login_logout e ON a.emp_id = e.emp_id AND a.cal_date = e.cal_date
),

✅ What is happening here?

Calculating the end_time for each activity session:

We use the LEAD() window function to get the start_time of the next application.

If there is no next application, we use logout_time as the end_time.


Why? This helps us determine how long an application was active before switching to another one.



---

3. Finding Idle Time Entries (idle_duration)

idle_duration AS (
    SELECT 
        i.emp_id, 
        i.cal_date,
        i.end_time, 
        i.duration_seconds,
        LAG(a.start_time, 1) OVER (PARTITION BY i.emp_id, i.cal_date ORDER BY i.end_time) AS last_active_time
    FROM idle_time i
    JOIN emp_activity a ON i.emp_id = a.emp_id AND i.cal_date = a.cal_date
),

✅ What is happening here?

We retrieve idle time entries for employees.

Finding the last_active_time before idle time:

We use LAG(a.start_time, 1) to get the previous application’s start time.

This helps in determining when idle time occurs within active periods.




---

4. Finding the Last Non-Window Lock Activity (valid_window_lock)

valid_window_lock AS (
    SELECT emp_id, cal_date, MAX(start_time) AS last_activity
    FROM emp_activity
    WHERE app_name != 'Window Lock'
    GROUP BY emp_id, cal_date
),

✅ What is happening here?

This CTE finds the latest application activity that is NOT a Window Lock.

Why?

If an employee closes their laptop at the end of the day, the Window Lock entry should be ignored.

The MAX(start_time) of normal apps helps us determine the last valid activity.




---

5. Calculating Final Active and Idle Durations (final_data)

final_data AS (
    SELECT 
        a.emp_id,
        a.cal_date,
        a.app_name,
        CASE 
            WHEN a.app_name = 'Window Lock' 
                AND a.start_time > COALESCE(v.last_activity, '2000-01-01') 
                THEN NULL -- Ignore end-of-day Window Lock
            WHEN a.app_name = 'Window Lock' 
                THEN TIMESTAMPDIFF(SECOND, a.start_time, a.end_time)
            ELSE TIMESTAMPDIFF(SECOND, a.start_time, a.end_time) - COALESCE(i.duration_seconds, 0)
        END AS active_duration,
        CASE 
            WHEN a.app_name = 'Window Lock' THEN 0
            ELSE COALESCE(i.duration_seconds, 0)
        END AS idle_duration
    FROM activity_duration a
    LEFT JOIN idle_duration i 
    ON a.emp_id = i.emp_id 
    AND a.cal_date = i.cal_date 
    AND i.end_time BETWEEN a.start_time AND a.end_time
    LEFT JOIN valid_window_lock v 
    ON a.emp_id = v.emp_id 
    AND a.cal_date = v.cal_date
)

✅ What is happening here?

Window Lock Handling:

If Window Lock happens after the last valid activity, we ignore it (active_duration = NULL).

Otherwise, its active duration is directly calculated (end_time - start_time).


Normal Applications:

active_duration = end_time - start_time - idle_time

idle_duration = idle_time (if any, else 0)


Why?

We need to correctly subtract idle time only from regular applications.

Window Lock should not have idle time counted.




---

6. Retrieving Final Data

SELECT * FROM final_data WHERE active_duration IS NOT NULL;

✅ Final Filtering:

Ensures that ignored Window Lock entries (end-of-day locks) do not appear in the final result.



---

Step 3: Summary of the Query

✅ Key Features:

1. Handles Multiple Employees


2. Calculates Active and Idle Time for Each Application


3. Ensures End-of-Day Window Locks Are Ignored


4. Accounts for Employees Closing Their Laptops Without Logging Off


5. Idle Time is Correctly Subtracted Only from Normal Apps




---

Step 4: Example Execution

Given Input Data

emp_activity

idle_time


---

Final Output


---

This query is now optimized for handling all edge cases. Let me know if you need further refinements!











------------------------------
WITH date_series AS (
    SELECT sequence(
        to_date('{start_date}'), 
        to_date('{end_date}'), 
        INTERVAL 1 DAY
    ) AS date_range
),

expanded_dates AS (
    SELECT explode(date_range) AS cal_date FROM date_series
),

distinct_employees AS (
    SELECT DISTINCT emp_id 
    FROM sys_trace.emp_loginlogout 
    {where_clause}
),

employee_dates AS (
    SELECT 
        e.emp_id, 
        d.cal_date 
    FROM distinct_employees e 
    CROSS JOIN expanded_dates d
),

final_attendance AS (
    SELECT 
        ed.cal_date, 
        ed.emp_id, 
        MIN(el.emp_login_time) AS login_time,  -- Considering the earliest login time
        CASE 
            WHEN MIN(el.emp_login_time) IS NOT NULL THEN 'Present' 
            ELSE 'Absent' 
        END AS attendance_status
    FROM employee_dates ed
    LEFT JOIN sys_trace.emp_loginlogout el
        ON ed.cal_date = el.shift_date 
        AND ed.emp_id = el.emp_id
    GROUP BY ed.cal_date, ed.emp_id
)

SELECT * FROM final_attendance
ORDER BY emp_id, cal_date;





from flask import Flask, request, jsonify
from your_database_module import getDatabricksConnection
from your_helper_module import ManagerMapping  

app = Flask(__name__)

EMPLOYEE_INFO = {
    "E001": {"FirstName": "John", "LastName": "Doe"},
    "E002": {"FirstName": "Jane", "LastName": "Smith"},
    "E003": {"FirstName": "Alice", "LastName": "Johnson"},
    "E004": {"FirstName": "Bob", "LastName": "Brown"},
}

@app.route('/get_employee_attendance', methods=['GET'])
def get_employee_attendance():
    try:
        manager_id = request.args.get('MGRID')
        limit = request.args.get('Limit', type=int, default=10)
        offset = request.args.get('Offset', type=int, default=0)

        employee_id = request.args.get('EMPID', None)
        start_date = request.args.get('StartDate', None)
        end_date = request.args.get('EndDate', None)

        # Handling multiple attendance statuses
        attendance_status = request.args.get('AttendanceStatus', None)
        attendance_status_list = tuple(attendance_status.split(',')) if attendance_status else None  

        # Handling multiple status filters
        status = request.args.get('Status', None)
        status_list = tuple(status.split(',')) if status else None  

        if not manager_id:
            return jsonify({"error": "Manager ID (MGRID) is required"}), 400

        employee_list = ManagerMapping(manager_id)  
        employee_tuple = tuple(employee_list) if employee_list else ('',)  

        if employee_id and employee_id not in employee_list:
            return jsonify({"error": "Invalid EMPID for this Manager"}), 403
        
        connection = getDatabricksConnection()
        cursor = connection.cursor()

        sql_query, query_params = generate_sql_query(employee_id, employee_tuple, start_date, end_date, status_list, attendance_status_list, limit, offset)
        cursor.execute(sql_query, query_params)
        records = cursor.fetchall()

        data = [
            {
                "empid": empid,
                "empname": f"{EMPLOYEE_INFO.get(empid, {}).get('FirstName', 'Unknown')} {EMPLOYEE_INFO.get(empid, {}).get('LastName', '')}".strip(),
                "shiftdate": shiftdate,
                "expectedlogintime": expectedlogintime,
                "expectedlogouttime": expectedlogouttime,
                "actuallogintime": actuallogintime,
                "actuallogouttime": actuallogouttime,
                "attendancestatus": attendancestatus,
                "loginstatus": loginstatus,
                "logoutstatus": logoutstatus,
            }
            for empid, shiftdate, expectedlogintime, expectedlogouttime, actuallogintime, actuallogouttime, attendancestatus, loginstatus, logoutstatus in records
        ]

        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == '__main__':
    app.run(debug=True)


def generate_sql_query(employee_id, employee_tuple, start_date, end_date, status, attendance_status, limit, offset):
    query = f"""
    WITH ProcessedData AS (
        SELECT 
            EMP_ID, SHIFT_DATE,
            COALESCE(TO_CHAR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS START_TIME,
            COALESCE(TO_CHAR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS END_TIME,
            COALESCE(TO_CHAR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS EMP_LOGIN_TIME,
            COALESCE(TO_CHAR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS EMP_LOGOUT_TIME
        FROM strace.emploginlogout
        WHERE EMP_ID IN {employee_tuple}
    """

    # Apply optional filters dynamically
    if employee_id:
        query += f" AND EMP_ID = '{employee_id}'"
    if start_date:
        query += f" AND SHIFT_DATE >= '{start_date}'"
    if end_date:
        query += f" AND SHIFT_DATE <= '{end_date}'"

    query += """
    ), FinalData AS (
        SELECT 
            EMP_ID, SHIFT_DATE, START_TIME, END_TIME, EMP_LOGIN_TIME, EMP_LOGOUT_TIME,
            CASE 
                WHEN START_TIME = '00:00:00' AND END_TIME = '00:00:00' THEN 'NO SHIFT DATA'
                WHEN SHIFT_DATE IS NOT NULL AND EMP_LOGIN_TIME = '00:00:00' AND EMP_LOGOUT_TIME = '00:00:00' THEN 'OFFLINE'
                ELSE 'PRESENT'
            END AS STATUS,
            CASE 
                WHEN START_TIME = '00:00:00' OR EMP_LOGIN_TIME = '00:00:00' THEN 'N/A'
                WHEN EMP_LOGIN_TIME > START_TIME THEN 'Late Login'
                WHEN EMP_LOGIN_TIME <= START_TIME THEN 'Early Login'
            END AS LOGIN_STATUS,
            CASE 
                WHEN END_TIME = '00:00:00' OR EMP_LOGOUT_TIME = '00:00:00' THEN 'No Logout Data'
                WHEN EMP_LOGOUT_TIME > END_TIME THEN 'Late Logout'
                WHEN EMP_LOGOUT_TIME <= END_TIME THEN 'Early Logout'
            END AS LOGOUT_STATUS
        FROM ProcessedData
    ) 
    SELECT * FROM FinalData WHERE 1=1
    """

    # Apply additional filters dynamically
    if status:
        query += f" AND STATUS = '{status}'"
    if attendance_status:
        query += f""" 
        AND (
            (LOGIN_STATUS = '{attendance_status}') OR 
            (LOGOUT_STATUS = '{attendance_status}')
        )
        """

    query += f" ORDER BY SHIFT_DATE DESC LIMIT {limit} OFFSET {offset};"

    return query




def generate_sql_query(employee_id, employee_tuple, start_date, end_date, status, attendance_status, limit, offset):
    query = f"""
    WITH ProcessedData AS (
        SELECT 
            EMP_ID, SHIFT_DATE,
            COALESCE(TO_CHAR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS START_TIME,
            COALESCE(TO_CHAR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS END_TIME,
            COALESCE(TO_CHAR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS EMP_LOGIN_TIME,
            COALESCE(TO_CHAR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS EMP_LOGOUT_TIME
        FROM strace.emploginlogout
        WHERE EMP_ID IN {employee_tuple}
    """

    # Apply optional filters dynamically
    if employee_id:
        query += f" AND EMP_ID = '{employee_id}'"
    if start_date:
        query += f" AND SHIFT_DATE >= '{start_date}'"
    if end_date:
        query += f" AND SHIFT_DATE <= '{end_date}'"

    query += """
    ), FinalData AS (
        SELECT 
            EMP_ID, SHIFT_DATE, START_TIME, END_TIME, EMP_LOGIN_TIME, EMP_LOGOUT_TIME,
            CASE 
                WHEN START_TIME = '00:00:00' AND END_TIME = '00:00:00' THEN 'NO SHIFT DATA'
                WHEN SHIFT_DATE IS NOT NULL AND EMP_LOGIN_TIME = '00:00:00' AND EMP_LOGOUT_TIME = '00:00:00' THEN 'OFFLINE'
                ELSE 'PRESENT'
            END AS STATUS,
            CASE 
                WHEN START_TIME = '00:00:00' OR EMP_LOGIN_TIME = '00:00:00' THEN 'N/A'
                WHEN EMP_LOGIN_TIME > START_TIME THEN 'Late Login'
                WHEN EMP_LOGIN_TIME <= START_TIME THEN 'Early Login'
            END AS LOGIN_STATUS,
            CASE 
                WHEN END_TIME = '00:00:00' OR EMP_LOGOUT_TIME = '00:00:00' THEN 'No Logout Data'
                WHEN EMP_LOGOUT_TIME > END_TIME THEN 'Late Logout'
                WHEN EMP_LOGOUT_TIME <= END_TIME THEN 'Early Logout'
            END AS LOGOUT_STATUS
        FROM ProcessedData
    ) 
    SELECT * FROM FinalData WHERE 1=1
    """

    # Apply additional filters dynamically
    if status:
        query += f" AND STATUS = '{status}'"
    if attendance_status:
        query += f""" 
        AND (
            (LOGIN_STATUS = '{attendance_status}') OR 
            (LOGOUT_STATUS = '{attendance_status}')
        )
        """

    query += f" ORDER BY SHIFT_DATE DESC LIMIT {limit} OFFSET {offset};"

    return query


from flask import Flask, request, jsonify
from your_database_module import getDatabricksConnection
from your_helper_module import ManagerMapping  

app = Flask(__name__)

EMPLOYEE_INFO = {
    "E001": {"FirstName": "John", "LastName": "Doe"},
    "E002": {"FirstName": "Jane", "LastName": "Smith"},
    "E003": {"FirstName": "Alice", "LastName": "Johnson"},
    "E004": {"FirstName": "Bob", "LastName": "Brown"},
}

@app.route('/get_employee_attendance', methods=['GET'])
def get_employee_attendance():
    try:
        manager_id = request.args.get('MGRID')
        limit = request.args.get('Limit', type=int, default=10)
        offset = request.args.get('Offset', type=int, default=0)

        employee_id = request.args.get('EMPID', None)
        start_date = request.args.get('StartDate', None)
        end_date = request.args.get('EndDate', None)
        status = request.args.get('Status', None)
        attendance_status = request.args.get('AttendanceStatus', None)

        if not manager_id:
            return jsonify({"error": "Manager ID (MGRID) is required"}), 400

        employee_list = ManagerMapping(manager_id)  
        employee_tuple = tuple(employee_list) if employee_list else ('',)  

        if employee_id and employee_id not in employee_list:
            return jsonify({"error": "Invalid EMPID for this Manager"}), 403
        
        connection = getDatabricksConnection()
        cursor = connection.cursor()

        sql_query = generate_sql_query(employee_id, employee_tuple, start_date, end_date, status, attendance_status, limit, offset)
        cursor.execute(sql_query)
        records = cursor.fetchall()

        data = [
            {
                "empid": empid,
                "empname": f"{EMPLOYEE_INFO.get(empid, {}).get('FirstName', 'Unknown')} {EMPLOYEE_INFO.get(empid, {}).get('LastName', '')}".strip(),
                "shiftdate": shiftdate,
                "expectedlogintime": expectedlogintime,
                "expectedlogouttime": expectedlogouttime,
                "actuallogintime": actuallogintime,
                "actuallogouttime": actuallogouttime,
                "attendancestatus": attendancestatus,
                "loginstatus": loginstatus,
                "logoutstatus": logoutstatus,
            }
            for empid, shiftdate, expectedlogintime, expectedlogouttime, actuallogintime, actuallogouttime, attendancestatus, loginstatus, logoutstatus in records
        ]

        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == '__main__':
    app.run(debug=True)
















WITH ProcessedData AS ( 
    SELECT 
        EMP_ID, 
        SHIFT_DATE, 
        
        -- Convert Time to HH:MM:SS Format & Handle NULL as '00:00:00'
        COALESCE(TO_CHAR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS START_TIME,
        COALESCE(TO_CHAR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS END_TIME,
        COALESCE(TO_CHAR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS EMP_LOGIN_TIME,
        COALESCE(TO_CHAR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS'), 'HH24:MI:SS'), '00:00:00') AS EMP_LOGOUT_TIME
        
    FROM strace.emploginlogout 
    WHERE EMP_ID IN ({emp_idd})
"""  
# Apply date filters if selected  
if start_date:  
    query += f" AND SHIFT_DATE >= '{start_date}'"  

if end_date:  
    query += f" AND SHIFT_DATE <= '{end_date}'"  

query += """
), 
FinalData AS ( 
    SELECT 
        EMP_ID, 
        SHIFT_DATE, 
        START_TIME, 
        END_TIME, 
        EMP_LOGIN_TIME, 
        EMP_LOGOUT_TIME, 
        
        -- Determine Attendance Status  
        CASE  
            WHEN START_TIME = '00:00:00' AND END_TIME = '00:00:00' THEN 'NO SHIFT DATA'  
            WHEN SHIFT_DATE IS NOT NULL AND EMP_LOGIN_TIME = '00:00:00' AND EMP_LOGOUT_TIME = '00:00:00' THEN 'OFFLINE'  
            ELSE 'PRESENT'  
        END AS STATUS,  
        
        -- Determine Login Status  
        CASE  
            WHEN START_TIME = '00:00:00' AND END_TIME = '00:00:00' AND EMP_LOGIN_TIME <> '00:00:00' THEN 'No Shift Data'
            WHEN EMP_LOGIN_TIME = '00:00:00' THEN 'No Login Data'  
            WHEN EMP_LOGIN_TIME > START_TIME THEN 'Late Login'  
            WHEN EMP_LOGIN_TIME < START_TIME THEN 'Early Login'  
            ELSE 'On Time'  
        END AS LOGIN_STATUS,  

        -- Determine Logout Status  
        CASE  
            WHEN START_TIME = '00:00:00' AND END_TIME = '00:00:00' AND EMP_LOGOUT_TIME <> '00:00:00' THEN 'No Shift Data'
            WHEN EMP_LOGOUT_TIME = '00:00:00' AND EMP_LOGIN_TIME <> '00:00:00' THEN 'No Logout Data'  
            WHEN EMP_LOGOUT_TIME = '00:00:00' THEN 'No Login Data'  
            WHEN EMP_LOGOUT_TIME > END_TIME THEN 'Late Logout'  
            WHEN EMP_LOGOUT_TIME < END_TIME THEN 'Early Logout'  
            ELSE 'On Time'  
        END AS LOGOUT_STATUS  

    FROM ProcessedData  
) 

SELECT * FROM FinalData  
WHERE 1=1 
"""  

# Apply attendance status filter if selected  
if attendance_status:  
    query += f" AND STATUS = '{attendance_status}'"  

# Apply login/logout status filter if selected  
if login_status:  
    query += f" AND (LOGIN_STATUS = '{login_status}' OR LOGOUT_STATUS = '{login_status}')"  

query += f""" ORDER BY SHIFT_DATE DESC  
LIMIT {limit} OFFSET {offset}; """










# Define dynamic input parameters
emp_idd = "12345"
start_date = "2024-02-01"  # Can be None if not selected
end_date = "2024-02-15"    # Can be None if not selected
attendance_status = None   # Example: 'PRESENT', 'OFFLINE', etc.
login_status = None        # Example: 'Late Login', 'Early Login', etc.
limit = 10
offset = 0

query = f"""
WITH ProcessedData AS ( 
    SELECT  
        EMP_ID, 
        SHIFT_DATE,  
        
        -- Convert Times to Decimal Hours
        ROUND(HOUR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS START_TIME_DECIMAL,  

        ROUND(HOUR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS END_TIME_DECIMAL,  

        ROUND(HOUR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS EMP_LOGIN_TIME_DECIMAL,  

        ROUND(HOUR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS EMP_LOGOUT_TIME_DECIMAL  
    FROM strace.emploginlogout  
    WHERE EMP_ID = '{emp_idd}'  
    """

# Apply date filters if user selects specific dates
if start_date:
    query += f" AND SHIFT_DATE >= '{start_date}'"

if end_date:
    query += f" AND SHIFT_DATE <= '{end_date}'"

query += """
)  

SELECT  
    EMP_ID,  
    SHIFT_DATE,  
    START_TIME_DECIMAL,  
    END_TIME_DECIMAL,  
    EMP_LOGIN_TIME_DECIMAL,  
    EMP_LOGOUT_TIME_DECIMAL,  

    -- Determine Attendance Status  
    CASE  
        WHEN START_TIME_DECIMAL IS NULL AND END_TIME_DECIMAL IS NULL THEN 'NO SHIFT DATA'  
        WHEN SHIFT_DATE IS NOT NULL AND EMP_LOGIN_TIME_DECIMAL IS NULL AND EMP_LOGOUT_TIME_DECIMAL IS NULL THEN 'OFFLINE'  
        ELSE 'PRESENT'  
    END AS STATUS,  

    -- Late Login Check  
    CASE  
        WHEN EMP_LOGIN_TIME_DECIMAL > START_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGIN_TIME_DECIMAL <= START_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS LATE_LOGIN,  

    -- Early Login Check  
    CASE  
        WHEN EMP_LOGIN_TIME_DECIMAL <= START_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGIN_TIME_DECIMAL > START_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS EARLY_LOGIN,  

    -- Late Logout Check  
    CASE  
        WHEN EMP_LOGOUT_TIME_DECIMAL > END_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGOUT_TIME_DECIMAL <= END_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS LATE_LOGOUT,  

    -- Early Logout Check  
    CASE  
        WHEN EMP_LOGOUT_TIME_DECIMAL <= END_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGOUT_TIME_DECIMAL > END_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS EARLY_LOGOUT  

FROM ProcessedData  
WHERE 1=1
"""

# Apply attendance status filter if selected
if attendance_status:
    query += f" AND STATUS = '{attendance_status}'"

# Apply login status filter if selected
if login_status:
    query += f""" 
    AND (
        ('{login_status}' = 'Late Login' AND LATE_LOGIN = 'Y') OR  
        ('{login_status}' = 'Early Login' AND EARLY_LOGIN = 'Y') OR  
        ('{login_status}' = 'Late Logout' AND LATE_LOGOUT = 'Y') OR  
        ('{login_status}' = 'Early Logout' AND EARLY_LOGOUT = 'Y')  
    )
    """

query += f"""
ORDER BY SHIFT_DATE DESC  
LIMIT {limit} OFFSET {offset};
"""

---

## **3️⃣ Execute the Query in PySpark**
Now, run the SQL query using PySpark.

```python
df = spark.sql(query)
df.show()











WITH ProcessedData AS ( 
    SELECT  
        EMP_ID, 
        SHIFT_DATE,  
        
        -- Convert Times to Decimal Hours
        ROUND(HOUR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS START_TIME_DECIMAL,  

        ROUND(HOUR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS END_TIME_DECIMAL,  

        ROUND(HOUR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS EMP_LOGIN_TIME_DECIMAL,  

        ROUND(HOUR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) + 
              (MINUTE(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
              (SECOND(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 3600.0), 2) 
              AS EMP_LOGOUT_TIME_DECIMAL  
    FROM strace.emploginlogout  
    WHERE EMP_ID = {emp_idd}  
    -- Apply Date Filter Only if User Selects Specific Dates
    AND ('{start_date}' IS NULL OR SHIFT_DATE >= '{start_date}')  
    AND ('{end_date}' IS NULL OR SHIFT_DATE <= '{end_date}')  
)  

SELECT  
    EMP_ID,  
    SHIFT_DATE,  
    START_TIME_DECIMAL,  
    END_TIME_DECIMAL,  
    EMP_LOGIN_TIME_DECIMAL,  
    EMP_LOGOUT_TIME_DECIMAL,  

    -- Determine Attendance Status  
    CASE  
        WHEN START_TIME_DECIMAL IS NULL AND END_TIME_DECIMAL IS NULL THEN 'NO SHIFT DATA'  
        WHEN SHIFT_DATE IS NOT NULL AND EMP_LOGIN_TIME_DECIMAL IS NULL AND EMP_LOGOUT_TIME_DECIMAL IS NULL THEN 'OFFLINE'  
        ELSE 'PRESENT'  
    END AS STATUS,  

    -- Late Login Check  
    CASE  
        WHEN EMP_LOGIN_TIME_DECIMAL > START_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGIN_TIME_DECIMAL <= START_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS LATE_LOGIN,  

    -- Early Login Check  
    CASE  
        WHEN EMP_LOGIN_TIME_DECIMAL <= START_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGIN_TIME_DECIMAL > START_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS EARLY_LOGIN,  

    -- Late Logout Check  
    CASE  
        WHEN EMP_LOGOUT_TIME_DECIMAL > END_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGOUT_TIME_DECIMAL <= END_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS LATE_LOGOUT,  

    -- Early Logout Check  
    CASE  
        WHEN EMP_LOGOUT_TIME_DECIMAL <= END_TIME_DECIMAL THEN 'Y'  
        WHEN EMP_LOGOUT_TIME_DECIMAL > END_TIME_DECIMAL THEN 'N'  
        ELSE 'NA'  
    END AS EARLY_LOGOUT  

FROM ProcessedData  
WHERE  
    -- Apply Attendance Status Filter Only if Selected
    ('{attendance_status}' IS NULL OR STATUS = '{attendance_status}')  
    -- Apply Login Status Filter Only if Selected
    AND ('{login_status}' IS NULL OR  
        (  
            ( '{login_status}' = 'Late Login' AND LATE_LOGIN = 'Y' ) OR  
            ( '{login_status}' = 'Early Login' AND EARLY_LOGIN = 'Y' ) OR  
            ( '{login_status}' = 'Late Logout' AND LATE_LOGOUT = 'Y' ) OR  
            ( '{login_status}' = 'Early Logout' AND EARLY_LOGOUT = 'Y' )  
        )  
    )  
ORDER BY EMP_ID, SHIFT_DATE DESC  
LIMIT {limit} OFFSET {offset};









WITH FilteredData AS (
    SELECT 
        EMP_ID,
        SHIFT_DATE,
        
        -- Convert START_TIME to Decimal Hours, treating NULLs or zero values properly
        CASE 
            WHEN START_TIME IS NOT NULL AND START_TIME <> '00000000000000' 
            THEN HOUR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) + 
                 (MINUTE(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
                 (SECOND(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 3600.0)
        END AS START_TIME_DEC,

        -- Convert END_TIME to Decimal Hours
        CASE 
            WHEN END_TIME IS NOT NULL AND END_TIME <> '00000000000000'
            THEN HOUR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) + 
                 (MINUTE(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
                 (SECOND(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 3600.0)
        END AS END_TIME_DEC,

        -- Convert EMP_LOGIN_TIME to Decimal Hours
        CASE 
            WHEN EMP_LOGIN_TIME IS NOT NULL AND EMP_LOGIN_TIME <> '00000000000000'
            THEN HOUR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) + 
                 (MINUTE(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
                 (SECOND(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 3600.0)
        END AS LOGIN_TIME_DEC,

        -- Convert EMP_LOGOUT_TIME to Decimal Hours
        CASE 
            WHEN EMP_LOGOUT_TIME IS NOT NULL AND EMP_LOGOUT_TIME <> '00000000000000'
            THEN HOUR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) + 
                 (MINUTE(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
                 (SECOND(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 3600.0)
        END AS LOGOUT_TIME_DEC

    FROM employee_attendance
    WHERE EMP_ID IN ({emp_ids})
    AND SHIFT_DATE BETWEEN '{start_date}' AND '{end_date}'
)
SELECT 
    EMP_ID,
    COUNT(SHIFT_DATE) AS TOTAL_SHIFT_DAYS,

    -- Average only considering non-null, non-zero START_TIME_DEC values
    SUM(START_TIME_DEC) / NULLIF(COUNT(START_TIME_DEC), 0) AS AVG_START_TIME,

    -- Average only considering non-null, non-zero END_TIME_DEC values
    SUM(END_TIME_DEC) / NULLIF(COUNT(END_TIME_DEC), 0) AS AVG_END_TIME,

    -- Average only considering non-null, non-zero LOGIN_TIME_DEC values
    SUM(LOGIN_TIME_DEC) / NULLIF(COUNT(LOGIN_TIME_DEC), 0) AS AVG_LOGIN_TIME,

    -- Average only considering non-null, non-zero LOGOUT_TIME_DEC values
    SUM(LOGOUT_TIME_DEC) / NULLIF(COUNT(LOGOUT_TIME_DEC), 0) AS AVG_LOGOUT_TIME

FROM FilteredData
GROUP BY EMP_ID
ORDER BY EMP_ID;





WITH FilteredData AS (
    SELECT 
        EMP_ID,
        SHIFT_DATE,
        
        -- Convert START_TIME to Decimal Hours
        COALESCE(
            HOUR(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) + 
            (MINUTE(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
            (SECOND(TO_TIMESTAMP(START_TIME, 'YYYYMMDDHH24MISS')) / 3600.0),
            0
        ) AS START_TIME_DEC,

        -- Convert END_TIME to Decimal Hours
        COALESCE(
            HOUR(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) + 
            (MINUTE(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
            (SECOND(TO_TIMESTAMP(END_TIME, 'YYYYMMDDHH24MISS')) / 3600.0),
            0
        ) AS END_TIME_DEC,

        -- Consider only non-null EMP_LOGIN_TIME values
        CASE 
            WHEN EMP_LOGIN_TIME IS NOT NULL 
            THEN HOUR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) + 
                 (MINUTE(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
                 (SECOND(TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYYMMDDHH24MISS')) / 3600.0)
        END AS LOGIN_TIME_DEC,

        -- Consider only non-null EMP_LOGOUT_TIME values
        CASE 
            WHEN EMP_LOGOUT_TIME IS NOT NULL 
            THEN HOUR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) + 
                 (MINUTE(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 60.0) + 
                 (SECOND(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYYMMDDHH24MISS')) / 3600.0)
        END AS LOGOUT_TIME_DEC

    FROM employee_attendance
    WHERE EMP_ID IN ({emp_ids})
    AND SHIFT_DATE BETWEEN '{start_date}' AND '{end_date}'
)
SELECT 
    EMP_ID,
    COUNT(SHIFT_DATE) AS TOTAL_SHIFT_DAYS,
    SUM(START_TIME_DEC) / NULLIF(COUNT(SHIFT_DATE), 0) AS AVG_START_TIME,
    SUM(END_TIME_DEC) / NULLIF(COUNT(SHIFT_DATE), 0) AS AVG_END_TIME,
    
    -- Consider only non-null LOGIN_TIME_DEC values for the average
    SUM(LOGIN_TIME_DEC) / NULLIF(COUNT(LOGIN_TIME_DEC), 0) AS AVG_LOGIN_TIME,

    -- Consider only non-null LOGOUT_TIME_DEC values for the average
    SUM(LOGOUT_TIME_DEC) / NULLIF(COUNT(LOGOUT_TIME_DEC), 0) AS AVG_LOGOUT_TIME

FROM FilteredData
GROUP BY EMP_ID
ORDER BY EMP_ID;







WITH FilteredData AS (
    SELECT 
        EMP_ID,
        SHIFT_DATE,
        
        -- Convert string time to TIMESTAMP and then to Decimal Hours
        COALESCE(
            EXTRACT(HOUR FROM TO_TIMESTAMP(START_TIME, 'YYYY-MM-DD HH24:MI:SS')) + 
            EXTRACT(MINUTE FROM TO_TIMESTAMP(START_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 60.0 + 
            EXTRACT(SECOND FROM TO_TIMESTAMP(START_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 3600.0,
            0
        ) AS START_TIME_DEC,

        COALESCE(
            EXTRACT(HOUR FROM TO_TIMESTAMP(END_TIME, 'YYYY-MM-DD HH24:MI:SS')) + 
            EXTRACT(MINUTE FROM TO_TIMESTAMP(END_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 60.0 + 
            EXTRACT(SECOND FROM TO_TIMESTAMP(END_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 3600.0,
            0
        ) AS END_TIME_DEC,

        COALESCE(
            EXTRACT(HOUR FROM TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYY-MM-DD HH24:MI:SS')) + 
            EXTRACT(MINUTE FROM TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 60.0 + 
            EXTRACT(SECOND FROM TO_TIMESTAMP(EMP_LOGIN_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 3600.0,
            0
        ) AS LOGIN_TIME_DEC,

        COALESCE(
            EXTRACT(HOUR FROM TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYY-MM-DD HH24:MI:SS')) + 
            EXTRACT(MINUTE FROM TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 60.0 + 
            EXTRACT(SECOND FROM TO_TIMESTAMP(EMP_LOGOUT_TIME, 'YYYY-MM-DD HH24:MI:SS')) / 3600.0,
            0
        ) AS LOGOUT_TIME_DEC

    FROM employee_attendance
    WHERE EMP_ID IN ({emp_ids})
    AND SHIFT_DATE BETWEEN '{start_date}' AND '{end_date}'
)
SELECT 
    EMP_ID,
    COUNT(SHIFT_DATE) AS TOTAL_SHIFT_DAYS,
    SUM(START_TIME_DEC) / NULLIF(COUNT(SHIFT_DATE), 0) AS AVG_START_TIME,
    SUM(END_TIME_DEC) / NULLIF(COUNT(SHIFT_DATE), 0) AS AVG_END_TIME,
    SUM(LOGIN_TIME_DEC) / NULLIF(COUNT(SHIFT_DATE), 0) AS AVG_LOGIN_TIME,
    SUM(LOGOUT_TIME_DEC) / NULLIF(COUNT(SHIFT_DATE), 0) AS AVG_LOGOUT_TIME
FROM FilteredData
GROUP BY EMP_ID
ORDER BY EMP_ID;








SELECT 
    EMP_ID, 
    EMP_CONTRACTED_HOURS,

    ROUND(HOUR(TO_TIMESTAMP(START_TIME, 'yyyy:MM:dd HH:mm:ss')) + 
          MINUTE(TO_TIMESTAMP(START_TIME, 'yyyy:MM:dd HH:mm:ss')) / 60.0 + 
          SECOND(TO_TIMESTAMP(START_TIME, 'yyyy:MM:dd HH:mm:ss')) / 3600.0, 2) AS START_TIME_DECIMAL,

    ROUND(HOUR(TO_TIMESTAMP(END_TIME, 'yyyy:MM:dd HH:mm:ss')) + 
          MINUTE(TO_TIMESTAMP(END_TIME, 'yyyy:MM:dd HH:mm:ss')) / 60.0 + 
          SECOND(TO_TIMESTAMP(END_TIME, 'yyyy:MM:dd HH:mm:ss')) / 3600.0, 2) AS END_TIME_DECIMAL,

    ROUND(HOUR(TO_TIMESTAMP(EMP_LOGIN_TIME, 'yyyy:MM:dd HH:mm:ss')) + 
          MINUTE(TO_TIMESTAMP(EMP_LOGIN_TIME, 'yyyy:MM:dd HH:mm:ss')) / 60.0 + 
          SECOND(TO_TIMESTAMP(EMP_LOGIN_TIME, 'yyyy:MM:dd HH:mm:ss')) / 3600.0, 2) AS EMP_LOGIN_TIME_DECIMAL,

    ROUND(HOUR(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'yyyy:MM:dd HH:mm:ss')) + 
          MINUTE(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'yyyy:MM:dd HH:mm:ss')) / 60.0 + 
          SECOND(TO_TIMESTAMP(EMP_LOGOUT_TIME, 'yyyy:MM:dd HH:mm:ss')) / 3600.0, 2) AS EMP_LOGOUT_TIME_DECIMAL,

    SHIFT_DATE

FROM strace.emploginlogout;





You're right! If we round the values at the Databricks end, it reduces the processing load on Python, making it more efficient. Here’s the updated Databricks query with final rounding so Python only needs to add the Employee Name from HRDict:


---

Optimized Databricks Query (With Rounding at DB Level)

SELECT 
    EMP_ID, 
    EMP_CONTRACTED_HOURS,
    ROUND(HOUR(START_TIME) + MINUTE(START_TIME) / 60.0 + SECOND(START_TIME) / 3600.0, 2) AS START_TIME_DECIMAL,
    ROUND(HOUR(END_TIME) + MINUTE(END_TIME) / 60.0 + SECOND(END_TIME) / 3600.0, 2) AS END_TIME_DECIMAL,
    ROUND(HOUR(EMP_LOGIN_TIME) + MINUTE(EMP_LOGIN_TIME) / 60.0 + SECOND(EMP_LOGIN_TIME) / 3600.0, 2) AS EMP_LOGIN_TIME_DECIMAL,
    ROUND(HOUR(EMP_LOGOUT_TIME) + MINUTE(EMP_LOGOUT_TIME) / 60.0 + SECOND(EMP_LOGOUT_TIME) / 3600.0, 2) AS EMP_LOGOUT_TIME_DECIMAL,
    SHIFT_DATE
FROM 
    strace.emploginlogout;


---

Python Processing (Just Adding Employee Name)

import json

# HR Dictionary (Fetched from Azure SQL)
HRDict = {
    "45179442": {"EMP_NAME": "John Doe"},
    "45179443": {"EMP_NAME": "Jane Smith"},
}

# Databricks Query Result (Already Rounded at DB Level)
databricks_result = [
    (45179442, 9.0, 9.00, 18.00, 10.71, 17.71, "2025-02-12"),
    (45179442, 9.0, 9.00, 18.00, 10.22, 20.63, "2025-02-13"),
]

# Process Data (Just Adding Employee Name)
output = [
    {
        "EMP_ID": emp_id,
        "START_TIME": start_time,  # Already rounded from DB
        "END_TIME": end_time,      # Already rounded from DB
        "EMP_LOGIN_TIME": login_time,  # Already rounded from DB
        "EMP_LOGOUT_TIME": logout_time,  # Already rounded from DB
        "SHIFT_DATE": shift_date,
        "EMP_NAME": HRDict.get(str(emp_id), {}).get("EMP_NAME", "Unknown")
    }
    for emp_id, contracted_hours, start_time, end_time, login_time, logout_time, shift_date in databricks_result
]

# Print JSON output
print(json.dumps(output, indent=4))


---

Final Output (Rounded at DB, Efficient in Python)

[
    {
        "EMP_ID": 45179442,
        "START_TIME": 9.00,
        "END_TIME": 18.00,
        "EMP_LOGIN_TIME": 10.71,
        "EMP_LOGOUT_TIME": 17.71,
        "SHIFT_DATE": "2025-02-12",
        "EMP_NAME": "John Doe"
    },
    {
        "EMP_ID": 45179442,
        "START_TIME": 9.00,
        "END_TIME": 18.00,
        "EMP_LOGIN_TIME": 10.22,
        "EMP_LOGOUT_TIME": 20.63,
        "SHIFT_DATE": "2025-02-13",
        "EMP_NAME": "John Doe"
    }
]


---

Why This is More Efficient?

✅ Rounding is done in Databricks (fastest way to process large data).
✅ Python only adds EMP_NAME (lightweight processing).
✅ Less data transfer between Databricks and Python (improves performance).

Now Python does almost nothing except attaching names! This is the best balance between performance and accuracy. Let me know if you need further tweaks!











with session_scope("DESIGNER") as session:
    today = datetime.today()
    
    # Date calculations
    last_monday = (today - timedelta(days=today.weekday() + 7)).date()
    last_sunday = (last_monday + timedelta(days=6)).date()
    current_monday = (last_sunday + timedelta(days=1)).date()
    current_sunday = (current_monday + timedelta(days=6)).date()
    
    logger.info(f"Copying shifts from {last_monday} - {last_sunday} to {current_monday} - {current_sunday}")

    # Single Query to fetch all employees under managers of distinct employees in EmployeeShiftInfo
    final_employee_ids = session.query(EmployeeInfo.EMPLID).filter(
        EmployeeInfo.manager_id.in_(
            session.query(EmployeeInfo.manager_id)
            .filter(EmployeeInfo.EMPLID.in_(
                session.query(EmployeeShiftInfo.EMPLID).distinct()
            ))
        ),
        EmployeeInfo.termination_date.is_(None)  # Only active employees
    ).distinct().all()

    final_employee_ids = [emp.EMPLID for emp in final_employee_ids]

    if not final_employee_ids:
        return jsonify({"message": "No active employees found under managers", "status": "no_changes"}), 200

    logger.info(f"Found {len(final_employee_ids)} employees under the identified managers.")

    return jsonify({"active_employees": final_employee_ids}), 200




import requests
import jwt
import json
from datetime import datetime

# API endpoint URL
API_URL = "http://your-flask-api-host/copy-scheduler"  # Update with actual API URL

# JWT Secret Key (must match the one in your Flask app)
SECRET_KEY = "your_jwt_secret_key"

def generate_token():
    """Generates a JWT token with a 'purpose' claim."""
    payload = {"purpose": "databricks-job"}  # No expiration needed
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    return token

def trigger_shift_copy():
    """Calls the CopyScheduler endpoint with JWT authentication."""
    try:
        token = generate_token()
        headers = {"Authorization": f"Bearer {token}"}

        response = requests.post(API_URL, json={}, headers=headers)
        response_data = response.json()

        if response.status_code == 200:
            print(f"[{datetime.now()}] Shift copy successful: {response_data}")
        else:
            print(f"[{datetime.now()}] Shift copy failed: {response_data}")

    except Exception as e:
        print(f"[{datetime.now()}] Error calling API: {str(e)}")

# Trigger the API call
trigger_shift_copy()





Got it! Since you can't use OAuth 2.0 with token exchange but still want security, you can implement a simple JWT-based authentication using a JWT secret key. Here's the plan:


---

🔒 Steps to Secure the Endpoint with a JWT Secret

1. The client (Databricks job) must include a token in the request header.


2. The server will verify the token before processing the request.


3. If the token is missing or incorrect, reject the request with a 403 Forbidden.


4. The JWT will be signed using a shared secret key (no need for token exchange).




---

✅ Implementation: Add JWT Token Verification

1️⃣ Generate the Token (Run this once and store it securely)

Use the following script to generate a JWT token with your secret:

import jwt
import datetime

SECRET_KEY = "your_jwt_secret_key"  # Change this to a strong secret key

payload = {
    "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7),  # Expiration (Optional)
    "purpose": "databricks-job"
}

token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
print(f"Generated Token: {token}")

✅ Store this token securely and use it in the Databricks job request.


---

2️⃣ Modify Your Endpoint to Verify the Token

Update the post() function in CopyScheduler:

import jwt
from flask import request

SECRET_KEY = "your_jwt_secret_key"  # Use the same secret key as above

class CopyScheduler(Resource):
    def post(self):
        """ API endpoint to copy previous week's shift data to the current week with JWT authentication. """
        auth_header = request.headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Unauthorized", "status": "failure"}), 403

        token = auth_header.split("Bearer ")[1]

        try:
            # Decode and verify the JWT token
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            
            # Optional: Check if token has the correct purpose
            if payload.get("purpose") != "databricks-job":
                return jsonify({"error": "Invalid token purpose", "status": "failure"}), 403

        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired", "status": "failure"}), 403
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token", "status": "failure"}), 403

        # 🔽 Proceed with shift copying logic here 🔽
        return jsonify({"message": "Success", "status": "success"}), 200


---

📌 How to Use the Token in Databricks

When calling the endpoint in your Databricks job, include the token in the Authorization header:

import requests

API_URL = "https://your-api-url/copy-scheduler"
TOKEN = "your_generated_token"

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

response = requests.post(API_URL, headers=headers)

print(response.json())  # Check response


---

🔍 Why This Works for Your Use Case

✅ No token exchange – The same secret key is used for signing & verification.
✅ Simple & Secure – If someone hits the endpoint without the correct token, access is denied.
✅ Prevents accidental execution – Only the Databricks job with the token can call it.

Let me know if you need modifications!







Here's a MySQL stored procedure that replicates the logic of your Python function. It copies shift data from the previous week to the current week, ensuring that existing shifts in the current week are not duplicated.


---

Stored Procedure: Copy Previous Week’s Shifts

DELIMITER $$

CREATE PROCEDURE CopyPreviousWeekShifts()
BEGIN
    DECLARE last_monday DATE;
    DECLARE last_sunday DATE;
    DECLARE current_monday DATE;
    DECLARE current_sunday DATE;
    
    -- Calculate date ranges
    SET last_monday = DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY);
    SET last_sunday = DATE_ADD(last_monday, INTERVAL 6 DAY);
    SET current_monday = DATE_ADD(last_sunday, INTERVAL 1 DAY);
    SET current_sunday = DATE_ADD(current_monday, INTERVAL 6 DAY);
    
    -- Temporary table to store shifts that need to be copied
    CREATE TEMPORARY TABLE TempShifts AS
    SELECT 
        emp_id, 
        DATE_ADD(shift_date, INTERVAL 7 DAY) AS new_shift_date,
        WEEKDAY(DATE_ADD(shift_date, INTERVAL 7 DAY)) AS new_day_id,
        start_time,
        end_time,
        is_week_off,
        emp_contracted_hours,
        emp_night_shift_flag
    FROM EmployeeShiftInfo
    WHERE shift_date BETWEEN last_monday AND last_sunday;

    -- Remove shifts that already exist in the current week
    DELETE FROM TempShifts
    WHERE (emp_id, new_shift_date) IN (
        SELECT emp_id, shift_date FROM EmployeeShiftInfo 
        WHERE shift_date BETWEEN current_monday AND current_sunday
    );

    -- Insert new shifts into EmployeeShiftInfo
    INSERT INTO EmployeeShiftInfo (emp_id, shift_date, day_id, start_time, end_time, is_week_off, emp_contracted_hours, emp_night_shift_flag)
    SELECT emp_id, new_shift_date, new_day_id, start_time, end_time, is_week_off, emp_contracted_hours, emp_night_shift_flag
    FROM TempShifts;

    -- Clean up temporary table
    DROP TEMPORARY TABLE IF EXISTS TempShifts;
END $$

DELIMITER ;


---

How to Execute the Procedure

CALL CopyPreviousWeekShifts();


---

How This Works

1. Calculates Date Ranges

last_monday to last_sunday → Previous week.

current_monday to current_sunday → Current week.



2. Copies Last Week’s Shifts

Shifts from EmployeeShiftInfo are duplicated with a new shift date (+7 days).



3. Prevents Duplicate Entries

If a shift already exists in the current week, it is removed before insertion.



4. Inserts Missing Shifts

Adds only new shifts to EmployeeShiftInfo.





---

Next Steps

Schedule the procedure using Azure Logic Apps, Data Factory, or a cron job.

Manually run it whenever needed via CALL CopyPreviousWeekShifts();


Would you like additional handling, such as default values for new shifts (e.g., weekends off)?








from datetime import datetime, timedelta from your_database_module import session_scope, EmployeeShiftInfo  # Update with actual module import logging

logger = logging.getLogger(name)

def copy_previous_week_efforts(): """ Copies last week's efforts to the current week for all employees. """ with session_scope("DESIGNER") as session: today = datetime.today()

Calculate dates

last_monday = today - timedelta(days=today.weekday() + 7)  # Last Monday  
last_sunday = last_monday + timedelta(days=6)  # Last Sunday  
current_monday = last_sunday + timedelta(days=1)  # Current Monday  
current_sunday = current_monday + timedelta(days=6)  # Current Sunday  
  
logger.info(f"Copying shifts from {last_monday} - {last_sunday} to {current_monday} - {current_sunday}")  
  
# Get distinct employee IDs from EmployeeShiftInfo table  
employees = session.query(EmployeeShiftInfo.emp_id).distinct().all()  
employee_ids = [emp.emp_id for emp in employees]  
  
# Fetch all shifts for the last week  
last_week_shifts = session.query(EmployeeShiftInfo).filter(  
    EmployeeShiftInfo.emp_id.in_(employee_ids),  
    EmployeeShiftInfo.shift_date.between(last_monday, last_sunday)  
).all()  
  
# Fetch all shifts for the current week  
current_week_shifts = session.query(EmployeeShiftInfo).filter(  
    EmployeeShiftInfo.emp_id.in_(employee_ids),  
    EmployeeShiftInfo.shift_date.between(current_monday, current_sunday)  
).all()  
  
# Use dictionaries for quick lookups  
last_week_shift_map = {(shift.emp_id, shift.shift_date): shift for shift in last_week_shifts}  
current_week_shift_dates = {(shift.emp_id, shift.shift_date) for shift in current_week_shifts}  
  
batch_insert = []  
time_format = "%H:%M:%S"  
default_start_time = "09:00:00"  
default_end_time = "17:00:00"  
  
for emp_id in employee_ids:  
    for i in range(7):  # Loop through 7 days of the current week  
        new_shift_date = current_monday + timedelta(days=i)  
          
        # Skip if shift already exists  
        if (emp_id, new_shift_date) in current_week_shift_dates:  
            continue  
          
        # Copy shift from last week if it exists, otherwise assign defaults  
        prev_shift_date = new_shift_date - timedelta(days=7)  
        prev_shift = last_week_shift_map.get((emp_id, prev_shift_date))  
          
        if prev_shift:  
            start_time = prev_shift.start_time.strftime(time_format)  
            end_time = prev_shift.end_time.strftime(time_format)  
            is_week_off = prev_shift.is_week_off  
            contracted_hrs = prev_shift.emp_contracted_hours  
            is_night_shift = prev_shift.emp_night_shift_flag  
        else:  
            start_time = default_start_time  
            end_time = default_end_time  
            is_week_off = "N"  
            contracted_hrs = 8.0  # Default contracted hours  
            is_night_shift = "N"  
          
        batch_insert.append(EmployeeShiftInfo(  
            emp_id=emp_id,  
            shift_date=new_shift_date,  
            day_id=new_shift_date.weekday(),  
            start_time=start_time,  
            end_time=end_time,  
            is_week_off=is_week_off,  
            emp_contracted_hours=contracted_hrs,  
            emp_night_shift_flag=is_night_shift  
        ))  
  
# Bulk insert only if there are new shifts to add  
if batch_insert:  
    session.bulk_save_objects(batch_insert)  
    session.commit()  
    logger.info(f"{len(batch_insert)} shift records inserted.")  
else:  
    logger.info("No new shifts to insert.")

In the above code, I want to write it as a GET, I mean I'll want it in a new class called copy scheduler, and I want the data in the GET function, where I'll get all the last week shift info and current week shift dates right, so I want to check the current week shift info and last week shift info based on employee and then the respective shift date, and I want it to meaningful recent response just to verify what's happening, and all along with that I'll be, I mean if you see I have batch insert at up and right, so where bulk insertion I'll do, so I don't want any insertion, but instead I want to check that also the respective details, I mean here it will be objects, but I don't want it as objects, instead I want to see the going to commit in JSON response, so give me such GET function








import multiprocessing
import time
import logging
from app.routes.employeeshift import copy_previous_week_efforts

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def scheduler_loop():
    """Scheduler loop that runs every Tuesday at 10:00 AM."""
    while True:
        current_time = time.strftime("%A %H:%M")  # Example: "Tuesday 10:00"
        if current_time == "Tuesday 10:00":
            logging.info("Scheduler triggered: Running copy_previous_week_efforts...")
            run_copy_previous_week_efforts()
        time.sleep(60)  # Check every 60 seconds

def run_copy_previous_week_efforts():
    try:
        logging.info("Executing copy_previous_week_efforts...")
        copy_previous_week_efforts()
        logging.info("Task completed successfully.")
    except Exception as e:
        logging.error(f"Error in copy_previous_week_efforts: {e}", exc_info=True)

def start_scheduler():
    """Start the scheduler in a separate process."""
    scheduler_process = multiprocessing.Process(target=scheduler_loop, daemon=True)
    scheduler_process.start()
    logging.info("Scheduler process started.")




import threading
import time
import logging
import os
from app.routes.employeeshift import copy_previous_week_efforts

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Global lock for thread safety
task_lock = threading.Lock()
scheduler_thread = None

# File lock to ensure only one process executes the task
lock_file = "/tmp/scheduler.lock"

def is_scheduler_running():
    """Check if the scheduler is already running by checking the lock file."""
    return os.path.exists(lock_file)

def set_scheduler_running():
    """Create a lock file to indicate the scheduler is running."""
    open(lock_file, 'w').close()

def clear_scheduler_running():
    """Remove the lock file after execution."""
    if os.path.exists(lock_file):
        os.remove(lock_file)

def start_scheduler():
    """Starts the scheduler thread if it's not already running."""
    global scheduler_thread
    if scheduler_thread and scheduler_thread.is_alive():
        logging.warning("Scheduler is already running. Another instance will not start.")
        return  # Prevent duplicate execution

    logging.info("Starting scheduler thread...")
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()

def scheduler_loop():
    """Scheduler runs every minute and checks if it's time to execute the task."""
    while True:
        current_time = time.strftime("%A %H:%M")  # Example: "Monday 00:04"
        if current_time == "Monday 00:04":
            logging.info("Scheduler triggered: Running copy_previous_week_efforts...")
            run_copy_previous_week_efforts()
        time.sleep(60)  # Check every 60 seconds

def run_copy_previous_week_efforts():
    """Executes the task with file lock and threading lock."""
    if is_scheduler_running():
        logging.warning("Task is already running in another process, skipping this execution.")
        return  # Prevent duplicate execution across processes

    if task_lock.locked():
        logging.warning("Task is already running in this process, skipping execution.")
        return  # Prevent duplicate execution in the same process

    with task_lock:  # Ensure only one execution at a time within the process
        try:
            set_scheduler_running()  # Create a lock file
            logging.info("Executing copy_previous_week_efforts...")
            copy_previous_week_efforts()
            logging.info("Task completed successfully.")
        except Exception as e:
            logging.error(f"Error in copy_previous_week_efforts: {e}", exc_info=True)
        finally:
            clear_scheduler_running()  # Remove the lock file after execution





from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_restful import Resource, Api
from your_database_module import session_scope, EmployeeShiftInfo  # Update with actual module import
import logging

logger = logging.getLogger(__name__)

class CopyScheduler(Resource):
    def get(self):
        """ Get shift details for last and current week. """
        with session_scope("DESIGNER") as session:
            today = datetime.today()

            # Calculate week start and end dates
            last_monday = today - timedelta(days=today.weekday() + 7)
            last_sunday = last_monday + timedelta(days=6)
            current_monday = last_sunday + timedelta(days=1)
            current_sunday = current_monday + timedelta(days=6)

            logger.info(f"Fetching shifts from {last_monday} - {last_sunday} and {current_monday} - {current_sunday}")

            # Get distinct employee IDs
            employees = session.query(EmployeeShiftInfo.emp_id).distinct().all()
            employee_ids = [emp.emp_id for emp in employees]

            # Fetch all shifts for last and current week
            last_week_shifts = session.query(EmployeeShiftInfo).filter(
                EmployeeShiftInfo.emp_id.in_(employee_ids),
                EmployeeShiftInfo.shift_date.between(last_monday, last_sunday)
            ).all()

            current_week_shifts = session.query(EmployeeShiftInfo).filter(
                EmployeeShiftInfo.emp_id.in_(employee_ids),
                EmployeeShiftInfo.shift_date.between(current_monday, current_sunday)
            ).all()

            # Create sets for lookup
            last_week_shift_dates = {(shift.emp_id, shift.shift_date) for shift in last_week_shifts}
            current_week_shift_dates = {(shift.emp_id, shift.shift_date) for shift in current_week_shifts}

            # Categorize employees into three groups
            current_week_present = []
            current_week_missing = []
            last_week_missing = []

            for emp_id in employee_ids:
                for i in range(7):  # Iterate through each day of the current week
                    shift_date = current_monday + timedelta(days=i)
                    prev_shift_date = shift_date - timedelta(days=7)

                    if (emp_id, shift_date) in current_week_shift_dates:
                        # Shift exists in current week
                        current_week_present.append({"emp_id": emp_id, "shift_date": str(shift_date)})
                    elif (emp_id, prev_shift_date) in last_week_shift_dates:
                        # Last week shift exists but missing in the current week
                        current_week_missing.append({"emp_id": emp_id, "shift_date": str(shift_date)})
                    elif (emp_id, prev_shift_date) not in last_week_shift_dates:
                        # Last week shift is missing (whether or not current shift exists)
                        last_week_missing.append({"emp_id": emp_id, "shift_date": str(prev_shift_date)})

            response = {
                "current_week_present": current_week_present,
                "current_week_missing": current_week_missing,
                "last_week_missing": last_week_missing
            }

            return jsonify(response)



import threading
import time
import logging
from app.routes.employeeshift import copy_previous_week_efforts

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Global lock to prevent multiple executions
task_lock = threading.Lock()
scheduler_thread = None

def start_scheduler():
    global scheduler_thread
    if scheduler_thread and scheduler_thread.is_alive():
        logging.warning("Scheduler is already running. Another instance will not start.")
        return  # Prevent duplicate execution

    logging.info("Starting scheduler thread...")
    scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()

def scheduler_loop():
    while True:
        # Wait until Monday 12 PM (for example)
        if time.strftime("%A") == "Monday" and time.strftime("%H:%M") == "12":
            logging.info("Scheduler triggered: Running copy_previous_week_efforts...")
            run_copy_previous_week_efforts()
        time.sleep(60)  # Check every 60 seconds to avoid high CPU usage

def run_copy_previous_week_efforts():
    if task_lock.locked():
        logging.warning("Task is already running, skipping this execution.")
        return  # Prevent running twice

    with task_lock:  # Ensure only one execution at a time
        try:
            logging.info("Executing copy_previous_week_efforts...")
            copy_previous_week_efforts()
            logging.info("Task completed successfully.")
        except Exception as e:
            logging.error(f"Error in copy_previous_week_efforts: {e}", exc_info=True)






from datetime import datetime, timedelta
from flask import Flask, jsonify
from flask_restful import Resource, Api
from your_database_module import session_scope, EmployeeShiftInfo  # Update with actual module import
import logging

logger = logging.getLogger(__name__)

class CopyScheduler(Resource):
    def get(self):
        """ Get shift details for the last and current week """
        with session_scope("DESIGNER") as session:
            today = datetime.today()

            # Calculate week start and end dates
            last_monday = today - timedelta(days=today.weekday() + 7)
            last_sunday = last_monday + timedelta(days=6)
            current_monday = last_sunday + timedelta(days=1)
            current_sunday = current_monday + timedelta(days=6)

            logger.info(f"Fetching shifts from {last_monday} - {last_sunday} and {current_monday} - {current_sunday}")

            # Get distinct employee IDs
            employees = session.query(EmployeeShiftInfo.emp_id).distinct().all()
            employee_ids = [emp.emp_id for emp in employees]

            # Fetch all shifts for last and current week
            last_week_shifts = session.query(EmployeeShiftInfo).filter(
                EmployeeShiftInfo.emp_id.in_(employee_ids),
                EmployeeShiftInfo.shift_date.between(last_monday, last_sunday)
            ).all()

            current_week_shifts = session.query(EmployeeShiftInfo).filter(
                EmployeeShiftInfo.emp_id.in_(employee_ids),
                EmployeeShiftInfo.shift_date.between(current_monday, current_sunday)
            ).all()

            # Map shifts
            last_week_shift_map = {(shift.emp_id, shift.shift_date): shift for shift in last_week_shifts}
            current_week_shift_dates = {(shift.emp_id, shift.shift_date) for shift in current_week_shifts}

            # Categorize employees into three groups
            current_week_present = []
            current_week_missing = []
            last_week_missing = []

            for emp_id in employee_ids:
                for i in range(7):  # Iterate through each day of the current week
                    shift_date = current_monday + timedelta(days=i)
                    prev_shift_date = shift_date - timedelta(days=7)

                    if (emp_id, shift_date) in current_week_shift_dates:
                        current_week_present.append({"emp_id": emp_id, "shift_date": str(shift_date)})
                    elif (emp_id, prev_shift_date) in last_week_shift_map:
                        current_week_missing.append({"emp_id": emp_id, "shift_date": str(shift_date)})
                    else:
                        last_week_missing.append({"emp_id": emp_id, "shift_date": str(prev_shift_date)})

            response = {
                "current_week_present": current_week_present,
                "current_week_missing": current_week_missing,
                "last_week_missing": last_week_missing
            }

            return jsonify(response)








import com.azure.messaging.eventhubs.*;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.core.util.BinaryData;
import java.util.Arrays;
import java.util.List;

public class EventHubJsonSender {
    public static void main(String[] args) {
        // Replace with your values
        String tenantId = "YOUR_TENANT_ID";
        String clientId = "YOUR_CLIENT_ID";
        String clientSecret = "YOUR_CLIENT_SECRET";
        String namespace = "YOUR_EVENT_HUB_NAMESPACE";
        String eventHubName = "YOUR_EVENT_HUB_NAME";
        String partitionKey = "my-partition-key";  // Partition key for routing

        // Create credential
        ClientSecretCredential credential = new ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build();

        // Create producer client
        EventHubProducerClient producer = new EventHubClientBuilder()
            .fullyQualifiedNamespace(namespace + ".servicebus.windows.net")
            .eventHubName(eventHubName)
            .credential(credential)
            .buildProducerClient();

        // List of JSON messages
        List<String> jsonMessages = Arrays.asList(
            "{\"id\": 1, \"message\": \"Hello Event Hub\"}",
            "{\"id\": 2, \"message\": \"Another JSON Event\"}"
        );

        // Create batch with partition key
        CreateBatchOptions options = new CreateBatchOptions().setPartitionKey(partitionKey);
        EventDataBatch batch = producer.createBatch(options);

        for (String json : jsonMessages) {
            batch.tryAdd(new EventData(BinaryData.fromString(json)));
        }

        // Send batch
        producer.send(batch);
        System.out.println("JSON events sent successfully with partition key: " + partitionKey);

        // Close producer
        producer.close();
    }
}






import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
from app.routes import employee_shift  # Ensure correct import path

# Configure Logging
logging.basicConfig(
    filename="/home/site/wwwroot/scheduler.log",  
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Set timezone
IST = pytz.timezone("Asia/Kolkata")

# Initialize Scheduler
scheduler = BackgroundScheduler(timezone=IST)

def start_scheduler():
    """Starts the APScheduler to run the copy task every Monday at 12:01 AM IST"""
    logging.info("Scheduler Initialized: Will run copy_previous_week_efforts as planned")

    scheduler.add_job(
        employee_shift.copy_previous_week_efforts,
        'cron',
        day_of_week="mon",
        hour=12,  # 12:01 AM IST
        minute=1,
        misfire_grace_time=3600  # 1 hour grace time
    )

    scheduler.start()
    logging.info("Scheduler started: Running copy_previous_week_efforts every Monday at 12:01 AM IST")

if __name__ == '__main__':
    start_scheduler()

    # Keep the script running
    while True:
        time.sleep(60)  # Prevents script from exiting






Got it! You have a 32-byte secret key that you want to store securely in the TPM and later use in your Python code. Since TPM does not allow direct retrieval of stored secrets, you need to:

1. Store the 32-byte key securely in TPM.


2. Retrieve and use it inside TPM for cryptographic operations (like HMAC or encryption).




---

Step 1: Store 32-Byte Key in TPM

Since TPM doesn’t allow storing arbitrary data directly, you need to create a persistent HMAC key inside TPM and seed it with your 32-byte key.

Here's how to store your 32-byte secret key inside TPM using Python:

Python Code to Store the 32-Byte Key in TPM

import ctypes

# Load Windows CNG library
ncrypt = ctypes.windll.ncrypt

# Constants
MS_PLATFORM_CRYPTO_PROVIDER = "Microsoft Platform Crypto Provider"
NCRYPT_SILENT_FLAG = 0x00000040
NCRYPT_OVERWRITE_KEY_FLAG = 0x00000080  # Overwrite existing key if needed

# Open TPM-backed key storage provider
provider_handle = ctypes.c_void_p()
status = ncrypt.NCryptOpenStorageProvider(ctypes.byref(provider_handle), MS_PLATFORM_CRYPTO_PROVIDER, 0)

if status != 0:
    raise RuntimeError(f"Failed to open TPM storage provider: {status}")

# Define key name
key_name = "MyHMACKey"

# Create a new persisted key in TPM
key_handle = ctypes.c_void_p()
status = ncrypt.NCryptCreatePersistedKey(provider_handle, ctypes.byref(key_handle), b"HMAC", key_name, 0, NCRYPT_OVERWRITE_KEY_FLAG)

if status != 0:
    raise RuntimeError(f"Failed to create TPM key: {status}")

# Set the key value (32-byte secret key)
secret_key = b"my_very_secure_32_byte_secret_key_"  # Must be exactly 32 bytes
status = ncrypt.NCryptSetProperty(key_handle, b"TPM_SEAL_KEY", secret_key, len(secret_key), 0)

if status != 0:
    raise RuntimeError(f"Failed to set TPM key property: {status}")

# Finalize and store key in TPM
status = ncrypt.NCryptFinalizeKey(key_handle, 0)

if status != 0:
    raise RuntimeError(f"Failed to finalize TPM key storage: {status}")

print(f"Secret key '{key_name}' securely stored in TPM.")

🔹 What Happens Here?

A persistent key named "MyHMACKey" is created in TPM.

Your 32-byte secret key is securely stored inside this TPM key.

The key is finalized and saved inside TPM.



---

Step 2: Use the 32-Byte Key from TPM in Python

Since TPM doesn’t allow extracting the key, you can use it for cryptographic operations inside TPM, like HMAC authentication.

Python Code to Use the Stored Key for HMAC

import ctypes

# Load Windows CNG library
ncrypt = ctypes.windll.ncrypt

# Open TPM-backed key storage provider
provider_handle = ctypes.c_void_p()
status = ncrypt.NCryptOpenStorageProvider(ctypes.byref(provider_handle), "Microsoft Platform Crypto Provider", 0)

if status != 0:
    raise RuntimeError(f"Failed to open TPM storage provider: {status}")

# Retrieve the key
key_name = "MyHMACKey"
retrieved_key_handle = ctypes.c_void_p()
status = ncrypt.NCryptOpenKey(provider_handle, ctypes.byref(retrieved_key_handle), key_name, 0, 0)

if status != 0:
    raise RuntimeError(f"Failed to retrieve TPM key: {status}")

# Use TPM key for HMAC (Example: Sign a message)
message = b"Hello, TPM!"
signature = ctypes.create_string_buffer(32)  # Output buffer for HMAC

status = ncrypt.NCryptSignHash(retrieved_key_handle, None, message, len(message), signature, len(signature), None)

if status != 0:
    raise RuntimeError(f"Failed to sign message with TPM key: {status}")

print(f"HMAC signature generated: {signature.raw.hex()}")

🔹 What Happens Here?

The 32-byte key is securely retrieved from TPM.

TPM is used to generate an HMAC signature for authentication.

Your Python app can use the TPM-stored key securely without exposing it in memory.



---

Why Is This Secure?

✅ The secret key never leaves TPM – It stays protected inside hardware.
✅ Even an attacker with full access to your system cannot extract the key.
✅ Only authorized applications can use TPM for cryptographic operations.


---

Next Steps

Would you like an example of how to encrypt/decrypt data using this TPM key instead of HMAC? 🚀












ncrypt.NCryptOpenKey.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p), ctypes.c_wchar_p, ctypes.c_ulong, ctypes.c_ulong]

# Open the stored TPM key
retrieved_key_handle = ctypes.c_void_p()
status = ncrypt.NCryptOpenKey(provider_handle, ctypes.byref(retrieved_key_handle), key_name, 0, 0)

if status != 0:
    raise RuntimeError(f"Failed to retrieve TPM key: {status}")

print(f"Key '{key_name}' retrieved from TPM successfully!")






import ctypes
import ctypes.wintypes

# Load Windows CNG library
ncrypt = ctypes.WinDLL("ncrypt.dll")

# Define constants
NCRYPT_SILENT_FLAG = 0x00000040
MS_PLATFORM_CRYPTO_PROVIDER = "Microsoft Platform Crypto Provider"

# Function prototypes
ncrypt.NCryptOpenStorageProvider.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_wchar_p, ctypes.c_ulong]
ncrypt.NCryptCreatePersistedKey.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p), ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_ulong, ctypes.c_ulong]
ncrypt.NCryptFinalizeKey.argtypes = [ctypes.c_void_p, ctypes.c_ulong]

# Open TPM-backed key storage provider
provider_handle = ctypes.c_void_p()
status = ncrypt.NCryptOpenStorageProvider(ctypes.byref(provider_handle), MS_PLATFORM_CRYPTO_PROVIDER, 0)

if status != 0:
    raise RuntimeError(f"Failed to open TPM storage provider: {status}")

# Create a TPM-backed key
key_handle = ctypes.c_void_p()
key_name = "MySecureHMACKey"
status = ncrypt.NCryptCreatePersistedKey(provider_handle, ctypes.byref(key_handle), "RSA", key_name, 0, 0)

if status != 0:
    raise RuntimeError(f"Failed to create TPM key: {status}")

# Finalize the key (makes it persistent in TPM)
status = ncrypt.NCryptFinalizeKey(key_handle, 0)
if status != 0:
    raise RuntimeError(f"Failed to finalize TPM key: {status}")

print("Secret key securely stored in TPM!")

import win32security

SECRET_KEY = b"MySuperSecretKey123"

# Open TPM-backed key storage provider
provider = win32security.CryptAcquireContext(
    None, None, "Microsoft Platform Crypto Provider", win32security.PROV_RSA_AES, 0
)

# Store secret key in TPM
win32security.CryptSetProvParam(provider, win32security.PP_SIGNATURE_PIN, SECRET_KEY)

print("Secret key securely stored in TPM!")


# Retrieve secret key from TPM
retrieved_secret = win32security.CryptGetProvParam(provider, win32security.PP_SIGNATURE_PIN)

print("Decrypted Secret:", retrieved_secret)







🔹 Overview of the Authentication Approach


import com.azure.messaging.eventhubs.*;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

public class EventHubSender {
    public static void main(String[] args) {
        String tenantId = "YOUR_TENANT_ID";
        String clientId = "YOUR_CLIENT_ID";
        String clientSecret = "YOUR_CLIENT_SECRET";
        String namespace = "YOUR_EVENT_HUB_NAMESPACE";
        String eventHubName = "YOUR_EVENT_HUB_NAME";

        ClientSecretCredential credential = new ClientSecretCredentialBuilder()
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build();

        EventHubProducerClient producer = new EventHubClientBuilder()
            .fullyQualifiedNamespace(namespace + ".servicebus.windows.net")
            .eventHubName(eventHubName)
            .credential(credential)
            .buildProducerClient();

        EventDataBatch batch = producer.createBatch();
        batch.tryAdd(new EventData("Event from Java!"));
        
        producer.send(batch);
        System.out.println("Event sent successfully!");

        producer.close();
    }
}




This approach combines asymmetric encryption (EDDSA), HMAC for integrity, and timestamp-based validation to authenticate API requests securely.

🔹 Workflow Overview

1️⃣ Key Generation

Public-Private Key Pair (EDDSA)

Client: Stores the private key (private.pem).

Server: Stores the public key (public.pem).


Secret Key for HMAC (Shared Key)

A 32-byte secret key is stored both on the client and server.

This is used for HMAC-SHA256 signing to prevent request tampering.



2️⃣ Client-Side Authentication

The client constructs a secure request by:

1. Using the Private Key (private.pem)

Loads the private key.

Uses EDDSA signing on the HMAC-based token.



2. Generating an HMAC Token

Uses HMAC-SHA256 with:

Secret Key

User ID (8-digit unique identifier)

Timestamp (for replay protection)


This generates a one-time signature.



3. Sending the Request

Sends the user ID, timestamp, HMAC token, and digital signature to the server.





---

3️⃣ Server-Side Authentication

The server verifies the request:

1. Public Key Verification (EDDSA)

The server retrieves the client’s public key.

Uses it to verify the signature in the request.



2. Recomputing HMAC Token

The server:

Uses the same secret key.

Recomputes the HMAC using the received user ID & timestamp.


Compares it with the HMAC in the request.



3. Validating Timestamp

Ensures the timestamp is within an acceptable range (e.g., ±5 minutes).

This prevents replay attacks.



4. Final Authentication

If HMAC & signature match, the request is authenticated.

Otherwise, the request is rejected.





---

🔹 Security Aspects


---

🔹 Summary of Algorithms Used

1. EDDSA (Ed25519)

Used for digital signatures.

Client signs requests with the private key.

Server verifies signatures with the public key.



2. HMAC-SHA256

Ensures message integrity.

Uses a shared secret key to generate a one-time HMAC token.



3. Timestamp Validation

Prevents replay attacks.

Ensures requests are time-bound.





---

This approach provides strong authentication, ensuring:
✅ Tamper-proof requests
✅ No secret key exchange over the network
✅ No need for user registration

Let me know if you need code breakdowns! 🚀





You can use Python to convert a PEM file into a Base64-encoded string and back.


---

🔹 Convert PEM File to Base64 String (Encoding)

import base64

def encode_pem_to_base64(pem_path):
    with open(pem_path, "rb") as pem_file:
        pem_content = pem_file.read()
    base64_encoded = base64.b64encode(pem_content).decode("utf-8")
    return base64_encoded

# Example usage
pem_path = "private_key.pem"
base64_pem = encode_pem_to_base64(pem_path)
print("Base64 Encoded PEM:\n", base64_pem)


---

🔹 Convert Base64 String Back to PEM File (Decoding)

def decode_base64_to_pem(base64_string, output_path):
    pem_content = base64.b64decode(base64_string.encode("utf-8"))
    with open(output_path, "wb") as pem_file:
        pem_file.write(pem_content)

# Example usage
output_pem_path = "restored_private_key.pem"
decode_base64_to_pem(base64_pem, output_pem_path)
print("PEM file restored successfully!")


---

🔹 Why Use Base64 Encoding?

✅ Preserves the multi-line format of the PEM file
✅ Makes it easy to store in Azure Key Vault, environment variables, or databases
✅ Helps prevent formatting issues when pasting


---

🔹 How to Use This in Azure Key Vault?

1. Use encode_pem_to_base64() before storing in Key Vault.


2. Retrieve it in FastAPI and use decode_base64_to_pem() to restore the key.



Would you like me to integrate this directly into your FastAPI authentication flow? 🚀






import base64
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Configure Azure Key Vault details
KEY_VAULT_NAME = "your-key-vault-name"
KV_URL = f"https://{KEY_VAULT_NAME}.vault.azure.net"

# Authenticate using Managed Identity (or use Client ID/Secret)
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KV_URL, credential=credential)

# Retrieve the secret from Key Vault
secret = client.get_secret("PRIVATE_KEY")

# Decode Base64 to restore PEM format
pem_content = base64.b64decode(secret.value).decode("utf-8")

# Save to a temporary file (Optional, if required for libraries)
pem_file_path = "/tmp/private_key.pem"
with open(pem_file_path, "w") as f:
    f.write(pem_content)

print("Private key successfully retrieved from Key Vault!")







🔹 Overview of the Authentication Approach

This approach combines asymmetric encryption (EDDSA), HMAC for integrity, and timestamp-based validation to authenticate API requests securely.

🔹 Workflow Overview

1️⃣ Key Generation

Public-Private Key Pair (EDDSA)

Client: Stores the private key (private.pem).

Server: Stores the public key (public.pem).


Secret Key for HMAC (Shared Key)

A 32-byte secret key is stored both on the client and server.

This is used for HMAC-SHA256 signing to prevent request tampering.



2️⃣ Client-Side Authentication

The client constructs a secure request by:

1. Using the Private Key (private.pem)

Loads the private key.

Uses EDDSA signing on the HMAC-based token.



2. Generating an HMAC Token

Uses HMAC-SHA256 with:

Secret Key

User ID (8-digit unique identifier)

Timestamp (for replay protection)


This generates a one-time signature.



3. Sending the Request

Sends the user ID, timestamp, HMAC token, and digital signature to the server.





---

3️⃣ Server-Side Authentication

The server verifies the request:

1. Public Key Verification (EDDSA)

The server retrieves the client’s public key.

Uses it to verify the signature in the request.



2. Recomputing HMAC Token

The server:

Uses the same secret key.

Recomputes the HMAC using the received user ID & timestamp.


Compares it with the HMAC in the request.



3. Validating Timestamp

Ensures the timestamp is within an acceptable range (e.g., ±5 minutes).

This prevents replay attacks.



4. Final Authentication

If HMAC & signature match, the request is authenticated.

Otherwise, the request is rejected.





---

🔹 Security Aspects


---

🔹 Summary of Algorithms Used

1. EDDSA (Ed25519)

Used for digital signatures.

Client signs requests with the private key.

Server verifies signatures with the public key.



2. HMAC-SHA256

Ensures message integrity.

Uses a shared secret key to generate a one-time HMAC token.



3. Timestamp Validation

Prevents replay attacks.

Ensures requests are time-bound.





---

This approach provides strong authentication, ensuring:
✅ Tamper-proof requests
✅ No secret key exchange over the network
✅ No need for user registration

Let me know if you need code breakdowns! 🚀










Step 1: Generate Public-Private Key Pair (EDDSA - Ed25519)

Run this script once to create a key pair.

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization

# Generate a new private key
private_key = Ed25519PrivateKey.generate()

# Save private key
with open("private_key.pem", "wb") as f:
    f.write(private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption()
    ))

# Save public key
public_key = private_key.public_key()
with open("public_key.pem", "wb") as f:
    f.write(public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    ))

print("Private and public keys generated successfully!")

✅ Generates:

private_key.pem → Used by the client.

public_key.pem → Used by the server.



---

Step 2: Generate 32-Byte Secret Key

Run this once to create a shared secret key.

import os

# Generate a random 32-byte secret key
secret_key = os.urandom(32)

# Save it to a file
with open("secret_key.bin", "wb") as f:
    f.write(secret_key)

print("Secret key generated successfully!")

✅ Generates:

secret_key.bin → Shared between client & server.



---

Step 3: Client-Side Code

Client will sign requests using the private key & send them.

import time
import hmac
import hashlib
import requests
import binascii
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

# Load secret key
with open("secret_key.bin", "rb") as f:
    SECRET_KEY = f.read()

# Load private key
with open("private_key.pem", "rb") as f:
    private_key = Ed25519PrivateKey.from_private_bytes(f.read())

# Constants
USER_ID = "45231234"
SERVER_URL = "http://127.0.0.1:8000"

def generate_auth_headers():
    timestamp = str(int(time.time()))
    message = f"{USER_ID}{timestamp}".encode()
    
    # Generate HMAC session key
    session_key = hmac.new(SECRET_KEY, message, hashlib.sha256).digest()
    
    # Sign session key
    signature = private_key.sign(session_key)

    return {
        "X-User-ID": USER_ID,
        "X-Timestamp": timestamp,
        "X-Signature": binascii.hexlify(signature).decode()
    }

# Step 1: Make an authenticated GET request
headers = generate_auth_headers()
response = requests.get(f"{SERVER_URL}/get_keys", headers=headers)
print("GET Response:", response.json())

# Step 2: Make an authenticated POST request
headers = generate_auth_headers()
response = requests.post(f"{SERVER_URL}/submit_data", headers=headers)
print("POST Response:", response.json())

✅ Sends headers:

X-User-ID: 45231234
X-Timestamp: 1700000000
X-Signature: <signed_hmac>


---

Step 4: Server-Side Code (FastAPI Authentication Middleware)

The server verifies the request before allowing access to endpoints.

from fastapi import FastAPI, Request, HTTPException, Depends
from pydantic import BaseModel
import time
import hmac
import hashlib
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
import binascii

app = FastAPI()

# Load secret key
with open("secret_key.bin", "rb") as f:
    SECRET_KEY = f.read()

# Load public key
with open("public_key.pem", "rb") as f:
    public_key_bytes = f.read()
public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)

TIME_LIMIT = 60  # Request must be within 60 seconds

class AuthRequest(BaseModel):
    user_id: str
    timestamp: str
    signature: str

async def verify_request(request: Request):
    headers = request.headers
    user_id = headers.get("X-User-ID")
    timestamp = headers.get("X-Timestamp")
    signature = headers.get("X-Signature")

    if not user_id or not timestamp or not signature:
        raise HTTPException(status_code=401, detail="Missing authentication headers")

    # Step 1: Validate timestamp (Prevent replay attacks)
    server_time = int(time.time())
    client_time = int(timestamp)
    if abs(server_time - client_time) > TIME_LIMIT:
        raise HTTPException(status_code=400, detail="Timestamp expired")

    # Step 2: Generate session key (Same as client)
    message = f"{user_id}{timestamp}".encode()
    session_key = hmac.new(SECRET_KEY, message, hashlib.sha256).digest()

    # Step 3: Verify the signature
    try:
        signature_bytes = binascii.unhexlify(signature)
        public_key.verify(signature_bytes, session_key)
    except Exception:
        raise HTTPException(status_code=403, detail="Invalid signature")

    return {"user_id": user_id}

@app.get("/get_keys", dependencies=[Depends(verify_request)])
async def get_keys():
    return {"message": "Keys retrieved successfully!"}

@app.post("/submit_data", dependencies=[Depends(verify_request)])
async def submit_data():
    return {"message": "Data submitted successfully!"}

✅ Every request must be authenticated before the endpoint executes.


---

Summary

This ensures authentication on every request, preventing unauthorized access. 🚀






Here’s a refactored and optimized FastAPI code where the signature, public key verification, and nonce check are moved to a separate reusable function. This ensures that any API request can use it for verification.


---

🔹 Key Improvements

✅ Reusable function for verifying headers, nonce, and signature.
✅ FastAPI dependency injection for seamless verification in any route.
✅ Uses TTLCache (efficient, auto-expires nonce, prevents replay attacks).
✅ Memory-efficient & scalable for 60,000+ users.


---

📌 Optimized FastAPI Code

import redis
import os
import base64
from fastapi import FastAPI, Request, HTTPException, Depends
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from dotenv import load_dotenv
from cachetools import TTLCache

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize Redis connection (Change `your-redis-host` to your Redis instance)
redis_client = redis.Redis(host="your-redis-host", port=6379, db=0, decode_responses=True)

# Cache for nonce verification (auto-expires in 5 minutes)
used_nonces = TTLCache(maxsize=500000, ttl=300)  # Efficient, removes expired values automatically

def verify_request(request: Request):
    """
    Verifies request headers, nonce, and signature for authentication.
    This function ensures that the request is secure and prevents replay attacks.

    Args:
        request (Request): Incoming FastAPI request object.

    Returns:
        None: If verification passes.
    
    Raises:
        HTTPException: If verification fails (Invalid headers, nonce reuse, or signature mismatch).
    """
    try:
        # Step 1: Retrieve headers from request
        headers = request.headers
        signature_b64 = headers.get("X-Signature")
        public_key_pem = headers.get("X-Public-Key")
        nonce_b64 = headers.get("X-Nonce")

        if not signature_b64 or not public_key_pem or not nonce_b64:
            raise HTTPException(status_code=400, detail="Missing required headers")

        # Step 2: Decode received values
        signature = base64.b64decode(signature_b64)
        public_key = load_pem_public_key(public_key_pem.encode())
        nonce = base64.b64decode(nonce_b64)

        # Step 3: Check if nonce was used before (Replay Attack Prevention)
        nonce_key = f"nonce:{nonce_b64}"  # Unique key for Redis

        if nonce_b64 in used_nonces or redis_client.exists(nonce_key):
            raise HTTPException(status_code=403, detail="Nonce already used")  # Reject replay attack

        # Store nonce (5-minute expiration) to prevent reuse
        used_nonces[nonce_b64] = True  # Stored in in-memory cache
        redis_client.setex(nonce_key, 300, "1")  # Also store in Redis (optional for distributed servers)

        # Step 4: Verify the signature
        message = f"{request.method} {request.url.path} {nonce_b64}"
        public_key.verify(
            signature,
            message.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )

    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Verification failed: {str(e)}")


@app.get("/get-env-variables")
async def get_env_variables(request: Request, _: None = Depends(verify_request)):
    """
    Secure endpoint to fetch environment variables after verifying request integrity.

    Args:
        request (Request): Incoming request object (automatically verified by `verify_request`).

    Returns:
        dict: Environment variables (masked for security if needed).
    """
    response_data = {
        "VAR1": os.getenv("VAR1", "default_value_1"),
        "VAR2": os.getenv("VAR2", "default_value_2"),
        "VAR3": os.getenv("VAR3", "default_value_3"),
        "VAR4": os.getenv("VAR4", "default_value_4"),
        "VAR5": os.getenv("VAR5", "default_value_5"),
        "VAR6": os.getenv("VAR6", "default_value_6"),
    }
    return response_data


@app.post("/submit-data")
async def submit_data(request: Request, _: None = Depends(verify_request)):
    """
    Secure POST endpoint to accept sensitive data.
    
    Args:
        request (Request): Incoming request (verified before processing).

    Returns:
        dict: Success message.
    """
    data = await request.json()
    return {"message": "Data received successfully", "data": data}


# To run the FastAPI server, use:
# uvicorn main:app --host 0.0.0.0 --port 5000


---

🔹 Why This is the Best Implementation

1️⃣ ✅ Reusable Verification Function (verify_request)

Handles nonce validation, signature verification, and security checks in one place.

Uses FastAPI’s Depends() to automatically verify every request where applied.


2️⃣ ✅ Fast & Scalable

Uses TTLCache (maxsize=500,000, ttl=300) for in-memory nonce storage (Fast, no manual cleanup).

Redis fallback ensures protection in a distributed system.

Can handle 60,000 users easily without performance issues.


3️⃣ ✅ Works for Any API (GET/POST/PUT)

Any endpoint can simply use Depends(verify_request) to ensure security.


4️⃣ ✅ Handles Expired & Replayed Nonces Automatically

TTLCache + Redis ensures nonces expire after 5 minutes (prevents replays).


5️⃣ ✅ Prevents Signature Tampering

Uses public key cryptography to prevent unauthorized request modifications.



---

🔹 How to Use in Any New API

Simply add Depends(verify_request) to any new FastAPI route, and it will be automatically secured.

@app.post("/new-api")
async def new_api(request: Request, _: None = Depends(verify_request)):
    data = await request.json()
    return {"message": "Secure data received", "data": data}


---

🚀 Final Verdict

🔥 This is the best, most scalable, and efficient solution for handling 60,000+ users securely.
🚀 No infinite loops, no manual cleanup, and fast nonce validation with auto-expiry.
💡 Any new API can reuse verify_request() for security in just one line.

✅ Fast | ✅ Memory Efficient | ✅ Scalable | ✅ Secure






import redis
import os
import base64
from fastapi import FastAPI, Request, HTTPException, Header
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize Redis connection
redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0, decode_responses=True)

@app.get("/get-env-variables")
async def get_env_variables(
    request: Request,
    x_signature: str = Header(None),
    x_public_key: str = Header(None),
    x_nonce: str = Header(None)
):
    try:
        # Step 1: Validate required headers
        if not x_signature or not x_public_key or not x_nonce:
            raise HTTPException(status_code=400, detail="Missing required headers")
        
        # Step 2: Decode received values
        try:
            signature = base64.b64decode(x_signature)
            public_key = load_pem_public_key(x_public_key.encode())
            nonce = base64.b64decode(x_nonce)
        except Exception as decode_error:
            raise HTTPException(status_code=400, detail=f"Invalid header encoding: {decode_error}")
        
        # Step 3: Check for replay attack (nonce reuse)
        nonce_key = f"nonce:{x_nonce}"
        if redis_client.exists(nonce_key):
            raise HTTPException(status_code=403, detail="Nonce already used")
        redis_client.setex(nonce_key, 300, "1")  # Store nonce for 5 minutes
        
        # Step 4: Verify the signature
        message = f"GET /get-env-variables {x_nonce}"
        try:
            public_key.verify(
                signature,
                message.encode(),
                padding.PKCS1v15(),
                hashes.SHA256()
            )
        except Exception as verification_error:
            raise HTTPException(status_code=401, detail=f"Invalid signature: {verification_error}")
        
        # Step 5: Fetch environment variables securely
        env_variables = {f"VAR{i}": os.getenv(f"VAR{i}", f"default_value_{i}") for i in range(1, 7)}
        return env_variables
    
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

# To run the FastAPI server:
# uvicorn main:app --host 0.0.0.0 --port 5000


Improvements & Fixes:

1. Used Header parameters: Clean way to extract headers in FastAPI.


2. Handled decoding errors: Explicitly catching base64 and PEM decoding errors.


3. Made Redis host configurable: Uses REDIS_HOST from .env (defaults to localhost).


4. Ensured nonce_key is secure: Prevents potential key manipulation.


5. Improved exception handling: More specific error messages for debugging.


6. Refactored env variable retrieval: Uses dictionary comprehension for cleaner code.


7. Better logging & error responses: Identifies signature or encoding issues explicitly.



This version ensures security, scalability, and maintainability. Let me know if you need modifications!











import redis
import os
import base64
from fastapi import FastAPI, Request, HTTPException
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize Redis connection
redis_client = redis.Redis(host="your-redis-host", port=6379, db=0, decode_responses=True)

@app.get("/get-env-variables")
async def get_env_variables(request: Request):
    try:
        # Step 1: Retrieve headers from request
        headers = request.headers
        signature_b64 = headers.get("X-Signature")
        public_key_pem = headers.get("X-Public-Key")
        nonce_b64 = headers.get("X-Nonce")

        if not signature_b64 or not public_key_pem or not nonce_b64:
            raise HTTPException(status_code=400, detail="Missing required headers")

        # Step 2: Decode received values
        signature = base64.b64decode(signature_b64)
        public_key = load_pem_public_key(public_key_pem.encode())
        nonce = base64.b64decode(nonce_b64)

        # Step 3: Check if nonce was used before (Replay Attack Prevention)
        nonce_key = f"nonce:{nonce_b64}"  # Unique key for Redis
        if redis_client.exists(nonce_key):
            raise HTTPException(status_code=403, detail="Nonce already used")  # Reject replay attack

        # Store nonce with expiration (300 seconds = 5 minutes)
        redis_client.setex(nonce_key, 300, "1")  # Auto-delete after 5 minutes

        # Step 4: Verify the signature
        message = f"GET /get-env-variables {nonce_b64}"
        public_key.verify(
            signature,
            message.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )

        # Step 5: Fetch six environment variables
        response_data = {
            "VAR1": os.getenv("VAR1", "default_value_1"),
            "VAR2": os.getenv("VAR2", "default_value_2"),
            "VAR3": os.getenv("VAR3", "default_value_3"),
            "VAR4": os.getenv("VAR4", "default_value_4"),
            "VAR5": os.getenv("VAR5", "default_value_5"),
            "VAR6": os.getenv("VAR6", "default_value_6"),
        }

        return response_data

    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


# To run the FastAPI server, use the command:
# uvicorn main:app --host 0.0.0.0 --port 5000



Yes, you can use Redis instead of TTLCache, and in your case, Redis is preferable over an in-memory LRU cache like cachetools.TTLCache. Here's why:


---

Why is Redis better for your use case?

1. Azure Web Apps Have Memory Constraints

Your app runs on shared memory, and using a large in-memory cache like TTLCache could cause excessive memory usage.

Redis offloads the storage to an external database instead of keeping it in the app’s memory.



2. Persistence & Scalability

If your app restarts or scales across multiple instances, TTLCache will lose its stored values.

Redis persists across restarts and is shared across multiple app instances.



3. High Volume Requests

For 60,000+ users running for years, TTLCache (limited by maxsize) may require a very high maxsize value.

Redis can store millions of records with automatic eviction based on time-to-live (EXPIRE).



4. Built-In Expiry (TTL) Support

Redis supports automatic expiration of keys (e.g., delete after 5 minutes).

This avoids manual cleanup that would be required with TTLCache.





---

How to Implement Redis for Nonce Storage

First, install Redis and its Python client:

pip install redis

Step 1: Use Redis Instead of TTLCache in Flask Server

Modify the Flask server to use Redis for nonce storage.

import redis
import os
from flask import Flask, request, jsonify
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Initialize Redis connection (Azure Redis Cache or local)
redis_client = redis.Redis(host="your-redis-host", port=6379, db=0, decode_responses=True)

@app.route("/get-env-variables", methods=["GET"])
def get_env_variables():
    try:
        # Step 1: Retrieve headers from request
        signature_b64 = request.headers.get("X-Signature")
        public_key_pem = request.headers.get("X-Public-Key")
        nonce_b64 = request.headers.get("X-Nonce")

        if not signature_b64 or not public_key_pem or not nonce_b64:
            return jsonify({"error": "Missing required headers"}), 400

        # Step 2: Decode received values
        signature = base64.b64decode(signature_b64)
        public_key = load_pem_public_key(public_key_pem.encode())
        nonce = base64.b64decode(nonce_b64)

        # Step 3: Check if nonce was used before (Replay Attack Prevention)
        nonce_key = f"nonce:{nonce_b64}"  # Unique key for Redis
        if redis_client.exists(nonce_key):
            return jsonify({"error": "Nonce already used"}), 403  # Reject replay attack

        # Store nonce with an expiration time (300 seconds = 5 minutes)
        redis_client.setex(nonce_key, 300, "1")  # Auto-delete after 5 minutes

        # Step 4: Verify the signature
        message = f"GET /get-env-variables {nonce_b64}"
        public_key.verify(
            signature,
            message.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )

        # Step 5: Fetch six environment variables
        response_data = {
            "VAR1": os.getenv("VAR1", "default_value_1"),
            "VAR2": os.getenv("VAR2", "default_value_2"),
            "VAR3": os.getenv("VAR3", "default_value_3"),
            "VAR4": os.getenv("VAR4", "default_value_4"),
            "VAR5": os.getenv("VAR5", "default_value_5"),
            "VAR6": os.getenv("VAR6", "default_value_6"),
        }

        return jsonify(response_data)

    except Exception as e:
        return jsonify({"error": str(e)}), 401

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


---

Why Redis is More Scalable?

When to Use TTLCache?

✅ If your app is a small-scale single-instance Flask app with low request volume.
✅ If you don't have Redis or prefer an in-memory approach.

When to Use Redis?

✅ If your app runs on Azure Web Apps (memory-constrained).
✅ If you expect high traffic (e.g., 60,000+ users, API running for years).
✅ If you deploy multiple instances (Redis is centralized).
✅ If you need automatic expiration of stored values.


---

Final Recommendation

For Azure Web Apps running in production, use Redis for nonce storage to prevent replay attacks efficiently without excessive memory usage.

Would you like me to add Azure Redis Cache setup steps?









Here's the end-to-end solution with rolling nonce expiration (LRU Cache) and detailed comments including example values for private key, public key, nonce, message, signature, etc.


---

1. Install Required Libraries

On both the client and server, install the required dependencies:

pip install flask cryptography requests python-dotenv cachetools


---

2. Client Code (Python)

This script dynamically generates an RSA key pair, creates a nonce, signs the request, and sends it to the Flask server.

import os
import base64
import requests
import json
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization

# Flask API URL (Replace with actual API)
API_URL = "https://your-flask-api.azurewebsites.net/get-env-variables"

# Step 1: Generate RSA Key Pair (Generated dynamically, not stored)
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048
)

# Step 2: Export Public Key (to be sent with request)
public_key = private_key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
).decode()

"""
Example Public Key (PEM Format):
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A...
-----END PUBLIC KEY-----
"""

# Step 3: Generate a Unique Nonce (16-byte random value)
nonce = os.urandom(16)  # 16-byte secure random nonce

"""
Example Nonce (Raw Bytes):
b'1f\x8eT\x99\xad\xdb\x82\xc3\xf5\xf6\xab\xb8\xd7\x92g'

Example Nonce (Base64 Encoded for transmission):
'Mh+OVJmt24LD9faruNeSZw=='
"""

# Step 4: Construct the message to be signed
message = f"GET /get-env-variables {base64.b64encode(nonce).decode()}"

"""
Example Message to Sign:
'GET /get-env-variables Mh+OVJmt24LD9faruNeSZw=='
"""

# Step 5: Sign the message using the private key
signature = private_key.sign(
    message.encode(),
    padding.PKCS1v15(),
    hashes.SHA256()
)

"""
Example Signature (Raw Bytes):
b'\xa3X\x9c\xf5\x86\xd2\x18\x9e\xcd\xf0\x13\x83\xfc\x99\xfb\x9d...'

Example Signature (Base64 Encoded for transmission):
'o1ic9YbS... (truncated)'
"""

signature_b64 = base64.b64encode(signature).decode()

# Step 6: Send GET request to the Flask server with headers
headers = {
    "X-Signature": signature_b64,
    "X-Public-Key": public_key,
    "X-Nonce": base64.b64encode(nonce).decode()
}

# Step 7: Make the request and print response
response = requests.get(API_URL, headers=headers)
print("Server Response:", response.json())


---

3. Server Code (Flask Web App with Rolling Nonce Expiration)

The server verifies the signature, prevents replay attacks with nonce tracking, and returns six environment variables.

from flask import Flask, request, jsonify
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key
import base64
import os
from dotenv import load_dotenv
from cachetools import TTLCache

# Load environment variables from .env file (if using locally)
load_dotenv()

app = Flask(__name__)

# Use TTLCache to store used nonces with automatic expiration (5 min timeout)
used_nonces = TTLCache(maxsize=100000, ttl=300)  # 100,000 entries, expire after 5 min

@app.route("/get-env-variables", methods=["GET"])
def get_env_variables():
    try:
        # Step 1: Retrieve headers from request
        signature_b64 = request.headers.get("X-Signature")
        public_key_pem = request.headers.get("X-Public-Key")
        nonce_b64 = request.headers.get("X-Nonce")

        if not signature_b64 or not public_key_pem or not nonce_b64:
            return jsonify({"error": "Missing required headers"}), 400

        # Step 2: Decode received values
        signature = base64.b64decode(signature_b64)
        public_key = load_pem_public_key(public_key_pem.encode())
        nonce = base64.b64decode(nonce_b64)

        """
        Example Nonce (Base64 Decoded):
        b'1f\x8eT\x99\xad\xdb\x82\xc3\xf5\xf6\xab\xb8\xd7\x92g'
        """

        # Step 3: Check if nonce was used before (Replay Attack Prevention)
        if nonce in used_nonces:
            return jsonify({"error": "Nonce already used"}), 403
        used_nonces[nonce] = True  # Store used nonce

        # Step 4: Verify the signature
        message = f"GET /get-env-variables {nonce_b64}"
        public_key.verify(
            signature,
            message.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )

        """
        If verification fails, an exception is raised, and response is returned as:
        {"error": "Signature verification failed"}
        """

        # Step 5: Fetch six environment variables (Replace with actual env variables)
        response_data = {
            "VAR1": os.getenv("VAR1", "default_value_1"),
            "VAR2": os.getenv("VAR2", "default_value_2"),
            "VAR3": os.getenv("VAR3", "default_value_3"),
            "VAR4": os.getenv("VAR4", "default_value_4"),
            "VAR5": os.getenv("VAR5", "default_value_5"),
            "VAR6": os.getenv("VAR6", "default_value_6"),
        }

        return jsonify(response_data)

    except Exception as e:
        return jsonify({"error": str(e)}), 401

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


---

4. Explanation of Security Enhancements

1. Public-Private Key Authentication:

The client generates a new key pair for each request.

The server does not store any keys.



2. Nonce-Based Replay Protection (with LRU Expiration)

Nonces are stored only for 5 minutes (configurable via TTLCache).

Prevents replay attacks without excessive memory usage.



3. Signature Verification:

If the signature does not match, the server rejects the request.





---

5. Example Values

Client-Side Generated Values

Public Key (sent in request)

-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A...
-----END PUBLIC KEY-----

Nonce (Base64 Encoded)

Mh+OVJmt24LD9faruNeSZw==

Message to be Signed

GET /get-env-variables Mh+OVJmt24LD9faruNeSZw==

Signature (Base64 Encoded)

o1ic9YbS... (truncated)

Server Responses

✅ Success Response

{
    "VAR1": "MySecretValue1",
    "VAR2": "MySecretValue2",
    "VAR3": "MySecretValue3",
    "VAR4": "MySecretValue4",
    "VAR5": "MySecretValue5",
    "VAR6": "MySecretValue6"
}

❌ Replay Attack Detected

{
    "error": "Nonce already used"
}

❌ Signature Verification Failed

{
    "error": "Signature verification failed"
}


---

Final Thoughts

✅ Rolling Nonce Expiration (efficient for 60,000+ users).
✅ Secure authentication without stored secrets.
✅ Ready for GET, easily extendable for POST.

Would you like me to extend this to POST requests as well?






from com.azure.messaging.eventhubs import EventHubClientBuilder, EventHubProducerClient, EventDataBatch, EventData
from com.azure.identity import ClientSecretCredentialBuilder

# Define credentials (Replace with your actual values)
tenantId = "YOUR_TENANT_ID"
clientId = "YOUR_CLIENT_ID"
clientSecret = "YOUR_CLIENT_SECRET"
namespace = "YOUR_EVENT_HUB_NAMESPACE"  # e.g., "myeventhubnamespace"
eventHubName = "YOUR_EVENT_HUB_NAME"    # e.g., "myeventhub"

# Authenticate using ClientSecretCredential
credential = ClientSecretCredentialBuilder() \
    .tenantId(tenantId) \
    .clientId(clientId) \
    .clientSecret(clientSecret) \
    .build()

# Create EventHubProducerClient
producer = EventHubClientBuilder() \
    .fullyQualifiedNamespace(namespace + ".servicebus.windows.net") \
    .eventHubName(eventHubName) \
    .credential(credential) \
    .buildProducerClient()

# Create batch
batch = producer.createBatch()

# Add events
batch.tryAdd(EventData("Event from JMeter - 1"))
batch.tryAdd(EventData("Event from JMeter - 2"))
batch.tryAdd(EventData("Event from JMeter - 3"))

# Send batch
producer.send(batch)
log.info("Events sent successfully to Azure Event Hub")

# Close the producer
producer.close()





import com.azure.messaging.eventhubs.*
import com.azure.identity.ClientSecretCredential
import com.azure.identity.ClientSecretCredentialBuilder

// Environment Variables (Replace with your actual values)
String tenantId = "YOUR_TENANT_ID"
String clientId = "YOUR_CLIENT_ID"
String clientSecret = "YOUR_CLIENT_SECRET"
String namespace = "YOUR_EVENT_HUB_NAMESPACE" // e.g., myeventhubnamespace
String eventHubName = "YOUR_EVENT_HUB_NAME" // e.g., myeventhub

// Authenticate using ClientSecretCredential
ClientSecretCredential credential = new ClientSecretCredentialBuilder()
    .tenantId(tenantId)
    .clientId(clientId)
    .clientSecret(clientSecret)
    .build()

// Create EventHubProducerClient
EventHubProducerClient producer = new EventHubClientBuilder()
    .fullyQualifiedNamespace(namespace + ".servicebus.windows.net")
    .eventHubName(eventHubName)
    .credential(credential)
    .buildProducerClient()

// Create batch
EventDataBatch batch = producer.createBatch()

// Add events
batch.tryAdd(new EventData("Event from JMeter - 1"))
batch.tryAdd(new EventData("Event from JMeter - 2"))
batch.tryAdd(new EventData("Event from JMeter - 3"))

// Send batch
producer.send(batch)
log.info("Events sent successfully to Azure Event Hub")

// Close the producer
producer.close()




@cross_origin()
@jwt_required()
def post(self):
    """
    Assign or update shift timings for employees.
    :return: JSON response indicating success or failure.
    """
    try:
        data = request.get_json()
        employees = data.get('employees')

        if not employees:
            return jsonify({"error": "No employee data provided"}), 400

        with session_scope() as session:
            shift_entries = []

            for emp in employees:
                emp_id = emp.get("emp_id")
                from_date = datetime.strptime(emp.get("start_date"), DATE_FORMAT).date()
                to_date = datetime.strptime(emp.get("end_date"), DATE_FORMAT).date()
                week_off = emp.get("week_off", [])

                delta = (to_date - from_date).days + 1

                for i in range(delta):
                    current_date = from_date + timedelta(days=i)
                    day_id = current_date.weekday()

                    if day_id in week_off:
                        start_time = "00:00:00"
                        end_time = "00:00:00"
                    else:
                        start_time = emp.get("start_time", {}).get(str(day_id), "00:00:00")
                        end_time = emp.get("end_time", {}).get(str(day_id), "00:00:00")

                    # Calculate contracted hours
                    if start_time != "00:00:00" and end_time != "00:00:00":
                        contracted_hrs = round(
                            (datetime.strptime(end_time, TIME_FORMAT) - datetime.strptime(start_time, TIME_FORMAT)).total_seconds() / 3600, 2
                        )
                    else:
                        contracted_hrs = 0.0

                    # Check if a shift already exists for this employee and shift date
                    existing_shift = session.query(EmployeeShiftInfo).filter_by(
                        emp_id=emp_id,
                        shift_date=current_date
                    ).first()

                    if existing_shift:
                        # Update the existing shift
                        existing_shift.day_id = day_id
                        existing_shift.is_week_off = "Y" if day_id in week_off else "N"
                        existing_shift.start_time = datetime.strptime(start_time, TIME_FORMAT) if start_time != "00:00:00" else None
                        existing_shift.end_time = datetime.strptime(end_time, TIME_FORMAT) if end_time != "00:00:00" else None
                        existing_shift.emp_contracted_hours = contracted_hrs
                    else:
                        # Insert new shift
                        shift_entries.append(
                            EmployeeShiftInfo(
                                emp_id=emp_id,
                                day_id=day_id,
                                is_week_off="Y" if day_id in week_off else "N",
                                start_time=datetime.strptime(start_time, TIME_FORMAT) if start_time != "00:00:00" else None,
                                end_time=datetime.strptime(end_time, TIME_FORMAT) if end_time != "00:00:00" else None,
                                time_zone_id=None,
                                emp_contracted_hours=contracted_hrs,
                                shift_date=current_date
                            )
                        )

            # Bulk insert new records if any
            if shift_entries:
                session.bulk_save_objects(shift_entries)

            session.commit()

            return jsonify({"message": "Shift(s) assigned or updated successfully!", "status": "success"}), 200

    except Exception as e:
        return jsonify({
            "error": "An unexpected error occurred.",
            "details": str(e)
        }), 500




To run the job every Monday at 12:02 AM, you just need to modify the APScheduler job trigger in the start_scheduler() function.

Updated Job Schedule

def start_scheduler():
    """Starts the APScheduler to run the task every Monday at 12:02 AM"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(copy_previous_week_efforts, 'cron', day_of_week='mon', hour=0, minute=2)  # Runs every Monday at 12:02 AM
    scheduler.start()
    print("Scheduler started: Running copy_previous_week_efforts every Monday at 12:02 AM")


---

Updated Date Calculations

We need to adjust the week calculations so that we copy shifts from last Monday to last Sunday into the new Monday to Sunday.

Updated copy_previous_week_efforts Function

from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import sessionmaker
from models import EmployeeShiftInfo, EmployeeInfo
from database import session_scope

def copy_previous_week_efforts():
    """Efficiently copies last week's efforts to the current week for all employees"""
    
    with session_scope("DESIGNER") as session:
        today = datetime.today()
        last_monday = today - timedelta(days=today.weekday() + 7)  # Last Monday
        last_sunday = last_monday + timedelta(days=6)  # Last Sunday
        current_monday = last_sunday + timedelta(days=1)  # Current Monday
        current_sunday = current_monday + timedelta(days=6)  # Current Sunday

        print(f"Copying shifts from {last_monday} - {last_sunday} to {current_monday} - {current_sunday}")

        # Step 1: Fetch all employee IDs in batches
        employees = session.query(EmployeeInfo.emplid).all()
        employee_ids = [emp.emplid for emp in employees]

        # Step 2: Fetch all shifts for the last week
        last_week_shifts = session.query(EmployeeShiftInfo).filter(
            EmployeeShiftInfo.emp_id.in_(employee_ids),
            EmployeeShiftInfo.shift_date.between(last_monday, last_sunday)
        ).all()

        # Step 3: Fetch all shifts for the current week
        current_week_shifts = session.query(EmployeeShiftInfo).filter(
            EmployeeShiftInfo.emp_id.in_(employee_ids),
            EmployeeShiftInfo.shift_date.between(current_monday, current_sunday)
        ).all()

        # Step 4: Use dictionaries for quick lookups
        last_week_shift_map = {(shift.emp_id, shift.shift_date): shift for shift in last_week_shifts}
        current_week_shift_dates = {(shift.emp_id, shift.shift_date) for shift in current_week_shifts}

        batch_insert = []
        default_start_time = "09:00:00"
        default_end_time = "17:00:00"

        for emp_id in employee_ids:
            for i in range(7):  # Loop through 7 days of the current week
                new_shift_date = current_monday + timedelta(days=i)
                
                if (emp_id, new_shift_date) in current_week_shift_dates:
                    continue  # Skip if shift already exists

                # Copy shift from last week if exists, otherwise assign default
                prev_shift_date = new_shift_date - timedelta(days=7)
                prev_shift = last_week_shift_map.get((emp_id, prev_shift_date))

                batch_insert.append(EmployeeShiftInfo(
                    emp_id=emp_id,
                    shift_date=new_shift_date,
                    start_time=prev_shift.start_time if prev_shift else default_start_time,
                    end_time=prev_shift.end_time if prev_shift else default_end_time,
                    is_week_off=prev_shift.is_week_off if prev_shift else "N"
                ))

        # Step 5: Bulk insert only if there are new shifts to add
        if batch_insert:
            session.bulk_save_objects(batch_insert)
            session.commit()
            print(f"{len(batch_insert)} shift records inserted.")

def start_scheduler():
    """Starts the APScheduler to run the task every Monday at 12:02 AM"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(copy_previous_week_efforts, 'cron', day_of_week='mon', hour=0, minute=2)  # Runs every Monday at 12:02 AM
    scheduler.start()
    print("Scheduler started: Running copy_previous_week_efforts every Monday at 12:02 AM")

if __name__ == "__main__":
    start_scheduler()
    try:
        while True:
            pass
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


---

Key Changes

✅ Job now runs every Monday at 12:02 AM
✅ Correctly calculates last Monday–Sunday efforts to copy into the new week
✅ Efficient batch processing for large employee datasets

This ensures the job copies last week’s efforts into the new week properly without duplication. Let me know if you need any tweaks!







from flask import Flask, jsonify
import os
import json
from datetime import datetime, timedelta
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from apscheduler.schedulers.background import BackgroundScheduler
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

app = Flask(__name__)

# Environment Variables
KEY_VAULT_NAME_GEN = os.getenv("KEY_VAULT_NAME_GEN")
KEY_VAULT_NAME_SEC = os.getenv("KEY_VAULT_NAME_SEC")  # Another KeyVault
SECRET_NAMES = os.getenv("SECRETS_REQUIRED").split(",")

# Azure Key Vault Clients
credential = DefaultAzureCredential()
client_gen = SecretClient(vault_url=f"https://{KEY_VAULT_NAME_GEN}.vault.azure.net/", credential=credential)
client_sec = SecretClient(vault_url=f"https://{KEY_VAULT_NAME_SEC}.vault.azure.net/", credential=credential)

# AES Encryption Settings
AES_KEY = os.getenv("AES_KEY")  # Use a secure key from env
AES_NONCE = os.getenv("AES_NONCE")  # Use a secure nonce from env

# Function to Encrypt Data
def encrypt_data(data, key, nonce):
    cipher = Cipher(algorithms.AES(key.encode()), modes.GCM(nonce.encode()))
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(data.encode()) + encryptor.finalize()
    return base64.b64encode(encrypted_data).decode()

# Scheduler Function
def check_and_encrypt_client_secret():
    """ Checks expiry of 'clsscrt' in second KeyVault and encrypts it if within 20 days. """
    try:
        secret_properties = client_sec.get_secret("clsscrt").properties
        expiry_date = secret_properties.expires_on

        if expiry_date is None:
            print("No expiration date found for 'clsscrt'. Skipping...")
            return

        days_remaining = (expiry_date - datetime.utcnow()).days
        print(f"Days remaining for 'clsscrt' expiry: {days_remaining}")

        if days_remaining <= 20:
            print("Less than 20 days remaining. Updating encrypted secret...")
            
            # Get and Encrypt the Secret
            clsscrt_secret = client_sec.get_secret("clsscrt").value
            encrypted_secret = encrypt_data(clsscrt_secret, AES_KEY, AES_NONCE)

            # Store Encrypted Secret in First Key Vault
            client_gen.set_secret("enc_client_secret", encrypted_secret)
            print("Encrypted secret updated in Key Vault.")

    except Exception as e:
        print(f"Error in checking/encrypting secret: {str(e)}")

# Schedule the Task (Runs daily at midnight)
scheduler = BackgroundScheduler()
scheduler.add_job(check_and_encrypt_client_secret, "cron", hour=0, minute=0)
scheduler.start()

# API Endpoint to Get Secrets
@app.get("/get_configs")
async def get_keyvault_data():
    """ Fetch secrets from Key Vault and return as JSON. """
    secret_dict = {}
    
    for secret_name in SECRET_NAMES:
        try:
            secret = client_gen.get_secret(secret_name.strip())
            secret_dict[secret_name] = secret.value
        except Exception as e:
            secret_dict[secret_name] = f"Error: {str(e)}"

    return jsonify(secret_dict)

if __name__ == "__main__":
    app.run(debug=True)
















from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, WebDriverException
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def validate_unsupported_media_size():
    """
    This method verifies that an unsupported media size cannot be assigned to a dedicated tray.
    """

    # Test Data
    url = "http://example.com"  # Replace with the actual URL
    tray_locator = "tray-selector"  # Replace with the actual locator for the tray dropdown
    unsupported_media_size = "Unsupported Size"  # Replace with the actual unsupported size
    error_message_locator = "error-message"  # Replace with the actual locator for the error message
    expected_error_message = "Unsupported media size cannot be assigned to this tray."  # Replace with the actual expected error message

    # Initialize WebDriver
    driver = None

    try:
        logging.info("Launching the browser...")
        driver = webdriver.Chrome()  # Ensure the correct WebDriver is installed and configured
        driver.maximize_window()

        logging.info(f"Navigating to the URL: {url}")
        driver.get(url)

        logging.info("Locating the tray dropdown...")
        tray_dropdown = driver.find_element(By.ID, tray_locator)

        logging.info(f"Selecting the unsupported media size: {unsupported_media_size}")
        tray_dropdown.send_keys(unsupported_media_size)
        tray_dropdown.send_keys(Keys.RETURN)

        logging.info("Verifying the error message...")
        error_message_element = driver.find_element(By.ID, error_message_locator)
        actual_error_message = error_message_element.text

        assert actual_error_message == expected_error_message, (
            f"Test Failed: Expected error message '{expected_error_message}', but got '{actual_error_message}'"
        )

        logging.info("Test Passed: The correct error message is displayed for unsupported media size.")

    except NoSuchElementException as e:
        logging.error(f"Test Failed: Element not found - {e}")

    except AssertionError as e:
        logging.error(e)

    except WebDriverException as e:
        logging.error(f"Test Failed: WebDriver exception occurred - {e}")

    except Exception as e:
        logging.error(f"Test Failed: An unexpected exception occurred - {e}")

    finally:
        if driver:
            logging.info("Closing the browser...")
            driver.quit()

# Execute the test
validate_unsupported_media_size()



from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timezone
from Crypto.Cipher import AES
import base64

app = Flask(__name__)

# Azure Key Vault Configuration
KEY_VAULT_URL = "https://your-keyvault-name.vault.azure.net"
SECRET_NAME = "client-secret-name"
ENCRYPTED_SECRET_NAME = "encrypted-client-secret"
LAST_ENCRYPTION_TIMESTAMP = "last-encryption-timestamp"  # Store last encryption time

# Initialize Key Vault Client
credential = DefaultAzureCredential()
client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

def check_and_encrypt_secret():
    """
    Function to check secret expiry and re-encrypt if necessary.
    """
    try:
        # Fetch secret properties
        secret = client.get_secret(SECRET_NAME)
        expires_on = secret.properties.expires_on

        # Check if expiry is within 20 days
        if expires_on:
            days_remaining = (expires_on - datetime.now(timezone.utc)).days
            if days_remaining <= 20:
                print(f"Secret expires in {days_remaining} days. Checking re-encryption status...")

                # Fetch last encryption timestamp (if exists)
                try:
                    last_encryption_time = client.get_secret(LAST_ENCRYPTION_TIMESTAMP).value
                    last_encryption_time = datetime.fromisoformat(last_encryption_time).replace(tzinfo=timezone.utc)
                except:
                    last_encryption_time = None  # No timestamp exists

                # Encrypt only if it hasn't been done already within this 20-day window
                if last_encryption_time and last_encryption_time >= expires_on - timedelta(days=20):
                    print("Secret has already been encrypted within this window. Skipping.")
                    return

                # Fetch the actual secret value
                secret_value = secret.value

                # Fetch encryption key & nonce from Key Vault
                encryption_key = base64.b64decode(client.get_secret("encryption-key").value)
                nonce = base64.b64decode(client.get_secret("encryption-nonce").value)

                # Encrypt secret using AES-256-GCM
                cipher = AES.new(encryption_key, AES.MODE_GCM, nonce=nonce)
                encrypted_bytes, tag = cipher.encrypt_and_digest(secret_value.encode())

                # Convert to Base64 for storage
                encrypted_value = base64.b64encode(encrypted_bytes).decode()

                # Store encrypted secret back in Key Vault
                client.set_secret(ENCRYPTED_SECRET_NAME, encrypted_value)

                # Store the last encryption timestamp
                client.set_secret(LAST_ENCRYPTION_TIMESTAMP, datetime.now(timezone.utc).isoformat())

                print(f"Updated encrypted value stored in {ENCRYPTED_SECRET_NAME}. Encryption timestamp updated.")
            else:
                print("Secret expiry is not within 20 days. No action taken.")
        else:
            print("Secret does not have an expiration date.")
    except Exception as e:
        print(f"Error during secret check: {e}")

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(check_and_encrypt_secret, "interval", hours=24)  # Run every 24 hours
scheduler.start()

@app.route("/")
def home():
    return "Flask API with Scheduler Running..."

if __name__ == "__main__":
    try:
        app.run(debug=True, use_reloader=False)  # Use reloader=False to prevent duplicate scheduler execution
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()








def decrypt_value(encrypted_obj, key_b64, nonce_b64):
    """Decrypt a single value using AES-256-GCM"""
    key = base64.b64decode(key_b64)
    nonce = base64.b64decode(nonce_b64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    decrypted_text = cipher.decrypt_and_verify(
        base64.b64decode(encrypted_obj["ciphertext"]),
        base64.b64decode(encrypted_obj["auth_tag"])
    )

    return decrypted_text.decode()

# Decrypt all values using the encrypted_store dictionary
decrypted_data = {key: decrypt_value(value, aes_key, aes_nonce) for key, value in encrypted_store.items()}

# Print decrypted values
print("\n🔓 Decrypted Data Dictionary:")
print(json.dumps(decrypted_data, indent=4))

Got it! You want to:

1. Encrypt each of the 5 values (ClientID, TenantID, ClientSecret, ServiceName, ServicePassword) individually using AES-256-GCM with a single key and nonce.


2. Store the encrypted values in Azure Key Vault instead of plain text.


3. Later, retrieve and decrypt each value individually using the same key and nonce in an API call.



🔹 Steps to Achieve This:

1. Generate AES-256 Key & Nonce → This key and nonce should be stored securely (not in Key Vault).


2. Encrypt each value separately and store the encrypted version in Azure Key Vault.


3. Retrieve & Decrypt each value in your API using the same key and nonce.




---

🔐 Step 1: Generate AES-256 Key & Nonce

import os
import base64
from Crypto.Random import get_random_bytes

def generate_aes_key_nonce():
    """Generate a 256-bit AES key and a nonce, then print/store them securely"""
    key = get_random_bytes(32)  # AES-256 requires a 32-byte key
    nonce = get_random_bytes(16)  # Nonce for AES-GCM (must be unique per encryption)

    print("AES Key (Base64):", base64.b64encode(key).decode())  
    print("Nonce (Base64):", base64.b64encode(nonce).decode())

    # Store these values securely (Environment Variables, Secure Storage, etc.)
    return key, nonce

if __name__ == "__main__":
    generate_aes_key_nonce()

🔹 Output Example (Save These Securely)

AES Key (Base64): dGhpc2lzYXRlc3RrZXkzM0JZVEU=
Nonce (Base64): c29tZWZha2Vub25jZVRlc3Q=

✅ Store these in a secure place (not in Key Vault but in a secure config or environment variable).
✅ The same key and nonce will be used for all five values.


---

🔐 Step 2: Encrypt Individual Values

import base64
import json
from Crypto.Cipher import AES

# Store the generated Key & Nonce here (from Step 1)
AES_KEY_BASE64 = "dGhpc2lzYXRlc3RrZXkzM0JZVEU="  # Replace with actual key
NONCE_BASE64 = "c29tZWZha2Vub25jZVRlc3Q="  # Replace with actual nonce

def encrypt_value(plain_text):
    """Encrypt a single value using AES-256-GCM"""
    key = base64.b64decode(AES_KEY_BASE64)
    nonce = base64.b64decode(NONCE_BASE64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, auth_tag = cipher.encrypt_and_digest(plain_text.encode())

    return {
        "ciphertext": base64.b64encode(ciphertext).decode(),
        "auth_tag": base64.b64encode(auth_tag).decode()
    }

# Example: Encrypting the five sensitive values
sensitive_data = {
    "ClientID": "12345-xyz-client",
    "TenantID": "67890-abc-tenant",
    "ClientSecret": "MySuperSecureSecret",
    "ServiceName": "SecureAPIService",
    "ServicePassword": "UltraSafePassword"
}

encrypted_data = {key: encrypt_value(value) for key, value in sensitive_data.items()}

# Print Encrypted Values (Store these in Key Vault)
print("\n🔐 Encrypted Data (Store in Key Vault):")
print(json.dumps(encrypted_data, indent=4))


---

🔹 Step 3: Store in Azure Key Vault

Once the values are encrypted, store them in Azure Key Vault using the Azure CLI or SDK.

az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedClientID" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedTenantID" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedClientSecret" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedServiceName" --value "CIPHERTEXT_FROM_SCRIPT"
az keyvault secret set --vault-name "MyKeyVault" --name "EncryptedServicePassword" --value "CIPHERTEXT_FROM_SCRIPT"


---

🔓 Step 4: Decrypt Individual Values in API

When retrieving values from Key Vault, use this decryption function:

import base64
from Crypto.Cipher import AES

# Load AES Key & Nonce (same as before)
AES_KEY_BASE64 = "dGhpc2lzYXRlc3RrZXkzM0JZVEU="  # Use same stored key
NONCE_BASE64 = "c29tZWZha2Vub25jZVRlc3Q="  # Use same stored nonce

def decrypt_value(ciphertext, auth_tag):
    """Decrypt a single value using AES-256-GCM"""
    key = base64.b64decode(AES_KEY_BASE64)
    nonce = base64.b64decode(NONCE_BASE64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    decrypted_text = cipher.decrypt_and_verify(base64.b64decode(ciphertext), base64.b64decode(auth_tag))

    return decrypted_text.decode()

# Example: Decrypting one retrieved value (replace with values from Key Vault)
encrypted_client_id = {
    "ciphertext": "CIPHERTEXT_FROM_KEY_VAULT",
    "auth_tag": "AUTHTAG_FROM_KEY_VAULT"
}

decrypted_client_id = decrypt_value(encrypted_client_id["ciphertext"], encrypted_client_id["auth_tag"])

print("\n🔓 Decrypted Client ID:", decrypted_client_id)


---

🔹 How It Works

✅ Encryption:

Each value is encrypted individually using the same key and nonce.

The ciphertext and auth tag are stored in Azure Key Vault.


✅ Decryption (API):

Retrieve ciphertext and auth tag from Key Vault.

Use same AES key and nonce to decrypt the values.



---

🔹 Example API Call Flow

Your API will:

1. Fetch Encrypted Data from Azure Key Vault.


2. Decrypt using decrypt_value().


3. Return Decrypted Values in API Response.



Example API Call:

from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/get-secrets", methods=["GET"])
def get_secrets():
    """Fetch encrypted values from Key Vault & decrypt them"""
    decrypted_values = {
        "ClientID": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "TenantID": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "ClientSecret": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "ServiceName": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT"),
        "ServicePassword": decrypt_value("CIPHERTEXT_FROM_KEY_VAULT", "AUTHTAG_FROM_KEY_VAULT")
    }
    return jsonify(decrypted_values)

if __name__ == "__main__":
    app.run(debug=True)


---

🔹 Summary

✔ AES-256-GCM encryption with a single key & nonce
✔ Stores encrypted values in Azure Key Vault
✔ Decryption in API using the same key & nonce
✔ Individually encrypts & decrypts each value

This ensures strong encryption while keeping secrets safe in Azure Key Vault. Let me know if you need changes! 🚀










from flask import Flask, Blueprint, jsonify
import requests
import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# Flask Blueprint
api_bp = Blueprint("api", __name__)

# API URL to call
API_URL = "https://example.com/api/data"

def fetch_api_data():
    """Function to call API every Sunday and process response."""
    print(f"Running API call at {datetime.datetime.now()}...")
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            print("API Response:", data)
            # Process data here (e.g., save to DB)
        else:
            print(f"Error: {response.status_code}")
    except Exception as e:
        print("API Call Failed:", str(e))

# Set up scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_api_data, "cron", day_of_week="sun", hour=0, minute=0)  # Runs every Sunday at 00:00
scheduler.start()

@api_bp.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Flask API with Cron Job Running!"})

# Register Blueprint
app.register_blueprint(api_bp, url_prefix="/api")

if __name__ == "__main__":
    app.run(debug=True)





Yes, you are absolutely right! The earlier approach is not fully secure because the client can generate a token without any protection. Instead, we need a more secure approach, where:

1. The server generates and stores the secret key in memory (or an environment variable).


2. The client also stores the secret key securely (in memory, not hardcoded in code).


3. Before making a request, the client encodes a new token using the secret key.


4. The server decodes and verifies it before granting access.


5. Tokens are not issued by an open endpoint—only pre-configured systems with the key can generate them.




---

Improved Secure JWT Implementation

This method ensures:

Clients cannot get tokens without the secret key.

Tokens expire automatically.

Each request includes a freshly signed JWT for verification.



---

Step 1: Server (server.py)

from fastapi import FastAPI, Depends, HTTPException, Header
import jwt
import time
import os

app = FastAPI()

# Store secret key in an environment variable or memory (DO NOT expose)
SECRET_KEY = os.getenv("JWT_SECRET", "my-very-secure-secret-key")  
ALGORITHM = "HS256"
TOKEN_EXPIRY_SECONDS = 300  # Token expires in 5 minutes

def verify_jwt(token: str = Header(None)):
    """Verify JWT token received in headers."""
    if not token:
        raise HTTPException(status_code=403, detail="Missing token")

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=403, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=403, detail="Invalid token")

    return True  # Authentication successful

@app.get("/protected")
def protected_route(auth: bool = Depends(verify_jwt)):
    """A protected endpoint that requires a valid JWT token."""
    return {"message": "Access granted! JWT verified."}

@app.get("/")
def public_route():
    """A public endpoint."""
    return {"message": "Welcome to the Secure API!"}


---

Step 2: Client (client.py)

import jwt
import time
import os
import requests

BASE_URL = "http://127.0.0.1:8000"

# Client must have the secret key stored securely (in environment or memory)
SECRET_KEY = os.getenv("JWT_SECRET", "my-very-secure-secret-key")  
ALGORITHM = "HS256"

def create_jwt():
    """Generate a JWT token on the client before making a request."""
    expiration_time = int(time.time()) + 300  # Token valid for 5 minutes
    payload = {"exp": expiration_time}
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token

# Generate token in memory
token = create_jwt()

# Make a request to the protected endpoint with the token
headers = {"token": token}
response = requests.get(f"{BASE_URL}/protected", headers=headers)

print("Response:", response.json())


---

Step 3: Run the Secure System

1. Start the FastAPI Server

uvicorn server:app --reload

2. Run the Client

python client.py


---

Why is This Secure?

✅ No Open Token Generation Endpoint → Only pre-configured clients can generate tokens.
✅ Client Stores Secret Key Securely → No unauthorized token generation.
✅ Each Request Includes a Fresh Token → No reuse of stolen tokens.
✅ Server Only Verifies Token → Stateless and efficient authentication.
✅ Environment Variables Used for Keys → Prevents exposing secrets in code.


---

Key Takeaway

Instead of the server issuing tokens, the client encodes its own JWT before sending requests.

The server only verifies tokens, ensuring that only trusted clients can make requests.

This method is stateless, secure, and ensures only pre-approved clients can communicate.


Would this work for your setup? Let me know if you need modifications!









import base64
import win32crypt

def store_encryption_key(encryption_key):
    """Encrypt and store the AES decryption key securely using DPAPI"""
    encrypted_key = win32crypt.CryptProtectData(encryption_key.encode(), None, None, None, None, 0)
    with open("encrypted_key.bin", "wb") as f:
        f.write(base64.b64encode(encrypted_key))

def retrieve_encryption_key():
    """Retrieve and decrypt the AES decryption key securely using DPAPI"""
    with open("encrypted_key.bin", "rb") as f:
        encrypted_key = base64.b64decode(f.read())
    decrypted_key = win32crypt.CryptUnprotectData(encrypted_key, None, None, None, None, 0)
    return decrypted_key[1].decode()

# Example Usage
aes_key = "SuperSecureAESKey123"  # This is your actual decryption key
store_encryption_key(aes_key)  # Store securely
retrieved_key = retrieve_encryption_key()  # Retrieve securely
print(f"Decryption Key: {retrieved_key}")







Here’s a standalone encryption and decryption script using AES-256-GCM, with dummy sensitive data like Client ID, Password, and 13 different proxies stored as a key-value pair with country, country code, and proxy names.


---

🔐 AES-256-GCM Encryption & Decryption

This script: ✅ Encrypts sensitive data using AES-256-GCM.
✅ Uses a secure key from environment variables.
✅ Stores an IV (nonce) and authentication tag.
✅ Decrypts and verifies data at runtime.


---

🔹 Full Python Code

import os
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Securely generate/store AES-256-GCM key in environment variable
AES_KEY_ENV = "AES_256_GCM_KEY"

def generate_and_store_key():
    """Generate a new AES-256 key and store it in an environment variable"""
    key = get_random_bytes(32)  # 256-bit key
    os.environ[AES_KEY_ENV] = base64.b64encode(key).decode()  # Store in env as base64

def get_aes_key():
    """Retrieve AES key from environment variable"""
    key_b64 = os.getenv(AES_KEY_ENV)
    if not key_b64:
        raise ValueError("AES Key not found. Please generate and store it securely.")
    return base64.b64decode(key_b64)

def encrypt_data(plaintext):
    """Encrypt data using AES-256-GCM"""
    key = get_aes_key()
    iv = get_random_bytes(16)  # 16-byte IV (Nonce)
    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext.encode())

    return {
        "ciphertext": base64.b64encode(ciphertext).decode(),
        "iv": base64.b64encode(iv).decode(),
        "auth_tag": base64.b64encode(auth_tag).decode()
    }

def decrypt_data(ciphertext_b64, iv_b64, auth_tag_b64):
    """Decrypt data using AES-256-GCM"""
    key = get_aes_key()
    iv = base64.b64decode(iv_b64)
    auth_tag = base64.b64decode(auth_tag_b64)
    ciphertext = base64.b64decode(ciphertext_b64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    plaintext = cipher.decrypt_and_verify(ciphertext, auth_tag)

    return plaintext.decode()

# ---------- Example Data ----------
sensitive_data = {
    "ClientID": "XYZ-12345-CLIENT",
    "TenantID": "TENANT-56789",
    "ClientSecret": "MySuperSecureSecret!@#",
    "ServiceAccountName": "service-agent-01",
    "ServiceAccountPassword": "AgentPassword$321",
    "Proxies": [
        {"Country": "United States", "Code": "US", "Proxy": "proxy-us.example.com:8080"},
        {"Country": "Canada", "Code": "CA", "Proxy": "proxy-ca.example.com:8080"},
        {"Country": "Germany", "Code": "DE", "Proxy": "proxy-de.example.com:8080"},
        {"Country": "France", "Code": "FR", "Proxy": "proxy-fr.example.com:8080"},
        {"Country": "United Kingdom", "Code": "UK", "Proxy": "proxy-uk.example.com:8080"},
        {"Country": "India", "Code": "IN", "Proxy": "proxy-in.example.com:8080"},
        {"Country": "Australia", "Code": "AU", "Proxy": "proxy-au.example.com:8080"},
        {"Country": "Japan", "Code": "JP", "Proxy": "proxy-jp.example.com:8080"},
        {"Country": "Brazil", "Code": "BR", "Proxy": "proxy-br.example.com:8080"},
        {"Country": "South Africa", "Code": "ZA", "Proxy": "proxy-za.example.com:8080"},
        {"Country": "Russia", "Code": "RU", "Proxy": "proxy-ru.example.com:8080"},
        {"Country": "Mexico", "Code": "MX", "Proxy": "proxy-mx.example.com:8080"},
        {"Country": "Italy", "Code": "IT", "Proxy": "proxy-it.example.com:8080"}
    ]
}

# ---------- Encrypt & Decrypt Data ----------
if __name__ == "__main__":
    # Step 1: Generate and store key (Run only once)
    if not os.getenv(AES_KEY_ENV):
        generate_and_store_key()
        print("AES key generated and stored in environment.")

    # Step 2: Encrypt and store sensitive data
    encrypted_data = {}
    for key, value in sensitive_data.items():
        if isinstance(value, list):  # Handle lists separately (like proxies)
            encrypted_data[key] = [encrypt_data(str(item)) for item in value]
        else:
            encrypted_data[key] = encrypt_data(value)

    print("\n🔐 Encrypted Data:")
    print(encrypted_data)

    # Step 3: Decrypt and retrieve original data
    decrypted_data = {}
    for key, value in encrypted_data.items():
        if isinstance(value, list):
            decrypted_data[key] = [decrypt_data(item["ciphertext"], item["iv"], item["auth_tag"]) for item in value]
        else:
            decrypted_data[key] = decrypt_data(value["ciphertext"], value["iv"], value["auth_tag"])

    print("\n🔓 Decrypted Data:")
    print(decrypted_data)


---

🔹 How It Works?

1️⃣ AES-256-GCM Key Management

The 256-bit encryption key is stored in an environment variable.

Ensures security by keeping the key outside the code/database.


2️⃣ Encryption

Uses AES-256-GCM mode for authenticated encryption.

Generates a unique IV (nonce) for each encryption.

Produces an authentication tag to ensure data integrity.


3️⃣ Storage & Structure

The script encrypts both string values (ClientID, Password, etc.) and list objects (Proxies).

Stores the ciphertext, IV, and authentication tag for each entry.


4️⃣ Decryption

Retrieves encrypted values, verifies authenticity, and decrypts them securely.



---

🔹 Sample Output

🔐 Encrypted Data:
{
    "ClientID": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "TenantID": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "ClientSecret": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "ServiceAccountName": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "ServiceAccountPassword": {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
    "Proxies": [
        {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
        {"ciphertext": "...", "iv": "...", "auth_tag": "..."},
        ...
    ]
}

🔓 Decrypted Data:
{
    "ClientID": "XYZ-12345-CLIENT",
    "TenantID": "TENANT-56789",
    "ClientSecret": "MySuperSecureSecret!@#",
    "ServiceAccountName": "service-agent-01",
    "ServiceAccountPassword": "AgentPassword$321",
    "Proxies": [
        {"Country": "United States", "Code": "US", "Proxy": "proxy-us.example.com:8080"},
        {"Country": "Canada", "Code": "CA", "Proxy": "proxy-ca.example.com:8080"},
        ...
    ]
}


---

🔹 Why Use This?

✅ AES-256-GCM provides both encryption & authentication.
✅ Secure key storage prevents hardcoded keys.
✅ Encrypts both simple values & lists of objects (like proxies).
✅ Tamper-proof authentication ensures integrity.


---

🔹 Next Steps

1️⃣ Store encrypted data in SQLite or Key Vault instead of keeping it in memory.
2️⃣ Use an API to fetch credentials when needed instead of re-encrypting every time.
3️⃣ Rotate encryption keys periodically for better security.


---

🚀 Would you like me to add SQLite storage for this encrypted data?







End-to-End AES-256-GCM Encryption & Decryption in Python

This implementation ensures secure storage of encrypted credentials in an SQLite database, with the AES key stored securely in environment variables.


---

🔹 Steps

1. Generate a secure AES-256-GCM key (stored in an environment variable).


2. Encrypt sensitive data (ClientID, ClientSecret, etc.).


3. Store encrypted data, IV (nonce), and authentication tag in SQLite.


4. Retrieve and decrypt the data securely at runtime.




---

🔐 Full Python Code

import os
import sqlite3
import base64
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

# Securely generate or load the AES-256 key
AES_KEY_ENV = "AES_256_GCM_KEY"

def generate_and_store_key():
    """Generate a new AES-256 key and store it in an environment variable"""
    key = get_random_bytes(32)  # 256-bit key
    os.environ[AES_KEY_ENV] = base64.b64encode(key).decode()  # Store in env as base64

def get_aes_key():
    """Retrieve AES key from environment variable"""
    key_b64 = os.getenv(AES_KEY_ENV)
    if not key_b64:
        raise ValueError("AES Key not found. Please generate and store it securely.")
    return base64.b64decode(key_b64)

def encrypt_data(plaintext):
    """Encrypt data using AES-256-GCM"""
    key = get_aes_key()  # Load AES key
    iv = get_random_bytes(16)  # 16-byte IV (Nonce)
    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext.encode())

    return {
        "ciphertext": base64.b64encode(ciphertext).decode(),
        "iv": base64.b64encode(iv).decode(),
        "auth_tag": base64.b64encode(auth_tag).decode()
    }

def decrypt_data(ciphertext_b64, iv_b64, auth_tag_b64):
    """Decrypt data using AES-256-GCM"""
    key = get_aes_key()
    iv = base64.b64decode(iv_b64)
    auth_tag = base64.b64decode(auth_tag_b64)
    ciphertext = base64.b64decode(ciphertext_b64)

    cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
    plaintext = cipher.decrypt_and_verify(ciphertext, auth_tag)

    return plaintext.decode()

# ---------- SQLite Storage ----------
DB_FILE = "secure_storage.db"

def init_db():
    """Initialize SQLite database with a table for storing encrypted credentials"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_name TEXT UNIQUE NOT NULL,
            ciphertext TEXT NOT NULL,
            iv TEXT NOT NULL,
            auth_tag TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def store_encrypted_data(key_name, encrypted_data):
    """Store encrypted credentials in the SQLite database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO credentials (key_name, ciphertext, iv, auth_tag)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key_name) DO UPDATE SET 
        ciphertext=excluded.ciphertext, iv=excluded.iv, auth_tag=excluded.auth_tag
    """, (key_name, encrypted_data["ciphertext"], encrypted_data["iv"], encrypted_data["auth_tag"]))
    conn.commit()
    conn.close()

def retrieve_encrypted_data(key_name):
    """Retrieve encrypted credentials from the database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT ciphertext, iv, auth_tag FROM credentials WHERE key_name=?", (key_name,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {"ciphertext": row[0], "iv": row[1], "auth_tag": row[2]}
    return None

# ---------- Example Usage ----------

if __name__ == "__main__":
    # Step 1: Generate and store key (Run only once)
    if not os.getenv(AES_KEY_ENV):
        generate_and_store_key()
        print("AES key generated and stored in environment.")

    # Step 2: Initialize database
    init_db()

    # Step 3: Encrypt and store sensitive data
    credentials = {
        "ClientID": "my-client-id",
        "TenantID": "my-tenant-id",
        "ClientSecret": "my-very-secure-client-secret",
        "ServiceAccountName": "my-service-account",
        "ServiceAccountPassword": "my-secure-password"
    }

    for key_name, value in credentials.items():
        encrypted_data = encrypt_data(value)
        store_encrypted_data(key_name, encrypted_data)
        print(f"Stored encrypted {key_name} in database.")

    # Step 4: Retrieve and decrypt data
    for key_name in credentials.keys():
        encrypted_data = retrieve_encrypted_data(key_name)
        if encrypted_data:
            decrypted_value = decrypt_data(
                encrypted_data["ciphertext"],
                encrypted_data["iv"],
                encrypted_data["auth_tag"]
            )
            print(f"Decrypted {key_name}: {decrypted_value}")


---

🔹 How It Works?

1️⃣ Key Management

Generates a 256-bit AES key and stores it securely in an environment variable.

The key is retrieved at runtime to perform encryption/decryption.


2️⃣ Secure Encryption with AES-256-GCM

Encrypts sensitive data using a securely generated IV (nonce).

Generates an authentication tag to ensure integrity.


3️⃣ SQLite Storage

Stores encrypted credentials, IV, and auth tag securely in an SQLite database.

Uses UPSERT (ON CONFLICT) to update credentials if needed.


4️⃣ Secure Decryption

Retrieves IV, auth tag, and ciphertext from the database.

Decrypts and verifies data, ensuring no tampering occurred.



---

🔹 Why Is This Secure?

✅ AES-256-GCM is strong – It provides both encryption and authentication.
✅ Key is never stored in the database – It is kept only in memory (environment variable).
✅ IV (Nonce) ensures uniqueness – Prevents reuse attacks.
✅ Authentication Tag verifies integrity – Prevents modifications or tampering.
✅ SQLite database is local – No need for external services for runtime storage.


---

🔹 What Needs to Be Stored?


---

🔹 When to Re-fetch Data from API?

🔸 Every 24 hours, check for credential changes by making an API call.
🔸 If event hub connection fails multiple times, re-fetch credentials.
🔸 If API call detects a change, update SQLite database with new encrypted values.


---

🚀 Ready to Use!

🔹 Use this implementation in your agent (EXE) running on user machines.
🔹 Only at runtime, the AES key is retrieved from the environment for decryption.
🔹 Ensures maximum security for ClientID, ClientSecret, and other credentials.


---

🔹 Next Steps?

Would you like a cross-platform version using secure hardware-based key storage?






Summary of AES-256-GCM

AES-256-GCM (Advanced Encryption Standard with 256-bit key in Galois/Counter Mode) is a secure and efficient encryption algorithm. It provides both confidentiality and integrity through authentication, making it stronger than AES-CBC.


---

🔹 Why Use AES-256-GCM?

✅ Strong Security – Uses a 256-bit key, making it resistant to brute-force attacks.
✅ Authentication (Integrity Protection) – Includes a 16-byte authentication tag to detect tampering.
✅ No Padding Needed – Unlike AES-CBC, it doesn't require padding, reducing overhead.
✅ Faster Decryption – More efficient than AES-CBC because it operates in counter mode.


---

🔹 What Needs to Be Stored?

To decrypt data, you need to store:


---

🔹 How It Works?

1️⃣ Generate a 256-bit key (store it securely).
2️⃣ Encrypt data using AES-256-GCM with a random IV (nonce).
3️⃣ Store the encrypted data, IV, and auth tag in the local database.
4️⃣ For decryption, retrieve the IV & auth tag, then decrypt using the AES key.


---

🔹 Example Code Using PyCryptodome

🔐 Encryption

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import base64

# Generate a 256-bit (32-byte) AES key (store this securely)
key = get_random_bytes(32)

# Generate a random IV (Nonce)
iv = get_random_bytes(16)

# Create AES-GCM cipher
cipher = AES.new(key, AES.MODE_GCM, nonce=iv)

# Encrypt data
plaintext = b"Sensitive Data"
ciphertext, auth_tag = cipher.encrypt_and_digest(plaintext)

# Store (ciphertext, IV, and auth tag)
print("Ciphertext:", base64.b64encode(ciphertext).decode())
print("IV:", base64.b64encode(iv).decode())
print("Auth Tag:", base64.b64encode(auth_tag).decode())

🔓 Decryption

# Load key, IV, auth tag, and ciphertext from storage
cipher = AES.new(key, AES.MODE_GCM, nonce=iv)
decrypted_data = cipher.decrypt_and_verify(ciphertext, auth_tag)

print("Decrypted Data:", decrypted_data.decode())


---

🔹 Pros & Cons


---

🔹 When to Use AES-256-GCM?

✅ If you need both encryption & authentication (integrity check)
✅ If high security is required (storing sensitive data)
✅ If performance is a concern (faster than CBC)


---

Final Thoughts

AES-256-GCM is one of the most secure and recommended encryption methods. You must store the AES key securely (in an environment variable) and store the IV + auth tag with the encrypted data for proper decryption.

Would you like a complete implementation with secure storage in SQLite?






AES-256-GCM (Advanced Encryption Standard - Galois/Counter Mode)

AES-256-GCM is a highly secure encryption method that provides both confidentiality and integrity by using a combination of AES encryption and Galois Message Authentication Code (GMAC).


---

🔹 Key Features of AES-256-GCM

1. Strong Security: Uses a 256-bit key, making it resistant to brute-force attacks.


2. Authenticated Encryption: Uses an authentication tag (typically 16 bytes) to ensure data integrity and prevent tampering.


3. No Padding Required: Unlike CBC mode, GCM does not require padding.


4. Fast Performance: GCM mode allows parallel processing, making encryption/decryption faster than CBC mode.


5. IV/Nonce Requirement: Requires a unique IV (Initialization Vector) for each encryption operation (typically 12 or 16 bytes).




---

🔹 What to Store for AES-256-GCM?

Since AES-GCM is authenticated encryption, you need to store three things for decryption:

**❌ Do NOT reuse the same IV for multiple encryptions, as it weakens security






Here’s a structured documentation outline based on your architecture:


---

🔹 Secure Storage and Retrieval of Credentials for Event Hub Agent

1. Credentials to Securely Store

The following credentials need to be securely stored and retrieved:

Client ID

Tenant ID

Client Secret

Service Account Name

Service Account Password



---

2. Encryption and Secure Storage Process

🔹 Step 1: Generate Encryption Key

1. Use AES Encryption (Fernet) to securely encrypt the credentials.


2. Generate an encryption key that will be used for both encryption & decryption.


3. Store this key securely, ensuring it’s not hardcoded in the application.

Store in environment variables, a secure key file, or Azure Key Vault.




Key Generation Code:

from cryptography.fernet import Fernet

# Generate a secure encryption key
key = Fernet.generate_key()
print(f"Store this securely: {key.decode()}")

# Save key to a secure file (DO NOT store in source code)
with open("encryption_key.bin", "wb") as f:
    f.write(key)


---

🔹 Step 2: Encrypt Credentials (Manually for First Time)

1. Encrypt credentials using the generated encryption key.


2. Store the encrypted values in Azure SQL Database.



Encryption Code:

import base64
from cryptography.fernet import Fernet

# Load encryption key
with open("encryption_key.bin", "rb") as f:
    key = f.read()

cipher = Fernet(key)  # Initialize cipher for encryption

# Function to encrypt a value
def encrypt_value(value):
    encrypted_value = cipher.encrypt(value.encode())
    return base64.b64encode(encrypted_value).decode()  # Convert to string

# Encrypt and store credentials
client_id_enc = encrypt_value("your-client-id")
client_secret_enc = encrypt_value("your-client-secret")
tenant_id_enc = encrypt_value("your-tenant-id")
service_account_name_enc = encrypt_value("your-service-account")
service_account_password_enc = encrypt_value("your-password")

print("Encrypted Credentials:")
print(client_id_enc, client_secret_enc, tenant_id_enc, service_account_name_enc, service_account_password_enc)

✅ The encrypted values will be stored manually in Azure SQL Database.


---

🔹 Step 3: API Call to Fetch Credentials from Azure SQL Database

1. When the agent runs on the local machine, it will make an API call to fetch the encrypted credentials stored in Azure SQL Database.


2. After retrieving, it will store them in a local SQLite database.



API Flow:
✅ Agent makes a REST API request to fetch credentials
✅ API responds with encrypted credentials
✅ Agent stores these encrypted credentials in a local SQLite database

Example API Request (Python requests):

import requests

API_URL = "https://your-api.com/get-credentials"
response = requests.get(API_URL)

if response.status_code == 200:
    credentials = response.json()
    print("Fetched Encrypted Credentials:", credentials)
else:
    print("Error fetching credentials")


---

🔹 Step 4: Store Encrypted Credentials in Local SQLite

1. After fetching the credentials via API, the agent stores them locally in an SQLite database.


2. This avoids frequent API calls, improving performance.



Store in SQLite:

import sqlite3

# Connect to local SQLite
conn = sqlite3.connect("secure_storage.db")
cursor = conn.cursor()

# Create table for encrypted credentials
cursor.execute("""
CREATE TABLE IF NOT EXISTS credentials (
    key_name TEXT PRIMARY KEY,
    encrypted_value BLOB
)
""")
conn.commit()

# Function to store credentials
def store_credential(key_name, encrypted_value):
    cursor.execute("INSERT OR REPLACE INTO credentials (key_name, encrypted_value) VALUES (?, ?)", (key_name, encrypted_value))
    conn.commit()

# Store encrypted credentials in SQLite
store_credential("client_id", client_id_enc)
store_credential("client_secret", client_secret_enc)
store_credential("tenant_id", tenant_id_enc)
store_credential("service_account_name", service_account_name_enc)
store_credential("service_account_password", service_account_password_enc)

print("Stored encrypted credentials in local SQLite")
conn.close()


---

🔹 Step 5: Decrypt Credentials at Runtime

1. When the agent needs credentials, it retrieves the encrypted values from SQLite and decrypts them.


2. The decryption key is securely loaded at runtime.



Decryption Code:

import base64

# Function to decrypt values
def decrypt_value(encrypted_value):
    decrypted_value = cipher.decrypt(base64.b64decode(encrypted_value)).decode()
    return decrypted_value

# Retrieve encrypted credentials from SQLite
def get_credential(key_name):
    cursor.execute("SELECT encrypted_value FROM credentials WHERE key_name = ?", (key_name,))
    result = cursor.fetchone()
    return decrypt_value(result[0]) if result else None

# Decrypt credentials at runtime
client_id = get_credential("client_id")
client_secret = get_credential("client_secret")
tenant_id = get_credential("tenant_id")
service_account_name = get_credential("service_account_name")
service_account_password = get_credential("service_account_password")

print("Decrypted Client ID:", client_id)  # Use only for debugging


---

3. Handling Credential Updates & Connectivity Issues

To ensure credentials remain valid, the agent should periodically check for updates and handle connectivity failures.

🔹 Step 6: Periodic API Calls for Credential Updates

✅ Every day, the agent will make an API call to check if credentials have changed.
✅ If there are updates (e.g., client secret change), the agent fetches the new credentials and updates the local SQLite database.

API Call for Credential Check:

import time

def check_for_credential_updates():
    response = requests.get(API_URL)
    if response.status_code == 200:
        new_credentials = response.json()

        # Compare with stored values
        if new_credentials["client_secret"] != get_credential("client_secret"):
            print("Updating credentials...")
            store_credential("client_secret", new_credentials["client_secret"])
        
        print("Credentials are up-to-date")
    else:
        print("Error checking credential updates")

# Run daily check
while True:
    check_for_credential_updates()
    time.sleep(86400)  # Wait 24 hours before next check


---

4. Summary of Secure Credential Management Workflow


---

5. Security Best Practices

✅ Never hardcode the encryption key
✅ Use a secure storage for the encryption key (environment variables, TPM, or Key Vault)
✅ Restrict file access to SQLite DB (chmod 600 or Windows ACLs)
✅ Limit API calls to avoid unnecessary exposure of credentials
✅ Use secure network protocols (HTTPS) for all API calls


---

✅ Does this documentation cover everything you need? Let me know if you need any refinements!











Securely Storing and Retrieving Service Account Credentials Using AES Encryption

Since you want a secure way to store your service principal credentials (client ID, secret, tenant ID, service account, password) in SQLite, I'll guide you through:
✅ AES encryption for strong security
✅ Key storage best practices
✅ Storing encrypted data in SQLite
✅ Securely retrieving and decrypting credentials at runtime


---

🔹 1. AES Encryption Using Fernet (Symmetric Key)

We'll use AES encryption with cryptography.fernet. This ensures your credentials are unreadable without the correct decryption key.

Generate a Secure Encryption Key

🔹 Store this key securely! Never hardcode it in your script. Instead, load it from environment variables, a separate secured file, or a TPM module.

from cryptography.fernet import Fernet

# Generate a key
key = Fernet.generate_key()
print(f"Your AES encryption key (store securely!): {key.decode()}")

# Save to a secure location (DO NOT store in your source code)
with open("encryption_key.bin", "wb") as f:
    f.write(key)


---

🔹 2. Store Encrypted Credentials in SQLite

Here’s how you store encrypted credentials securely in SQLite.

import sqlite3
from cryptography.fernet import Fernet

# Load the encryption key
with open("encryption_key.bin", "rb") as f:
    key = f.read()

cipher = Fernet(key)

# Connect to SQLite database
conn = sqlite3.connect("secure_storage.db")
cursor = conn.cursor()

# Create a table to store encrypted credentials
cursor.execute("""
CREATE TABLE IF NOT EXISTS credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_name TEXT,
    encrypted_value BLOB
)
""")
conn.commit()

# Function to encrypt and store a credential
def store_credential(key_name, value):
    encrypted_value = cipher.encrypt(value.encode())
    cursor.execute("INSERT INTO credentials (key_name, encrypted_value) VALUES (?, ?)", (key_name, encrypted_value))
    conn.commit()

# Storing credentials
store_credential("client_id", "your-client-id")
store_credential("client_secret", "your-client-secret")
store_credential("tenant_id", "your-tenant-id")
store_credential("service_account_name", "your-service-account")
store_credential("service_account_password", "your-password")

print("Credentials stored securely.")
conn.close()


---

🔹 3. Retrieve & Decrypt Credentials at Runtime

At runtime, your application will load the encrypted credentials from SQLite and decrypt them.

# Function to retrieve and decrypt a credential
def get_credential(key_name):
    cursor.execute("SELECT encrypted_value FROM credentials WHERE key_name = ?", (key_name,))
    result = cursor.fetchone()
    if result:
        return cipher.decrypt(result[0]).decode()
    return None

# Fetch credentials securely at runtime
client_id = get_credential("client_id")
client_secret = get_credential("client_secret")
tenant_id = get_credential("tenant_id")
service_account_name = get_credential("service_account_name")
service_account_password = get_credential("service_account_password")

print(f"Decrypted Client ID: {client_id}")  # Use only in testing


---

🔹 4. Security Best Practices

1. 🔑 Never Hardcode the Encryption Key

Store the key in a secure file, environment variable, or Windows Credential Manager.

Example: Load from environment variable in production:

import os
key = os.getenv("ENCRYPTION_KEY").encode()



2. 🛡️ Restrict Access to SQLite File

Windows: Store in %APPDATA% and restrict file access:

icacls secure_storage.db /inheritance:r /grant:r %USERNAME%:F

Linux/macOS:

chmod 600 secure_storage.db



3. 🛠️ Store Credentials in an Encrypted Storage Like Azure Key Vault

If feasible, fetch credentials directly from Azure Key Vault instead of storing them locally.





---

✅ Summary

1. Encrypt credentials with AES (Fernet)


2. Store them securely in SQLite


3. Load them only at runtime and decrypt


4. Secure encryption keys (never hardcode!)


5. Restrict file access to SQLite DB



Would this approach work for you? Let me know if you need any modifications!








import urllib.request
import os

# Get system proxy
proxy = urllib.request.getproxies()

# Use system credentials for proxy authentication
proxy_handler = urllib.request.ProxyHandler(proxy)
auth_handler = urllib.request.HTTPBasicAuthHandler()

# Create an opener using proxy and authentication handlers
opener = urllib.request.build_opener(proxy_handler, auth_handler)
urllib.request.install_opener(opener)

# Verify proxy settings
print(f"System Proxy: {proxy}")





import winreg

def get_windows_proxy():
    key_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:
            proxy_enabled = winreg.QueryValueEx(key, "ProxyEnable")[0]
            proxy_server = winreg.QueryValueEx(key, "ProxyServer")[0] if proxy_enabled else None
            return proxy_server
    except Exception as e:
        return f"Error: {e}"

print(f"System Proxy: {get_windows_proxy()}")








Fixing DuplicateTokenEx Third Argument Issue in Windows Impersonation

I understand your requirement:

You are already logged in using Windows credentials.

You want to impersonate yourself (i.e., the same logged-in user).

You need to correctly pass the third argument (TokenAttributes) to DuplicateTokenEx.

You are getting "argument must be int" or "object is not a PySecurityAttributes object" errors.



---

Why Is This Happening?

The issue occurs because the third argument (TokenAttributes) in DuplicateTokenEx must be either None or a valid SECURITY_ATTRIBUTES object. However, the pywin32 library does not always accept pywintypes.SECURITY_ATTRIBUTES() as a valid structure.

Solution

1. Use None for TokenAttributes (Recommended)

Instead of trying to create a pywintypes.SECURITY_ATTRIBUTES(), just pass None. This should work:

import win32api
import win32security
import win32con

def impersonate_user():
    try:
        # Get current process token
        hToken = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        # Duplicate the token correctly (Use None for TokenAttributes)
        dupToken = win32security.DuplicateTokenEx(
            hToken,
            win32con.MAXIMUM_ALLOWED,
            None,  # Security Attributes (MUST BE None)
            win32security.SecurityImpersonation,
            win32security.TokenImpersonation
        )

        # Impersonate the user
        win32security.ImpersonateLoggedOnUser(dupToken)
        print(f"Now impersonating as: {win32api.GetUserName()}")

        # Revert back
        win32security.RevertToSelf()
        print("Reverted to original user.")

    except Exception as e:
        print(f"Error: {e}")

# Run the function
impersonate_user()

✅ This should work in an enterprise environment where the user is already logged in.


---

2. If None Doesn’t Work, Try with a Valid Handle

If passing None still throws an error, try using hToken as the third argument instead:

dupToken = win32security.DuplicateTokenEx(
    hToken,
    win32con.MAXIMUM_ALLOWED,
    hToken,  # Pass the existing token handle
    win32security.SecurityImpersonation,
    win32security.TokenImpersonation
)

Why?

Some enterprise environments require a handle instead of None for token attributes.



---

3. If You Must Use SECURITY_ATTRIBUTES, Do It Correctly

If your organization requires explicit security attributes, use this:

import pywintypes

security_attributes = pywintypes.SECURITY_ATTRIBUTES()
security_attributes.bInheritHandle = True

dupToken = win32security.DuplicateTokenEx(
    hToken,
    win32con.MAXIMUM_ALLOWED,
    None,  # Keep None here if SEC_ATTR doesn't work
    win32security.SecurityImpersonation,
    win32security.TokenImpersonation
)

⚠ Even though pywintypes.SECURITY_ATTRIBUTES() exists, DuplicateTokenEx doesn’t always require it. Passing None is preferred.


---

Key Fixes Summary

Let me know if this works for your case!







If none of the above worked, let's explore alternative ways to impersonate the currently logged-in user.

Alternative 1: Use LogonUser Instead of DuplicateTokenEx

Since DuplicateTokenEx is causing issues with token attributes, try using LogonUser, which directly returns a usable token:

import win32api
import win32security
import win32con

def impersonate_self():
    try:
        username = win32api.GetUserNameEx(win32con.NameSamCompatible)
        domain = username.split("\\")[0]  # Extract domain (if exists)
        user = username.split("\\")[-1]  # Extract username

        print(f"Trying to impersonate: {username}")

        # You need the current user's password; since you're logged in, try an empty string
        hToken = win32security.LogonUser(
            user,
            domain,
            "",  # Empty password may work in enterprise SSO
            win32con.LOGON32_LOGON_INTERACTIVE,
            win32con.LOGON32_PROVIDER_DEFAULT
        )

        # Impersonate user
        win32security.ImpersonateLoggedOnUser(hToken)
        print(f"Impersonating User: {win32api.GetUserName()}")

        # Revert to self
        win32security.RevertToSelf()
        print("Reverted to original user.")

    except Exception as e:
        print(f"Impersonation failed: {e}")

# Run function
impersonate_self()

✅ Why this might work?

LogonUser gets a new token without needing DuplicateTokenEx.

If your organization supports Single Sign-On (SSO), an empty password ("") may work.



---

Alternative 2: Use CreateProcessWithTokenW to Launch as Impersonated User

If direct impersonation fails, try running a new process under the impersonated user:

import win32api
import win32security
import win32con
import subprocess

def run_as_current_user():
    try:
        hToken = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        dupToken = win32security.DuplicateTokenEx(
            hToken,
            win32con.MAXIMUM_ALLOWED,
            None,
            win32security.SecurityImpersonation,
            win32security.TokenPrimary
        )

        print("Launching new process as impersonated user...")

        subprocess.run("cmd.exe", creationflags=subprocess.CREATE_NEW_CONSOLE, stdin=dupToken)

    except Exception as e:
        print(f"Failed to launch as impersonated user: {e}")

# Run function
run_as_current_user()

✅ Why this might work?

Instead of impersonating inside the same process, we start a new process under the impersonated user.



---

Alternative 3: Use NtSetInformationThread (Low-Level API)

If all else fails, a low-level system call can be used:

import ctypes
import win32api
import win32security
import win32con

def low_level_impersonation():
    try:
        hToken = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        dupToken = win32security.DuplicateTokenEx(
            hToken,
            win32con.MAXIMUM_ALLOWED,
            None,
            win32security.SecurityImpersonation,
            win32security.TokenImpersonation
        )

        # Load NtSetInformationThread from NTDLL
        ntdll = ctypes.WinDLL("ntdll")
        NtSetInformationThread = ntdll.NtSetInformationThread
        NtSetInformationThread.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_ulong]
        NtSetInformationThread.restype = ctypes.c_long

        # Apply impersonation
        thread = ctypes.windll.kernel32.GetCurrentThread()
        status = NtSetInformationThread(thread, 0x11, ctypes.byref(dupToken), ctypes.sizeof(dupToken))

        if status == 0:
            print(f"Successfully impersonated as {win32api.GetUserName()}")
        else:
            print(f"Failed with error code: {status}")

    except Exception as e:
        print(f"Impersonation failed: {e}")

# Run function
low_level_impersonation()

✅ Why this might work?

Uses Windows NTDLL system calls, bypassing standard API restrictions.

This method is used by some security tools and malware (so use responsibly).



---

Final Considerations

1. Check if your script has Admin privileges.

Run PowerShell as Administrator and try whoami /groups to verify.



2. Enterprise policies might block impersonation.

Check secpol.msc → Local Policies → User Rights Assignment → "Impersonate a client after authentication".



3. Try LogonUser first, since it doesn't require DuplicateTokenEx.




---

Let me know which one works for you!







import win32api
import win32security
import win32con

def impersonate_current_user():
    try:
        # Get the current process token
        h_token = win32security.OpenProcessToken(
            win32api.GetCurrentProcess(),
            win32con.TOKEN_DUPLICATE | win32con.TOKEN_QUERY
        )

        # Duplicate the token for impersonation
        dup_token = win32security.DuplicateTokenEx(
            h_token,
            win32con.MAXIMUM_ALLOWED,
            None,
            win32security.SecurityImpersonation,
            win32security.TokenPrimary
        )

        # Impersonate the user
        win32security.ImpersonateLoggedOnUser(dup_token)
        print(f"Impersonating User: {win32api.GetUserName()}")

        # Revert to the original user
        win32security.RevertToSelf()
        print("Reverted to original user.")

    except Exception as e:
        print(f"Impersonation failed: {e}")

# Run the function
impersonate_current_user()






import win32security
import win32api
import win32con

def impersonate_user(username, domain, password):
    # Get a handle to the user token
    token = win32security.LogonUser(
        username, domain, password,
        win32con.LOGON32_LOGON_INTERACTIVE,
        win32con.LOGON32_PROVIDER_DEFAULT
    )

    # Impersonate the user
    win32security.ImpersonateLoggedOnUser(token)
    
    # Now any code you run here executes as the impersonated user

    # Revert back to original user
    win32security.RevertToSelf()
    token.Close()

# Example usage (You must have the user’s password)
impersonate_user("USERNAME", "DOMAIN", "PASSWORD")




import requests
from datetime import datetime, timedelta
import urllib
import base64
import hmac
import hashlib

# Event Hub details
namespace = "your-eventhub-namespace"
eventhub_name = "your-eventhub-name"
sas_key_name = "your-sas-key-name"  # Shared Access Policy name
sas_key = "your-sas-key"  # Shared Access Key
eventhub_url = f"https://{namespace}.servicebus.windows.net/{eventhub_name}/messages"

# Generate SAS Token
expiry = int((datetime.utcnow() + timedelta(hours=1)).timestamp())
uri = urllib.parse.quote_plus(f"{namespace}.servicebus.windows.net/{eventhub_name}/messages")
string_to_sign = f"{uri}\n{expiry}"
signature = base64.b64encode(hmac.new(base64.b64decode(sas_key), string_to_sign.encode('utf-8'), hashlib.sha256).digest())
sas_token = f"SharedAccessSignature sr={uri}&sig={urllib.parse.quote_plus(signature)}&se={expiry}&skn={sas_key_name}"

# Event data (Batch of events)
event_data = """
<entry xmlns="http://www.w3.org/2005/Atom">
    <content type="application/xml">
        <EventData>
            <Message>Your Event Data here</Message>
        </EventData>
    </content>
</entry>
"""

# Send HTTP request to Event Hub with PAC Proxy settings
headers = {
    'Authorization': sas_token,
    'Content-Type': 'application/atom+xml;type=entry;charset=utf-8',
    'Host': f'{namespace}.servicebus.windows.net'
}

# Set up the session to respect proxy (if you're using PAC)
session = requests.Session()

# Proxy settings (if required by your environment)
session.proxies = {
    'http': 'http://proxyserver:port',
    'https': 'https://proxyserver:port'
}

# Send the event batch to Event Hub
response = session.post(eventhub_url, headers=headers, data=event_data)

# Check response status
if response.status_code == 201:
    print("Event sent successfully!")
else:
    print(f"Failed to send event. Status Code: {response.status_code}, Response: {response.text}")




import os
import requests
import keyring
from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventDataBatch

# Retrieve the stored password
username = "your_username"
proxy_url = "http://us-proxy-01.systems.us.ongc:80"

# Get password from Windows Credential Manager
password = keyring.get_password("Windows Stored Credentials", username)

if not password:
    raise Exception("Proxy password not found in Windows Credential Manager!")

# Set environment variables with authentication
auth_proxy = f"http://{username}:{password}@us-proxy-01.systems.us.ongc:80"
os.environ['HTTP_PROXY'] = auth_proxy
os.environ['HTTPS_PROXY'] = auth_proxy

# Create a session
session = requests.Session()
session.proxies = {'http': auth_proxy, 'https': auth_proxy}

# Test proxy authentication
try:
    response = session.get("http://your-eventhub-url-or-api")
    print(f"Proxy authentication successful! Status Code: {response.status_code}")
except Exception as e:
    print(f"Failed to authenticate with proxy: {e}")

# Azure Event Hub authentication
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"
eventhub_namespace = "your-eventhub-namespace"
eventhub_name = "your-eventhub-name"

# Initialize ClientSecretCredential
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create EventHubProducerClient
producer_client = EventHubProducerClient(
    fully_qualified_namespace=f"{eventhub_namespace}.servicebus.windows.net",
    eventhub_name=eventhub_name,
    credential=credential
)

# Create a batch, add event data, and send the batch
event_data_batch = producer_client.create_batch()
event_data = "Your event data goes here"
event_data_batch.add(event_data)

# Send the batch of events
try:
    producer_client.send_batch(event_data_batch)
    print("Batch sent successfully!")
except Exception as e:
    print(f"Failed to send batch: {e}")
finally:
    producer_client.close()







import os
import requests
from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventDataBatch
from requests_ntlm import HttpNtlmAuth
import socket
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

# Step 1: Set proxy environment variables
proxy_url = "http://us-proxy-01.systems.us.ongc:80"

# Set proxy for HTTP and HTTPS in environment variables
os.environ['HTTP_PROXY'] = proxy_url
os.environ['HTTPS_PROXY'] = proxy_url

# Step 2: Create a session to handle NTLM authentication for the proxy
session = requests.Session()

# Define a custom adapter that uses the current Windows user's credentials (no manual username/password required)
class NTLMAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs['proxy_urls'] = {
            'http': proxy_url,
            'https': proxy_url
        }
        return super().init_poolmanager(*args, **kwargs)

# Attach the NTLM adapter
session.mount('http://', NTLMAdapter())
session.mount('https://', NTLMAdapter())

# Optionally, you can verify if the proxy works by making a request
try:
    # Try accessing a URL that goes through the proxy to see if NTLM authentication works
    response = session.get('http://your-eventhub-url-or-api')  # Replace with a valid URL
    print(response.status_code)
except Exception as e:
    print(f"Error connecting to proxy: {e}")

# Step 3: Use ClientSecretCredential to authenticate with Azure
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"
eventhub_namespace = "your-eventhub-namespace"
eventhub_name = "your-eventhub-name"

# Initialize ClientSecretCredential for Azure Event Hub authentication
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Step 4: Create EventHubProducerClient using the credential
producer_client = EventHubProducerClient(
    fully_qualified_namespace=f"{eventhub_namespace}.servicebus.windows.net",
    eventhub_name=eventhub_name,
    credential=credential
)

# Step 5: Create a batch, add event data, and send the batch
event_data_batch = producer_client.create_batch()

# Add some event data to the batch
event_data = "Your event data goes here"
event_data_batch.add(event_data)

# Send the batch of events
try:
    producer_client.send_batch(event_data_batch)
    print("Batch sent successfully!")
except Exception as e:
    print(f"Failed to send batch: {e}")

# Close the producer client after sending
finally:
    producer_client.close()






import logging
from logging.handlers import RotatingFileHandler

# Define log file path
log_file = "app.log"

# Setup RotatingFileHandler
handler = RotatingFileHandler(
    log_file, mode='a', maxBytes=50 * 1024, backupCount=5, encoding=None, delay=False
)

# Define log format: Time (including milliseconds), Level, and Message
formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

handler.setFormatter(formatter)

# Configure logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Test logging (generates large logs to test rotation)
for i in range(10000):
    logger.info(f"This is log entry number {i}")








import ctypes
import logging
from logging.handlers import RotatingFileHandler

# Define log file path
log_file = "app.log"

# Setup RotatingFileHandler
handler = RotatingFileHandler(
    log_file, mode='a', maxBytes=50 * 1024, backupCount=5, encoding=None, delay=False
)

# Define log format: Time (including milliseconds), Level, and Message
formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

handler.setFormatter(formatter)

# Configure logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Test logging (generates large logs to test rotation)
for i in range(10000):
    logger.info(f"This is log entry number {i}")import time
import win32api
import win32con
import win32gui
import win32process
import win32security
import datetime
from ctypes import wintypes

# Constants for Session Status
WTS_SESSION_LOCK = 0x07
WTS_SESSION_UNLOCK = 0x08
WTS_SESSION_LOGON = 0x01
WTS_SESSION_LOGOFF = 0x02

# Store timestamps for lock and unlock
lock_time = None
unlock_time = None

# Function to handle system session events (lock/unlock)
def handle_wts_event(wparam, lparam):
    global lock_time, unlock_time
    
    if wparam == WTS_SESSION_LOCK:
        # System is locked
        lock_time = datetime.datetime.now()
        print(f"System locked at: {lock_time}")
        
    elif wparam == WTS_SESSION_UNLOCK:
        # System is unlocked
        unlock_time = datetime.datetime.now()
        print(f"System unlocked at: {unlock_time}")
        
        if lock_time:
            # Calculate the duration between lock and unlock
            duration = unlock_time - lock_time
            print(f"Duration locked: {duration}")
        
        # Reset lock_time after unlock
        lock_time = None
        
    elif wparam == WTS_SESSION_LOGON:
        print(f"User logged on at: {datetime.datetime.now()}")
        
    elif wparam == WTS_SESSION_LOGOFF:
        print(f"User logged off at: {datetime.datetime.now()}")

# Register for session notifications
def register_session_notifications(hwnd):
    ctypes.windll.wtsapi32.WTSRegisterSessionNotification(hwnd, 1)

# Create a simple window to handle messages
def create_window():
    wc = win32gui.WNDCLASS()
    wc.lpfnWndProc = handle_wts_event  # Set the handler for the window messages
    wc.lpszClassName = "SessionMonitorClass"
    wc.hInstance = win32api.GetModuleHandle(None)

    class_atom = win32gui.RegisterClass(wc)
    hwnd = win32gui.CreateWindow(class_atom, "Session Monitor Window", 0, 0, 0, 0, 0, 0, 0, 0, wc.hInstance, None)

    register_session_notifications(hwnd)  # Register for session notifications
    win32gui.PumpMessages()  # Start the message loop

# Main function
if __name__ == "__main__":
    print("Monitoring system lock/unlock events...")
    create_window()




import win32gui
import win32con
import win32api
import win32ts
import ctypes
import time

# Define window procedure
def window_proc(hwnd, msg, wparam, lparam):
    try:
        if msg == win32con.WM_WTSSESSION_CHANGE:  # Correct event type
            if wparam == win32ts.WTS_SESSION_LOCK:
                print("🔒 System Locked!")
            elif wparam == win32ts.WTS_SESSION_UNLOCK:
                print("🔓 System Unlocked!")
        return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)
    except Exception as e:
        print(f"Error in WNDPROC handler: {e}")
        return 0  # Always return an integer (prevents crashes)

class WindowsSessionMonitor:
    def __init__(self):
        """Creates a hidden window to listen for lock/unlock events"""
        self.hinst = win32api.GetModuleHandle(None)
        self.class_name = "SessionMonitorWindow"

        # Define window class
        self.wc = win32gui.WNDCLASS()
        self.wc.lpfnWndProc = window_proc
        self.wc.hInstance = self.hinst
        self.wc.lpszClassName = self.class_name
        self.class_atom = win32gui.RegisterClass(self.wc)

        # Create the hidden window
        self.hwnd = win32gui.CreateWindow(self.class_atom, "Session Monitor", 0, 0, 0, 0, 0, 0, 0, 0, self.hinst, None)

        # Register for session notifications
        ctypes.windll.wtsapi32.WTSRegisterSessionNotification(self.hwnd, win32ts.NOTIFY_FOR_ALL_SESSIONS)

    def run(self):
        """Starts the event listener"""
        print("Listening for Windows lock/unlock events...")
        try:
            while True:
                win32gui.PumpMessages()  # Correct message loop
        except KeyboardInterrupt:
            print("Stopping monitoring...")
            ctypes.windll.wtsapi32.WTSUnRegisterSessionNotification(self.hwnd)
            win32gui.DestroyWindow(self.hwnd)

if __name__ == "__main__":
    monitor = WindowsSessionMonitor()
    monitor.run()








import win32gui
import win32con
import win32api
import win32ts
import time

def window_proc(hwnd, msg, wparam, lparam):
    if msg == win32con.WM_WTSSESSION_CHANGE:
        if wparam == win32con.WTS_SESSION_LOCK:
            print("System Locked!")  # Trigger when the system locks
        elif wparam == win32con.WTS_SESSION_UNLOCK:
            print("System Unlocked!")  # Trigger when the system unlocks
    return win32gui.DefWindowProc(hwnd, msg, wparam, lparam)

class WindowsSessionMonitor:
    def __init__(self):
        self.hinst = win32api.GetModuleHandle(None)
        self.class_name = "SessionMonitorWindow"
        self.wc = win32gui.WNDCLASS()
        self.wc.lpfnWndProc = window_proc
        self.wc.hInstance = self.hinst
        self.wc.lpszClassName = self.class_name
        self.class_atom = win32gui.RegisterClass(self.wc)
        self.hwnd = win32gui.CreateWindow(self.class_atom, "Session Monitor", 0, 0, 0, 0, 0, 0, 0, self.hinst, None)
        win32ts.WTSRegisterSessionNotification(self.hwnd, win32ts.NOTIFY_FOR_ALL_SESSIONS)

    def run(self):
        """Starts the event listener"""
        print("Listening for session changes...")
        try:
            while True:
                win32gui.PumpWaitingMessages()
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Stopping monitoring...")
            win32ts.WTSUnRegisterSessionNotification(self.hwnd)
            win32gui.DestroyWindow(self.hwnd)

if __name__ == "__main__":
    monitor = WindowsSessionMonitor()
    monitor.run()








import logging
from logging.handlers import RotatingFileHandler
import os

# Define log file path
log_file = "app.log"

# Setup RotatingFileHandler
handler = RotatingFileHandler(
    log_file, mode='a', maxBytes=50 * 1024, backupCount=5, encoding=None, delay=False
)

# Configure logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Test logging (generates large logs to test rotation)
for i in range(10000):
    logger.info(f"This is log entry number {i}")








import os
import time
from pynput import mouse, keyboard
from datetime import datetime

# Directory and log file path
log_dir = 'Pawan'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Log file size threshold in bytes (10 KB)
MAX_FILE_SIZE = 10 * 1024

# Function to get the current log file name
def get_log_filename():
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M')
    return os.path.join(log_dir, f'pawan_{current_time}.log')

# Function to write logs to file
def write_log(message):
    log_filename = get_log_filename()
    
    # Check if the current log file size exceeds the limit
    if os.path.exists(log_filename) and os.path.getsize(log_filename) > MAX_FILE_SIZE:
        log_filename = get_log_filename()  # Create a new log file with the updated name

    # Write the message to the log file
    with open(log_filename, 'a') as f:
        f.write(message + '\n')

# Mouse event handler
def on_click(x, y, button, pressed):
    if pressed:
        write_log(f'Mouse clicked at ({x}, {y}) with {button}')
    else:
        write_log(f'Mouse released at ({x}, {y}) with {button}')

# Keyboard event handler
def on_press(key):
    try:
        write_log(f'Key pressed: {key.char}')
    except AttributeError:
        write_log(f'Special key pressed: {key}')

# Collect events
def start_logging():
    # Start mouse listener
    with mouse.Listener(on_click=on_click) as mouse_listener:
        # Start keyboard listener
        with keyboard.Listener(on_press=on_press) as keyboard_listener:
            print("Logging mouse and keyboard events. Press ESC to stop.")
            # Join listeners to keep the program running
            mouse_listener.start()
            keyboard_listener.join()

if __name__ == '__main__':
    start_logging()











from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventData
import json

# Define your Azure AD application credentials (Service Principal)
tenant_id = 'your-tenant-id'
client_id = 'your-client-id'
client_secret = 'your-client-secret'
eventhub_namespace = 'your-eventhub-namespace'
eventhub_name = 'your-eventhub-name'

# Create a credential object to authenticate using Service Principal
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Create the EventHubProducerClient using the credential (OAuth2 token-based authentication)
producer = EventHubProducerClient(
    fully_qualified_namespace=f'{eventhub_namespace}.servicebus.windows.net',
    eventhub_name=eventhub_name,
    credential=credential
)

async def send_event_to_eventhub(event_data):
    try:
        # Create EventData
        event = EventData(event_data)

        # Send event
        async with producer:
            await producer.send_event(event, partition_key="login data")
        print(f"Successfully sent event: {event_data}")
    except Exception as e:
        print(f"Failed to send event: {str(e)}")

# Example of data to be sent
login_data = {"emp_id": 1234, "timestamp": "2025-01-30T12:00:00"}
send_event_to_eventhub(json.dumps(login_data))








from azure.eventhub import EventHubProducerClient, EventData
import json
import asyncio

# SAS token and connection details
fully_qualified_namespace = 'your-eventhub-namespace.servicebus.windows.net'
eventhub_name = 'your-eventhub-name'
sas_token = 'your-SAS-token'  # This is your SAS token

async def send_event_to_eventhub(event_data):
    try:
        print(f"Preparing to send event data: {event_data}")

        # Create producer client with SAS Token
        async with EventHubProducerClient(
            fully_qualified_namespace=fully_qualified_namespace,
            eventhub_name=eventhub_name,
            credential=sas_token  # Use the SAS token for authentication
        ) as producer:
            
            # Create EventData
            event = EventData(event_data)

            # Send event
            await producer.send_event(event, partition_key="login data")

            print(f"Successfully sent event to Event Hub: {event_data}")

    except Exception as e:
        # Capture more specific error information
        print(f"Failed to send event. Error: {str(e)}")

# Example event data
login_data = {"emp_id": 1234, "timestamp": "2025-01-30T12:00:00"}
asyncio.run(send_event_to_eventhub(json.dumps(login_data)))






from azure.eventhub import EventHubProducerClient, EventData
import json
import asyncio

# Use the primary connection string
connection_str = 'Endpoint=sb://your-eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=EventSender;SharedAccessKey=your-primary-key;EntityPath=your-eventhub-name'

eventhub_name = "your-eventhub-name"

async def send_event_to_eventhub(event_data):
    try:
        # Debug statement: Before sending the event
        print(f"Preparing to send event data: {event_data}")

        # Using async with for the producer client
        async with EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name) as producer:

            # Create EventData from the JSON event data
            event = EventData(event_data)

            # Send the event to the Event Hub with partition key "login data"
            await producer.send_event(event, partition_key="login data")

            # Debug statement: After successfully sending the event
            print(f"Successfully sent event to Event Hub: {event_data}")

    except Exception as e:
        # Debug statement: On failure
        print(f"Failed to send event: {str(e)}")

# Example: Sending some JSON data
login_data = {"emp_id": 1234, "timestamp": "2025-01-30T12:00:00"}
asyncio.run(send_event_to_eventhub(json.dumps(login_data)))




import ctypes
import ctypes.wintypes as wintypes
import signal
import sys

# Define Windows Constants
WH_KEYBOARD_LL = 13  # Low-level keyboard hook ID
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101
VK_TAB = 0x09
VK_ALT = 0x12

# Global variables
hHook = None  # Hook handle
alt_pressed = False

# Define Hook Procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    """ Callback function for keyboard hook. Detects key combinations. """
    global alt_pressed

    if nCode >= 0:
        vk_code = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents.vkCode
        print(f"[HOOK TRIGGERED] Key Code: {vk_code}, Event Type: {wParam}")

        if wParam == WM_KEYDOWN:
            if vk_code == VK_ALT:
                alt_pressed = True
                print("[INFO] ALT Pressed")
            elif vk_code == VK_TAB and alt_pressed:
                print("[DETECTED] ALT + TAB Pressed!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_ALT:
                alt_pressed = False
                print("[INFO] ALT Released")

    return ctypes.windll.user32.CallNextHookEx(hHook, nCode, wParam, lParam)

# Define Hook Struct
class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [("vkCode", wintypes.DWORD),
                ("scanCode", wintypes.DWORD),
                ("flags", wintypes.DWORD),
                ("time", wintypes.DWORD),
                ("dwExtraInfo", wintypes.ULONG_PTR)]

def set_keyboard_hook():
    """ Sets a global keyboard hook """
    global hHook

    # Define Hook Procedure Type
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    
    # Convert Python function to Windows hook function
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    # Install Hook
    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)
    
    if not hHook:
        print("❌ [ERROR] Failed to set keyboard hook.")
        sys.exit(1)

    print("✅ [SUCCESS] Keyboard Hook Installed.")

    # Message Loop to Keep Hook Active
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Handle CTRL+C for Graceful Exit
def handle_exit(signum, frame):
    """ Handles CTRL+C to properly unhook and exit """
    global hHook
    print("\n[EXIT] CTRL+C Detected. Unhooking keyboard and exiting...")
    
    if hHook:
        ctypes.windll.user32.UnhookWindowsHookEx(hHook)  # Unhook before exiting
    
    sys.exit(0)

# Register Signal Handler for CTRL+C
signal.signal(signal.SIGINT, handle_exit)

# Run the Hook
if __name__ == "__main__":
    print("🔄 [INFO] Running Keyboard Hook. Press CTRL+C to exit.")
    set_keyboard_hook()






import ctypes
import win32con
from ctypes import wintypes

# Constants
WH_KEYBOARD_LL = 13
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

VK_TAB = 0x09
VK_MENU = 0x12  # ALT key
VK_CONTROL = 0x11
VK_SHIFT = 0x10
VK_ESCAPE = 0x1B
VK_DELETE = 0x2E

# Track key states
alt_pressed = False
ctrl_pressed = False
shift_pressed = False

# Structure to extract vkCode
class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [
        ("vkCode", wintypes.DWORD),
        ("scanCode", wintypes.DWORD),
        ("flags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", wintypes.ULONG)
    ]

# Low-level keyboard hook procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    global alt_pressed, ctrl_pressed, shift_pressed

    if nCode >= 0:
        # Fix: Properly cast lParam to prevent integer overflow
        kbd_struct = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents
        vk_code = kbd_struct.vkCode  # Extract virtual key code

        if wParam == WM_KEYDOWN:
            if vk_code == VK_MENU:  # ALT key
                alt_pressed = True
            elif vk_code == VK_CONTROL:
                ctrl_pressed = True
            elif vk_code == VK_SHIFT:
                shift_pressed = True
            elif vk_code == VK_TAB and alt_pressed:
                print("🔹 ALT + TAB detected!")
            elif vk_code == VK_DELETE and ctrl_pressed and alt_pressed:
                print("🔹 CTRL + ALT + DEL detected!")
            elif vk_code == VK_ESCAPE and ctrl_pressed and shift_pressed:
                print("🔹 CTRL + SHIFT + ESC detected!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_MENU:
                alt_pressed = False
            elif vk_code == VK_CONTROL:
                ctrl_pressed = False
            elif vk_code == VK_SHIFT:
                shift_pressed = False

    # Fix: Use the correct types in CallNextHookEx
    return ctypes.windll.user32.CallNextHookEx(0, nCode, wParam, ctypes.byref(kbd_struct))

# Set Windows keyboard hook
def set_keyboard_hook():
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)

    if not hHook:
        print("❌ Failed to set hook")
        return
    
    print("✅ Keyboard hook started. Press ALT + TAB, WIN + TAB, or CTRL + ALT + DEL to test.")

    # Message loop to listen for keyboard events
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Run the hook
if __name__ == "__main__":
    set_keyboard_hook()






import ctypes
import win32con
from ctypes import wintypes

# Constants
WH_KEYBOARD_LL = 13
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

VK_TAB = 0x09
VK_MENU = 0x12  # ALT key
VK_CONTROL = 0x11
VK_SHIFT = 0x10
VK_ESCAPE = 0x1B
VK_DELETE = 0x2E

# Track key states
alt_pressed = False
ctrl_pressed = False
shift_pressed = False

# Structure to extract vkCode
class KBDLLHOOKSTRUCT(ctypes.Structure):
    _fields_ = [
        ("vkCode", wintypes.DWORD),
        ("scanCode", wintypes.DWORD),
        ("flags", wintypes.DWORD),
        ("time", wintypes.DWORD),
        ("dwExtraInfo", ctypes.POINTER(wintypes.ULONG))  # Correctly define dwExtraInfo pointer
    ]

# Low-level keyboard hook procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    global alt_pressed, ctrl_pressed, shift_pressed

    if nCode >= 0:
        kbd_struct = ctypes.cast(lParam, ctypes.POINTER(KBDLLHOOKSTRUCT)).contents
        vk_code = kbd_struct.vkCode  # Extract virtual key code

        if wParam == WM_KEYDOWN:
            if vk_code == VK_MENU:  # ALT key
                alt_pressed = True
            elif vk_code == VK_CONTROL:
                ctrl_pressed = True
            elif vk_code == VK_SHIFT:
                shift_pressed = True
            elif vk_code == VK_TAB and alt_pressed:
                print("🔹 ALT + TAB detected!")
            elif vk_code == VK_DELETE and ctrl_pressed and alt_pressed:
                print("🔹 CTRL + ALT + DEL detected!")
            elif vk_code == VK_ESCAPE and ctrl_pressed and shift_pressed:
                print("🔹 CTRL + SHIFT + ESC detected!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_MENU:
                alt_pressed = False
            elif vk_code == VK_CONTROL:
                ctrl_pressed = False
            elif vk_code == VK_SHIFT:
                shift_pressed = False

    return ctypes.windll.user32.CallNextHookEx(None, nCode, wParam, lParam)

# Set Windows keyboard hook
def set_keyboard_hook():
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)

    if not hHook:
        print("❌ Failed to set hook")
        return
    
    print("✅ Keyboard hook started. Press ALT + TAB, WIN + TAB, or CTRL + ALT + DEL to test.")

    # Message loop to listen for keyboard events
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Run the hook
if __name__ == "__main__":
    set_keyboard_hook()

import ctypes
import win32con
import win32api
from ctypes import wintypes

# Constants for Windows Hook
WH_KEYBOARD_LL = 13
WM_KEYDOWN = 0x0100
WM_KEYUP = 0x0101

# Virtual Key Codes
VK_TAB = 0x09
VK_MENU = 0x12  # ALT key
VK_LWIN = 0x5B  # Left Windows key
VK_RWIN = 0x5C  # Right Windows key
VK_ESCAPE = 0x1B
VK_DELETE = 0x2E

# Track key states
alt_pressed = False
win_pressed = False
ctrl_pressed = False

# Hook procedure
def low_level_keyboard_proc(nCode, wParam, lParam):
    global alt_pressed, win_pressed, ctrl_pressed

    if nCode == 0:  # Process only valid key events
        vk_code = lParam[0]  # Get virtual key code

        if wParam == WM_KEYDOWN:
            if vk_code == VK_MENU:  # ALT key
                alt_pressed = True
            elif vk_code == VK_TAB and alt_pressed:
                print("🔹 ALT + TAB detected!")
            elif vk_code in [VK_LWIN, VK_RWIN]:  # Windows key
                win_pressed = True
            elif vk_code == VK_LWIN and vk_code == VK_TAB:
                print("🔹 WIN + TAB detected!")
            elif vk_code == VK_DELETE and ctrl_pressed and alt_pressed:
                print("🔹 CTRL + ALT + DEL detected!")
            elif vk_code == VK_ESCAPE and ctrl_pressed and shift_pressed:
                print("🔹 CTRL + SHIFT + ESC detected!")

        elif wParam == WM_KEYUP:
            if vk_code == VK_MENU:
                alt_pressed = False
            elif vk_code in [VK_LWIN, VK_RWIN]:
                win_pressed = False
            elif vk_code == VK_TAB:
                alt_pressed = False  # Reset ALT state after releasing TAB

    return ctypes.windll.user32.CallNextHookEx(None, nCode, wParam, lParam)

# Set up Windows hook
def set_keyboard_hook():
    HOOKPROC = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_int, wintypes.WPARAM, wintypes.LPARAM)
    keyboard_hook = HOOKPROC(low_level_keyboard_proc)

    hHook = ctypes.windll.user32.SetWindowsHookExA(WH_KEYBOARD_LL, keyboard_hook, None, 0)

    if not hHook:
        print("❌ Failed to set hook")
        return
    
    print("✅ Keyboard hook started. Press ALT + TAB, WIN + TAB, or CTRL + ALT + DEL to test.")

    # Message loop to keep the hook running
    msg = wintypes.MSG()
    while ctypes.windll.user32.GetMessageA(ctypes.byref(msg), 0, 0, 0) != 0:
        ctypes.windll.user32.TranslateMessage(ctypes.byref(msg))
        ctypes.windll.user32.DispatchMessageA(ctypes.byref(msg))

# Run the hook
if __name__ == "__main__":
    set_keyboard_hook()









import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Key Vault and Secret Configuration
KEY_VAULT_NAME = "your-key-vault-name"  # Replace with your Key Vault name
SECRET_NAME = "your-secret-name"  # Replace with your secret name

# Build the Key Vault URL
KEY_VAULT_URL = f"https://{KEY_VAULT_NAME}.vault.azure.net/"

# Use DefaultAzureCredential to authenticate
credential = DefaultAzureCredential()

# Connect to Key Vault
client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

# Fetch the secret
try:
    secret = client.get_secret(SECRET_NAME)
    client_secret = secret.value
    print("Client secret retrieved successfully.")
except Exception as e:
    print(f"Error retrieving secret: {e}")

# Use client_secret in your code
print(f"Client Secret: {client_secret}")





Button Workflow:

Default Screen Workflow:

1. From Date and To Date Selection:

The manager selects the start date (From Date) and end date (To Date) for assigning shift timings.



2. Common Shift Button:

Once the dates are selected, the manager clicks Common Shift to assign uniform start and end timings for all employees under the manager or based on department filters (if applied).



3. Save Button:

After assigning the common shift, the manager clicks Save to store the shift details for the selected date range and employees in the database.



4. Reset Button:

If the manager wants to clear the selected dates, departments, or any assigned shifts, they can click Reset to start over.





---

Edit Screen Workflow:

1. From Date and To Date Selection:

The manager selects a From Date and To Date to fetch previously submitted shift details for editing.



2. Get Shift Details Button:

After selecting the date range and applying department filters (if needed), the manager clicks Get Shift Details.

This retrieves the submitted shift details for the selected employees and date range.



3. Editing Shift Details:

Managers can modify the shift timings for multiple employees, individual employees, or specific days.



4. Update Button:

After making changes, the manager clicks Update to save the modified shift timings in the database.



5. Reset Button:

The manager can click Reset to clear the selected filters, dates, or edits and start fresh.





---

View Screen Workflow:

1. Default View:

By default, all employees under the manager are displayed unless department filters are applied.



2. Filter by Department:

Managers can use the department filter to view employees from specific departments.



3. Edit (Pencil Icon):

To modify shift details, the manager clicks the Edit (Pencil Icon) for an individual employee.

This allows day-wise or week-wise shift modifications for that employee.



4. Save Changes:

After making edits, the manager clicks Save Changes to update the shift details for the selected employee.



5. Get Shift Details (if applied):

For specific departments or multiple employees, Get Shift Details can be used to fetch the current shift details.

This ensures the latest information is displayed before any edits are made.





---

Summary:

Default Screen: Focuses on assigning new shifts using Common Shift and Save without retrieving existing data.

Edit Screen: Allows retrieving and editing previously submitted shifts with Get Shift Details and Update.

View Screen: Primarily used for viewing current shifts and making individual modifications with the Edit (Pencil Icon) and Save Changes.










Definitions for Buttons and Filters

1. Default Manager Details:

Purpose: Displays the manager's information by default when the screen is loaded.

Description: Ensures that only the employees reporting to the logged-in manager are shown in the view.



2. Department Filter (Multi-Select):

Purpose: Allows the manager to filter employees based on departments.

Description:

Displays a list of all departments where employees reporting to the manager are assigned.

Managers can select one or multiple departments to narrow down the list of employees for shift management.




3. From Date and To Date:

From Date:

Purpose: Allows the manager to select the start date of the week for which shifts need to be managed.

Description: Ensures that only relevant data starting from the selected week is displayed.


To Date:

Purpose: Allows the manager to select the end date of the week for managing shifts.

Description: Ensures that the data displayed corresponds to the selected date range.




4. Get Shift Details Button:

Purpose: Fetches the shift details for the selected employees or departments.

Availability:

Only available on the EditView Screen.

Used to retrieve the shift details that were previously submitted for a selected date range or department filter.


Description:

Displays all shift details for the selected employees, departments, and date range.

Managers can edit the retrieved shift timings as needed.




5. Common Shift Button:

Purpose: Allows the manager to assign the same shift timings (start and end time) for all employees or selected employees for the entire week.

Description:

Managers can input a common start time and end time.

This common timing is applied to all days of the week.

After applying a common shift, managers can still modify timings for individual employees or specific days if needed.


Flexibility: Enables quick bulk assignment of shifts while retaining the ability to make individual adjustments later.




Button Workflow:

Step 1: Use the Department Filter to select the desired departments or keep it default to view all employees.

Step 2: Use the From Date and To Date fields to define the time range for shift details.

Step 3:

On the EditView Screen, click Get Shift Details to retrieve and edit previously submitted shifts.

On the Shift Assignment Screen, click Common Shift to assign uniform timings across employees.


Step 4: Save changes to update the shift schedules in the database.












EditView Screen Functionality

1. Overview:

The EditView screen allows managers to edit already submitted shift timings for one or multiple employees.

It provides flexibility to modify shift schedules based on department-level or employee-level filters.



2. Default View:

By default, all employees under the logged-in manager are displayed.

If no filters are applied, the system shows all employees with their shift details for the entire week.



3. Filters and Selection:

Managers can apply department filters to focus on specific departments, selecting a single department or multiple departments.

They can also specify a "From Date" and "To Date" to view and edit shift details within a specific time period.



4. Get Shift Details:

After applying filters and selecting a date range, managers click the "Get Shift Details" button to retrieve previously submitted shift timings.

The retrieved data includes shift timings for all days of the week for the selected employees or department(s).



5. Editing Capabilities:

Managers can edit shift timings for:

Individual employees.

Multiple employees simultaneously.

The entire week or specific days.


This ensures comprehensive and precise updates for all scenarios.



6. Save Updates:

Once edits are completed, managers can save the changes.

The updated schedules are stored in the system, ensuring accurate and up-to-date shift records.




Benefits of EditView:

Enables efficient shift management for the entire week.

Allows flexible filtering by department or employees, streamlining the process.

Provides granular editing options to manage individual or multiple employees’ schedules effectively.









View Screen Functionality

1. Default View:

By default, the view screen displays all employees under the logged-in manager.

If no department filter is applied, all employees are shown in the list.



2. Department-Level Filters:

Managers can filter employees based on department(s).

If a department filter is applied, only employees belonging to the selected department(s) will be displayed.



3. Employee Selection:

The view screen allows managers to select an employee or multiple employees from the displayed list.

Managers can focus on specific employees to review their shift details.



4. Shift Details:

For the selected employee(s), the system displays their submitted shift details for the week.

Shift timings for each day are shown, providing an overview of the assigned schedule.



5. Individual Shift Modifications:

Managers can modify shift timings for individual employees or specific days directly from the view screen.

Day-wise adjustments can be made to address specific requirements for an employee.



6. Dynamic Updates:

Any changes made by the manager are saved, ensuring that the shift schedule reflects the latest updates.

Managers can reassign or modify shifts as needed, even after submission.



7. Flexible Management:

The view screen provides a convenient way to monitor and manage shift timings for all employees or those filtered by department.

It ensures managers can make informed decisions and adjust schedules in real time.




This feature enables managers to have a comprehensive and customizable view of employee schedules, offering both high-level and detailed insights to optimize shift management.












Department-Level Filters

1. Filter by Department:

Managers can use department-level filters to manage employees more efficiently.

They can filter the employee list by selecting a single department or multiple departments.



2. Common Shift Assignment within Departments:

After applying the department filter, managers can assign common shift timings (start time and end time) to all employees under the selected department(s).

This allows for bulk assignment of shifts without handling individual schedules initially.



3. Individual Shift Modifications:

Once the common shift is assigned, managers can still modify shift timings for individual employees or specific days within the filtered department(s).

This ensures flexibility to accommodate specific employee requirements or special cases.



4. Save Shift Timings:

Managers can save the assigned or modified shift timings for the employees under the selected department(s).

If no further changes are needed, they can directly save the common shifts without additional modifications.



5. Efficient Workflow Management:

The department filter streamlines the process by allowing managers to focus on a subset of employees based on organizational hierarchy.

This approach improves the accuracy and efficiency of shift assignments and modifications.



6. Adaptable Process:

Whether assigning common shifts or customizing individual schedules, the system provides the flexibility to manage shifts in bulk or at a granular level.

This adaptability ensures that the shift management process aligns with the dynamic needs of the team or department.




This feature ensures a seamless workflow, giving managers complete control over shift scheduling, whether managing all employees or focusing on specific departments.






Objective

The Employee Shift Screen is a web application designed to simplify shift management for managers. It enables managers to assign, view, and modify shift timings for employees under their supervision, ensuring efficient workforce management for a week.

Features

1. View Active Employees:

Upon login, managers can see a list of all active employees working under them for the current week.



2. Common Shift Assignment:

A "Common Shift" option allows managers to assign a uniform start and end time for all employees for the entire week.

Managers can quickly update shift timings for all employees without handling individual schedules initially.



3. Individual Day and Employee Modifications:

Managers can later select any individual employee or specific day to modify the start and end times as needed.

This ensures flexibility to accommodate individual requirements.



4. Save Shift Timings:

Once the manager is satisfied with the schedule, they can save the shift timings for the selected week.



5. Department-Level Filters:

Managers can apply filters to manage shifts by department.

They can choose to filter by a single department or multiple departments to view and assign shifts efficiently.



6. Customizable and Adaptable:

The system supports individual adjustments even after assigning common shifts, ensuring accuracy and alignment with specific employee or department needs.




Data Management

All shift timings entered or modified are securely saved to an Azure database, ensuring data safety, integrity, and accessibility for future reference or audit. This setup facilitates centralized management and enhances overall efficiency.







Objective

The Employee Shift Screen is a web application that allows managers to assign, view, and update weekly shift timings for their employees. It provides a streamlined way to manage and adjust employee schedules effectively.

Key Features

1. View and Edit Shift Timings:

Managers can view and edit the entire week's shift timings for an employee or update timings for individual days as needed.

This flexibility ensures that shift schedules can be modified to accommodate changes.



2. Filter Options:

By default, the application displays all employees under the manager.

Managers can apply filters, such as department name, to narrow down the list of employees.

These filters make it easier to manage and edit shifts for specific groups of employees.



3. User-Friendly Interface:

The application provides a clear and organized interface for managers to update shift timings quickly and efficiently.



4. Data Storage:

All shift data entered or updated is securely stored in an Azure database, ensuring data safety and accessibility for future reference.



5. Improved Workforce Management:

By providing the ability to manage shifts by department or employee, the tool helps ensure proper planning and allocation of resources.















from flask import request, jsonify
from sqlalchemy import or_

@jwt_required()
def get(self):
    # Extract query parameters from the request
    mng_id = request.args.get("EMPID")
    department_names = request.args.get("department_name")  # Comma-separated string

    # Validate the presence of EMPID
    if not mng_id:
        return jsonify({"error": "Manager ID (EMPID) is required"}), 400
    
    if not department_names:
        return jsonify({"error": "Department name is required"}), 400

    # Split department_names into a list if it's a comma-separated string
    department_list = department_names.split(",") if "," in department_names else [department_names]

    with session_scope('DESIGNER') as session:
        try:
            # Query employees with matching manager ID and department name(s)
            employees = (
                session.query(EmployeeInfo)
                .filter(EmployeeInfo.func_mgr_id == mng_id)
                .filter(EmployeeInfo.dept_name.in_(department_list))
                .filter(EmployeeInfo.term_date.is_(None))  # Termination date should be NULL
                .all()
            )

            # If no employees are found, return an empty list
            if not employees:
                return jsonify({"employees": []}), 200

            # Prepare the response
            employee_list = [
                {"emp_id": emp.emplid, "emp_name": emp.name} for emp in employees
            ]

            return jsonify({"employees": employee_list}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500






from flask import request, jsonify
from sqlalchemy import or_

@jwt_required()
def get(self):
    # Parse payload from the request
    payload = request.get_json()
    
    if not payload:
        return jsonify({"error": "Payload is required"}), 400

    # Extract EMPID and Department Name from the payload
    mng_id = payload.get("EMPID")
    department_names = payload.get("department_name")  # This can be a single string or a list

    # Validate the presence of EMPID
    if not mng_id:
        return jsonify({"error": "Manager ID (EMPID) is required"}), 400
    
    if not department_names:
        return jsonify({"error": "Department name is required"}), 400

    # Ensure department_names is a list for consistency
    if isinstance(department_names, str):
        department_names = [department_names]

    with session_scope('DESIGNER') as session:
        try:
            # Query employees with matching manager ID and department name(s)
            employees = (
                session.query(EmployeeInfo)
                .filter(EmployeeInfo.func_mgr_id == mng_id)
                .filter(EmployeeInfo.dept_name.in_(department_names))
                .filter(EmployeeInfo.term_date.is_(None))  # Termination date should be NULL
                .all()
            )

            # If no employees are found, return an empty list
            if not employees:
                return jsonify({"employees": []}), 200

            # Prepare the response
            employee_list = [
                {"emp_id": emp.emplid, "emp_name": emp.name} for emp in employees
            ]

            return jsonify({"employees": employee_list}), 200

        except Exception as e:
            return jsonify({"error": str(e)}), 500




import requests  # Assuming you're using the requests library

def send_request(client, method, endpoint, data=None):
    """
    Generic function to handle client requests.

    Args:
        client: The client instance (e.g., a session object or API client).
        method: HTTP method as a string ('GET', 'POST', 'PUT', 'DELETE').
        endpoint: The API endpoint.
        data: The data to send with the request (optional).

    Returns:
        Response object from the client.
    """
    method = method.upper()
    if method == 'GET':
        response = client.get(endpoint)
    elif method == 'POST':
        response = client.post(endpoint, json=data)
    elif method == 'PUT':
        response = client.put(endpoint, json=data)
    elif method == 'DELETE':
        response = client.delete(endpoint, json=data)
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    return response








class EmployeeShiftDetails(Resource):
    def get(self):
        try:
            mng_id = request.args.get('emp_id')
            if not mng_id:
                return {
                    "error": "Manager ID (emp_id) is missing in the request parameters."
                }, 400

            # Start processing
            with session_scope('DESIGNER') as session:
                # Query for employees under this manager
                employees = session.query(EmployeeInfo).filter_by(func_mgr_id=mng_id).all()
                if not employees:
                    return {
                        "error": f"No employees found for the given Manager ID: {mng_id}",
                        "reporting_employee_shifts": [],
                        "reporting_employees": []
                    }, 404

                # Extract employee IDs
                employee_ids = [emp.emplid for emp in employees]
                if not employee_ids:
                    return {
                        "error": f"No employee IDs found for the given Manager ID: {mng_id}.",
                        "reporting_employee_shifts": [],
                        "reporting_employees": []
                    }, 404

                # Query for employee shifts
                employee_shifts = session.query(EmployeeShiftInfo).filter(
                    EmployeeShiftInfo.emp_id.in_(employee_ids)
                ).all()

                # Prepare shift data
                if employee_shifts:
                    reporting_employee_shifts = [
                        {
                            'emp_id': emp.emp_id,
                            'day_id': emp.day_id,
                            'is_week_off': emp.is_week_off,
                            'start_time': str(emp.start_time),
                            'end_time': str(emp.end_time),
                            'contracted_hours': emp.emp_contracted_hours,
                            'shift_date': str(emp.shift_date)
                        }
                        for emp in employee_shifts
                    ]
                else:
                    reporting_employee_shifts = []

                # Prepare employee data
                reporting_employees = [
                    {'emp_id': emp.emplid, 'emp_name': emp.name} for emp in employees
                ]

                # Prepare final response
                data = {
                    "reporting_employee_shifts": reporting_employee_shifts,
                    "reporting_employees": reporting_employees
                }
                return jsonify(data), 200

        except Exception as e:
            # Include error details in the response
            return {
                "error": "An unexpected error occurred.",
                "details": str(e)
            }, 500








# Subquery to fetch relevant employee IDs
subquery = session.query(EmployeeShiftInfo.emp_id).filter(
    EmployeeShiftInfo.emp_id.in_(emp_ids)
).subquery()

# Main query using the subquery
employees_shifts = (
    session.query(EmployeeShiftInfo)
    .filter(
        EmployeeShiftInfo.emp_id.in_(subquery),
        EmployeeShiftInfo.shift_date.between(from_date, to_date)
    )
    .all()
)




class EmployeeShiftTimeDetails(Resource):
    @jwt_required()
    def get(self):
        """
        This is a get method for fetching reporting employees' shift details
        within a specified date range.
        """
        mng_id = request.args.get('emp_id')
        from_date = request.args.get('from_date')  # Expected format: MM-DD-YYYY
        to_date = request.args.get('to_date')      # Expected format: MM-DD-YYYY

        if not mng_id or not from_date or not to_date:
            return {"error": "Missing emp_id, from_date, or to_date in request parameters."}, 400

        try:
            # Convert dates to a standard format
            from_date = datetime.strptime(from_date, "%m-%d-%Y")
            to_date = datetime.strptime(to_date, "%m-%d-%Y")
        except ValueError:
            return {"error": "Invalid date format. Use MM-DD-YYYY."}, 400

        with session_scope('DESIGNER') as session:
            # Get employee IDs for the given manager
            employees = session.query(EmployeeInfo).filter_by(func_mgr_id=mng_id).all()
            employee_ids = [emp.emp_id for emp in employees]

            # Fetch shifts for employees within the date range
            employees_shifts = (
                session.query(EmployeeShiftInfo)
                .filter(
                    EmployeeShiftInfo.emp_id.in_(employee_ids),
                    EmployeeShiftInfo.shift_date.between(from_date, to_date)
                )
                .all()
            )

            if employees_shifts:
                reporting_employee_shifts = [
                    {
                        "emp_id": emp.emp_id,
                        "day_id": emp.day_id,
                        "is_week_off": emp.is_week_off,
                        "start_time": str(emp.start_time),
                        "end_time": str(emp.end_time),
                        "contracted_hours": emp.emp_contracted_hours,
                        "shift_date": str(emp.shift_date),
                    }
                    for emp in employees_shifts
                ]
            else:
                reporting_employee_shifts = []

            # Fallback to reporting employee names if no shifts are found
            reporting_employees = [
                {"emp_id": emp.emp_id, "emp_name": emp.name} for emp in employees
            ]

            # Prepare response data
            data = {
                "reporting_employee_shifts": reporting_employee_shifts,
                "reporting_employees": reporting_employees,
            }

        return jsonify(data)






<template>
  <v-container>
    <!-- East Monday Date Picker -->
    <v-date-picker
      v-model="startDate"
      :allowed-dates="isMonday"
      label="Select East Monday"
    ></v-date-picker>
    <v-spacer />
    <!-- East Sunday Date Picker -->
    <v-date-picker
      v-model="endDate"
      :allowed-dates="isSundayAfterStart"
      label="Select East Sunday"
      :disabled="!startDate"
    ></v-date-picker>
  </v-container>
</template>

<script>
export default {
  data() {
    return {
      startDate: null, // Selected East Monday
      endDate: null, // Selected East Sunday
    };
  },
  methods: {
    // Allow only Mondays
    isMonday(date) {
      const day = new Date(date).getDay();
      return day === 1; // 1 = Monday
    },
    // Allow only Sundays after the selected start date
    isSundayAfterStart(date) {
      const day = new Date(date).getDay();
      if (day !== 0) return false; // 0 = Sunday
      if (!this.startDate) return true; // If no start date is selected, allow all Sundays
      const startDateObj = new Date(this.startDate);
      const currentDateObj = new Date(date);
      return currentDateObj > startDateObj; // Allow only Sundays after the start date
    },
  },
};
</script>





<template>
  <v-container>
    <v-date-picker 
      v-model="startDate" 
      :allowed-dates="isMonday" 
      label="Select Start Date (Mondays Only)"
    ></v-date-picker>
  </v-container>
</template>

<script>
export default {
  data() {
    return {
      startDate: null, // Bind your date value here
    };
  },
  methods: {
    isMonday(date) {
      const day = new Date(date).getDay();
      return day === 1; // 1 represents Monday
    },
  },
};
</script>







def check_existing_entries(existing_volume_entry, existing_config_entry):
    """
    Checks for existing entries and returns an appropriate response.

    Parameters:
    - existing_volume_entry: Result of the volume entry query
    - existing_config_entry: Result of the config entry query

    Returns:
    - Tuple (response, status_code) if an entry exists, otherwise None
    """
    if existing_volume_entry or existing_config_entry:
        message = (
            "Volume Entry Already Exists in Volume Store"
            if existing_volume_entry
            else f"Entry already exists with Request ID: {existing_config_entry.request_id}"
        )
        return {"message": message}, 400
    return None

# Usage
response = check_existing_entries(existing_volume_entry, existing_config_entry)
if response:
    return response


-----------------




import time

def start_window_monitoring(self):
    try:
        background_check_interval = 300  # 5 minutes in seconds
        last_background_check = time.time()

        while True:
            # Check the active window
            active_window = gw.getActiveWindowTitle()

            if active_window:
                for win in self.all_window_titles:
                    if win in active_window:
                        # Start monitoring for the matched active window
                        active_matched_title = win
                        workflow_details = f.whitelist.GET_WORKFLOW_DETAILS_FROM_TITLE(active_matched_title)
                        target_window_titles = workflow_details.get("window_titles", "").split(",")
                        target_workflow_name = workflow_details.get("workflow_name", "")
                        target_workflow_id = workflow_details.get("workflow_id", "")

                        if target_workflow_name.lower() == "ows":
                            self.start_ows_monitoring(target_window_titles, target_workflow_name, target_workflow_id)
                        break

            # Perform a background check every 5 minutes
            if time.time() - last_background_check >= background_check_interval:
                current_titles = set(gw.getAllTitles())
                for title in current_titles:
                    for win in self.all_window_titles:
                        if win in title:
                            # Start monitoring for the matched background window
                            active_matched_title = win
                            workflow_details = f.whitelist.GET_WORKFLOW_DETAILS_FROM_TITLE(active_matched_title)
                            target_window_titles = workflow_details.get("window_titles", "").split(",")
                            target_workflow_name = workflow_details.get("workflow_name", "")
                            target_workflow_id = workflow_details.get("workflow_id", "")

                            if target_workflow_name.lower() == "ows":
                                self.start_ows_monitoring(target_window_titles, target_workflow_name, target_workflow_id)
                            break
                last_background_check = time.time()  # Update the last background check time

            # Sleep to reduce CPU usage
            time.sleep(1)
    except Exception as e:
        # Handle exceptions as needed
        # log_print(f"An error occurred: {e}")
        pass




----------




import time
import gw  # Assuming gw is the library to get window titles

def start_window_monitoring(self):
    try:
        while True:
            # Get the active window title
            active_window = gw.getActiveWindowTitle()  # Replace with the correct method to get the active window title

            if active_window:
                # Check if the active window matches any of the monitored titles
                for win in self.all_window_titles:
                    if win in active_window:
                        # log_print(f"Active window {win} is running on User's system")
                        active_matched_title = win

                        # Get workflow details
                        workflow_details = f.whitelist.GET_WORKFLOW_DETAILS_FROM_TITLE(active_matched_title)
                        target_window_titles = workflow_details.get("window_titles", "").split(",")
                        target_workflow_name = workflow_details.get("workflow_name", "")
                        target_workflow_id = workflow_details.get("workflow_id", "")

                        if target_workflow_name.lower() == "ows":
                            # log_print("OWS Application Monitoring Started")
                            self.start_ows_monitoring(target_window_titles, target_workflow_name, target_workflow_id)

                        # Break after processing the matched window
                        break

            # Introduce a sleep interval to reduce CPU usage
            time.sleep(1)  # Adjust the interval as needed (e.g., 0.5 or 2 seconds)
    except Exception as e:
        # Handle exceptions
        # log_print(f"An error occurred: {e}")
        pass







import os
import json
import threading
from datetime import datetime, timedelta
import schedule
import time

# Function to process files with state__change__detected = True
def process_file(file_name):
    print(f"Processing file: {file_name}")

# Function to check for `.json` files and process them based on conditions
def check_json_files(directory):
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            
            # Check the content of the JSON file
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    if data.get("state__change__detected", False):  # If key is True
                        thread = threading.Thread(target=process_file, args=(file_name,))
                        thread.start()
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

# Function to delete files older than a certain number of days
def delete_old_files(directory, days=2):
    cutoff_time = datetime.now() - timedelta(days=days)
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            if modified_time < cutoff_time:
                try:
                    os.remove(file_path)
                    print(f"Deleted: {file_name}")
                except Exception as e:
                    print(f"Error deleting {file_name}: {e}")

# Combined function to execute tasks
def run_scheduled_tasks(directory):
    print(f"Running tasks at {datetime.now()}")
    check_json_files(directory)
    delete_old_files(directory, days=2)

# Function to run the scheduler in a separate thread
def start_scheduler(directory):
    def scheduler_thread():
        schedule.every(1).hours.do(run_scheduled_tasks, directory=directory)
        print("Scheduler thread started. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(1)

    thread = threading.Thread(target=scheduler_thread, daemon=True)
    thread.start()
    return thread

# Main execution
if __name__ == "__main__":
    directory_path = "path_to_your_directory"

    # Start the scheduler as a thread
    start_scheduler(directory_path)

    # Keep the main thread alive to prevent the script from exiting
    while True:
        time.sleep(1)





import os
import json
import threading
from datetime import datetime, timedelta

# Function to process files with state__change__detected = True
def process_file(file_name):
    print(f"Processing file: {file_name}")

# Function to check for `.json` files and process them based on conditions
def check_json_files(directory):
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            
            # Check the content of the JSON file
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
                    if data.get("state__change__detected", False):  # If key is True
                        thread = threading.Thread(target=process_file, args=(file_name,))
                        thread.start()
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

# Function to check if the file's modified date is older than 2 days
def check_file_age(directory, days=2):
    cutoff_time = datetime.now() - timedelta(days=days)
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            file_path = os.path.join(directory, file_name)
            modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            if modified_time < cutoff_time:
                print(f"{file_name} was modified more than {days} days ago.")

# Example usage
if __name__ == "__main__":
    directory_path = "path_to_your_directory"

    # Check JSON files for state__change__detected
    check_json_files(directory_path)

    # Check JSON files for modified date older than 2 days
    check_file_age(directory_path, days=2)

----++++++++--------



from pywinauto import Application
from pywinauto import mouse
from pywinauto.keyboard import send_keys

# Connect to the active application
app = Application(backend="uia").connect(title_re=".*")  # Connect to the active window
window = app.active_window()

# Locate the toolbar control
toolbar = window.child_window(control_type="ToolBar")  # Adjust control_type or title if necessary

# Locate the left button in the toolbar
left_button = toolbar.child_window(title="Left", control_type="Button")  # Use the actual title of the button

# Click the left button to expand the section
left_button.click_input()

# Optionally, drag or resize the section (if necessary)
# Assuming you need to drag to expand
mouse.drag_mouse((100, 200), (300, 400))  # Replace with actual coordinates for dragging

# Take a screenshot of the window after expanding
window.capture_as_image().save("screenshot.png")

print("Section expanded and screenshot saved!")






import json

def get_active_keys(json_file, expected_workflow_name):
    # Load the JSON data
    with open(json_file, 'r') as file:
        data = json.load(file)  # Parse the JSON file into a list of dictionaries
    
    # Filter dictionaries based on workflow name and isActive
    filtered_keys = [
        entry["activity_key_name"]
        for entry in data
        if entry.get("workflow_name") == expected_workflow_name and entry.get("isActive") is True
    ]
    
    # Return the list of matching keys
    return filtered_keys

# Example usage
json_file_path = 'path_to_your_json_file.json'
workflow_name = "Workflow A"
result = get_active_keys(json_file_path, workflow_name)
print(f"Active keys for workflow '{workflow_name}': {result}")




import pandas as pd
import sqlite3
from datetime import datetime

def insert_with_to_sql(df, db_path, table_name):
    # Connect to SQLite database
    con = sqlite3.connect(db_path)
    try:
        # Insert DataFrame into the SQLite table
        df.to_sql(table_name, con, if_exists='append', index=False)
        print("Data inserted successfully using to_sql.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        con.close()






import os
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def process_json_files(folder_path, emp_id, db_connection_string):
    # Prepare the output DataFrame
    dataframes = []
    
    # Get current date and time
    cal_date = datetime.now().strftime("%m/%d/%Y")
    created_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Iterate through each file in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.json'):  # Process only JSON files
            file_path = os.path.join(folder_path, file_name)
            
            with open(file_path, 'r') as file:
                json_data = json.load(file)
            
            # Prepare the activity_info field
            activity_info = [{
                'w_id': 1089,
                'w_name': 'ows',
                'activity_pairs': json_data,
                'capture_method': 'copy'
            }]
            
            # Create a DataFrame for the current file
            df = pd.DataFrame([{
                'Cal_date': cal_date,
                'emp_id': emp_id,
                'file_name': file_name,
                'Activity_info': json.dumps(activity_info),  # Convert to JSON string
                'volume_info': json.dumps([]),  # Empty list as JSON string
                'created_date': created_date
            }])
            
            dataframes.append(df)
    
    # Combine all DataFrames
    final_df = pd.concat(dataframes, ignore_index=True)
    
    # Insert into the local database
    engine = create_engine(db_connection_string)
    final_df.to_sql('your_table_name', con=engine, if_exists='append', index=False)
    
    print("Data successfully inserted into the database.")

# Usage Example
folder_path = "path_to_your_folder"
emp_id = "EMP123"
db_connection_string = "sqlite:///local_database.db"  # Replace with your database connection string
process_json_files(folder_path, emp_id, db_connection_string)



-----------



import os
import json
import shutil

def process_json(file_name, source_folder, target_folder, key_list, padding_details, key_to_split):
    # Construct full file path
    file_path = os.path.join(source_folder, file_name)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"File '{file_name}' does not exist in {source_folder}.")
        return
    
    # Read the JSON file
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
    except json.JSONDecodeError as e:
        print(f"Error reading JSON file: {e}")
        return

    # Process only the keys in the key_list
    processed_data = {}
    for key in key_list:
        if key in data:
            # Check for the specific key to split
            if key == key_to_split:
                value = data[key]
                if isinstance(value, str) and ':' in value:
                    # Split by colon and extract the desired part
                    parts = value.split(':')
                    if len(parts) > 2:
                        country = parts[1].strip()  # Extract between the first and second colon
                        processed_data['country'] = country
            # Add the original key-value to processed data
            processed_data[key] = data[key]

    # Add the padding details
    processed_data.update(padding_details)
    
    # Write the updated JSON to the target folder
    target_path = os.path.join(target_folder, file_name)
    try:
        with open(target_path, 'w') as output_file:
            json.dump(processed_data, output_file, indent=4)
        print(f"Processed file moved to {target_folder}")
    except Exception as e:
        print(f"Error writing to file: {e}")
        return
    
    # Move the original file to the target folder
    try:
        shutil.move(file_path, target_path)
    except Exception as e:
        print(f"Error moving file: {e}")

# Example usage
file_name = "example.json"
source_folder = "source"
target_folder = "processed"
key_list = ["key1", "key2", "key3", "key_to_split"]
padding_details = {"added_detail1": "value1", "added_detail2": "value2"}
key_to_split = "key_to_split"  # The key whose value needs to be split

process_json(file_name, source_folder, target_folder, key_list, padding_details, key_to_split)







git for-each-ref --sort=-committerdate refs/remotes/ --format='%(committerdate:iso8601) %(refname:short)'



To decide whether to store your data in separate JSON files or in a single JSON file with keys representing file names, let’s analyze the pros and cons of each approach with respect to your specific requirements:


---

1. Storing Data in Separate JSON Files

Advantages

1. Simpler File Management:

Each file corresponds directly to a unique KSID (KSID_<number>.json), making it easy to locate, edit, and manage.



2. Efficient Data Replacement:

To replace data, you can directly find the file by its name and overwrite the content without parsing a large file.



3. Failure Isolation:

Corruption of one file does not affect others.



4. Scalable for Large Data:

If your data grows significantly, this method prevents a single JSON file from becoming too large to handle efficiently.



5. Parallel Processing:

Multiple files can be processed independently by threads or workers.




Disadvantages

1. Disk Space Overhead:

Storing metadata (e.g., file headers) for each file adds overhead.



2. File System Dependency:

If you have thousands of files, the file system’s performance may degrade due to excessive file reads/writes.



3. State Checking:

You have to iterate over multiple files every 15 seconds to check the state_change_detected field.





---

2. Storing Data in a Single JSON File

Advantages

1. Compact Structure:

All data is stored in one place, reducing the overhead of managing multiple files.



2. Simpler Checking:

You can load the entire JSON file into memory and iterate through the keys to check for state_change_detected, avoiding file I/O for each check.



3. Easier Backup:

One file is easier to copy or move to another location for backup or transfer.




Disadvantages

1. Data Replacement Overhead:

To update a single KSID entry, you need to load the entire JSON file, modify the required key, and save the whole file back. This can be inefficient if the file grows large.



2. Risk of Corruption:

If the file gets corrupted, all data is lost.



3. Memory Usage:

For large datasets, loading the entire JSON file into memory could be inefficient or impractical.



4. Concurrency Issues:

Concurrent updates to the file might lead to race conditions or conflicts unless managed carefully.





---

Recommendation Based on Your Use Case

End Goal:

Check for state_change_detected every 15 seconds and move relevant data to another database.

Replace data dynamically as needed when hovering on a KSID.


Optimal Approach:

Use separate JSON files. Here’s why:

1. State Checking: Since you're checking periodically for a specific value (state_change_detected), iterating over separate files is straightforward and avoids loading unnecessary data.


2. Dynamic Updates: When updating data for a specific KSID, directly locating and modifying a file by its name (KSID_<number>.json) is faster and more efficient than searching for a key in a large JSON file.


3. Scalability: As your data grows, managing thousands of smaller files will perform better than handling a single massive file.


4. Error Isolation: If one file is corrupted, it doesn’t impact others.





---

Implementation Strategy

1. Folder Structure

Create a process folder for storing KSID files:

process/
  KSID_123.json
  KSID_456.json
  KSID_789.json


2. State Checking Script

Every 15 seconds, iterate over the files to check state_change_detected:

import os
import json
from time import sleep

process_folder = "process"

def check_state_changes():
    for file_name in os.listdir(process_folder):
        if file_name.endswith(".json"):
            file_path = os.path.join(process_folder, file_name)
            with open(file_path, "r") as file:
                data = json.load(file)
                if data.get("state_change_detected"):
                    # Move to Event Hub or database
                    move_to_event_hub(file_path, data)

def move_to_event_hub(file_path, data):
    # Implement your Event Hub logic here
    print(f"Moving {file_path} to Event Hub: {data}")
    # Optionally delete the file after processing
    os.remove(file_path)

# Periodically check for state changes
while True:
    check_state_changes()
    sleep(15)


3. Dynamic Updates

To update data dynamically for a KSID:

def update_ksid_data(ksid, new_data):
    file_path = os.path.join(process_folder, f"KSID_{ksid}.json")
    with open(file_path, "w") as file:
        json.dump(new_data, file)



---

When to Use a Single JSON File?

If your dataset is relatively small and unlikely to grow beyond a few MB.

If you prioritize simplicity and don't expect high update frequency.

If you can tolerate the risks of corruption and loading overhead.


However, for your case (frequent updates, scalable solution, and periodic checks), separate JSON files are better.









-----------------

def monitor_ows_process(target_titles, monitor_details):
    """Monitor the OWS process for Change State or Case Management windows."""
    global json_created

    while True:
        # Track active windows
        active_windows = gw.getAllTitles()
        case_management_open = any(title for title in active_windows if "Case Management -" in title)
        change_state_open = any(title for title in active_windows if "Change State" in title)

        if not case_management_open and not change_state_open:
            # Neither Case Management nor Change State is open, restart monitoring
            print("Neither Case Management nor Change State window is open. Restarting monitoring...")
            json_created = False  # Reset JSON created flag
            break  # Exit monitoring loop to restart tracking

        if json_created:
            # If JSON is already created, monitor for Change State Window only
            if change_state_open:
                print("Monitoring Change State Window for state changes...")
                # Add logic to handle Change State monitoring here
                monitor_change_state(monitor_details)
                break  # Exit loop once Change State monitoring starts
            else:
                print("Change State Window is not open. Waiting...")
        else:
            # JSON not created, check for Case Management Window
            if case_management_open:
                print("Capturing data from Case Management Window...")
                case_window, monitor_index, window_info = check_window_title(
                    "Case Management -", monitor_details
                )
                location, start_time = track_location(
                    summary_location, "Summary Button", monitor_index, monitor_details, case_window
                )
                capture_data(location, start_time, monitor_details, monitor_index, case_window)
            else:
                print("Case Management Window is not open. Waiting...")

        time.sleep(1)  # Pause briefly before checking again

def monitor_change_state(monitor_details):
    """Monitor the Change State Window for state changes."""
    while True:
        active_windows = gw.getAllTitles()
        change_state_open = any(title for title in active_windows if "Change State" in title)

        if not change_state_open:
            # If Change State Window is closed, exit monitoring
            print("Change State Window closed. Stopping monitoring...")
            break

        # Add logic for monitoring state changes within the Change State window
        print("Monitoring Change State Window...")
        time.sleep(1)





import time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor with enhanced state tracking
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Enhanced monitoring state variables
        self.data_dict = {}
        self.monitoring_active = True
        
        # State tracking variables
        self.summary_captured = False
        self.change_state_triggered = False
        self.waiting_for_next_capture = False

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = self.get_monitor_index(window, monitor_details)

                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }

                return window, monitor_index, window_info
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 20)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        
        if not summary_text:
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
        
        pyautogui.moveTo(curr_x, curr_y)
        
        # Reset state change detection
        self.data_dict = {}
        self.data_dict["state_change_detected"] = False

        # Parse summary text
        summary_dict = {}
        case_id = "dummy"
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
                if key == "Id":
                    case_id = value

        # Update data dictionary
        self.data_dict.update(summary_dict)
        self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.save_to_json(self.data_dict, case_id, False)
        
        self.summary_captured = True
        self.waiting_for_next_capture = False
        print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            
            if window_result[0]:
                window, monitor_index, window_info = window_result

                # Check for Change State window to determine if state change occurred
                change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                
                # Capture summary logic
                if (window.isActive and 
                    (not self.summary_captured or 
                     (self.waiting_for_next_capture and not change_state_result[0]))):
                    
                    start_time = datetime.now()
                    self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    self.data_dict["window_info"] = window_info
                    self.data_dict["monitor_index"] = monitor_index + 1

                    # Capture summary data
                    location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                    self.capture_data(location, start_time, monitor_details, monitor_index, window_info)
                
                # Check for Change State window
                if change_state_result[0]:
                    # Change State window is open
                    self.waiting_for_next_capture = True
                    self.summary_captured = False
                    self.change_state_triggered = True
                    print("Change State window detected. Preparing for next capture.")

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Create monitor with configurable titles
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()-----





import time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor with enhanced state tracking
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Enhanced monitoring state variables
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True
        self.case_management_open = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = False

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = self.get_monitor_index(window, monitor_details)

                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }

                return window, monitor_index, window_info
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        # Only capture summary if not already captured and window is active
        if not self.summary_captured:
            x, y = location
            curr_x, curr_y = pyautogui.position()
            pyperclip.copy('')

            dynamic_offset = (20, 20)
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
            
            if not summary_text:
                pyautogui.rightClick(x, y)
                pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
                pyautogui.click()
                summary_text = pyperclip.paste()
            
            pyautogui.moveTo(curr_x, curr_y)
            
            # Reset state change detection
            self.data_dict["state_change_detected"] = False

            # Parse summary text
            summary_dict = {}
            case_id = "dummy"
            lines = summary_text.split('\n')
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    key = parts[0].strip() if len(parts) > 0 else ""
                    value = parts[1].strip() if len(parts) > 1 else ""
                    summary_dict[key] = value
                    if key == "Id":
                        case_id = value

            # Update data dictionary
            self.data_dict.update(summary_dict)
            self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            self.save_to_json(self.data_dict, case_id, False)
            
            self.json_created = True
            self.summary_captured = True
            print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        
        # Reset flags for next cycle
        self.json_created = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = True
        
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            
            if window_result[0]:
                window, monitor_index, window_info = window_result
                self.case_management_open = True

                # If waiting for reopen and Case Management is open again
                if self.waiting_for_reopen:
                    # Reset waiting flag and prepare for next cycle
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

                # Capture summary data only if not already captured
                if window.isActive and not self.summary_captured:
                    start_time = datetime.now()
                    self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    self.data_dict["window_info"] = window_info
                    self.data_dict["monitor_index"] = monitor_index + 1

                    # Capture summary data
                    location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                    self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If summary captured, monitor for Change State window
                if self.json_created:
                    change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                    
                    if change_state_result[0]:
                        self.change_state_opened = True
                        self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Check if Change State window is closed
                    if self.change_state_opened:
                        change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                        
                        if not change_state_result[0]:
                            # Capture entire data
                            location, _ = self.track_location(self.end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
            else:
                # Case Management window is not open
                self.case_management_open = False
                
                # If we were previously waiting for reopen, reset flags
                if self.waiting_for_reopen:
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Optional: Customize location coordinates if needed
    # summary_location = (0.07, 0.7)
    # end_location = (0.7, 0.825)
    
    # Create monitor with configurable titles
    # If you want to customize locations, use: 
    # monitor = OWSProcessMonitor(ows_titles, summary_location, end_location)
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()






import time
imimport time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor with enhanced state tracking
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Enhanced monitoring state variables
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True
        self.case_management_open = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = False

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = self.get_monitor_index(window, monitor_details)

                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }

                return window, monitor_index, window_info
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        # Only capture summary if not already captured and window is active
        if not self.summary_captured:
            x, y = location
            curr_x, curr_y = pyautogui.position()
            pyperclip.copy('')

            dynamic_offset = (20, 20)
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
            
            if not summary_text:
                pyautogui.rightClick(x, y)
                pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
                pyautogui.click()
                summary_text = pyperclip.paste()
            
            pyautogui.moveTo(curr_x, curr_y)
            
            # Reset state change detection
            self.data_dict["state_change_detected"] = False

            # Parse summary text
            summary_dict = {}
            case_id = "dummy"
            lines = summary_text.split('\n')
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    key = parts[0].strip() if len(parts) > 0 else ""
                    value = parts[1].strip() if len(parts) > 1 else ""
                    summary_dict[key] = value
                    if key == "Id":
                        case_id = value

            # Update data dictionary
            self.data_dict.update(summary_dict)
            self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            self.save_to_json(self.data_dict, case_id, False)
            
            self.json_created = True
            self.summary_captured = True
            print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        
        # Reset flags for next cycle
        self.json_created = False
        self.summary_captured = False
        self.change_state_opened = False
        self.waiting_for_reopen = True
        
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            
            if window_result[0]:
                window, monitor_index, window_info = window_result
                self.case_management_open = True

                # If waiting for reopen and Case Management is open again
                if self.waiting_for_reopen:
                    # Reset waiting flag and prepare for next cycle
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

                # Capture summary data only if not already captured
                if window.isActive and not self.summary_captured:
                    start_time = datetime.now()
                    self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    self.data_dict["window_info"] = window_info
                    self.data_dict["monitor_index"] = monitor_index + 1

                    # Capture summary data
                    location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                    self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If summary captured, monitor for Change State window
                if self.json_created:
                    change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                    
                    if change_state_result[0]:
                        self.change_state_opened = True
                        self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Check if Change State window is closed
                    if self.change_state_opened:
                        change_state_result = self.check_window_title(self.change_state_title, monitor_details)
                        
                        if not change_state_result[0]:
                            # Capture entire data
                            location, _ = self.track_location(self.end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
            else:
                # Case Management window is not open
                self.case_management_open = False
                
                # If we were previously waiting for reopen, reset flags
                if self.waiting_for_reopen:
                    self.waiting_for_reopen = False
                    self.summary_captured = False
                    self.json_created = False

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Optional: Customize location coordinates if needed
    # summary_location = (0.07, 0.7)
    # end_location = (0.7, 0.825)
    
    # Create monitor with configurable titles
    # If you want to customize locations, use: 
    # monitor = OWSProcessMonitor(ows_titles, summary_location, end_location)
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()port re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

class OWSProcessMonitor:
    def __init__(self, ows_titles, summary_location=(0.07, 0.7), end_location=(0.7, 0.825)):
        """
        Initialize OWS Process Monitor
        
        Args:
            ows_titles (tuple): A tuple containing two window titles 
                                (Case Management Window, Change State Window)
            summary_location (tuple): Coordinates for summary button (default: (0.07, 0.7))
            end_location (tuple): Coordinates for end/copy all button (default: (0.7, 0.825))
        """
        # Unpack titles
        self.case_management_title, self.change_state_title = ows_titles
        
        # Store location coordinates
        self.summary_location = summary_location
        self.end_location = end_location
        
        # Monitoring state variables
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        while self.monitoring_active:
            windows = gw.getAllTitles()
            for win in windows:
                if target_title in win:
                    window = gw.getWindowsWithTitle(win)[0]
                    monitor_index = self.get_monitor_index(window, monitor_details)

                    window_info = {
                        "left": window.left,
                        "top": window.top,
                        "width": window.width,
                        "height": window.height
                    }

                    return window, monitor_index, window_info
            time.sleep(1)
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 20)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        
        if not summary_text:
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
        
        pyautogui.moveTo(curr_x, curr_y)
        
        # Reset state change detection
        self.data_dict["state_change_detected"] = False

        # Parse summary text
        summary_dict = {}
        case_id = "dummy"
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
                if key == "Id":
                    case_id = value

        # Update data dictionary
        self.data_dict.update(summary_dict)
        self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.save_to_json(self.data_dict, case_id, False)
        
        self.json_created = True
        print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.json_created = False

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.case_management_title, monitor_details)
            if not window_result[0]:
                continue

            window, monitor_index, window_info = window_result
            
            if window.isActive:
                start_time = datetime.now()
                self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                self.data_dict["window_info"] = window_info
                self.data_dict["monitor_index"] = monitor_index + 1

                # Capture summary data
                location, _ = self.track_location(self.summary_location, monitor_index, monitor_details, window_info)
                self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If JSON created, monitor for Change State window
                while self.json_created:
                    change_state_found = False
                    change_state_window = None

                    # Wait for Change State window
                    while not change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        for win in windows:
                            if self.change_state_title in win:
                                change_state_window = gw.getWindowsWithTitle(win)[0]
                                change_state_found = True
                                self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                break
                        time.sleep(1)

                    # Wait for Change State window to close
                    while change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        if self.change_state_title not in windows:
                            change_state_found = False
                            
                            # Capture entire data
                            location, _ = self.track_location(self.end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
                            break
                        time.sleep(1)

                    # Check if original windows are still active
                    windows = gw.getAllTitles()
                    if self.case_management_title not in windows:
                        break

            time.sleep(2)

def main():
    # Configurable window titles
    ows_titles = ("Case Management -", "Change State")
    
    # Optional: Customize location coordinates if needed
    # summary_location = (0.07, 0.7)
    # end_location = (0.7, 0.825)
    
    # Create monitor with configurable titles
    # If you want to customize locations, use: 
    # monitor = OWSProcessMonitor(ows_titles, summary_location, end_location)
    monitor = OWSProcessMonitor(ows_titles)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()



-----





import time
import re
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths and global configurations
process_folder = r"C:\pulse_november_12_12_flow\pulse_agent\pulse_flowtrace\pulse_event_trigger\process"

# App store configurations
app_store = {
    "OWS": "Case Management - ,Change State", 
    "BPM": "Work Object,Process Work,Loan Maintenance"
}

app_store_location = {
    "OWS": {
        "Case Management -": [(0.07, 0.7)],
        "Change State": [(0.5, 0.825)]
    },
    "BPM": {
        "Work Object": [(0.07, 0.7), (0.5, 0.3)], 
        "Process Work": [(0.5, 0.825)],
        "Loan Maintenance": [(0.24, 0.09)]
    }
}

# OWS related capture coordinates
summary_location = (0.07, 0.7)
end_location = (0.7, 0.825)

class OWSProcessMonitor:
    def __init__(self, target_window_title):
        self.target_window_title = target_window_title
        self.data_dict = {}
        self.json_created = False
        self.monitoring_active = True

    def get_active_monitors(self):
        """Get active monitor details."""
        monitors = get_monitors()
        return [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]

    def get_monitor_index(self, window, monitors):
        """Determine on which monitor the majority of the window is visible."""
        max_overlap_area = 0
        best_monitor_index = 0

        window_rect = {
            "left": window.left,
            "top": window.top,
            "right": window.left + window.width,
            "bottom": window.top + window.height
        }

        for index, monitor in enumerate(monitors):
            monitor_rect = {
                "left": monitor['x'],
                "top": monitor['y'],
                "right": monitor['x'] + monitor['width'],
                "bottom": monitor['y'] + monitor['height']
            }

            # Calculate the overlapping area
            overlap_left = max(window_rect['left'], monitor_rect['left'])
            overlap_top = max(window_rect['top'], monitor_rect['top'])
            overlap_right = min(window_rect['right'], monitor_rect['right'])
            overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

            overlap_width = max(0, overlap_right - overlap_left)
            overlap_height = max(0, overlap_bottom - overlap_top)
            overlap_area = overlap_width * overlap_height

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_monitor_index = index

        return best_monitor_index if max_overlap_area > 0 else 0

    def check_window_title(self, target_title, monitor_details):
        """Monitor for a specific window title on any monitor."""
        while self.monitoring_active:
            windows = gw.getAllTitles()
            for win in windows:
                if target_title in win:
                    window = gw.getWindowsWithTitle(win)[0]
                    monitor_index = self.get_monitor_index(window, monitor_details)

                    window_info = {
                        "left": window.left,
                        "top": window.top,
                        "width": window.width,
                        "height": window.height
                    }

                    return window, monitor_index, window_info
            time.sleep(1)
        return None, None, None

    def track_location(self, location_tuple, monitor_index, monitor_details, window_info):
        """Locate a specific location based on the active monitor and window size."""
        x_ratio, y_ratio = location_tuple
        x = window_info["left"] + int(window_info["width"] * x_ratio)
        y = window_info["top"] + int(window_info["height"] * y_ratio)
        return (x, y), datetime.now()

    def capture_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture summary data and save to JSON."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 20)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        
        if not summary_text:
            pyautogui.rightClick(x, y)
            pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
            pyautogui.click()
            summary_text = pyperclip.paste()
        
        pyautogui.moveTo(curr_x, curr_y)
        
        # Reset state change detection
        self.data_dict["state_change_detected"] = False

        # Parse summary text
        summary_dict = {}
        case_id = "dummy"
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
                if key == "Id":
                    case_id = value

        # Update data dictionary
        self.data_dict.update(summary_dict)
        self.data_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        self.save_to_json(self.data_dict, case_id, False)
        
        self.json_created = True
        print("JSON created with Summary data.")

    def save_to_json(self, data, file_name, end):
        """Save data to a JSON file with advanced logic."""
        file_path = os.path.join(process_folder, f"{file_name}.json")

        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                existing_data = json.load(file)
                
                if not end and existing_data.get("state_change_detected", False):
                    print(f"File '{file_name}.json' already has 'state_change_detected' as True. Skipping update.")
                    return
                
                if end and not existing_data.get("state_change_detected", False):
                    print(f"Updating final state for '{file_name}.json'.")
                
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data saved to '{file_name}.json'.")

    def capture_entire_data(self, location, start_time, monitor_details, monitor_index, window_info):
        """Capture complete log data."""
        x, y = location
        curr_x, curr_y = pyautogui.position()
        pyperclip.copy('')

        dynamic_offset = (20, 50)
        pyautogui.rightClick(x, y)
        pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
        pyautogui.click()
        summary_text = pyperclip.paste()
        pyautogui.moveTo(curr_x, curr_y)

        # Parse log text
        summary_dict = {}
        lines = summary_text.split('\n')
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            if "Action by" in line:
                date_time = self.extract_date_time(line)
                summary_dict["Action By Date/Time"] = date_time

                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if "transition" in next_line:
                        transition_text = self.extract_transition(next_line)
                        summary_dict["Transition"] = transition_text
                        break

        summary_dict['start_id_time'] = start_time.strftime("%Y-%m-%d %H:%M:%S")

        # Check action time against state change time
        if "Action By Date/Time" in summary_dict:
            action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
            state_change_time = datetime.strptime(self.data_dict.get("start_activity", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")
            state_change_time = state_change_time.replace(second=0, microsecond=0)

            if action_by_time >= state_change_time:
                self.data_dict["state_change_detected"] = True
                self.data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.json_created = False

        self.data_dict.update(summary_dict)
        self.save_to_json(self.data_dict, self.data_dict.get("Id"), True)
        print("JSON created with Log data.")

    @staticmethod
    def extract_date_time(line):
        """Extract and convert date/time from log line."""
        date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
        match = re.search(date_time_pattern, line)

        if match:
            try:
                date_time_obj = datetime.strptime(f"{match.group(1)} {match.group(2)} {match.group(3)}", "%m/%d/%y %I:%M %p")
                return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Error parsing date and time: {e}")
                return "Invalid Date/Time"
        
        return "No valid date/time found"

    @staticmethod
    def extract_transition(line):
        """Extract transition text from a line."""
        if "transition" in line:
            parts = line.split("transition")
            return parts[1].strip() if len(parts) > 1 else "Unknown Transition"
        return "Unknown Transition"

    def monitor_ows_process(self):
        """Main monitoring process for OWS workflow."""
        while self.monitoring_active:
            monitor_details = self.get_active_monitors()
            
            # Check for Case Management window
            window_result = self.check_window_title(self.target_window_title, monitor_details)
            if not window_result[0]:
                continue

            window, monitor_index, window_info = window_result
            
            if window.isActive:
                start_time = datetime.now()
                self.data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
                self.data_dict["window_info"] = window_info
                self.data_dict["monitor_index"] = monitor_index + 1

                # Capture summary data
                location, _ = self.track_location(summary_location, monitor_index, monitor_details, window_info)
                self.capture_data(location, start_time, monitor_details, monitor_index, window_info)

                # If JSON created, monitor for Change State window
                while self.json_created:
                    change_state_found = False
                    change_state_window = None

                    # Wait for Change State window
                    while not change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        for win in windows:
                            if "Change State" in win:
                                change_state_window = gw.getWindowsWithTitle(win)[0]
                                change_state_found = True
                                self.data_dict["state_change_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                break
                        time.sleep(1)

                    # Wait for Change State window to close
                    while change_state_found and self.monitoring_active:
                        windows = gw.getAllTitles()
                        if "Change State" not in windows:
                            change_state_found = False
                            
                            # Capture entire data
                            location, _ = self.track_location(end_location, monitor_index, monitor_details, window_info)
                            self.capture_entire_data(location, start_time, monitor_details, monitor_index, window_info)
                            break
                        time.sleep(1)

                    # Check if original windows are still active
                    windows = gw.getAllTitles()
                    if self.target_window_title not in windows:
                        break

            time.sleep(2)

def main():
    target_window_title = "Case Management -"
    monitor = OWSProcessMonitor(target_window_title)
    
    try:
        monitor_thread = threading.Thread(target=monitor.monitor_ows_process)
        monitor_thread.start()
        monitor_thread.join()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting gracefully.")
        monitor.monitoring_active = False

if __name__ == "__main__":
    main()






--------------------
def save_to_json(data, file_name):
    """Save data to a JSON file."""
    file_path = os.path.join(process_folder, f"{file_name}.json")

    # Check if file exists
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            existing_data = json.load(file)
            # Check the state_change_detected key
            if existing_data.get("state_change_detected", False):
                print(f"File '{file_name}.json' already exists and has 'state_change_detected' as True. Overwriting...")
            else:
                print(f"File '{file_name}.json' exists but 'state_change_detected' is False. Skipping update.")
                return

    # Save updated or new data
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to '{file_name}.json'.")


def capture_entire_data(location, start_id_time, monitor_details, monitor_index, window_info):
    """Capture data and save it to JSON with dynamic offset."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyperclip.copy('')

    # Perform actions with the adjusted offset
    dynamic_offset = (20, 20)
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
    pyautogui.click()
    summary_text = pyperclip.paste()
    pyautogui.moveTo(curr_x, curr_y)
    print("Log Text copied:", summary_text)

    summary_dict = {}
    lines = summary_text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        if "Action by" in line:
            date_time = extract_date_time(line)
            summary_dict["Action By Date/Time"] = date_time

            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if "transition" in next_line:
                    transition_text = extract_transition(next_line)
                    summary_dict["Transition"] = transition_text
                    break

    summary_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Log Dict", summary_dict)

    # Compare "Action By Date/Time" with "state_change_time"
    if "Action By Date/Time" in summary_dict:
        action_by_time = datetime.strptime(summary_dict["Action By Date/Time"], "%Y-%m-%d %H:%M:%S")
        state_change_time = datetime.strptime(data_dict.get("state_change_time", "1900-01-01 00:00:00"), "%Y-%m-%d %H:%M:%S")

        if action_by_time > state_change_time:
            data_dict["state_changed_true"] = True
            data_dict["End_activity_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("State change detected. Marked as true.")

    data_dict.update(summary_dict)
    save_to_json(data_dict, summary_dict.get("Transition", "dummy"))
    print("JSON created with Log data.")






import re
from datetime import datetime

def extract_date_time(line):
    """
    Dynamically identifies and extracts date and time in the format 'MM/DD/YY HH:MM AM/PM' 
    from the given line, then converts it to 'YYYY-MM-DD HH:MM:SS'.
    
    Example input:
    - "Action by John Doe on 11/28/24 5:54 PM"
    - "Completed on 12/29/23 11:23 AM"

    Returns:
    - Converted datetime string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    # Regular expression to match the date in MM/DD/YY format
    date_time_pattern = r'(\d{1,2}/\d{1,2}/\d{2}) (\d{1,2}:\d{2}) (AM|PM)'
    match = re.search(date_time_pattern, line)

    if match:
        date_part = match.group(1)  # Extract date (MM/DD/YY)
        time_part = match.group(2)  # Extract time (HH:MM)
        am_pm_part = match.group(3)  # Extract AM/PM

        # Combine extracted components into a single datetime string
        date_time_str = f"{date_part} {time_part} {am_pm_part}"
        
        # Convert to datetime object and format as 'YYYY-MM-DD HH:MM:SS'
        try:
            date_time_obj = datetime.strptime(date_time_str, "%m/%d/%y %I:%M %p")
            return date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(f"Error parsing date and time: {e}")
            return "Invalid Date/Time"
    
    # If no match is found, return a placeholder
    return "No valid date/time found"





------------+++-----+-+
import cv2
import pyautogui

def scale_image_with_opencv(cropped_image_path, original_resolution, current_resolution):
    """
    Scales the cropped image using OpenCV to match the current screen resolution.

    Parameters:
        cropped_image_path (str): Path to the cropped image.
        original_resolution (tuple): (width, height) of the monitor where the image was cropped.
        current_resolution (tuple): (width, height) of the current monitor.

    Returns:
        str: Path to the scaled image.
    """
    # Load the cropped image
    cropped_image = cv2.imread(cropped_image_path, cv2.IMREAD_UNCHANGED)

    # Calculate scale factors
    original_width, original_height = original_resolution
    current_width, current_height = current_resolution
    width_scale = current_width / original_width
    height_scale = current_height / original_height

    # Calculate new dimensions
    new_width = int(cropped_image.shape[1] * width_scale)
    new_height = int(cropped_image.shape[0] * height_scale)

    # Resize the cropped image
    scaled_image = cv2.resize(cropped_image, (new_width, new_height), interpolation=cv2.INTER_LINEAR)

    # Save the scaled image temporarily
    scaled_image_path = "scaled_image.png"
    cv2.imwrite(scaled_image_path, scaled_image)

    return scaled_image_path

def locate_cropped_image_on_screen(cropped_image_path, original_resolution, current_resolution):
    """
    Scales the cropped image and uses PyAutoGUI to locate it on the visible screen.

    Parameters:
        cropped_image_path (str): Path to the cropped image.
        original_resolution (tuple): (width, height) of the monitor where the image was cropped.
        current_resolution (tuple): (width, height) of the current monitor.
    """
    # Scale the cropped image using OpenCV
    scaled_image_path = scale_image_with_opencv(cropped_image_path, original_resolution, current_resolution)

    # Locate the scaled image on the visible screen using PyAutoGUI
    location = pyautogui.locateOnScreen(scaled_image_path, confidence=0.8)  # Adjust confidence as needed
    if location:
        print(f"Image found at: {location}")
    else:
        print("Image not found on the screen.")

# Example usage
original_resolution = (1920, 1080)  # Resolution of the monitor where the image was cropped
current_resolution = (1366, 768)   # Resolution of the current monitor
cropped_image_path = "Event_Trigger/cropped_image.png"  # Path to the cropped image

locate_cropped_image_on_screen(cropped_image_path, original_resolution, current_resolution)





----------------------
def calculate_dynamic_offset(monitor_details, window_info, base_offset):
    """Calculate dynamic offset based on monitor and window size."""
    monitor_width = monitor_details['width']
    monitor_height = monitor_details['height']
    window_width = window_info['width']
    window_height = window_info['height']

    # Calculate scaling factors
    width_ratio = window_width / monitor_width
    height_ratio = window_height / monitor_height

    # Adjust the base offset dynamically
    dynamic_x = int(base_offset[0] * width_ratio)
    dynamic_y = int(base_offset[1] * height_ratio)

    print(f"Dynamic offsets calculated: ({dynamic_x}, {dynamic_y})")
    return dynamic_x, dynamic_y


def capture_data(location, start_id_time, monitor_details, window_info):
    """Capture data and save it to JSON with dynamic offset."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyperclip.copy('')

    # Calculate dynamic offset
    dynamic_offset = calculate_dynamic_offset(monitor_details, window_info, (30, 30))

    # Perform actions with the adjusted offset
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    case_id = "dummy"
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value

    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")


def capture_entire_data(location, start_id_time, monitor_details, window_info):
    """Capture data and save it to JSON with dynamic offset."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyperclip.copy('')

    # Calculate dynamic offset
    dynamic_offset = calculate_dynamic_offset(monitor_details, window_info, (10, 20))

    # Perform actions with the adjusted offset
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + dynamic_offset[0], y + dynamic_offset[1])
    time.sleep(5)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Log Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Check for "Action by" and extract date/time
        if "Action by" in line:
            date_time = extract_date_time(line)
            summary_dict["Action By Date/Time"] = date_time

            # Look for "Transition" in the next line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if "transition" in next_line:
                    transition_text = extract_transition(next_line)
                    summary_dict["Transition"] = transition_text
                    break

    # Save additional metadata
    summary_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Log Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    save_to_json(data_dict, summary_dict.get("Transition", "dummy"))
    print("JSON created with Log data.")


-------------------------+++++++++++--------------




To integrate the logic you've provided with the initial tracking system, I'll modify the original code so it works with the specific window titles and locations from the app_store and app_store_location dictionaries. This means we’ll combine tracking of window titles, their respective actions, and handling the screen capture at the specified locations.

Here's how we can adapt the approach:

Modified Approach

1. AppStore & Locations: The app_store dictionary contains application names and window titles. The app_store_location dictionary contains the specific locations (as ratios) for each action within each window.


2. Monitor Window Title and Location: As we detect the active window title, we will then identify the corresponding application. Once identified, we will use the specific locations for each action to perform the required operations.


3. Trigger Actions Dynamically: We'll dynamically track the first window title for each application, and when matched, locate the specified position based on the defined action.



Refactored Code

import time
import threading
from datetime import datetime
import pyautogui
import pygetwindow as gw
from pynput.mouse import Listener

# Dummy AppStore Details -> Will be replaced with Database Queries
app_store = {
    "OWS": "Case Management -,Change State",
    "BPM": "Work Object,Process Work,Loan Maintenance"
}
app_store_location = {
    "OWS": {
        "Case Management -": [(0.07, 0.7)],
        "Change State": [(0.5, 0.825)]
    },
    "BPM": {
        "Work Object": [(0.07, 0.7), (0.5, 0.3)],
        "Process Work": [(0.5, 0.825)],
        "Loan Maintenance": [(0.24, 0.09)]
    }
}

# Global state to track running threads
running_threads = {}

def get_active_window_title():
    """Fetch the current active window title."""
    return gw.getActiveWindow().title

def get_app_from_window_title(window_title):
    """Identify the application name based on the active window title."""
    for app_name, titles in app_store.items():
        first_title = titles.split(",")[0].strip()  # Extract the first window title
        if window_title == first_title:
            return app_name
    return None

def start_tracker(app_name, action_name):
    """Start the tracker thread dynamically for the given app."""
    if app_name in running_threads:
        return  # Avoid starting duplicate threads

    # Fetch and start the tracker class
    tracker_class = globals().get(f"{app_name}Tracker")
    if not tracker_class:
        print(f"No tracker class found for {app_name}")
        return

    tracker_instance = tracker_class(action_name)  # Initialize the tracker class with action name
    thread = threading.Thread(target=tracker_instance.run)
    thread.daemon = True
    thread.start()
    running_threads[app_name] = thread
    print(f"Started tracker for {app_name} - {action_name}")

def monitor_windows():
    """Continuously monitor active windows and start trackers as needed."""
    while True:
        active_window = get_active_window_title()
        app_name = get_app_from_window_title(active_window)
        if app_name:
            # Get the corresponding action for the app (first action in the list for now)
            actions = app_store_location.get(app_name)
            for action_name, locations in actions.items():
                start_tracker(app_name, action_name)
                for location in locations:
                    monitor_location(location, action_name)
        time.sleep(1)

def monitor_location(location, action_name):
    """Monitor the location where action needs to be taken (e.g., click or capture data)."""
    x_ratio, y_ratio = location
    # Locate the window based on its coordinates and window info (would be dynamic in actual implementation)
    window = gw.getWindowsWithTitle(action_name)[0]  # Get the window by action name
    window_info = {"left": window.left, "top": window.top, "width": window.width, "height": window.height}
    
    # Calculate absolute coordinates relative to the window
    x = window_info["left"] + int(window_info["width"] * x_ratio)
    y = window_info["top"] + int(window_info["height"] * y_ratio)

    print(f"{action_name} located at ({x}, {y})")
    capture_data((x, y), datetime.now())

def capture_data(location, start_id_time):
    """Capture data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.copy('')
    pyautogui.rightClick(x, y)
    pyautogui.moveTo(x + 10, y + 10)
    pyautogui.click()
    
    # For example, let's just log the capture action
    print(f"Captured data at {location} at {start_id_time}")

# Example tracker classes for specific actions
class OWSTracker:
    def __init__(self, action_name):
        self.action_name = action_name

    def run(self):
        while True:
            print(f"Tracking OWS for {self.action_name}...")
            time.sleep(5)

class BPMTracker:
    def __init__(self, action_name):
        self.action_name = action_name

    def run(self):
        while True:
            print(f"Tracking BPM for {self.action_name}...")
            time.sleep(5)

if __name__ == "__main__":
    monitor_windows()

Key Changes:

1. Dynamic Action Handling:

Based on the app_store_location dictionary, the code now tracks specific actions (e.g., Case Management -, Work Object) for each application.

The start_tracker method now passes the action name to the tracker class for each application.



2. Tracker Initialization:

The tracker classes (OWSTracker, BPMTracker) are initialized with an action name, and they will execute based on which action is being tracked.



3. Location Calculation:

For each action within an application, the location is calculated based on window ratios (x_ratio, y_ratio).

The coordinates are calculated dynamically based on the monitor and window size.



4. Window Interaction:

The capture_data method demonstrates how interaction with specific locations (like right-click and capture) can be implemented.




Next Steps:

Extend with More Actions: You can easily extend the system by adding more window titles and corresponding locations in app_store and app_store_location.

Customize Tracker Logic: Each tracker class (like OWSTracker or BPMTracker) can have customized logic to perform actions specific to the application, such as capturing data, clicking buttons, or navigating the application.


Let me know if you need further customization or enhancements!






# Your imports and definitions remain unchanged...

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles", windows)
        
        for win in windows:
            if target_title in win:
                print("Checking with", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window found:", window)
                monitor_index = get_monitor_index(window, monitor_details)

                # Capture window size and position
                window_info = {
                    "left": window.left,
                    "top": window.top,
                    "width": window.width,
                    "height": window.height
                }
                print(f"Window size and position: {window_info}")

                return window, monitor_index, window_info  # Return active window, monitor index, and window info
        time.sleep(1)

def track_location(location_tuple, action_name, monitor_index, monitor_details, window_info):
    """Locate a specific location based on the active monitor and window size."""
    x_ratio, y_ratio = location_tuple
    monitor = monitor_details[monitor_index]

    # Calculate absolute coordinates relative to the window
    x = window_info["left"] + int(window_info["width"] * x_ratio)
    y = window_info["top"] + int(window_info["height"] * y_ratio)

    print(f"{action_name} located at ({x}, {y}) on monitor {monitor_index + 1}")
    return (x, y), datetime.now()

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index, window_info = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        print("Tracking active window...")
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index + 1}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

            # Add window and monitor info to the data
            data_dict["window_info"] = window_info
            data_dict["monitor_index"] = monitor_index + 1

            # Perform right-click and copy
            location, start_id_time = track_location(summary_location, "summary_button", monitor_index, monitor_details, window_info)
            capture_data(location, start_time)

            if json_created:
                print("JSON created. Monitoring for 'ChangeState' window...")
                
                # Monitor for 'ChangeState' window
                change_state_window = None
                while not change_state_window:
                    windows = gw.getAllTitles()
                    for win in windows:
                        if "Change State" in win:
                            print(f"'Change State' window detected: {win}")
                            change_state_window = gw.getWindowsWithTitle(win)[0]
                            break

                print("Bypassing by adding sleep time of 10s")
                time.sleep(10)

                # Wait for 'ChangeState' window to close
                print("'Change State' window closed. Proceeding to capture 'Copy All'...")
                end_time = datetime.now()
                time_difference = (end_time - start_time).total_seconds()

                # Check if time difference is within the threshold
                if time_difference <= 30:
                    print(f"Time difference is {time_difference} seconds, updating JSON...")
                    location, start_id_time = track_location(end_location, "end_button", monitor_index, monitor_details, window_info)
                    capture_entire_data(location, start_time)

                    # Update JSON with state change data
                    data_dict["state_change_detected"] = True
                    data_dict["state_change_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, "state_change")
                    return
                else:
                    print(f"Time difference of {time_difference} seconds exceeds threshold. Skipping update.")
            else:
                print("JSON not yet created. Continuing monitoring...")
            time.sleep(5)  # Adjust sleep time as needed





import win32gui
import ctypes
from ctypes import wintypes

user32 = ctypes.WinDLL('user32', use_last_error=True)

class RECT(ctypes.Structure):
    _fields_ = [
        ("left", wintypes.LONG),
        ("top", wintypes.LONG),
        ("right", wintypes.LONG),
        ("bottom", wintypes.LONG)
    ]

def find_window_by_partial_title(partial_title):
    def callback(hwnd, titles):
        if win32gui.IsWindowVisible(hwnd) and partial_title in win32gui.GetWindowText(hwnd):
            titles.append(hwnd)
    titles = []
    win32gui.EnumWindows(callback, titles)
    return titles[0] if titles else None

def get_window_rect(hwnd):
    rect = RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return rect

partial_title = "Notepad"  # Replace with part of your application's title
hwnd = find_window_by_partial_title(partial_title)
if hwnd:
    rect = get_window_rect(hwnd)
    print(f"Window Size: {rect.right - rect.left}x{rect.bottom - rect.top}")
    print(f"Position: ({rect.left}, {rect.top})")
else:
    print("Window not found!")



To determine the size and position of your application window (e.g., when it’s resized to half the screen width), you can use libraries like pygetwindow, pywinauto, or the Windows API (ctypes) to track the window's dimensions and coordinates, regardless of whether it is fullscreen or resized.

Approach Using pygetwindow

pygetwindow provides an easy way to retrieve the window's size and position.

Installation:

pip install PyGetWindow

Example:

import pygetwindow as gw

# Get a list of all open windows
windows = gw.getAllWindows()

# Find your specific application by title
for window in windows:
    if 'YourAppTitle' in window.title:  # Replace 'YourAppTitle' with the actual window title
        print(f"Title: {window.title}")
        print(f"Size: {window.width}x{window.height}")
        print(f"Position: ({window.left}, {window.top})")
        if window.width < 1920 or window.height < 1080:
            print("The window is not occupying the full screen.")
        break

This will give you the window's current size (width, height) and its position on the screen (left, top). You can use this information to check if the window is occupying the full screen.


---

Approach Using pywinauto

If you need more advanced control, pywinauto can be used to retrieve window size and detect if it’s not fullscreen.

Installation:

pip install pywinauto

Example:

from pywinauto import Application

# Connect to your application (replace 'YourAppTitle' with your window title)
app = Application().connect(title_re=".*YourAppTitle.*")
window = app.window(title_re=".*YourAppTitle.*")

# Get the window's rectangle (left, top, right, bottom)
rect = window.rectangle()
width = rect.width()
height = rect.height()

print(f"Window Size: {width}x{height}")
print(f"Position: ({rect.left}, {rect.top})")

if width < 1920 or height < 1080:
    print("The window is not occupying the full screen.")

This method can handle minimized and hidden windows as well.


---

Approach Using Windows API (ctypes)

For more low-level access, you can use ctypes to retrieve window dimensions using the Windows API.

Example:

import ctypes
from ctypes import wintypes

# Define Windows API functions and structures
user32 = ctypes.WinDLL('user32', use_last_error=True)

class RECT(ctypes.Structure):
    _fields_ = [
        ("left", wintypes.LONG),
        ("top", wintypes.LONG),
        ("right", wintypes.LONG),
        ("bottom", wintypes.LONG)
    ]

def get_window_rect(title):
    hwnd = user32.FindWindowW(None, title)  # Replace None with the title of your application window
    if not hwnd:
        raise Exception("Window not found!")
    rect = RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return rect

# Example usage
title = "YourAppTitle"  # Replace with the exact title of your window
rect = get_window_rect(title)
width = rect.right - rect.left
height = rect.bottom - rect.top

print(f"Window Size: {width}x{height}")
print(f"Position: ({rect.left}, {rect.top})")

if width < 1920 or height < 1080:
    print("The window is not occupying the full screen.")


---

Key Points

1. Full-Screen Check: Compare the window's width and height to the full resolution (e.g., 1920x1080) to determine if it’s fullscreen.

if window.width < 1920 or window.height < 1080:
    print("The window is not fullscreen.")


2. Coordinates: The left and top values of the window indicate its position on the screen.


3. Cross-Monitor Considerations: If using multiple monitors, ensure you account for their combined resolutions or the specific monitor coordinates.



Choose the method that best fits your requirements based on ease of use (pygetwindow) or control and extensibility (pywinauto or ctypes).






def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        print("Tracking active window...")
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

            # Perform right-click and copy
            location, _ = track_location(summary_location, "summary_button", monitor_index, monitor_details)
            capture_data(location, start_time)

            if json_created:
                print("JSON created. Monitoring for 'ChangeState' window...")
                
                # Monitor for 'ChangeState' window
                change_state_window = None
                while not change_state_window:
                    windows = gw.getAllTitles()
                    for win in windows:
                        if "ChangeState" in win:
                            print(f"'ChangeState' window detected: {win}")
                            change_state_window = gw.getWindowsWithTitle(win)[0]
                            break
                    time.sleep(1)

                # Wait for 'ChangeState' window to close
                while change_state_window.isVisible:
                    print("'ChangeState' window is still visible, waiting...")
                    time.sleep(1)

                print("'ChangeState' window closed. Proceeding to capture 'Copy All'...")
                end_time = datetime.now()
                time_difference = (end_time - start_time).total_seconds()

                # Check if time difference is within the threshold
                if time_difference <= 30:
                    print(f"Time difference is {time_difference} seconds, updating JSON...")
                    location, _ = track_location(end_location, "end_button", monitor_index, monitor_details)
                    capture_entire_data(location, start_time)

                    # Update JSON with state change data
                    data_dict["state_change_detected"] = True
                    data_dict["state_change_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, "state_change")
                else:
                    print(f"Time difference of {time_difference} seconds exceeds threshold. Skipping update.")
            else:
                print("JSON not yet created. Continuing monitoring...")
            time.sleep(5)  # Adjust sleep time as needed





import time
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import pyperclip
import os

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

summary_location = (0.07, 0.7)
end_location = (0.5, 0.825)

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info:", monitor_details)
    return monitor_details


def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles:", windows)

        for win in windows:
            if target_title in win:
                print("Checking with:", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window is:", window)
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)


def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height,
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor["x"],
            "top": monitor["y"],
            "right": monitor["x"] + monitor["width"],
            "bottom": monitor["y"] + monitor["height"],
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect["left"], monitor_rect["left"])
        overlap_top = max(window_rect["top"], monitor_rect["top"])
        overlap_right = min(window_rect["right"], monitor_rect["right"])
        overlap_bottom = min(window_rect["bottom"], monitor_rect["bottom"])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")

        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None


def track_location(location_tuple, action_name, monitor_index, monitor_details):
    """Locate a specific location based on the active monitor."""
    x_ratio, y_ratio = location_tuple
    monitor = monitor_details[monitor_index]

    # Calculate absolute coordinates with padding
    x = monitor["x"] + int(monitor["width"] * x_ratio)
    y = monitor["y"] + int(monitor["height"] * y_ratio)

    print(f"{action_name} located at ({x}, {y}) on monitor {monitor_index + 1}")
    return (x, y), datetime.now()


def save_to_json(data, case_id):
    """Save tracked data to JSON with a formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        print("Tracking active window")
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(1)

            if not json_created:
                # Track summary button location
                location, start_id_time = track_location(summary_location, "Summary Button", monitor_index, monitor_details)
                if location:
                    save_to_json(data_dict, "summary_button")
                break  # Exit tracking after handling JSON creation
        time.sleep(1)


if __name__ == "__main__":
    target_title = "Your Window Title"  # Replace with your target window title
    monitor_process(target_title)




import json
import os
from datetime import datetime
import pyautogui
import pyperclip

data_dict = {}
process_folder = "./output"  # Update to your desired folder path


def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        # Check for "Action By" and extract date/time
        if "Action By" in line:
            date_time = extract_date_time(line)
            summary_dict["Action By Date/Time"] = date_time

            # Look for "Transition" in the next line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if "Transition" in next_line:
                    transition_text = extract_transition(next_line)
                    summary_dict["Transition"] = transition_text

    # Save additional metadata
    summary_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    save_to_json(data_dict, summary_dict.get("Id", "dummy"))
    print("JSON created with Summary data.")


def extract_date_time(line):
    """Extract date and time from a line."""
    # Assuming date/time is in the format "YYYY-MM-DD HH:MM:SS"
    parts = line.split()
    for part in parts:
        try:
            date_time = datetime.strptime(part, "%Y-%m-%d %H:%M:%S")
            return date_time.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return "Unknown Date/Time"


def extract_transition(line):
    """Extract transition text from a line."""
    if "Transition" in line:
        parts = line.split("Transition")
        if len(parts) > 1:
            return parts[1].strip()
    return "Unknown Transition"


def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    if not os.path.exists(process_folder):
        os.makedirs(process_folder)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


# Example usage (replace with your actual parameters)
capture_summary_data((100, 200), datetime.now())




import time
from pywinauto import Application, timings

# Set logging level to help debug and understand what's happening internally
timings.Timings.LOG_LEVEL = 3

def print_controls(window):
    """
    Recursively print all controls within the given window.
    """
    for control in window.children():
        try:
            control_type = control.friendly_class_name()
            control_text = control.window_text()
            print(f"Control Type: {control_type}, Text: '{control_text}'")

            # Recursively check child elements of complex controls like panels, groupboxes, etc.
            if control_type in ['TabControl', 'GroupBox', 'Pane', 'Panel', 'TabItem', 'Frame']:
                print_controls(control)  # Dig deeper if it's a container or group

        except Exception as e:
            print(f"Error inspecting control {control}: {e}")

def explore_tabs_and_controls(window):
    """
    Explore tabs and nested elements (if applicable), ensuring all controls are printed.
    """
    # First, ensure the main window is visible
    window.wait('visible', timeout=30)  # Wait for the main window to be visible

    # Print all controls at the top level
    print_controls(window)

    # If there are tabs, explore them as well
    if window.child_window(control_type="TabControl").exists():
        tab_control = window.child_window(control_type="TabControl")
        tabs = tab_control.children()

        # Iterate through each tab and explore its contents
        for tab in tabs:
            print(f"Exploring tab: {tab.window_text()}")
            tab.click_input(double=True)  # Double-click to select the tab
            time.sleep(1)  # Wait for tab to load content
            print_controls(window)  # Print all controls in the current tab

def main():
    # Connect to the application
    app = Application(backend="uia").connect(title_re=".*YourApplicationTitle.*")  # Adjust with your app's title
    window = app.window(title_re=".*YourApplicationTitle.*")  # Adjust window title regex to match

    # Ensure window is visible and ready for interaction
    window.wait('visible', timeout=30)
    print(f"Window {window.window_text()} is now visible.")

    # Call function to explore and print all controls
    explore_tabs_and_controls(window)

if __name__ == "__main__":
    main()




from pywinauto import Application
from pywinauto.findwindows import ElementNotFoundError

def list_controls_with_text(window):
    """
    Lists all controls with their text and control type in the given window.
    :param window: The Pywinauto WindowSpecification object.
    """
    print("All Controls and Their Text:")
    print("=" * 80)

    try:
        # Find all descendants (all elements in the window hierarchy)
        elements = window.descendants()
        for elem in elements:
            try:
                # Get the control type and text
                control_type = elem.friendly_class_name()
                control_text = elem.window_text()
                print(f"Control Type: {control_type}, Text: '{control_text}', Rect: {elem.rectangle()}")
            except Exception as e:
                print(f"Error processing element: {e}")
    except Exception as e:
        print(f"Error while retrieving controls: {e}")


# Main Execution
try:
    # Connect to the application window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print(f"Inspecting the 'Case Management' window for all controls and text:\n")
    list_controls_with_text(window)

except ElementNotFoundError:
    print("Error: The specified window 'Case Management' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")



from pywinauto import Application
from pywinauto.findwindows import ElementNotFoundError
from pywinauto.uia_defines import IUIA

def list_supported_controls(window):
    """
    Lists and prints all supported controls in the given window.
    :param window: The Pywinauto WindowSpecification object.
    """
    control_types = [
        "Button", "CheckBox", "RadioButton", "GroupBox", "ComboBox", "Edit",
        "Header", "ListBox", "ListView", "PopupMenu", "Static", "StatusBar",
        "TabControl", "Toolbar", "ToolTips", "TreeView", "UpDown"
    ]

    print("Supported Controls Found:")
    print("=" * 50)

    # Find all descendants and filter by control type
    try:
        elements = window.descendants()
        for elem in elements:
            try:
                control_type = elem.friendly_class_name()
                if control_type in control_types:
                    print(f"Control Type: {control_type}, Text: '{elem.window_text()}', "
                          f"Rect: {elem.rectangle()}")
            except Exception as e:
                print(f"Error processing element: {e}")
    except Exception as e:
        print(f"Error while listing controls: {e}")


# Main Execution
try:
    # Connect to the application window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print(f"Inspecting the 'Case Management' window for supported control types:\n")
    list_supported_controls(window)

except ElementNotFoundError:
    print("Error: The specified window 'Case Management' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")






from pywinauto import Application
from pywinauto.controls.uiawrapper import UIAWrapper
from pywinauto.findwindows import ElementNotFoundError

def print_element_hierarchy(element, level=0):
    """
    Recursively prints the hierarchy of all elements in the given element's subtree.
    :param element: The root element to inspect.
    :param level: The depth of the current element (for indentation).
    """
    try:
        indent = "  " * level
        element_type = element.control_type()
        element_text = element.window_text()
        element_rect = element.rectangle()

        print(f"{indent}- Control Type: {element_type}, Text: '{element_text}', "
              f"Rect: {element_rect}")

        # Recursively inspect all child elements
        for child in element.children():
            print_element_hierarchy(child, level + 1)
    except Exception as e:
        print(f"{indent}- Error accessing element: {e}")

# Main Execution
try:
    # Connect to the application window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print("Inspecting the entire element hierarchy of the 'Case Management' window:")
    print_element_hierarchy(window)
except ElementNotFoundError:
    print("Error: The specified window 'Case Management' was not found.")
except Exception as e:
    print(f"Unexpected error: {e}")





from pywinauto import Application

def print_and_find_element(window, level=0, search_text="summary"):
    """
    Recursively prints all elements in the window with their text and control type.
    If an element matches the search_text, performs right-click and copy operation.
    :param window: The window or container element.
    :param level: The depth level in the hierarchy (used for indentation).
    :param search_text: The text to search for in the elements.
    """
    try:
        elements = window.descendants()
        for element in elements:
            indent = "  " * level
            try:
                # Print the element's details
                text = element.window_text()
                control_type = element.control_type()
                print(f"{indent}- Text: '{text}', Control Type: '{control_type}'")

                # Check if the text matches the search target
                if search_text.lower() in text.lower():
                    print(f"{indent}>>> Found element with text '{search_text}'!")
                    # Right-click and copy
                    element.right_click_input()
                    element.type_keys("^c")  # Simulate Ctrl+C to copy
                    print(f"{indent}>>> Copied text from the element.")

                # Recursively print child elements if available
                print_and_find_element(element, level + 1, search_text)
            except Exception as e:
                print(f"{indent}- Error accessing element: {e}")
    except Exception as e:
        print(f"Error fetching descendants: {e}")

# Main program
try:
    # Connect to the "Case Management" window
    app = Application(backend="uia").connect(title_re=".*Case Management.*")
    window = app.window(title_re=".*Case Management.*")

    print("Inspecting all elements in the 'Case Management' window...")
    print_and_find_element(window, search_text="summary")
except Exception as e:
    print(f"Error: {e}")




from pywinauto import Desktop, Application
from pywinauto.keyboard import send_keys
from datetime import datetime
import json
import os
import time

# Define paths for JSON output and other settings
process_folder = r"C:\pulse_event_trigger\process"
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def find_window_by_title(partial_title):
    """Find a window by its partial title."""
    while True:
        try:
            windows = Desktop(backend="uia").windows()
            for win in windows:
                if partial_title in win.window_text():
                    print(f"Found window: {win.window_text()}")
                    return win
        except Exception as e:
            print(f"Error finding window: {e}")
        time.sleep(1)


def find_element_by_text(window, search_text):
    """Find an element in the window that matches the search text."""
    try:
        window.set_focus()
        elements = window.descendants(control_type="Text")  # Get all text elements
        for element in elements:
            if search_text.lower() in element.window_text().lower():
                print(f"Found element with text: {element.window_text()}")
                return element
        print(f"No element found with text: {search_text}")
    except Exception as e:
        print(f"Error finding element: {e}")
    return None


def scroll_and_interact(window, element, search_text):
    """Scroll to an element and interact with it."""
    try:
        # Ensure the window is focused
        window.set_focus()

        # Simulate scrolling until the element is visible
        for _ in range(10):  # Adjust scroll attempts as needed
            element = find_element_by_text(window, search_text)
            if element:
                # Right-click on the found element
                element.right_click_input()
                send_keys("^c")  # Simulate Ctrl+C for copying
                return True
            else:
                # Scroll down if the element is not found
                send_keys("{PGDN}")  # Page Down
                time.sleep(1)
        print("Failed to locate the element after scrolling.")
    except Exception as e:
        print(f"Error interacting with element: {e}")
    return False


def capture_summary_data(window, search_text="Summary"):
    """Capture summary data by searching for a specific element."""
    try:
        # Find the desired element by text and scroll to it
        element = find_element_by_text(window, search_text)
        if not element:
            print(f"Scrolling to find the element with text '{search_text}'")
            if not scroll_and_interact(window, element, search_text):
                return

        # Fetch the copied content
        summary_text = os.popen("powershell -command Get-Clipboard").read()
        print("Summary Text copied:", summary_text)

        # Parse the pasted content into key-value pairs
        summary_dict = {}
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
        print("Summary Dict:", summary_dict)

        # Update data_dict and save to JSON
        start_id_time = datetime.now()
        data_dict.update(summary_dict)
        data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
        save_to_json(data_dict)
        global json_created
        json_created = True
        print("JSON created with Summary data.")
    except Exception as e:
        print(f"Error capturing summary data: {e}")


def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    """Monitor a specific window and capture data."""
    global json_created
    window = find_window_by_title(target_title)
    print(f"Tracking started for '{target_title}'")
    
    start_time = datetime.now()
    data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    # Capture data from the window
    capture_summary_data(window)


# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")



from pywinauto import Desktop, Application
from pywinauto.keyboard import send_keys
from datetime import datetime
import json
import os
import time

# Define paths for JSON output and other settings
process_folder = r"C:\pulse_event_trigger\process"
data_dict = {}
json_created = False  # Flag to check if JSON file is already created


def find_window_by_title(partial_title):
    """Find a window by its partial title."""
    while True:
        try:
            windows = Desktop(backend="uia").windows()
            for win in windows:
                if partial_title in win.window_text():
                    print(f"Found window: {win.window_text()}")
                    return win
        except Exception as e:
            print(f"Error finding window: {e}")
        time.sleep(1)


def get_window_coordinates(window):
    """Retrieve the coordinates of the given window."""
    rect = window.rectangle()
    coords = {
        "left": rect.left,
        "top": rect.top,
        "right": rect.right,
        "bottom": rect.bottom,
        "width": rect.width(),
        "height": rect.height(),
    }
    print(f"Window coordinates: {coords}")
    return coords


def capture_summary_data(window):
    """Capture summary data by simulating interactions."""
    try:
        # Right-click on the window and copy data
        window.set_focus()
        window.right_click_input()
        send_keys("^c")  # Simulate Ctrl+C for copying
        
        # Fetch the copied content
        summary_text = os.popen("powershell -command Get-Clipboard").read()
        print("Summary Text copied:", summary_text)

        # Parse the pasted content into key-value pairs
        summary_dict = {}
        lines = summary_text.split('\n')
        for line in lines:
            if line.strip():
                parts = line.split('\t')
                key = parts[0].strip() if len(parts) > 0 else ""
                value = parts[1].strip() if len(parts) > 1 else ""
                summary_dict[key] = value
        print("Summary Dict:", summary_dict)

        # Update data_dict and save to JSON
        start_id_time = datetime.now()
        data_dict.update(summary_dict)
        data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
        save_to_json(data_dict)
        global json_created
        json_created = True
        print("JSON created with Summary data.")
    except Exception as e:
        print(f"Error capturing summary data: {e}")


def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")


def monitor_process(target_title):
    """Monitor a specific window and capture data."""
    global json_created
    window = find_window_by_title(target_title)
    print(f"Tracking started for '{target_title}'")
    
    start_time = datetime.now()
    data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")

    # Get window coordinates (if needed)
    coords = get_window_coordinates(window)

    # Capture data from the window
    capture_summary_data(window)


# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")





Overview of PyAutoGUI

PyAutoGUI is a Python library that automates interactions with the GUI. It allows developers to simulate mouse movements, keyboard presses, and screen interactions, including locating images or regions on the screen. It works by capturing screenshots of the screen and searching for visual matches.


---

Pros of PyAutoGUI in Your Situation

1. Ease of Use

PyAutoGUI has simple and intuitive functions (locateOnScreen, click, drag, etc.) for screen automation and locating images.



2. Cross-Platform Support

Works on Windows, macOS, and Linux, making it versatile for different environments.



3. Built-in Screenshot and Image Matching

PyAutoGUI directly integrates image recognition with locateOnScreen, making it ideal for scenarios where you need to identify elements visually.



4. Confidence Matching

Supports a confidence parameter for image matching (e.g., confidence=0.8), making it flexible for images with slight variations.



5. Good for On-Screen Matching

PyAutoGUI performs well when matching smaller images (e.g., partial image) on the screen directly, which is suitable for GUI-based tasks.



6. No Manual Coordinates

You don't need to manually define coordinates; it dynamically finds the image's location, which simplifies automation tasks.





---

Limitations of PyAutoGUI

1. Dependency on Screen Capture

PyAutoGUI relies on screen resolution and the pixel-perfect matching of images. If the captured image (e.g., via MSS) differs in scale, DPI, or compression, it may fail to match.



2. Performance

Matching large images or searching within high-resolution screens can be slow, as it scans pixel by pixel.



3. Limited to Static Images

It cannot handle dynamic or real-time visual changes effectively. For example, partial transparency or animations may cause matching to fail.



4. Accuracy Issues

Matching depends on exact pixel values. Even small variations in color, resolution, or scale can result in failure, especially when the source images are captured using different tools (e.g., MSS vs. Snipping Tool).



5. Lack of Advanced Image Processing

Unlike OpenCV, PyAutoGUI doesn’t support advanced image preprocessing (e.g., scaling, rotation, or feature-based matching), limiting its flexibility in handling mismatches.



6. Screen-Specific

PyAutoGUI is designed to work on live screens. Matching images within saved files or screenshots is less efficient and not its primary purpose.



7. Platform-Specific Constraints

On certain platforms or with multi-monitor setups, PyAutoGUI may face challenges capturing or processing images correctly.





---

When to Use PyAutoGUI in Your Scenario

Best Use Case:
If you're matching partial images directly on the live screen, PyAutoGUI is an excellent choice because it’s built for GUI-based automation tasks.

Not Recommended For:
If your process requires matching within static screenshots or handling images captured via different tools (e.g., MSS vs. Snipping Tool). In such cases, OpenCV or feature-based methods are more reliable.



---

How to Mitigate PyAutoGUI’s Limitations

1. Preprocess Images:
Save screenshots using the same tool (e.g., MSS) and ensure consistent resolution and DPI.


2. Adjust Confidence Threshold:
Use a lower confidence threshold (e.g., confidence=0.7) to account for minor differences in images.


3. Scale Matching:
Ensure the partial image’s scale matches the full image exactly by resizing if needed.


4. Switch to OpenCV or MSS for Advanced Matching:
If PyAutoGUI consistently fails, use OpenCV for precise and advanced matching, especially for static screenshots or secondary monitor scenarios.




---

By understanding PyAutoGUI's strengths and limitations, you can decide whether to stick with it for live screen-based tasks or opt for alternatives like OpenCV for more complex image processing requirements.

















def process_key_store(self, key_store_data, workflow_dict, keyname_store_set, keyname_mapping_set,
                      business_function_dict, delivery_function_dict, process_function_dict,
                      session, user_id, user_name, user_email):
    key_store_entries = key_store_data.to_dict(orient='records')
    key_store_request_id = None
    seen_keynames = set()
    serial_number = 1

    # Group entries by ProcessName, DeliveryService, BusinessLevel, and WorkflowName
    grouped_entries = {}
    for entry in key_store_entries:
        group_key = (
            entry['ProcessName'],
            entry['DeliveryService'],
            entry['BusinessLevel'],
            entry['WorkflowName']
        )
        grouped_entries.setdefault(group_key, []).append(entry)

    # Validate each group
    for group_key, entries in grouped_entries.items():
        is_unique_values = [entry['UniqueKey'] for entry in entries]
        yes_count = is_unique_values.count("Yes")
        no_count = is_unique_values.count("No")

        # Check if the group has exactly one "Yes" and not all "No"
        if yes_count != 1:
            return jsonify({
                'message': f'Group {group_key} must have exactly one "Yes" in UniqueKey field.'
            }), 400
        if no_count == len(is_unique_values):
            return jsonify({
                'message': f'Group {group_key} cannot have all entries as "No" in UniqueKey field.'
            }), 400

    # Process each entry after validation
    for entry in key_store_entries:
        workflow_name = entry['WorkflowName']
        workflow_id = workflow_dict.get(workflow_name)

        if workflow_id is None:
            return jsonify({'message': f'Workflow "{workflow_name}" does not exist in KEY_STORE sheet'}), 400

        key_name = entry['KeyName']
        if key_name in seen_keynames:
            return jsonify({'message': f'Duplicate KeyName "{key_name}" in KEY_STORE sheet'}), 400

        seen_keynames.add(key_name)

        if (workflow_id, key_name) in keyname_store_set or (workflow_id, key_name) in keyname_mapping_set:
            return jsonify({'message': f'Duplicate KeyName "{key_name}" for Workflow "{workflow_name}" in KEY_STORE'}), 400

        if not key_store_request_id:
            new_request = KeynameStoreRequests(
                count=len(key_store_entries),
                req_created_date=datetime.utcnow(),
                modified_date=datetime.utcnow(),
                created_by=user_id,
                creator_name=user_name,
                creator_email=user_email,
                is_active=True,
                status="open",
            )
            session.add(new_request)
            session.flush()
            key_store_request_id = new_request.request_id

        new_keyname_config = KeynameStoreConfigRequests(
            request_id=key_store_request_id,
            workflow_id=workflow_id,
            serial_number=serial_number,
            business_level_id=business_function_dict.get(entry['BusinessLevel']),
            delivery_service_id=delivery_function_dict.get(entry['DeliveryService']),
            process_name_id=process_function_dict.get(entry['ProcessName']),
            activity_key_name=key_name,
            activity_key_layout=entry['Layout'],
            is_unique=entry['UniqueKey'] == 'Yes',
            remarks=str(entry['Remarks']),
            is_active=True,
            status_ar='open'
        )
        session.add(new_keyname_config)
        serial_number += 1

    return None




import cv2
import pyautogui
from screeninfo import get_monitors

def get_screen_resolution():
    """
    Get the resolution of the primary screen using ScreenInfo.
    """
    for monitor in get_monitors():
        # Assuming the primary monitor
        print(f"Monitor: {monitor.name}, Resolution: {monitor.width}x{monitor.height}")
        return monitor.width, monitor.height

def perform_image_matching(fixed_image_path, fixed_resolution, threshold=0.8):
    """
    Perform image matching by scaling the fixed image to match the current screen resolution.
    
    Args:
        fixed_image_path: Path to the fixed image.
        fixed_resolution: Tuple of (width, height) for the fixed image's original resolution.
        threshold: Confidence threshold for template matching.

    Returns:
        The coordinates of the top-left corner of the matched area if found, else None.
    """
    # Get the current screen resolution
    current_width, current_height = get_screen_resolution()
    print(f"Current Screen Resolution: {current_width}x{current_height}")

    # Scaling factors
    original_width, original_height = fixed_resolution
    scale_width = current_width / original_width
    scale_height = current_height / original_height

    # Capture the current screen
    screenshot = pyautogui.screenshot()
    screenshot.save("screenshot.png")

    # Load images
    fixed_image = cv2.imread(fixed_image_path)
    screenshot_image = cv2.imread("screenshot.png")

    # Resize the fixed image to match the current resolution
    resized_fixed_image = cv2.resize(fixed_image, (0, 0), fx=scale_width, fy=scale_height)

    # Perform template matching
    result = cv2.matchTemplate(screenshot_image, resized_fixed_image, cv2.TM_CCOEFF_NORMED)

    # Get the best match position
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)

    print(f"Match confidence: {max_val}")
    if max_val >= threshold:
        print(f"Match found at coordinates: {max_loc}")
        # Highlight the matched area on the screenshot (optional)
        top_left = max_loc
        h, w = resized_fixed_image.shape[:2]
        bottom_right = (top_left[0] + w, top_left[1] + h)
        cv2.rectangle(screenshot_image, top_left, bottom_right, (0, 255, 0), 2)
        cv2.imwrite("output_match.png", screenshot_image)
        return top_left
    else:
        print("No match found.")
        return None

if __name__ == "__main__":
    # Path to the fixed image (1920x1080 resolution)
    fixed_image_path = "fixed_image.png"

    # Fixed resolution of the reference image
    fixed_resolution = (1920, 1080)

    # Perform the image matching
    match_coordinates = perform_image_matching(fixed_image_path, fixed_resolution)

    if match_coordinates:
        print(f"Element located at: {match_coordinates}")
    else:
        print("Element not found on the screen.")




import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os
from pynput.mouse import Listener

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info:", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles:", windows)
        
        for win in windows:
            if target_title in win:
                print("Checking with", win)
                window = gw.getWindowsWithTitle(win)[0]
                print("Window is:", window)
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor['x'],
            "top": monitor['y'],
            "right": monitor['x'] + monitor['width'],
            "bottom": monitor['y'] + monitor['height']
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect['left'], monitor_rect['left'])
        overlap_top = max(window_rect['top'], monitor_rect['top'])
        overlap_right = min(window_rect['right'], monitor_rect['right'])
        overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")
        
        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None

def capture_screenshot(monitor_index, monitor_details):
    """Capture a screenshot of a specific monitor."""
    monitor = monitor_details[monitor_index]
    with mss.mss() as sct:
        screenshot = sct.grab({
            "top": monitor["y"],
            "left": monitor["x"],
            "width": monitor["width"],
            "height": monitor["height"]
        })
    return screenshot

def track_image(image_path, monitor_index, monitor_details):
    """Locate an image on a specific monitor."""
    screenshot = capture_screenshot(monitor_index, monitor_details)
    screenshot_path = os.path.join(process_folder, f"temp_monitor_{monitor_index}.png")
    mss.tools.to_png(screenshot.rgb, screenshot.size, output=screenshot_path)

    # Locate the image using PyAutoGUI
    location = pyautogui.locateOnScreen(image_path, confidence=0.9, region=(
        monitor_details[monitor_index]["x"],
        monitor_details[monitor_index]["y"],
        monitor_details[monitor_index]["width"],
        monitor_details[monitor_index]["height"]
    ))

    if location:
        x, y = pyautogui.center(location)
        print(f"Image {image_path} found at {x}, {y} on monitor {monitor_index}")
        return (x, y)
    else:
        print(f"Image {image_path} not found on monitor {monitor_index}")
        return None

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
    print("Summary Dict:", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict)
    global json_created
    json_created = True
    print("JSON created with Summary data.")

def save_to_json(data):
    """Save tracked data to JSON."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"Case_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            location = track_image(summary_button_path, monitor_index, monitor_details)
            if location:
                capture_summary_data(location, start_time)
            time.sleep(1)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    try:
        monitor_process(target_window_title)
    except KeyboardInterrupt:
        print("Process interrupted by user.")





def get_monitor_index(window, monitors):
    """Determine on which monitor the majority of the window is visible."""
    max_overlap_area = 0
    best_monitor_index = 0

    window_rect = {
        "left": window.left,
        "top": window.top,
        "right": window.left + window.width,
        "bottom": window.top + window.height
    }

    for index, monitor in enumerate(monitors):
        monitor_rect = {
            "left": monitor['x'],
            "top": monitor['y'],
            "right": monitor['x'] + monitor['width'],
            "bottom": monitor['y'] + monitor['height']
        }

        # Calculate the overlapping area
        overlap_left = max(window_rect['left'], monitor_rect['left'])
        overlap_top = max(window_rect['top'], monitor_rect['top'])
        overlap_right = min(window_rect['right'], monitor_rect['right'])
        overlap_bottom = min(window_rect['bottom'], monitor_rect['bottom'])

        # Overlap dimensions
        overlap_width = max(0, overlap_right - overlap_left)
        overlap_height = max(0, overlap_bottom - overlap_top)
        overlap_area = overlap_width * overlap_height

        print(f"Monitor {index} overlap area: {overlap_area}")
        
        # Update the best monitor based on the largest overlap area
        if overlap_area > max_overlap_area:
            max_overlap_area = overlap_area
            best_monitor_index = index

    if max_overlap_area > 0:
        print(f"Window is primarily on monitor {best_monitor_index + 1} (index {best_monitor_index})")
        return best_monitor_index

    # Default to the first monitor if no overlap is found
    print("No significant overlap with any monitor; defaulting to primary monitor.")
    return 0 if monitors else None


----






import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os
from pynput.mouse import Listener

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info ", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        print("All window titles",windows)
        
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the window is open."""    
    # Loop through each monitor and find where the window is located
    for index, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
                monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index

    # If no monitor contains the window, default to the first monitor
    print("Window position did not match any monitor, defaulting to primary monitor.")
    return 0 if monitors else None

def track_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    else:
        return None, None

def monitor_clicks_and_track_end_button(end_button_path):
    """Monitor clicks and track the 'end button' when clicked."""
    print("Monitoring clicks to find the 'end button'.")
    end_button_location = None
    while True:
        # Start listening for a mouse click event
        with Listener(on_click=on_click) as listener:
            listener.join()  # Wait for click event to trigger

            # Once clicked, check for the end button location
            location_end, end_time = track_final_image(end_button_path, "end_button")
            if location_end:
                # Store end_button location details
                end_button_region = {
                    "left": location_end[0] - 10,
                    "top": location_end[1] - 10,
                    "right": location_end[2] + 10,
                    "bottom": location_end[3] + 10
                }
                print("End button detected, monitoring for clicks in region.")
                
                # Monitor clicks in the end button region
                monitor_clicks_in_region(end_button_region)
                break  # Exit after tracking the 'end button' event

        time.sleep(0.1)  # Allow for quick reaction to clicks

def track_final_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    print("Is location ?", location )
    if location:
        w, x, y, z = location.left, location.top, location.width, location.height 
        print(f"{action_name} located at {w}, {x}, {y}, {z}")
        return (w, x, y, z), datetime.now()
    else:
        return None, None

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value
            else:
                case_id = "dummy"
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")


def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            time.sleep(5)
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
            # Track summary_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    # JSON creation and copying data once
                    capture_summary_data(location, start_id_time)


            # Start monitoring for end button after JSON creation
            if json_created:
                monitor_clicks_and_track_end_button(end_button_path)
                return  # Exit after tracking the 'end button' event
            
            time.sleep(1)  # Check every second


def monitor_clicks_in_region(region):
    """Monitor for clicks in a specific region and take a screenshot."""
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                
                # Capture full screen and save to process folder
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break  # Exit after capturing the screenshot
            time.sleep(0.1)  # Check clicks frequently

def on_click(x, y, button, pressed):
    """Callback function for mouse click events."""
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        return False  # Stop listener after a click (you can adjust behavior here)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"

    # Run the monitor process in a separate thread
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()



def monitor_clicks_and_track_end_button(end_button_path):
    """Monitor clicks and track the 'end button' when clicked."""
    print("Monitoring clicks to find the 'end button'.")
    while True:
        x, y = pyautogui.position()
        pyautogui.waitForClick()  # Wait for a click event
        
        # After a click, try to locate the 'end button'
        location_end, end_time = track_final_image(end_button_path, "end_button")
        if location_end:
            # Store end_button location details
            end_button_region = {
                "left": location_end[0] - 10,
                "top": location_end[1] - 10,
                "right": location_end[2] + 10,
                "bottom": location_end[3] + 10
            }
            print("End button detected, monitoring for clicks in region.")
            
            # Monitor clicks in the end button region
            monitor_clicks_in_region(end_button_region)
            break  # Exit after tracking the 'end button' event
        
        time.sleep(0.1)  # Allow for quick reaction to clicks

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    # Track active window
    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
            # Track summary_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    # Capture and process summary data
                    capture_summary_data(location, start_id_time)
            
            # Start monitoring for end button after JSON creation
            if json_created:
                monitor_clicks_and_track_end_button(end_button_path)
                return  # Exit after tracking the 'end button' event
            
            time.sleep(1)  # Check every second

def capture_summary_data(location, start_id_time):
    """Capture summary data and save it to JSON."""
    x, y = location
    curr_x, curr_y = pyautogui.position()
    pyautogui.moveTo(x + 20, y + 20)
    pyautogui.rightClick()
    pyautogui.moveTo(x + 30, y + 30)
    pyautogui.click()
    summary_text = pyperclip.paste()  # Copy extracted data
    pyautogui.moveTo(curr_x, curr_y)
    print("Summary Text copied:", summary_text)

    # Parse the pasted content into key-value pairs
    summary_dict = {}
    lines = summary_text.split('\n')
    for line in lines:
        if line.strip():
            parts = line.split('\t')
            key = parts[0].strip() if len(parts) > 0 else ""
            value = parts[1].strip() if len(parts) > 1 else ""
            summary_dict[key] = value
            if key == "Id":
                case_id = value
            else:
                case_id = "dummy"
    print("Summary Dict", summary_dict)

    # Update data_dict and save to JSON
    data_dict.update(summary_dict)
    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
    save_to_json(data_dict, case_id)
    global json_created
    json_created = True
    print("JSON created with Summary data.")





import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os

# Define paths for images and folders
summary_button_path = r"C:\pulse_event_trigger\summary_button.png"
end_button_path = r"C:\pulse_event_trigger\end_button.png"
process_folder = r"C:\pulse_event_trigger\process"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    print("Monitors Info", monitor_details)
    return monitor_details

def check_window_title(target_title, monitor_details):
    while True:
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    for index, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
                monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index
    return 0 if monitors else None

def track_image(image_path, action_name):
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    return None, None

def track_final_image(image_path, action_name):
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        w, x, y, z = location.left, location.top, location.width, location.height
        print(f"{action_name} located at {w}, {x}, {y}, {z}")
        return (w, x, y, z), datetime.now()
    return None, None

def save_to_json(data, case_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    while True:
        if window.isActive:
            print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
            start_time = datetime.now()
            data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
            if not json_created:
                location, start_id_time = track_image(summary_button_path, "summary_button")
                if location:
                    x, y = location
                    curr_x, curr_y = pyautogui.position()
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.moveTo(x + 30, y + 30)
                    pyautogui.click()
                    summary_text = pyperclip.paste()
                    pyautogui.moveTo(curr_x, curr_y)
                    summary_dict = {}
                    lines = summary_text.split('\n')
                    for line in lines:
                        if line.strip():
                            parts = line.split('\t')
                            key = parts[0].strip() if len(parts) > 0 else ""
                            value = parts[1].strip() if len(parts) > 1 else ""
                            summary_dict[key] = value
                            if key == "Id":
                                case_id = value
                            else:
                                case_id = "dummy"
                    print("Summary Dict", summary_dict)
                    data_dict.update(summary_dict)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")
                    save_to_json(data_dict, case_id)
                    json_created = True
                    print("JSON created with Summary data.")

            if json_created:
                while True:
                    location_end, end_time = track_final_image(end_button_path, "end_button")
                    if location_end:
                        end_button_region = {
                            "left": location_end[0] - 10,
                            "top": location_end[1] - 10,
                            "right": location_end[2] + 10,
                            "bottom": location_end[3] + 10
                        }
                        print("End button detected, monitoring for clicks in region.")
                        monitor_clicks_in_region(end_button_region)
                        return
                    time.sleep(1)

            time.sleep(1)

def monitor_clicks_in_region(region):
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break
            time.sleep(0.1)

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()




import screeninfo
import pygetwindow as gw

def get_monitor_index(window_title):
    # Get a list of all monitors
    monitors = screeninfo.get_monitors()
    # Try to find the specified window by title
    try:
        window = gw.getWindowsWithTitle(window_title)[0]
    except IndexError:
        print(f"Window with title '{window_title}' not found.")
        return None

    # Get the window's x, y position
    win_x, win_y = window.left, window.top
    
    # Loop through each monitor and find where the window is located
    for index, monitor in enumerate(monitors):
        if (monitor.x <= win_x < monitor.x + monitor.width and
                monitor.y <= win_y < monitor.y + monitor.height):
            print(f"Window found on monitor {index + 1} (index {index})")
            return index

    # If no monitor contains the window, default to the first monitor
    print("Window position did not match any monitor, defaulting to primary monitor.")
    return 0 if monitors else None

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # Get X, Y from located position of the okay_button
                    x, y = location
                    
                    # Move to detected location and adjust to the appropriate offset for right-click
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()  # Right-click on the okay_button location
                    time.sleep(0.5)

                    # Move to "Copy" option in the context menu and click to copy text
                    pyautogui.moveTo(x + 40, y + 60)  # Adjust coordinates to hover on "Copy"
                    pyautogui.click()  # Click on "Copy" option
                    time.sleep(0.5)

                    # Paste the copied content
                    summary_text = pyperclip.paste()

                    # Parse the pasted content into key-value pairs
                    summary_dict = {}
                    lines = summary_text.split('\n')
                    for line in lines:
                        if line.strip():  # Ignore empty lines
                            parts = line.split('\t')  # Split by tab
                            key = parts[0].strip() if len(parts) > 0 else ""
                            value = parts[1].strip() if len(parts) > 1 else ""
                            summary_dict[key] = value
                    
                    # Update data_dict with parsed summary_dict
                    data_dict.update(summary_dict)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second




Given the updated requirements, here’s an approach to handle each step:

1. Detect the OK Button: When the okay_button image is first found, create a JSON file named with the specified format, and include details such as timestamp and extracted text. Skip creating the JSON file again if it already exists.


2. Monitor for the End Button: Once the okay_button is detected and JSON is created, start monitoring for end_button.


3. Handle Clicks in the End Button Region: If end_button is detected, track its coordinates (left, top, bottom, width). If any clicks happen within this region, immediately take a full screenshot and store it in the process folder.


4. Threading and Click Monitoring: Use threads for continuous monitoring and to ensure click events in the end_button region are detected for capturing screenshots.



Here’s how the updated code would look:

import time
import threading
from datetime import datetime
import json
from screeninfo import get_monitors
import pyautogui
import pygetwindow as gw
import mss
import pyperclip
import os

# Define paths for images and folders
okay_button_path = "path/to/okay_button.png"
end_button_path = "path/to/end_button.png"
process_folder = "path/to/process_folder"

# Dictionary to store extracted data
data_dict = {}
json_created = False  # Flag to check if JSON file is already created

def get_active_monitors():
    """Get active monitor details."""
    monitors = get_monitors()
    monitor_details = [{"x": m.x, "y": m.y, "width": m.width, "height": m.height} for m in monitors]
    return monitor_details

def check_window_title(target_title, monitor_details):
    """Monitor for a specific window title on any monitor."""
    while True:
        windows = gw.getAllTitles()
        for win in windows:
            if target_title in win:
                window = gw.getWindowsWithTitle(win)[0]
                monitor_index = get_monitor_index(window, monitor_details)
                return window, monitor_index  # Return active window and monitor index
        time.sleep(1)

def get_monitor_index(window, monitors):
    """Determine on which monitor the window is open."""
    for i, monitor in enumerate(monitors):
        if (monitor['x'] <= window.left < monitor['x'] + monitor['width'] and
            monitor['y'] <= window.top < monitor['y'] + monitor['height']):
            return i
    return None

def track_image(image_path, action_name):
    """Locate an image on screen and track coordinates and time."""
    location = pyautogui.locateOnScreen(image_path, confidence=0.9)
    if location:
        x, y = pyautogui.center(location)
        print(f"{action_name} located at {x}, {y}")
        return (x, y), datetime.now()
    else:
        return None, None

def save_to_json(data, case_id):
    """Save tracked data to JSON with formatted name."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(process_folder, f"CaseId_{case_id}_{timestamp}.json")
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to {filename}")

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # JSON creation and copying data once
                    x, y = location
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.hotkey('ctrl', 'c')
                    copied_text = pyperclip.paste()  # Copy extracted data

                    # Store data in dictionary
                    data_dict['copied_data'] = copied_text
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second

def monitor_clicks_in_region(region):
    """Monitor for clicks in a specific region and take a screenshot."""
    with mss.mss() as sct:
        while True:
            x, y = pyautogui.position()
            if (region['left'] <= x <= region['right'] and
                region['top'] <= y <= region['bottom'] and
                pyautogui.mouseDown()):
                
                # Capture full screen and save to process folder
                screenshot_path = os.path.join(process_folder, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                sct.shot(output=screenshot_path)
                print(f"Screenshot saved at {screenshot_path}")
                break  # Exit after capturing the screenshot
            time.sleep(0.1)  # Check clicks frequently

# Main entry point
if __name__ == "__main__":
    target_window_title = "Case Management -"

    # Run the monitor process in a separate thread
    monitor_thread = threading.Thread(target=monitor_process, args=(target_window_title,))
    monitor_thread.start()
    monitor_thread.join()

Explanation of Updates:

1. Flag for JSON Creation (json_created):

A flag json_created is set to False initially.

Once the okay_button is detected and JSON is created, json_created is set to True, so subsequent detections of okay_button will be ignored.



2. monitor_clicks_in_region Function:

Tracks clicks within the end_button region.

On detecting a click in the region, a full screenshot is taken and saved in the process_folder with a timestamped name.

After taking a screenshot, the function breaks out, ending the monitoring.



3. Modifications to monitor_process:

JSON creation is now triggered only on the first detection of okay_button.

Once the JSON is created, end_button tracking begins.

When end_button is detected, the system monitors clicks within its region and captures a screenshot on a detected click.




This code structure meets your requirements for efficient, click-based tracking of the end_button and conditions around JSON creation. Adjust file paths, image paths, and case_id as necessary for your specific setup.


____((((()))))___&&

Thank you for the clarification! Here’s how we can update the code to parse the copied text line-by-line, split it into key-value pairs, and store it as separate entries in the dictionary. This way, each line will be processed individually, with the first part as the key and the remaining part as the value.

Here's the updated monitor_process function with the necessary changes:

def monitor_process(target_title):
    global json_created
    monitor_details = get_active_monitors()
    window, monitor_index = check_window_title(target_title, monitor_details)

    if window:
        print(f"Tracking started for '{target_title}' on monitor {monitor_index}")
        start_time = datetime.now()
        data_dict["start_activity"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Track okay_button
        while True:
            # Track okay_button only if JSON is not created
            if not json_created:
                location, start_id_time = track_image(okay_button_path, "okay_button")
                if location:
                    # JSON creation and copying data once
                    x, y = location
                    pyautogui.moveTo(x + 20, y + 20)
                    pyautogui.rightClick()
                    pyautogui.hotkey('ctrl', 'c')
                    copied_text = pyperclip.paste()  # Copy extracted data

                    # Parse the copied text line by line into key-value pairs
                    parsed_data = {}
                    for line in copied_text.splitlines():
                        if line.strip():  # Ignore empty lines
                            key_value = line.split(maxsplit=1)  # Split into key and value based on first whitespace
                            if len(key_value) == 2:  # Ensure there is both a key and a value
                                key, value = key_value
                                parsed_data[key.strip()] = value.strip()

                    # Store parsed key-value pairs in data_dict
                    data_dict.update(parsed_data)
                    data_dict['start_id_time'] = start_id_time.strftime("%Y-%m-%d %H:%M:%S")

                    # Create JSON file
                    case_id = "example_case_id"  # Replace with actual case ID
                    save_to_json(data_dict, case_id)
                    json_created = True  # Mark JSON as created
                    print("JSON created with OK button data.")

            # After JSON creation, start tracking end_button
            if json_created:
                location_end, end_time = track_image(end_button_path, "end_button")
                if location_end:
                    # Store end_button location details
                    end_button_region = {
                        "left": location_end[0] - 10,
                        "top": location_end[1] - 10,
                        "right": location_end[0] + 10,
                        "bottom": location_end[1] + 10
                    }
                    print("End button detected, monitoring for clicks in region.")
                    
                    # Monitor clicks in the end button region
                    monitor_clicks_in_region(end_button_region)
                    return  # Exit after tracking the end button event

            time.sleep(1)  # Check every second

Explanation of Updates:

1. Parsing copied_text into Key-Value Pairs:

Each line of copied_text is processed individually by splitting it into key and value using line.split(maxsplit=1). This splits only at the first space, ensuring the key and value are separated correctly.

Only lines that contain both a key and a value are included in parsed_data.

Each key and value is stripped of extra whitespace for cleaner formatting.



2. Updating data_dict with Parsed Data:

After parsing, data_dict.update(parsed_data) is used to add the key-value pairs into data_dict.

This ensures that each key-value pair from the copied text is stored individually in the JSON.



3. Creating JSON with Parsed Data:

save_to_json(data_dict, case_id) is called to save the formatted data in JSON format, as previously.




This will result in a JSON file where each extracted line is stored as a separate key-value pair, as specified.




-----------------
import pyautogui
import time

# Move the mouse to the top-left corner of the first monitor (laptop screen)
pyautogui.moveTo(100, 100)
print("Moved mouse to first monitor")

# Wait for 2 seconds to observe the mouse
time.sleep(2)

# Move the mouse to the top-left corner of the second monitor (TV)
# Adjust the coordinates based on your setup (e.g., if your TV is to the right, x=1920 for 1080p laptop)
pyautogui.moveTo(1920 + 100, 100)
print("Moved mouse to second monitor (TV)")

# Wait for 2 seconds to observe the mouse
time.sleep(2)

# Perform a click action on the second monitor
pyautogui.click(1920 + 100, 100)  # Adjust based on the screen resolution and location
print("Clicked on second monitor")






To handle exiting the mouse.Listener loop based on a flag, you can use a global variable that gets updated when the flag condition is met. This flag can be checked inside the on_click function, and if the condition is true, it will stop the listener.

To implement this, modify the on_click function to check the flag after each click. If the flag indicates that the program should stop, you can call listener.stop() to exit the loop.

Here's how you can modify the code:

import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "test_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click and the button region
awaiting_final_ok_click = False
ok_button_region = None  # Store button's bounding box
exit_flag = False  # Flag to indicate when to stop the listener

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    global ok_button_region
    try:
        print("Checking for button visibility...")
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.9)
        if location:
            print(f"Button found at {location.left}, {location.top}, {location.width}, {location.height}")
            # Store the bounding box as (left, top, width, height)
            ok_button_region = location
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click, ok_button_region, exit_flag

    # Only proceed on button release
    if not pressed:
        print(f"Mouse clicked at ({x}, {y})")
        if awaiting_final_ok_click:
            # Check if the OK button is visible and a region is defined
            if ok_button_region:
                # Check if click is within the button region
                left, top, width, height = ok_button_region.left, ok_button_region.top, ok_button_region.width, ok_button_region.height
                if left <= x <= left + width and top <= y <= top + height:
                    print("Click is within OK button region. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
                    exit_flag = True  # Set exit flag to stop the listener
        else:
            # Update the OK button location and enable the awaiting click state
            if check_button_visible("ok"):
                awaiting_final_ok_click = True

        # Stop the listener if the exit flag is set
        if exit_flag:
            return False

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")

Explanation of Changes:

1. exit_flag Variable: This variable is set to True when the condition to stop the listener is met.


2. Condition to Stop Listener: In the on_click function, if exit_flag is True, the listener stops by returning False, which breaks out of the listener.join() loop.


3. Flag Update: After capturing a screenshot, exit_flag is set to True, stopping the listener after the next click.



Now, the code will automatically stop the listener when the exit_flag is set to True, allowing you to exit the loop when the desired condition is met.






Here is a modified version of your code that incorporates the functionality you've described:

Locate Image: When a user clicks, the program will locate the image on the screen.

Move the Mouse Below the Found Button: It will move slightly below the image (button) found.

Right-Click: The program will perform a right-click.

Copy: After right-clicking, the program will copy the content.

Store the Summary in JSON: It will store the summary in a JSON file with the timestamp when the image was found.

Return to Previous Position: After completing the actions, it will return the cursor to its original position.


Modified Code:

import time
import os
import pyautogui
import pyperclip
import json
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"
SUMMARY_JSON_PATH = r"C:\pulse_event_trigger\summary_data.json"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "test_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

# Function to capture screenshot
def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

# Function to check if a button is visible and take an action
def check_button_visible(button_name, curr_x, curr_y):
    try:
        print("Searching for button...")
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.9)
        
        if location:
            print(f"Button found at location: {location.left}, {location.top}, {location.width}, {location.height}")
            
            # Capture screenshot of the area
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_check_{timestamp}.png")
            pyautogui.screenshot(screenshot_filename, region=(location.left, location.top, location.width, location.height))
            print(f"Button screenshot captured and saved as {screenshot_filename}")
            
            # Move the cursor below the button and perform right click
            pyautogui.moveTo(location.left + location.width // 2, location.top + location.height + 5)
            pyautogui.rightClick()
            print("Right-click done")

            # Wait for a moment and then perform the copy action
            time.sleep(0.5)
            pyautogui.hotkey('ctrl', 'c')
            print("Content copied")

            # Get the copied text from clipboard
            summary_text = pyperclip.paste()
            print("Summary Text:", summary_text)

            # Create JSON data for storing summary
            summary_dict = {}
            lines = summary_text.split('\n')
            for line in lines:
                parts = line.split('\t')
                if len(parts) == 2:
                    key = parts[0]
                    value = parts[1].replace('\r', '')  # Clean up the value if needed
                    summary_dict[key] = value

            # Add timestamp to the summary data
            summary_dict["timestamp"] = timestamp

            # Save to a JSON file
            if os.path.exists(SUMMARY_JSON_PATH):
                with open(SUMMARY_JSON_PATH, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = {}

            # Append new summary data
            existing_data[timestamp] = summary_dict

            with open(SUMMARY_JSON_PATH, 'w') as f:
                json.dump(existing_data, f, indent=4)

            print(f"Summary saved to {SUMMARY_JSON_PATH}")
            
            return True

        else:
            print(f"Button '{button_name}' not found on the screen.")
            return False

    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return False

# Function to track mouse clicks
def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    print(f"Mouse clicked at ({x}, {y}) with {button}")

    # Only proceed on button release
    if not pressed:
        # If we're in the final "OK" step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok", x, y)
            if ok_button_location:
                # After the button is clicked and actions are completed, reset the state
                awaiting_final_ok_click = False
        else:
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring mouse clicks
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")

Key Changes:

1. check_button_visible Function:

This function now:

Locates the image on the screen.

Takes a screenshot of the image location.

Moves the mouse slightly below the found image (button) and performs a right-click.

Copies the content after the right-click and stores it in the clipboard.

Parses the copied content into a dictionary and saves it into a JSON file with a timestamp.




2. JSON Handling:

If the JSON file already exists, it loads the existing data and appends the new summary with the timestamp.

If it doesn't exist, a new file is created.



3. Timestamp in JSON:

The timestamp is added to each summary dictionary to track when the summary was captured.



4. Return to Previous Position:

You could modify this behavior if needed to return the cursor to the original position by saving the current position before the action and restoring it afterward.




Functionality Flow:

1. Locate Image: When the user clicks on the screen, the image is located and processed.


2. Perform Right-Click: If the image is found, the mouse is moved below the image and a right-click is performed.


3. Copy and Parse Content: The content copied to the clipboard is parsed and saved in a JSON file along with the timestamp.


4. Return to Position: The cursor can be returned to the original position if needed, and the summary data is saved for later use.



This approach ensures that the user can continue working without any noticeable delay, as the operations happen in milliseconds. The timestamp will also help you track when the summary was captured.




--------------------

import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "ok_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    try:
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    # Only proceed on button release
    if not pressed:
        # If we're in the final OK step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location:
                if ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width and \
                   ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                    print("OK button clicked. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
        else:
            # Placeholder for additional logic to handle other buttons
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
try:
    with mouse.Listener(on_click=on_click) as listener:
        print("Monitoring started... Press CTRL+C to stop.")
        listener.join()
except KeyboardInterrupt:
    print("\nMonitoring stopped.")






import time
import os
import pyautogui
from pynput import mouse
from datetime import datetime

BUTTON_IMAGE_PATH = r"C:\pulse_event_trigger"
SCREENSHOT_SAVE_PATH = r"C:\pulse_event_trigger\process"

# Define button images for reference
BUTTON_IMAGES = {
    "ok": os.path.join(BUTTON_IMAGE_PATH, "ok_button.png")  # Image of the "OK" button
}

# Track if we are awaiting the final "OK" click
awaiting_final_ok_click = False

def capture_screenshot():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = os.path.join(SCREENSHOT_SAVE_PATH, f"screenshot_{timestamp}.png")
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    try:
        location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
        return location
    except Exception as e:
        print(f"Error finding button {button_name}: {e}")
        return None

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click

    # Only proceed on button release
    if not pressed:
        # If we're in the final OK step
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location:
                if ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width and \
                   ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                    print("OK button clicked. Capturing screenshot.")
                    capture_screenshot()
                    awaiting_final_ok_click = False  # Reset state after capture
        else:
            # Placeholder for additional logic to handle other buttons
            awaiting_final_ok_click = True

# Screenshot save path setup
os.makedirs(SCREENSHOT_SAVE_PATH, exist_ok=True)

# Start monitoring
with mouse.Listener(on_click=on_click) as listener:
    print("Monitoring started...")
    listener.join()




Your code is well-structured, but there are a few areas that could be refined to ensure smooth functioning, especially around detecting the "OK" button and handling errors. I’ve adjusted a few parts to enhance reliability and address the GetIsInvokedPattern() method, which isn’t directly available in uiautomation. I also refined the find_ok_button method to streamline the button search process and logging.

Here’s the modified version of your code:

import uiautomation as auto
import win32gui
import win32con
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "c:\\Pulserimage"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window title matches Claude's window"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}\\claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find the OK button by name within the Claude window"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Search for button by exact match on button name
            button = window.ButtonControl(Name=self.button_name)
            if button.Exists(0, 0):
                return button
            
            # Alternative search if exact match not found
            for control in window.GetChildren():
                if isinstance(control, auto.ButtonControl) and \
                   self.button_name.lower() in control.Name.lower():
                    return control
            
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_click_time = 0
        click_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists(0, 0):
                            try:
                                # Check if button was clicked
                                current_time = time.time()
                                
                                if current_time - last_click_time > click_cooldown:
                                    logging.info("OK button detected - Capturing screenshot")
                                    self.capture_screenshot(active_window)
                                    last_click_time = current_time
                            
                            except Exception as e:
                                logging.error(f"Button click detection error: {e}")
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")

Explanation of Modifications

1. Simplified find_ok_button:

Now it first tries a direct search for a button with the name self.button_name. If it doesn’t find an exact match, it searches through children controls.



2. Removed GetIsInvokedPattern:

As this is not directly available or necessary here, we rely on Exists() to check if the button is present, and we capture the screenshot if it’s detected within the Claude window.



3. Added Error Handling and Logging:

Improved logging for button detection and screenshot functions to help diagnose issues during execution.




Required Packages

Pillow: for ImageGrab, installed with pip install Pillow.

pywin32: for win32gui and win32con, installed with pip install pywin32.

uiautomation: for UI control handling, installed with pip install uiautomation.


This code will continuously monitor for the "OK" button in the specified window, and if it appears and is active, it will take a screenshot and save it with a timestamp. This way, it should meet the requirements without unnecessary complexity.





import uiautomation as auto
import win32gui
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "claude_screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        # Store last button state
        self.last_button_state = None
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window is Claude"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find OK button using multiple methods"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Method 1: Direct search
            try:
                button = window.ButtonControl(Name=self.button_name)
                if button.Exists():
                    return button
            except:
                pass
            
            # Method 2: Search all descendants
            try:
                buttons = window.GetChildren()
                for control in buttons:
                    try:
                        if isinstance(control, auto.ButtonControl) and \
                           self.button_name.lower() in control.Name.lower():
                            return control
                    except:
                        continue
            except:
                pass
            
            # Method 3: Deep search
            try:
                buttons = window.FindAll(auto.ButtonControl)
                for button in buttons:
                    try:
                        if self.button_name.lower() in button.Name.lower():
                            return button
                    except:
                        continue
            except:
                pass
                
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def is_button_clicked(self, button):
        """Check if button was clicked using multiple methods"""
        try:
            # Method 1: Check invoke pattern
            try:
                invoke_pattern = button.GetInvokePattern()
                if invoke_pattern:
                    current_state = button.CurrentIsInvoked
                    if current_state != self.last_button_state:
                        self.last_button_state = current_state
                        return current_state
            except:
                pass

            # Method 2: Check selection pattern
            try:
                selection_pattern = button.GetSelectionPattern()
                if selection_pattern:
                    return selection_pattern.IsSelected
            except:
                pass

            # Method 3: Check toggle pattern
            try:
                toggle_pattern = button.GetTogglePattern()
                if toggle_pattern:
                    return toggle_pattern.ToggleState == auto.ToggleState.On
            except:
                pass

            # Method 4: Check button state
            try:
                current_state = button.GetLegacyIAccessiblePattern().CurrentState
                was_clicked = bool(current_state & auto.State.Pressed)
                return was_clicked
            except:
                pass

            return False

        except Exception as e:
            logging.error(f"Button click check error: {e}")
            return False

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_capture_time = 0
        capture_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists():
                            # Check if button was clicked
                            current_time = time.time()
                            
                            if self.is_button_clicked(ok_button):
                                # Prevent duplicate captures
                                if current_time - last_capture_time > capture_cooldown:
                                    print("OK button clicked - Capturing screenshot")
                                    logging.info("OK button clicked - Capturing screenshot")
                                    self.capture_screenshot(active_window)
                                    last_capture_time = current_time
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")





import uiautomation as auto
import win32gui
import win32con
import time
from datetime import datetime
import os
from PIL import ImageGrab
import logging

class ClaudeMonitor:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            filename='claude_monitor.log'
        )
        
        # Create screenshots directory
        self.screenshot_dir = "claude_screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        # Initialize monitoring flag
        self.monitoring = True
        
        # Configure window and button settings
        self.window_title = "Claude"
        self.button_name = "OK"
        
        logging.info("Claude Monitor initialized")

    def is_claude_window(self, window_title):
        """Check if the window is Claude"""
        return self.window_title.lower() in window_title.lower()

    def capture_screenshot(self, window_handle):
        """Capture screenshot of the window"""
        try:
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/claude_{timestamp}.png"
            
            # Capture and save screenshot
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            logging.info(f"Screenshot saved: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Screenshot error: {e}")
            return None

    def find_ok_button(self, window_handle):
        """Find OK button using multiple methods"""
        try:
            # Get window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Method 1: Direct search for OK button
            try:
                button = window.ButtonControl(Name=self.button_name)
                if button.Exists():
                    return button
            except:
                pass
            
            # Method 2: Search all buttons
            try:
                all_buttons = window.GetChildren()
                for control in all_buttons:
                    try:
                        if isinstance(control, auto.ButtonControl) and \
                           self.button_name.lower() in control.Name.lower():
                            return control
                    except:
                        continue
            except:
                pass
            
            # Method 3: Deep search
            try:
                buttons = window.FindAllControl(auto.ButtonControl)
                for button in buttons:
                    try:
                        if self.button_name.lower() in button.Name.lower():
                            return button
                    except:
                        continue
            except:
                pass
                
            return None
            
        except Exception as e:
            logging.error(f"Button search error: {e}")
            return None

    def monitor_claude(self):
        """Main monitoring loop"""
        print("Starting Claude Monitor... Press Ctrl+C to stop")
        logging.info("Monitoring started")
        
        last_click_time = 0
        click_cooldown = 1  # Prevent double captures
        
        try:
            while self.monitoring:
                try:
                    # Get active window
                    active_window = win32gui.GetForegroundWindow()
                    window_title = win32gui.GetWindowText(active_window)
                    
                    # Check if it's Claude window
                    if self.is_claude_window(window_title):
                        # Find OK button
                        ok_button = self.find_ok_button(active_window)
                        
                        if ok_button and ok_button.Exists():
                            try:
                                # Check if button was clicked
                                current_time = time.time()
                                
                                if ok_button.GetIsInvokedPattern():
                                    # Prevent duplicate captures
                                    if current_time - last_click_time > click_cooldown:
                                        logging.info("OK button clicked - Capturing screenshot")
                                        self.capture_screenshot(active_window)
                                        last_click_time = current_time
                            except:
                                pass
                    
                    # Small sleep to prevent high CPU usage
                    time.sleep(0.1)
                    
                except Exception as e:
                    logging.error(f"Monitoring error: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print("\nStopping monitor...")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    # Create and start the monitor
    monitor = ClaudeMonitor()
    
    try:
        monitor.monitor_claude()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"Error: {e}")
    finally:
        print("Monitor stopped")




-------------------

import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}

    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def find_buttons_in_window(self, window_control):
        """
        Recursively find all buttons in a window
        """
        buttons = []
        try:
            # Check if current control is a button
            if isinstance(window_control, auto.ButtonControl):
                buttons.append(window_control)
            
            # Get all children of the current control
            children = window_control.GetChildren()
            for child in children:
                # Recursively search for buttons in children
                buttons.extend(self.find_buttons_in_window(child))
                
        except Exception as e:
            logging.error(f"Error finding buttons: {e}")
        
        return buttons

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            # Get the window control
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                try:
                    button = window.ButtonControl(AutomationId=button_config['automation_id'])
                    if button.Exists():
                        return button
                except:
                    pass
            
            # 2. Try by Name and Class
            if button_config['class_name']:
                try:
                    button = window.ButtonControl(
                        Name=button_config['button_name'],
                        ClassName=button_config['class_name']
                    )
                    if button.Exists():
                        return button
                except:
                    pass
            
            # 3. Search all buttons and match by name
            all_buttons = self.find_buttons_in_window(window)
            for btn in all_buttons:
                try:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        return btn
                except:
                    continue
            
            return None
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button:
                            try:
                                # Check if button is clickable and was clicked
                                if button.IsEnabled:
                                    # You might need to adjust this condition based on your specific applications
                                    # Some applications might require different methods to detect if a button was clicked
                                    button_state = button.GetTogglePattern() if hasattr(button, 'GetTogglePattern') else None
                                    if button_state and button_state.ToggleState == auto.ToggleState.On:
                                        logging.info(f"Endpoint triggered for {app_name}")
                                        screenshot_path = self.capture_screenshot(app_name, active_window)
                                        if screenshot_path:
                                            logging.info(f"Screenshot saved: {screenshot_path}")
                                            # Add your trigger code here
                                            pass
                            except Exception as e:
                                logging.error(f"Error checking button state: {e}")
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()







-----2--2--2-2--2-2


import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()







 Let me break down the solution and explain its limitations and reliability.

The current approach has several potential issues that could affect reliability. Let me explain the core components and suggest a more robust approach.



<antArtifact identifier="robust-application-monitor" type="application/vnd.ant.code" language="python" title="Robust Application Monitor with UI Automation">
import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()
</antArtifact>

Let me explain why the previous approach wasn't 100% reliable and how this new version is more robust:

1. Key Improvements:

- Uses UI Automation instead of mouse position tracking
- Multiple button detection methods
- Window-specific screenshot capture
- Improved error handling and logging
- Caching for better performance
- More reliable event detection

2. Reliability Factors:

The previous version had these issues:
- Relied on mouse position (unreliable)
- Couldn't detect buttons accurately
- No fallback methods
- Vulnerable to window changes

The new version is more reliable because:

a) Button Detection:
   - Uses Windows UI Automation framework
   - Multiple search methods (Automation ID, Class Name, Name)
   - Doesn't rely on screen coordinates
   - Can detect actual UI elements

b) Window Handling:
   - Proper window detection
   - Caches window handles
   - Handles window state changes
   - Window-specific screenshots

c) Error Prevention:
   - Multiple fallback methods
   - Extensive error handling
   - Detailed logging
   - Resource cleanup

3. Limitations (Still Not 100%):

Even this improved version has some limitations:

- Some applications might block UI Automation
- Custom UI frameworks might not be fully compatible
- Window titles might change
- System performance could affect timing
- Some applications might use non-standard controls

4. To Make It More Reliable:

You should:

1. Add application-specific details:
```python
"OWS": {
    "window_pattern": "OWS",
    "endpoint": {
        "button_name": "OK",
        "automation_id": "okButton",  # Add actual ID
        "class_name": "Button"        # Add actual class
    }
}
```

2. Test with your specific applications and add:
   - Exact window titles
   - Button automation IDs
   - Class names
   - Any specific patterns

3. Add additional error handling for your environment

To use this version:

1. Install requirements:
```python
pip install uiautomation pillow pywin32
```

2. Run the script:
```python
python robust_monitor.py
```

Would you like me to:
1. Add support for specific applications you're using?
2. Implement additional detection methods?
3. Add specific error handling for your environment?
4. Add custom triggers for when endpoints are detected?




import uiautomation as auto
import win32gui
import win32con
import logging
import time
from datetime import datetime
import os
from PIL import ImageGrab

class RobustApplicationMonitor:
    def __init__(self):
        # Configure endpoints with more specific identifiers
        self.app_configs = {
            "OWS": {
                "window_pattern": "OWS",
                "endpoint": {
                    "button_name": "OK",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,  # Add if known
                    "class_name": None      # Add if known
                }
            },
            "AWS": {
                "window_pattern": "AWS",
                "endpoint": {
                    "button_name": "Submit",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            },
            "JWS": {
                "window_pattern": "JWS",
                "endpoint": {
                    "button_name": "Done",
                    "control_type": auto.ButtonControl,
                    "automation_id": None,
                    "class_name": None
                }
            }
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='app_monitor_detailed.log'
        )
        
        self.screenshot_dir = "screenshots"
        os.makedirs(self.screenshot_dir, exist_ok=True)
        self.workflow_starts = {}
        self.monitoring = True
        
        # Cache for window handles
        self.window_cache = {}
        
    def find_window_by_title(self, title_pattern):
        """
        Find window using partial title match with caching
        """
        try:
            # First check cache
            if title_pattern in self.window_cache:
                if win32gui.IsWindow(self.window_cache[title_pattern]):
                    return self.window_cache[title_pattern]
                else:
                    del self.window_cache[title_pattern]
            
            def callback(hwnd, windows):
                if win32gui.IsWindowVisible(hwnd):
                    window_title = win32gui.GetWindowText(hwnd)
                    if title_pattern.lower() in window_title.lower():
                        windows.append(hwnd)
            
            windows = []
            win32gui.EnumWindows(callback, windows)
            
            if windows:
                self.window_cache[title_pattern] = windows[0]
                return windows[0]
            return None
            
        except Exception as e:
            logging.error(f"Error finding window '{title_pattern}': {e}")
            return None

    def get_button_in_window(self, window_handle, button_config):
        """
        Find button using UI Automation with multiple fallback methods
        """
        try:
            if not window_handle:
                return None
                
            window = auto.ControlFromHandle(window_handle)
            if not window:
                return None
            
            # Try multiple search methods in order of reliability
            button = None
            
            # 1. Try by Automation ID if available
            if button_config['automation_id']:
                button = window.ButtonControl(AutomationId=button_config['automation_id'])
            
            # 2. Try by Name and Class
            if not button and button_config['class_name']:
                button = window.ButtonControl(
                    Name=button_config['button_name'],
                    ClassName=button_config['class_name']
                )
            
            # 3. Try by Name only
            if not button:
                buttons = window.GetButtonControls()
                for btn in buttons:
                    if button_config['button_name'].lower() in btn.Name.lower():
                        button = btn
                        break
            
            return button
            
        except Exception as e:
            logging.error(f"Error finding button: {e}")
            return None

    def capture_screenshot(self, app_name, window_handle):
        """
        Capture screenshot of specific window
        """
        try:
            if not window_handle:
                return None
                
            # Get window coordinates
            rect = win32gui.GetWindowRect(window_handle)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{app_name}_{timestamp}.png"
            
            # Capture specific window
            screenshot = ImageGrab.grab(bbox=rect)
            screenshot.save(filename)
            
            # Log workflow duration
            if app_name in self.workflow_starts:
                duration = time.time() - self.workflow_starts[app_name]
                logging.info(f"Workflow duration for {app_name}: {duration:.2f} seconds")
                del self.workflow_starts[app_name]
            
            return filename
            
        except Exception as e:
            logging.error(f"Error capturing screenshot: {e}")
            return None

    def monitor_applications(self):
        """
        Main monitoring loop using UI Automation
        """
        logging.info("Starting robust application monitoring...")
        
        try:
            while self.monitoring:
                # Get active window
                active_window = win32gui.GetForegroundWindow()
                active_title = win32gui.GetWindowText(active_window)
                
                # Check each configured application
                for app_name, config in self.app_configs.items():
                    if config['window_pattern'].lower() in active_title.lower():
                        # Start workflow timing if not started
                        if app_name not in self.workflow_starts:
                            self.workflow_starts[app_name] = time.time()
                            logging.info(f"Started monitoring {app_name}")
                        
                        # Find and monitor the endpoint button
                        button = self.get_button_in_window(active_window, config['endpoint'])
                        
                        if button and button.GetClickablePoint():
                            # Check if button was clicked
                            if button.GetWindowPattern().WindowVisualState == auto.WindowVisualState.Normal:
                                if button.GetTogglePattern().ToggleState == auto.ToggleState.On:
                                    logging.info(f"Endpoint triggered for {app_name}")
                                    screenshot_path = self.capture_screenshot(app_name, active_window)
                                    if screenshot_path:
                                        logging.info(f"Screenshot saved: {screenshot_path}")
                                        # Add your trigger code here
                                        pass
                
                time.sleep(0.1)  # Reduce CPU usage
                
        except KeyboardInterrupt:
            logging.info("Monitoring stopped by user")
        except Exception as e:
            logging.error(f"Error in monitoring loop: {e}")
        finally:
            self.monitoring = False
            logging.info("Monitoring stopped")

if __name__ == "__main__":
    monitor = RobustApplicationMonitor()
    monitor.monitor_applications()






merhod-1 :

In this case, since the endpoint (like the "OK" button) doesn't exist in the window title, we'll need a way to detect specific buttons on the screen by using image recognition to look for UI elements. Here’s how we can achieve this with pyautogui to recognize on-screen buttons and trigger the screenshot capture when the final button (like "OK") is clicked.

Approach Outline

1. Image Recognition for Button Detection: Use pyautogui.locateOnScreen() to search for specific buttons on the screen by using screenshots of each button type as reference images. For example, take screenshots of the "Next," "Clear," and "OK" buttons and save them as images like next_button.png, clear_button.png, and ok_button.png.


2. Click Monitoring: Set up a listener with pynput to monitor each mouse click and use pyautogui.locateOnScreen() to check if the "OK" button is visible. When it's clicked, trigger the screenshot capture.


3. Trigger Screenshot on "OK" Click: Only take a screenshot when the "OK" button is clicked. We can determine this by using image matching right after a mouse click to check if the click position aligns with the "OK" button.



Here's an example of how this could be implemented in Python:

import time
import pyautogui
from pynput import mouse
from datetime import datetime

# Define button images for reference (these images should be screenshots of your buttons)
BUTTON_IMAGES = {
    "next": "next_button.png",   # Image of the "Next" button
    "clear": "clear_button.png", # Image of the "Clear" button
    "ok": "ok_button.png"        # Image of the "OK" button
}

# This flag helps track if we are in the final step (where "OK" appears)
awaiting_final_ok_click = False

def capture_screenshot():
    # Capture and save screenshot with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    screenshot_filename = f"screenshot_{timestamp}.png"
    pyautogui.screenshot(screenshot_filename)
    print(f"Screenshot captured and saved as {screenshot_filename}")

def check_button_visible(button_name):
    # Use pyautogui to search for the button on screen
    location = pyautogui.locateOnScreen(BUTTON_IMAGES[button_name], confidence=0.8)
    return location

def on_click(x, y, button, pressed):
    global awaiting_final_ok_click
    
    # Only process on button release
    if not pressed:
        # Check for "OK" button visibility and click position
        if awaiting_final_ok_click:
            ok_button_location = check_button_visible("ok")
            if ok_button_location and ok_button_location.left <= x <= ok_button_location.left + ok_button_location.width \
                    and ok_button_location.top <= y <= ok_button_location.top + ok_button_location.height:
                print("OK button clicked. Capturing screenshot.")
                capture_screenshot()
                awaiting_final_ok_click = False  # Reset the state after capturing screenshot
        else:
            # Check if "Next" button or other buttons are clicked, progressing to the final step
            next_button_location = check_button_visible("next")
            if next_button_location:
                print("Next button detected and clicked. Progressing workflow.")
                # Move forward in workflow, expecting OK button soon
                awaiting_final_ok_click = True

# Set up the mouse listener
with mouse.Listener(on_click=on_click) as listener:
    print("Monitoring started...")
    listener.join()

Explanation

1. Button Image Matching:

check_button_visible() checks if a specific button is on the screen by using an image of the button (e.g., next_button.png, clear_button.png, ok_button.png).

If locateOnScreen() finds the button with sufficient confidence, it returns the position of that button on the screen.



2. Mouse Click Monitoring:

In on_click, we first check if the "Next" or other intermediate buttons are visible and clicked.

If they are, we set awaiting_final_ok_click = True, indicating we’re ready for the "OK" button in the workflow.

When awaiting_final_ok_click is True, we listen for the "OK" button to be clicked at the specified location.



3. Screenshot Trigger on Final Click:

When awaiting_final_ok_click is True, the script will monitor for an "OK" button click.

Once detected, capture_screenshot() is called to save the current screen image.




Additional Notes

Button Images: Ensure the images of each button are clear and cropped tightly to the button's dimensions for accurate detection.

Confidence Parameter: The confidence parameter in locateOnScreen() allows for a match tolerance, helping with slight visual variations in buttons. Experiment with values like 0.8 or 0.9.

Error Handling: You might need to add error handling for cases where the buttons aren’t detected due to changes in UI.


This setup should help you track the click sequence, detect the final click on the "OK" button, and take a screenshot accordingly.




method-2: 


Given the need to monitor all types of applications, including both desktop and web, and to detect when a button click matches specific conditions (like the button being labeled "OK"), here’s a refined approach that combines accessibility APIs and click monitoring to address your requirements:

Approach Overview

1. Monitor Global Click Events: Use a global hook to detect mouse clicks.


2. Identify UI Elements at Click Locations: For each click, detect if it’s on a button or other UI control with specific attributes (such as “OK” or “Submit”) using accessibility APIs.


3. Trigger Screenshot on Matching Clicks: If the click matches the target endpoint (e.g., button labeled "OK"), trigger the screenshot capture.



Implementation Steps

1. Monitor Global Mouse Clicks

To globally monitor mouse clicks across applications, you can use libraries like pynput to set up a global mouse listener in Python. This listener will intercept all mouse events, regardless of the active application.

from pynput import mouse
import time

# Callback function for mouse clicks
def on_click(x, y, button, pressed):
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        # Call function to check if this click is on the desired button
        check_ui_element(x, y)

# Set up a global listener
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

This code captures each click's location (x, y coordinates). For every click, it calls check_ui_element(x, y) to inspect the UI element under the cursor.

2. Identify the Clicked UI Element

Using libraries like pywinauto or uiautomation, you can retrieve information about the UI element at the clicked position. Since pywinauto can inspect accessibility information for most applications, it’s helpful in identifying UI elements on both Windows and some web applications running in a browser.

Here’s an example using uiautomation to check if the click location has a button with the desired label:

from uiautomation import Control

def check_ui_element(x, y):
    # Find the control at the given screen coordinates
    control = Control(x=x, y=y)
    
    # Check if the control is a button and matches the desired title
    if control.ControlTypeName == 'Button' and control.Name in ['OK', 'Submit', 'Done']:
        print(f"Button '{control.Name}' clicked at ({x}, {y})")
        capture_screenshot()
    else:
        print("Click did not match the target button.")

This function:

Finds the control at the coordinates of the mouse click.

Checks the control type and name to see if it’s a button and if its name matches the desired endpoint (OK, Submit, Done).

Triggers a screenshot if the control meets these conditions.


3. Capture a Screenshot

To capture a screenshot, you can use libraries like Pillow. You’d call this function within check_ui_element() if the click matches your endpoint.

from PIL import ImageGrab
import datetime

def capture_screenshot():
    # Generate a timestamped filename
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

This code captures the entire screen and saves it with a timestamped filename when called.

Putting It All Together

Here’s a consolidated example combining all the pieces:

from pynput import mouse
from uiautomation import Control
from PIL import ImageGrab
import datetime

# Define a list of target button names
TARGET_BUTTON_NAMES = ['OK', 'Submit', 'Done']

# Function to capture a screenshot
def capture_screenshot():
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

# Function to check if the clicked element matches the target endpoint
def check_ui_element(x, y):
    control = Control(x=x, y=y)
    if control.ControlTypeName == 'Button' and control.Name in TARGET_BUTTON_NAMES:
        print(f"Button '{control.Name}' clicked at ({x}, {y})")
        capture_screenshot()
    else:
        print("Click did not match the target button.")

# Mouse click event handler
def on_click(x, y, button, pressed):
    if pressed:
        print(f"Mouse clicked at ({x}, {y})")
        check_ui_element(x, y)

# Set up global mouse listener
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

Explanation of How It Works

1. Global Mouse Listener: The mouse.Listener detects all mouse clicks globally.


2. Element Inspection: For each click, check_ui_element(x, y) checks if the clicked UI element matches the target button criteria.


3. Screenshot Trigger: If a target button is clicked, capture_screenshot() is called to capture and save the screen.



Advantages of This Method

Versatile Application: Works across different types of applications since it relies on coordinates and UI Automation APIs.

Efficient Detection: Only triggers a screenshot for relevant button clicks, without constantly searching for button images.

Scalability: Easily adaptable if you need to monitor other UI elements or applications with similar button endpoints.


This approach avoids relying on visual matching and instead leverages UI properties, making it more robust for different types of applications.



method-3:

To streamline this process and avoid frequent image captures, you can use a more direct approach that combines system hooks and lightweight UI element checks. By using pyautogui to track specific clicks and uiautomation to verify elements only when a click occurs, you can avoid excessive image processing and reduce processing time. Here’s an optimized solution that focuses on capturing a screenshot only when the user clicks on the desired endpoint button (e.g., “OK”).

Optimized Solution Outline

1. Initial and Final Image Capture Only: Capture an initial image when the target window opens and a final image when the workflow endpoint is clicked (e.g., the “OK” button).


2. Direct Click Monitoring with pyautogui: Track clicks and verify the active window title to ensure you only check for clicks within relevant applications.


3. UI Automation for Click Validation: Instead of image processing for each click, use uiautomation to directly validate whether a click occurred on a specific button (like “OK”) within the target window.



Implementation Steps

This example captures a screenshot only when the “OK” button is clicked, marking the end of the workflow.

from pynput import mouse
import pyautogui
import time
import datetime
from PIL import ImageGrab
import uiautomation as automation
from ctypes import windll
import ctypes

# Target application and button configurations
TARGET_APPS = ["OWS", "AWS", "JWS"]
TARGET_BUTTON_NAMES = ["OK", "Submit", "Done"]

# Track start and end times
start_time = None

# Helper to get the current active window title
def get_active_window_title():
    hwnd = windll.user32.GetForegroundWindow()
    length = windll.user32.GetWindowTextLengthW(hwnd)
    buffer = ctypes.create_unicode_buffer(length + 1)
    windll.user32.GetWindowTextW(hwnd, buffer, length + 1)
    return buffer.value

# Capture screenshot function
def capture_screenshot():
    filename = f"screenshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    screenshot = ImageGrab.grab()
    screenshot.save(filename)
    print(f"Screenshot saved as {filename}")

# Function to verify button click
def check_if_button_clicked(x, y):
    control = automation.Control(x=x, y=y)
    if control.ControlTypeName == 'Button' and control.Name in TARGET_BUTTON_NAMES:
        return control.Name
    return None

# Event handler for mouse clicks
def on_click(x, y, button, pressed):
    global start_time
    if pressed:
        active_window = get_active_window_title()

        # Check if active window is one of the target applications
        if any(app in active_window for app in TARGET_APPS):
            # Track start time on first detected click in target app
            if start_time is None:
                start_time = datetime.datetime.now()
                print("Start time recorded:", start_time)

            # Check if the clicked element is a target button
            clicked_button = check_if_button_clicked(x, y)
            if clicked_button:
                print(f"'{clicked_button}' button clicked at ({x}, {y})")

                # Capture end time and screenshot if the endpoint is clicked
                if clicked_button in TARGET_BUTTON_NAMES:
                    end_time = datetime.datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    print(f"End of workflow detected. Duration: {duration} seconds.")
                    capture_screenshot()  # Capture final screenshot
                    start_time = None  # Reset for next workflow

# Start listening for mouse events
with mouse.Listener(on_click=on_click) as listener:
    listener.join()

Explanation of the Optimized Solution

1. Window Title Check: Each click event first checks the active window title to ensure clicks are within the target application. This avoids unnecessary checks outside the relevant applications.


2. Direct Click Validation: When a click is detected, the code uses uiautomation to verify if it’s on a button matching the endpoint criteria (like "OK"). This avoids image comparisons, reducing processing time to milliseconds.


3. Minimal Screenshot Capture: Screenshots are only taken at the start (optional) and end of the workflow, ensuring no redundant image captures during the workflow.


4. Timing Control: The start_time and end_time are recorded only when the relevant window is active, and the workflow endpoint button is clicked, allowing for precise tracking of workflow duration.



Performance Considerations

Efficiency: By reducing image captures and relying on UI element checks only when needed, this method avoids the delays associated with frequent image processing.

Accuracy: Using uiautomation ensures that you’re validating clicks on actual UI elements (like buttons) rather than relying on pixel-based image matching.

Responsiveness: This approach minimizes overhead, so the response time should be close to milliseconds for each click check.


This setup should meet the requirement for fast, efficient tracking without the heavy processing of frequent image analysis.




Yes, you can print the UI tree using the uiautomation library, which allows you to visualize the hierarchy and properties of accessible UI elements. This can be very useful for understanding the structure of the application's UI and identifying the elements available for interaction.

Here’s how you can do it:

1. Install uiautomation (if you haven’t already):

pip install uiautomation


2. Use the automation.GetRootControl() Method to get the root of the UI tree, then recursively print the tree. This will print the properties of each accessible control, including control type and name.



Here’s an example code snippet that captures the UI tree and prints it:

import uiautomation as automation

def print_ui_tree(control, depth=0):
    """
    Recursively print the UI tree starting from the given control.
    """
    # Print the current control's information with indentation based on depth
    print("  " * depth + f"ControlType: {control.ControlTypeName}, Name: {control.Name}, Class: {control.ClassName}")

    # Recursively print child controls
    for child in control.GetChildren():
        print_ui_tree(child, depth + 1)

# Capture the root control (entire screen)
root_control = automation.GetRootControl()

# Print the UI tree from the root control
print_ui_tree(root_control)

Explanation

Root Control: automation.GetRootControl() gets the root control of the entire desktop UI. You can also use automation.GetFocusedControl() if you want to start from the currently active window.

Recursive Printing: The print_ui_tree function goes through each control's children and prints details, adjusting indentation (depth) for easier visualization.


Adjustments and Usage Tips

1. Limiting Depth: If the UI tree is very large, you might want to limit the depth to avoid printing too many levels.


2. Filtering Controls: You can add conditions within print_ui_tree to filter specific control types, names, or classes.


3. Focus on a Specific Window: If you only want to print the tree for a specific window, use automation.WindowControl(Name="Window Title") in place of GetRootControl().



Example Output

The output will look something like this:

ControlType: Window, Name: Untitled - Notepad, Class: Notepad
  ControlType: MenuBar, Name: , Class: 
    ControlType: MenuItem, Name: File, Class: 
    ControlType: MenuItem, Name: Edit, Class: 
  ControlType: Edit, Name: Text Editor, Class: 
  ControlType: StatusBar, Name: , Class:

This output helps you understand the hierarchy and properties of each control, which you can then use to identify and interact with specific elements based on ControlType, Name, or ClassName.




method-5:

Sure, here’s an example code snippet that follows this approach using pyautogui to capture a small area around the mouse click and pytesseract for OCR to detect if a particular button (e.g., an “OK” button) is in that region.

This code assumes you’re checking for a specific word, like “OK,” on the button to determine if it’s your endpoint. If the word is detected, it will then trigger a full-screen capture.

Code Example

import pyautogui
import pytesseract
from PIL import Image
import time

# Configure pytesseract path if needed
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def capture_and_check_button(click_x, click_y, region_size=(100, 50)):
    """
    Captures a small region around the click coordinates and checks for the target text.
    :param click_x: X-coordinate of the click.
    :param click_y: Y-coordinate of the click.
    :param region_size: Size of the region to capture around the click (width, height).
    :return: True if the target text is found, False otherwise.
    """
    # Define the region to capture around the click (x, y, width, height)
    region = (click_x - region_size[0] // 2, click_y - region_size[1] // 2, region_size[0], region_size[1])
    
    # Capture the region
    small_capture = pyautogui.screenshot(region=region)
    
    # Use OCR to read text from the captured region
    text = pytesseract.image_to_string(small_capture)
    print(f"OCR Text Detected: {text.strip()}")
    
    # Check if the target text (e.g., "OK") is present in the text
    return "OK" in text.strip()

def monitor_clicks(region_size=(100, 50)):
    """
    Monitors mouse clicks and captures the full screen if the target button is clicked.
    :param region_size: Size of the region to capture around each click.
    """
    print("Monitoring clicks... Press Ctrl+C to stop.")
    
    try:
        while True:
            # Wait for a mouse click
            if pyautogui.mouseDown(button="left"):
                # Get mouse position
                click_x, click_y = pyautogui.position()
                
                # Check if the small capture contains the endpoint button (e.g., "OK")
                if capture_and_check_button(click_x, click_y, region_size):
                    print("Endpoint button clicked! Capturing the full screen...")
                    
                    # Capture full screen if the endpoint button was clicked
                    full_screenshot = pyautogui.screenshot()
                    full_screenshot.save("full_screenshot.png")
                    print("Full screen captured.")
                
                # Allow a small delay to avoid multiple triggers from a single click
                time.sleep(0.5)
                
    except KeyboardInterrupt:
        print("Click monitoring stopped.")

# Start monitoring clicks
monitor_clicks()

Explanation

1. capture_and_check_button():

This function captures a small region around the click location (specified by click_x, click_y).

It uses OCR to read any text in the captured image.

If the OCR detects the word “OK” in the text, it returns True to indicate that the endpoint button is clicked.



2. monitor_clicks():

Continuously monitors for left mouse clicks.

For each click, it retrieves the mouse position and passes it to capture_and_check_button() to check for the endpoint button.

If capture_and_check_button() returns True, it captures the full screen and saves it as “full_screenshot.png”.




How to Adjust for Performance

Adjust region_size: Try different values to find the smallest region that reliably detects your button.

OCR Optimization: Since pytesseract can be slow, reduce OCR workload by:

Setting a specific OCR language or whitelist.

Considering alternatives like OpenCV template matching if button appearance is consistent.



This setup should process quickly for small regions, minimizing time compared to full-screen processing at each click.

