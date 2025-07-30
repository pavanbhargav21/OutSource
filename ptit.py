📝 Analysis: Data Missing in Dashboard Due to Transfer Window Constraints

We are currently running a Pulse agent with both AppTrace and Systrace modules. The Systrace module is responsible for capturing keyboard and mouse activity events. These captured events are stored locally on the user's machine and are transferred to the Event Hub during specific transfer windows.

🔄 Transfer Behavior:

For Systrace data to be transferred, the user system must be active during the transfer window.

If a user is only active once in a week during this window, all accumulated data in the local database is sent at once.

This can result in millions of flow records being transferred from a single agent/user in one go.


This pattern applies across all users. Given that keyboard and mouse activity data is high in volume, the overall data load becomes massive, especially when multiple users transfer bulk data simultaneously.


---

⏱️ Impact of Transfer and Processing Windows

The transfer window is limited to 8 hours daily.

Our Databricks processing pipelines run every hour.

During the 8-hour window, the volume of incoming data is extremely large.

As a result, some user data doesn't get processed in time and does not appear in the dashboard.


This is because:

The dashboard displays data from either the raw or gold layer.

If the raw data is not yet ingested, it won't be transformed to the gold layer.

Hence, keyboard/mouse activity may be missing from the dashboard for affected users.



---

🌍 Regional Differences

This issue is predominantly seen in West Europe, where data movement is restricted to scheduled transfer windows.

In contrast, East Europe operates with a continuous data movement strategy (every 20 seconds from local DB to Event Hub).

This frequent transfer avoids data buildup, resulting in smaller, manageable data volumes at any given time.

Therefore, East Europe does not experience the same issue.


