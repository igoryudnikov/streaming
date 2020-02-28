package streaming

import java.sql.Timestamp

case class UserActionsWindow(unix_time: Timestamp, category_id: String, ip: String, `type`: String, window: Window)
