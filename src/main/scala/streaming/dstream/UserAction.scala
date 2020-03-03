package streaming.dstream

import java.sql.Timestamp

case class UserAction(unix_time: Timestamp, category_id: String, ip: String, `type`: String)