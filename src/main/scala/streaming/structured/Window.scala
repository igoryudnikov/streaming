package streaming.structured

import java.sql.Timestamp

case class Window(start: Timestamp, end: Timestamp)
