package streaming.dstream

case class UserActionsWindow(ip: String, categories: Long, clicks: Long, views: Long, ratio: Double, total: Long, alreadyStored: Boolean)
