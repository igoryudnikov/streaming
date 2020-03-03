package streaming.structured

case class UserActionAggregation(ip: String, clicks: Int, views: Int, ratio: Double, requestsPerWindow: Int, categories: Int, alreadyStored: Boolean = false)
