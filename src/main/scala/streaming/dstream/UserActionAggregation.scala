package streaming.dstream

case class UserActionAggregation(clicks: Int, views: Int, ratio: Double, requestsPerWindow: Int, totalRequests: Int)