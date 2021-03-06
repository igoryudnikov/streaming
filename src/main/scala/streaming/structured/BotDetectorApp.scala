package streaming.structured

import java.util.UUID.randomUUID

import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.IgniteContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{from_json, lit, unix_timestamp, window}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object BotDetectorApp {

  def main(args: Array[String]) {
    val banTimeSecs = 600
    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName("Bot Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.output.ttl", banTimeSecs)
      .enableHiveSupport
      .getOrCreate()

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val stream = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_actions")
      .option("startingOffsets", "earliest")
      .load()

    import sparkSession.implicits._

    val userSchema = new StructType()
      .add("unix_time", "Timestamp")
      .add("category_id", "String")
      .add("ip", "String")
      .add("type", "String")

    val res = stream
      .selectExpr("CAST(value AS STRING)")
      .where(from_json($"value", userSchema).isNotNull)
      .select(from_json($"value", userSchema).as[UserAction])
      .withWatermark("unix_time", "30 seconds")
      .withColumn("window", window($"unix_time", "10 minutes", slideDuration = "1 minute"))
      .as[UserActionsWindow]
      .groupByKey(r => (r.ip, r.window))
      .mapGroups((key, actions) => {
        var clicks = 0
        var views = 0
        var categories: Set[Int] = Set()
        var total = 0
        actions.foreach { action =>
          action.`type` match {
            case "click" => clicks = clicks + 1
            case "view" => views = views + 1
          }
          categories = categories ++ Set(action.category_id.toInt)
          total = total + 1
        }
        val ratio = if (views == 0) clicks else clicks.toDouble / views.toDouble
        UserActionAggregation(key._1, clicks, views, ratio, total, categories.size)
      })

    val storedBots = sparkSession.sparkContext.cassandraTable("botdetection", "stored_bots")
      .select("ip")
      .map(row => (row.get[String]("ip"), row.get[Long]("banUpTo")))

    val igniteContext = new IgniteContext(sparkSession.sparkContext, () => new IgniteConfiguration())

    val cacheRdd = igniteContext.fromCache(new CacheConfiguration[String, Long](randomUUID().toString))

    cacheRdd.savePairs(storedBots)

    val storedBotsDF = cacheRdd
      .toDF("ip", "banUpTo")
      .withColumn("now", unix_timestamp())
      .withColumn("alreadyStored", lit(true))
      .where($"banUpTo" > $"now")
      .as("storedBots")

    res.join(storedBotsDF, res("ip") === storedBotsDF("ip"), "left")
      .select(res("ip"), $"clicks", $"views", $"categories", $"ratio", $"requestsPerWindow", $"storedBots.alreadyStored")
      .na.fill(value = false, Array("alreadyStored"))
      .na.fill(value = 0, Array("ratio"))
      .withColumn("currentTime", unix_timestamp)
      .as[UserActionAggregation]
      .filter(r => r.clicks > 10 && !r.alreadyStored)
      .filter(r => r.ratio > 3 || r.requestsPerWindow > 250 || r.categories > 10)
      .writeStream
      .foreachBatch { (batchDF: Dataset[UserActionAggregation], _) =>
        val timestamp = System.currentTimeMillis / 1000
        val filtered = batchDF
          .map(z => z.ip)
          .except(cacheRdd.map(z => z._1).toDS())

        filtered
          .toDF("ip")
          .write
          .cassandraFormat("stored_bots", "botdetection")
          .mode(SaveMode.Overwrite)
          .option("confirm.truncate", true)
          .save

        cacheRdd.savePairs(filtered.map(z => (z, timestamp + banTimeSecs)).rdd)
      }.start()
      .awaitTermination()
  }
}
