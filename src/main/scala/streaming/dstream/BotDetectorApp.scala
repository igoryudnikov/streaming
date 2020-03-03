package streaming.dstream

import com.datastax.spark.connector._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{count, countDistinct, from_json, sum, when, window}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object BotDetectorApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName("Bot Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport
      .getOrCreate()

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    import sparkSession.implicits._

    val userSchema = new StructType()
      .add("unix_time", "Timestamp")
      .add("category_id", "String")
      .add("ip", "String")
      .add("type", "String")

    val value = classOf[StringDeserializer]
    val kafkaParams = Map(
      "key.deserializer" -> value,
      "value.deserializer" -> value,
      "group.id" -> "test_cons",
      "bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val streamingContext = StreamingContext.getActiveOrCreate(() => {
      val sc = new StreamingContext(sparkSession.sparkContext, Seconds(10))
      // sc.checkpoint(checkpointDir)
      KafkaUtils.createDirectStream[String, String](
        sc,
        PreferConsistent,
        Subscribe[String, String](Set("user_actions"), kafkaParams)
      )
        .transform {
          message =>
            val sc = message.sparkContext
            val res: DataFrame = message.map(record => record.value())
              .toDF("value")
              .where(from_json($"value", userSchema).isNotNull)
              .select(from_json($"value", userSchema).as[UserAction])
              .groupBy($"ip", window($"unix_time", "10 minutes", slideDuration = "1 minute"))
              .agg(
                sum(when($"type" === "click", 1).otherwise(0)).as("clicks"),
                sum(when($"type" === "view", 1).otherwise(0)).as("views"),
                countDistinct("category_id").as("categories"),
                count("ip").as("total")
              )
              .withColumn("ratio", $"clicks" / $"views")
              .toDF()
              .as("runningBots")

            val storedBots: RDD[(String, Boolean)] = sc.cassandraTable("botdetection", "stored_bots")
              .select("ip")
              .map(row => (row.get[String]("ip"), true))

            res
              .join(storedBots.toDF("ip", "alreadyStored").as("storedBots"),
                $"storedBots.ip" === $"runningBots.ip",
                "left")
              .select("runningBots.ip", "clicks", "views", "categories", "ratio", "total", "alreadyStored")
              .na.fill(value = false, Array("alreadyStored"))
              .na.fill(value = 0, Array("ratio"))
              .as[UserActionsWindow]
              .rdd
        }
        .filter(r => r.clicks > 10 && !r.alreadyStored)
        .filter(r => r.ratio > 3 || r.total > 250 || r.categories > 10)
        .foreachRDD(r => {
          r.saveToCassandra("botdetection", "stored_bots", SomeColumns("ip"))
          r.map(r => r.ip).distinct().foreach(z => {
            println("RED CODE, satellites launched, codes loaded...")
            println("Bot detected: " + z)
          })
        })

      sc
    })

    streamingContext.start
    streamingContext.awaitTermination

  }

}
