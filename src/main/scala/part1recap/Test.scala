package part1recap


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Test extends App {
  val spark = SparkSession.builder()
    .appName("Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    (1, "login", "2023-06-01 10:00:00"),
    (1, "click", "2023-06-01 10:05:00"),
    (1, "logout", "2023-06-01 10:20:00"),
    (1, "login", "2023-06-01 10:30:00"),  // prev_event_timestamp = 10:00:00 (same user, same event_type)
    (2, "login", "2023-06-01 09:00:00"),
    (2, "click", "2023-06-01 09:15:00")
  )

  val df = data
    .toDF("user_id", "event_type", "event_timestamp")
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))

  // Window: partition by user_id AND event_type, order by event_timestamp
  // rowsBetween: from unbounded preceding to 1 row before current (-1)
  val windowSpec = Window
    .partitionBy("user_id", "event_type")
    .orderBy("event_timestamp")
    .rowsBetween(Window.unboundedPreceding, -1)

  // Add prev_event_timestamp: max(event_timestamp) where event_timestamp < current row's timestamp
  val resultDF = df.withColumn(
    "prev_event_timestamp",
    max("event_timestamp").over(windowSpec)
  )

  resultDF.show(truncate = false)

  /*
   * Expected Result:
   * +-------+----------+-------------------+-------------------+
   * |user_id|event_type|event_timestamp    |prev_event_timestamp|
   * +-------+----------+-------------------+-------------------+
   * |1      |click     |2023-06-01 10:05:00|null               |  <- first click for user 1
   * |1      |login     |2023-06-01 10:00:00|null               |  <- first login for user 1
   * |1      |login     |2023-06-01 10:30:00|2023-06-01 10:00:00|  <- second login, prev = 10:00:00
   * |1      |logout    |2023-06-01 10:20:00|null               |  <- first logout for user 1
   * |2      |click     |2023-06-01 09:15:00|null               |  <- first click for user 2
   * |2      |login     |2023-06-01 09:00:00|null               |  <- first login for user 2
   * +-------+----------+-------------------+-------------------+
   *
   * Reflection:
   * - Window function partitions by (user_id, event_type) to group events per user per type
   * - orderBy("event_timestamp") ensures chronological order within partitions
   * - rowsBetween(unboundedPreceding, -1) defines frame: all rows before current row (excluding current row)
   * - max("event_timestamp").over(windowSpec) finds max timestamp where timestamp < current row's timestamp
   * - First event of each (user_id, event_type) combination has null prev_event_timestamp
   */
}