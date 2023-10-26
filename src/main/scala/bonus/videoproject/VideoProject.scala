package bonus.videoproject

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VideoProject extends App {

  val spark = SparkSession.builder()
    .appName("Video Project Fook")
    .config("spark.master", "local")
    .getOrCreate()

  val filesPath = "src/main/resources/data/bonus/videoproject"

  lazy val videosSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("url", StringType),
    StructField("creation_timestamp", LongType),
    StructField("creator_id", IntegerType),
    StructField("private", IntegerType)
  ))

  lazy val usersSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("fname", StringType),
    StructField("lname", StringType),
    StructField("email", StringType),
    StructField("country", StringType),
    StructField("subscription", IntegerType),
    StructField("categories", StringType),
    StructField("updated", TimestampType)
  ))
  lazy val eventInnerSchema = StructType(Array(
    StructField("user_id", IntegerType),
    StructField("video_id", IntegerType),
    StructField("event", StringType),
    StructField("timestamp", LongType),
    StructField("tags", ArrayType(StringType)),
    StructField("comment", StringType)
  ))

  lazy val eventsSchemaNested = StructType(Array(
    StructField("user_id", IntegerType),
    StructField("video_id", IntegerType),
    StructField("event", StringType),
    StructField("timestamp", LongType),
    StructField("tags", ArrayType(StringType)),
    StructField("comment", StringType),
    StructField("events", ArrayType(eventInnerSchema))
  ))

  lazy val videosDF = spark.read
    .schema(videosSchema)
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv(filesPath + "/videos/videos.csv")

  lazy val usersDF = spark.read
    .schema(usersSchema)
    .option("dateFormat", "dd.MM.yyyy'T'HH:mm:ss")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv(filesPath + "/users/users.csv")
    .withColumn("categories", split(col("categories"), ";"))

  lazy val eventsDF = spark.read
    .schema(eventsSchemaNested)
    .json(filesPath + "/events/events.jsonl")

  lazy val eventsExplodeddDF = eventsDF
    .withColumn("exploded_event", explode(col("events")))
    .select(
      col("exploded_event.*")
    )

  eventsExplodeddDF.show(30)

  lazy val eventsSimpledDF: DataFrame = eventsDF
    .select(
      col("user_id"),
      col("video_id"),
      col("event"),
      col("timestamp"),
      col("tags"),
      col("comment")
    ).filter("user_id is not null")

  lazy val eventsFinalDF = eventsExplodeddDF.unionAll(eventsSimpledDF)

  // The results:
//  videosDF.show()
//  usersDF.show()
//  eventsExtractedDF.show()
//  eventsExtractedDF.printSchema()
//  eventsFlattenDF.printSchema()
//  eventsExtractedDF.show(30)
//  eventsFinalDF.show(30)

  // writing:
  videosDF.write
    .mode(SaveMode.Append)
    .save(s"$filesPath/videos/out/videos.parquet")

  usersDF.write
    .mode(SaveMode.Append)
    .save(s"$filesPath/users/out/users.parquet")

  lazy val eventsPartitionedByDate = eventsFinalDF
    .withColumn("date_col", from_unixtime(col("timestamp"), "yyyy-MM-dd"))
    .withColumn("year", year(col("date_col")))
    .withColumn("month", month(col("date_col")))
    .withColumn("day", dayofmonth(col("date_col")))
    .drop("date_col")

  eventsPartitionedByDate.show(2)

  eventsPartitionedByDate.write
    .mode(SaveMode.Append)
    .partitionBy("year", "month", "day")
    .format("parquet")
    .save(s"$filesPath/events/out/parquet/")

}
