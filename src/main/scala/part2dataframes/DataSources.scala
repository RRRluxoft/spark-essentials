package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val path = "src/main/resources/data/cars.json"

  val carsDF = spark.read
    .format("json")
//    .option("inferSchema", "true")
    .schema(carsSchema)
    .option("mode", "failFast")
    .option("path", path)
    .load()

  val carsDFWithOptionMap = spark.read
    .format("json")
//    .schema(carsSchema)
    .options(Map(
      "mode"        -> "failFast",
      "path"        -> path,
      "inferSchema" -> "true"
    ))
    .load()

  // writing DFs
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "path" -> "src/main/resources/data/cars_dupes.json"
    ))
    .save()

  // JSON flags:
  spark.read
//    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")
//    .option("path", path)
//    .load()
    // instead of 3:
    .json(path)

  // CVS flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stockCsvPath = "src/main/resources/data/stocks.csv"

  spark.read
//    .format("cvs")
    .schema(stockSchema)
    .options(Map(
      "dateFormat" -> "MMM dd yyyy",
      "header"     -> "true",
      "sep"        -> ",",
      "nullValue"  -> ""
    ))
//    .load(stockCsvPath)
    .csv(stockCsvPath) // instead of `.format("cvs")`

  // Parquet: used by default !!!
  carsDF.write
    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/cars.parquet")
    .save("src/main/resources/data/cars.parquet") // by default `PARQUET`

  // Text file:
  spark.read.text("src/main/resources/data/textFile.txt").show()

  /**
    * Reading from a remote DB:
    */
  private val driver = "org.postgresql.Driver"
  private val url = "jdbc:postgresql://localhost:5432/rtjvm"
  private val user = "docker"
  private val pwd = "docker"
  private val dbtable = "public.employees"

  val employeesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver"   -> driver,
      "url"      -> url,
      "user"     -> user,
      "password" -> pwd,
      "dbtable"  -> dbtable
    ))
    .load()

  employeesDF.show()

  /**
    * read movies.json
    */
  val moviesDF_oldSchool = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  moviesDF.show()

  // write to CSV:
  moviesDF.write.mode(SaveMode.Overwrite).options(Map(
    "dateFormat" -> "MMM dd yyyy",
    "header"     -> "true",
    "sep"        -> ",",
    "nullValue"  -> ""))
//    .format("csv")
    .csv("src/main/resources/data/movies.csv")

  // write to Parquet:
  moviesDF.write.mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  // write to DB:
  moviesDF.write
    .format("jdbc")
    .options(Map(
      "driver"   -> driver,
      "url"      -> url,
      "user"     -> user,
      "password" -> pwd,
      "dbtable"  -> "public.movies"
    ))
    .save()

}
