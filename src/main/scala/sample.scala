import org.apache.spark.sql.{Row, SparkSession}
import com.couchbase.spark.sql._

object sample extends App {

  val spark = SparkSession.builder.appName("Sample ETL").getOrCreate()

  // creating a dataframe from a mysql table
  // dbname: infosys
  // tablename: test
  val df = spark.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://localhost:3306/infosys?user=root&password=",
      "dbtable" -> "infosys.test",
      "driver" -> "com.mysql.jdbc.Driver"
    )).load()

  // once we have a dataframe we can use sql-like transformations
  val upper = df.selectExpr("id", "upper(name) as name", "date")
  // and filters
  val filtered = upper.filter("name is not null")

  //before inserting to couchbase we need to map the id field
  val prep = filtered.withColumn("META_ID", df.col("id").cast("string"))

  // and now we can write to couchbase
  val writer = new DataFrameWriterFunctions(prep.write)
  writer.couchbase(Map("spark.couchbase.nodes" -> "127.0.0.1",
    "spark.couchbase.bucket.default" -> "",
    "com.couchbase.username" -> "Administrator",
    "com.couchbase.password" -> ""
  ))

}
