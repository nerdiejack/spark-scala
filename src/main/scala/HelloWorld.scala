import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object HelloWorld {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark test")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark test")
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val raw = spark.read.text("src/main/resources/access.clean.log")
    val df = raw.map(_.getString(0)
      .split(" "))
      .map(d => (d(0), d(3).replace("[", ""), d(5), d(6), d(8)))
      .toDF("remote_host", "timestamp", "request_type", "url", "status_code")
    df.show(10)

    val count404 = df.filter($"status_code"  === "404").count()
    println(count404)
    val count204 = df.filter($"status_code" === "204")count()
    println(count204)
    val df2 = df.na.fill("404", Seq("status_code"))
    //df2.show(10)
    val df3 = df.withColumn("Date", to_date(unix_timestamp(df.col("timestamp"),
      "dd/MMM/yyyy").cast("timestamp")))
    df3.show(10)
    df3.groupBy("status_code").count().sort(desc("count")).show()
    val remoteIPs = df3.select("remote_host").distinct.count
    println(remoteIPs)

  }
}

