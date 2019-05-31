import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object wikiWordGuesser extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark Optimization example")
    .getOrCreate()

  val bigramsDF = spark.read.parquet("data/bigrams.parquet")
  val query = "word == "
  val query1 = bigramsDF.select("followedBy","count").where(query + "'anarchy'").orderBy(desc("count"))
  query1.show()
  val query2 = bigramsDF.select("followedBy","count").where(query + "'philosophy'").orderBy(desc("count"))
  query2.show()
  val query3 = bigramsDF.select("followedBy","count").where(query + "'read'").orderBy(desc("count"))
  query3.show()
}
