import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object wikiWordGuesser extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark Optimization example")
    .getOrCreate()

  val bigramsDF = spark.read.parquet("data/bigrams.parquet").orderBy(desc("count")).cache()

  val query = "word == "
  val query1 = bigramsDF.where(query + "'anarchy'").select("followedBy","count")
  query1.show()
  val query2 = bigramsDF.where(query + "'philosophy'").select("followedBy","count")
  query2.show()
  val query3 = bigramsDF.where(query + "'read'").select("followedBy","count")
  query3.show()
}
