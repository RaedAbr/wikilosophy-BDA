import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, lower, split}

object bigramCounter extends App {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark Optimization example")
    .getOrCreate()
  import spark.implicits._

  val wikiTextDF = wikiReader.parseWiki(spark).select("text")

  val bigramsDF = new NGram().setN(2).setInputCol("text").setOutputCol("ngrams").transform(wikiTextDF)
    .drop("text")
    .withColumn("ngrams", explode($"ngrams"))
    .withColumn("ngrams", lower(col("ngrams")))
    .groupBy($"ngrams").count()
    .withColumn("ngrams", split(col("ngrams"), " "))
    .select(
      $"ngrams".getItem(0).as("word"),
      $"ngrams".getItem(1).as("followedBy"),
      $"count"
    )
  bigramsDF.write.parquet("data/bigrams.parquet")
}