import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, explode, lower}

object wikiWordCounter extends App {

//  Logger.getLogger("org").setLevel(Level.ERROR)
//  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Optimization example")
    .getOrCreate()
  import spark.implicits._

  val wikiTextDF = wikiReader.parseWiki(spark)
    .withColumn("text", explode($"text"))
    .withColumn("text", lower($"text"))
    .filter($"text" =!= "")
    .select($"text".as("word"))

  val stopWordsDF = spark.read.csv("data/stopwords.csv").toDF("stopword")
  val filteredDF = wikiTextDF.join(stopWordsDF.select($"stopword".as("word")), Seq("word"),"left_anti")
  val wordsCountDF = filteredDF.groupBy($"word").count()

  wordsCountDF.orderBy(desc("count")).show()
  wordsCountDF.orderBy("count").show()
  wordsCountDF.write.parquet("data/wordsCount.parquet")
}
