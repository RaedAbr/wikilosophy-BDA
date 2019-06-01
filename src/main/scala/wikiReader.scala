import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object wikiReader {
  def parseWiki(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val jsonPath = "data/wikipedia.json"
    val baseDF = spark.read.json(jsonPath)
    baseDF.count()

    val linkBalise = """<[^>]*>{1}"""
    val wikiLink = """\[\[.*\||\[\[|\]\]"""
    val noPunctuation = """[^\w\s-]"""
    val whiteSpace = """\s+"""
    val cleanTextDF = baseDF.withColumn("text", regexp_replace($"text", "\n\n", " "))
        .withColumn("text", regexp_replace($"text", linkBalise, ""))
        .withColumn("text", regexp_replace($"text", wikiLink, ""))
        .withColumn("text", regexp_replace($"text", noPunctuation, ""))
        .withColumn("text", regexp_replace($"text", whiteSpace, " "))
        .withColumn("text", split(col("text"), " "))
    cleanTextDF
  }
}
