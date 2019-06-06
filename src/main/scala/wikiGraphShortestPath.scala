import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object wikiGraphShortestPath extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Wikipedia graph pagerank")
    .getOrCreate()
  import spark.implicits._

  val wikiGraph = wikiGraphReader.parseWikiGraph(spark)


  val srcArticleName = "Philosophy"
  val srcArticle = wikiGraph.vertices.filter(x => x._2.title == srcArticleName).first()

  val shortestPaths = shortestPath.run(wikiGraph.reverse, srcArticle._1)
//  shortestPaths.vertices.filter(v => v._2._3 == 1).take(20).foreach(v =>
//    println("Path from " + srcArticle._2.title + " to " + v._2._1.title + ": " + v._2._2 + " distance: " + v._2._3)
//  )
  val shortestPathsDF = shortestPaths.vertices.map(x => (x._1, x._2._1.title, x._2._2.reverse, x._2._3)).toDF("id", "title", "path", "distance")
  shortestPathsDF.sort($"distance").write.parquet("s3://wikilosophy-data/shortestPathsToPhilosophy.parquet")
}
