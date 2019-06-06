import org.apache.spark.sql.SparkSession

object wikiGraphHits extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Wikipedia graph hubs and authorities")
    .getOrCreate()
  import spark.implicits._


  val wikiGraph = wikiGraphReader.parseWikiGraph(spark)

  val scoresG = hits.run(wikiGraph, 5)
  val scoresDF = scoresG.vertices.map(x => (x._1, x._2._1.title, x._2._2.hubScore, x._2._2.authScore)).toDF("id", "title", "hubScore", "authScore")
  scoresDF.write.parquet("s3://wikilosophy-data/hitsScores.parquet")
//  val hubs = scoresG.vertices.collect().sortBy(_._2._2.hubScore).reverse
//  val authorities = scoresG.vertices.collect().sortBy(_._2._2.authScore).reverse
//  println("Hubs")
//  hubs.take(100).foreach(x => println(x._1, x._2._1.title, x._2._2.hubScore))
//  println("Authorities")
//  authorities.take(100).foreach(x => println(x._1, x._2._1.title, x._2._2.authScore))
}
