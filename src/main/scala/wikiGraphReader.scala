import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

class VertexProperty()
case class WikiArticle(title: String) extends VertexProperty

object wikiGraphReader {
  def parseWikiGraph(spark: SparkSession): Graph[WikiArticle, Double] = {
//    val verticesPath = "data/testGraph_vertices.txt"
//    val edgesPath = "data/testGraph_edges.txt"
//    val verticesPath = "data/idWithString.txt"
//    val edgesPath = "data/edgesWithId.txt"
    val verticesPath = "s3a://wikilosophy-data/titles-sorted.txt"
    val edgesPath = "s3a://wikilosophy-data/links-simple-sorted.txt"
    val verticesRDD = spark.sparkContext.textFile(verticesPath)
      .zipWithIndex()
      .map(x => x match { case (title: String, id: Long) => (id+1.toLong, wikiArticle(title.replace("_"," ")))})

//      .map(line => {
//        val splitLine = line.split("\t").toList
//        val id = splitLine(0).toLong
//        val title = splitLine(1)
//        (id, WikiArticle(title))
//      })

    val edgesRDD = spark.sparkContext.textFile(edgesPath).flatMap(line => {
      val split = line.split(": ").toList
      val id = split.head.toLong
      split.tail.head.split(" ").toList.map(link => Edge(id, link.toLong , 1.0))
    })


    val defaultArticle = WikiArticle("Wrong article")
    Graph(verticesRDD, edgesRDD)
//    Graph(null, null)
  }
}
