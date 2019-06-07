package WikiParser

import java.io.{File, PrintWriter}

import org.spark_project.dmg.pmml.True

import scala.io.Source

object StringToId extends App{
  val edgePath = "data\\output\\edges.txt"
  val indexPath = "data\\enwiki-20190101-pages-articles-multistream-index.txt"
  val idToStrings = scala.collection.mutable.Map.empty[String, Int]


  val f2 = new File("data\\output\\edgesWithId.txt")
  val w = new PrintWriter(f2)
  val f3 = new File("data\\output\\idWithString.txt")
  val w2 = new PrintWriter(f3)

  // Foreach line in the index file, build a map: page id as key, the page title as value
  for (line <- Source.fromFile(indexPath).getLines()){
    val array = line.split(":", 3)
    idToStrings += array(2) -> array(1).toInt
  }

  // Foreach line in the file containing the edges, replace the page title by it's id
  for (line <- Source.fromFile(edgePath).getLines()){
    val array = line.split("\t", 2)
    val node = idToStrings.getOrElse(array(0), None)
    var successors:String = ""
    if(node != None){
      try {
        array(1).split(";;").foreach(idToStrings.get(_) match {
          case Some(x) => successors += x + " "
          case _ => Unit
        })
      } catch {
        //No tab character, ignore line
        case e:Exception => println(line);println(array.foreach(println))
      } finally {}
      if (!successors.isEmpty){
        w.println(node + ": " + successors)
        w.flush()
      }
    }
  }

  for((k,v) <- idToStrings){
    w2.println(v + "\t" + k)
    w2.flush()
  }

  w.close()
  w2.close()
}
