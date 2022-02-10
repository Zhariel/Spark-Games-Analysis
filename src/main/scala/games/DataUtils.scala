package games

import org.apache.spark.sql.SparkSession

import java.io.File

class DataUtils {
   val spark = SparkSession
    .builder()
    .appName(name = "Playtime Viewership Evaluation")
    .master("local[*]")
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
