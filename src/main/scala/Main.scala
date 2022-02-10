import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import games.{DataUtils, GameData}

object Main extends App {

  val u = new GameData("prout")

  val d = u.extract_dataframes_in_folder("")
  val frame: Option[DataFrame] = d.get("AmongUs")
//  print(frame.show(3))
}