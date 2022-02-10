import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import games.{DataUtils, GameData, SteamData}

object Main extends App {

  val u = new SteamData()

  val d = u.extract_dataframes_in_folder()
  val frame: Option[DataFrame] = d.get("AmongUs")
  print(frame)
//  print(frame.show(3))
}