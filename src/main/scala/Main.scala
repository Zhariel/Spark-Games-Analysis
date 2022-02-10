import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import games.{DataUtils, GameData, SteamData, TwitchData}

object Main extends App {

  val steam = new SteamData()
  val twitch = new TwitchData()

  val s = steam.extract_dataframes_in_folder()
  val t = twitch.extract_dataframes_in_folder()

  val frame = steam.clean_dataframe(s.head)
  val frame2 = steam.clean_dataframe(t.head)

  println(frame)
  println(frame2)

//  print(frame.show(3))
}