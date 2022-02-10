import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import games.{DataUtils, GameData, SteamData, TwitchData}

object Main extends App {

  val steam = new SteamData()
  val twitch = new TwitchData()

  val steam_data = steam.extract_dataframes_in_folder()
  val twitch_data = twitch.extract_dataframes_in_folder()

  val s = steam.clean_dataframe(steam_data.head)
  val t = twitch.clean_dataframe(twitch_data.head)

  println(s)
  println(t)




}