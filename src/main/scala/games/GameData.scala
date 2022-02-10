package games
import case_classes.{SteamGame, TwitchWatch}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset}

class GameData(var name: String) {
  val utils = new DataUtils
  val spark = utils.spark

  def extract_dataframes_in_folder(folder_path: String): Map[String, DataFrame] ={
    val utils = new DataUtils()
    val list_of_games = utils.getListOfFiles(folder_path)

    val games_names_list = list_of_games.map(x => x.getName.substring(0, x.getName.length-4))
    val df_list = list_of_games.map(x => get_file_to_df(x.getPath))

    (games_names_list zip df_list).toMap
  }

  def get_file_to_df(path: String): DataFrame = {
     spark.read
    .format("csv")
    .option("header",value = true)
    .option("sep", ",")
    .load(path)
  }

  def get_df_steam_to_ds(df: DataFrame) : Dataset[SteamGame] = {
    df.as(ExpressionEncoder[SteamGame])
  }

  def get_df_twitch_to_ds(df: DataFrame) : Dataset[TwitchWatch] = {
    df.as(ExpressionEncoder[TwitchWatch])
  }
}
