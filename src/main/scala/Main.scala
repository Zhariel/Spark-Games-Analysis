import games.GameData
import org.apache.spark.sql.DataFrame

object Main extends App {

  val u = new GameData("prout")

  val d = u.extract_dataframes_in_folder("data/steam_games")
  val df_amongus = u.get_file_to_df("data/twitch_games/AmongUs.csv")
  val ds = u.get_df_twitch_to_ds(df_amongus)
  val frame: Option[DataFrame] = d.get("AmongUs")
  ds.show()
}