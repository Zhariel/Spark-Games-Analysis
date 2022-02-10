import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import games.{DataUtils, GameData, SteamData, TwitchData, Transformations}

object Main extends App {

  val steam = new SteamData()
  val twitch = new TwitchData()

  val steam_frame = steam.clean_dataframe(steam.extract_dataframes_in_folder().head)
  val twitch_frame = twitch.clean_dataframe(twitch.extract_dataframes_in_folder().head)

  println(steam_frame)
  println(twitch_frame)

  val transformations = new Transformations()

  //Voir notre dataframe
  transformations.view_table(steam_frame).show()

  //Afficher le mois avec le plus de joueurs
  transformations.more_players(steam_frame).show()

  // Ratio player/ viewer par mois
  transformations.ratio_viewer(steam_frame, twitch_frame).show()

  //Affichage du nombre des nouveaux streams depuis 2019
  transformations.nouveaux_streams(twitch_frame).show()

  //Le plus gros peak de joueur par rapport au nombre de nouveaux viewers le mois précédent
  transformations.plus_gros_peak(steam_frame, twitch_frame).show()
}