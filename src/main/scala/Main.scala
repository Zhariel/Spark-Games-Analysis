import games.{SteamData, TwitchData}

object Main extends App {

  val steam = new SteamData()
  val twitch = new TwitchData()

  val s = steam.extract_dataframes_in_folder()
  val t = twitch.extract_dataframes_in_folder()

  val frame = steam.clean_dataframe(s.head)
  val frame2 = steam.clean_dataframe(t.head)

  val ddf = steam.get_df_steam_to_ds(frame)

  //Voir notre dataframe
  steam.view_table(frame).show()

  //Afficher le mois avec le plus de joueurs
  steam.more_players(frame).show()

  // Ratio player/ viewer par mois
  steam.ratio_viewer(frame, frame2).show()

  //Affichage du nombre des nouveaux streams depuis 2019
  steam.nouveaux_streams(frame2).show()

  //Le plus gros peak de joueur par rapport au nombre de nouveaux viewers le mois précédent
  steam.plus_gros_peak(frame, frame2).show()


//  print(frame.show(3))
}