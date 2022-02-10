import games.{SteamData, TwitchData}

object Main extends App {

  val steam = new SteamData()
  val twitch = new TwitchData()

  val s = steam.extract_dataframes_in_folder()
  val t = twitch.extract_dataframes_in_folder()

  val frame = steam.clean_dataframe(s.head)
  val frame2 = steam.clean_dataframe(t.head)

  val ddf = steam.get_df_steam_to_ds(frame)

  steam.view_table(frame).show()


//  print(frame.show(3))
}